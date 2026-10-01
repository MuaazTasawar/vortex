pub mod error;
pub mod extractors;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod routes;
pub mod ws;

use cluster::{Election, Gossip};
use engine::{RingBuffer, WindowAggregator};
use infra::checkpoint_repo::CheckpointRepo;
use infra::config::Settings;
use plugins::TransformRegistry;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;

pub struct AppState {
    pub db_pool: sqlx::PgPool,
    pub settings: Settings,
    pub ingestion: Arc<RingBuffer<domain::Event<'static>>>,
    pub aggregator: RwLock<WindowAggregator>,
    pub transform_registry: TransformRegistry,
    pub stats_tx: broadcast::Sender<String>,
    pub gossip: Arc<Gossip>,
    pub election: Arc<Election>,
    pub checkpoint_repo: CheckpointRepo,
    pub metrics: metrics::Metrics,
}

pub async fn build_state(
    settings: Settings,
    db_pool: sqlx::PgPool,
) -> anyhow::Result<Arc<AppState>> {
    let node_id = uuid::Uuid::new_v4().to_string();
    let gossip_addr: std::net::SocketAddr = settings.gossip_bind_addr.parse()?;
    let gossip = Arc::new(Gossip::bind(node_id.clone(), gossip_addr).await?);
    let (_probe_handle, _recv_handle, election_rx) = gossip.clone().spawn();
    for seed in &settings.gossip_seeds {
        if let Ok(addr) = seed.parse() {
            let _ = gossip.join(addr).await;
        }
    }
    let election = Election::new(node_id, gossip.clone());
    tokio::spawn(election.clone().run(election_rx));

    let (stats_tx, _) = broadcast::channel(64);
    let checkpoint_repo = CheckpointRepo::new(db_pool.clone());

    Ok(Arc::new(AppState {
        db_pool,
        settings,
        ingestion: Arc::new(RingBuffer::with_capacity(1024)),
        aggregator: RwLock::new(WindowAggregator::new(Duration::from_secs(1))),
        transform_registry: TransformRegistry::with_native_transforms(),
        stats_tx,
        gossip,
        election,
        checkpoint_repo,
        metrics: metrics::Metrics::default(),
    }))
}

pub async fn drain_ingestion_once(state: &Arc<AppState>) -> usize {
    let mut drained = 0;
    while let Some(event) = state.ingestion.try_pop() {
        state.aggregator.write().await.ingest(&event);
        drained += 1;
    }
    drained
}

pub async fn consume_ingestion_loop(state: Arc<AppState>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("consume_ingestion_loop shutting down");
                break;
            }
            _ = async {
                if drain_ingestion_once(&state).await == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            } => {}
        }
    }
}

pub async fn broadcast_stats_loop(state: Arc<AppState>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("broadcast_stats_loop shutting down");
                break;
            }
            _ = tick.tick() => {
                let snapshot: Vec<((u64, domain::Window), engine::WindowStats)> =
                    state.aggregator.read().await.finalize().into_iter().collect();
                if let Ok(json) = serde_json::to_string(&snapshot) {
                    let _ = state.stats_tx.send(json);
                }
            }
        }
    }
}

/// Checks every 60s for windows past `retention` and removes them from
/// memory. 60s check interval against a (default) 600s retention gives
/// a wide safety margin over the 5s checkpoint-persist interval -- every
/// window is persisted roughly 100+ times before it's ever evicted.
pub async fn evict_windows_loop(
    state: Arc<AppState>,
    cancel: CancellationToken,
    retention: Duration,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("evict_windows_loop shutting down");
                break;
            }
            _ = tick.tick() => {
                let cutoff_ms = chrono::Utc::now().timestamp_millis() - (retention.as_millis() as i64);
                let removed = state.aggregator.write().await.evict_older_than(cutoff_ms);
                if removed > 0 {
                    state.metrics.windows_evicted_total.fetch_add(removed as u64, Ordering::Relaxed);
                    tracing::info!(removed, "evicted aged-out windows from memory");
                }
            }
        }
    }
}

pub async fn persist_checkpoints_loop(state: Arc<AppState>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("persist_checkpoints_loop shutting down");
                break;
            }
            _ = tick.tick() => {
                let snapshot: Vec<((u64, domain::Window), engine::WindowStats)> =
                    state.aggregator.read().await.finalize().into_iter().collect();
                for ((stream_id, window), stats) in snapshot {
                    let stats_json = match serde_json::to_value(stats) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to serialize window stats for checkpoint");
                            continue;
                        }
                    };
                    match state
                        .checkpoint_repo
                        .upsert(stream_id as i64, window.start_ms, window.end_ms, &stats_json)
                        .await
                    {
                        Ok(()) => {
                            state.metrics.checkpoint_writes_total.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            state.metrics.checkpoint_write_failures_total.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(error = %e, stream_id, "checkpoint upsert failed, will retry next tick");
                        }
                    }
                }
            }
        }
    }
}
