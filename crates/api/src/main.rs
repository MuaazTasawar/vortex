mod error;
mod extractors;
mod handlers;
mod middleware;
mod routes;
mod ws;

use cluster::{Election, Gossip};
use engine::{RingBuffer, WindowAggregator};
use infra::checkpoint_repo::CheckpointRepo;
use infra::config::Settings;
use plugins::TransformRegistry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let settings = Settings::load()?;
    tracing::info!(addr = %settings.http_bind_addr, "starting vortex gateway");

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&settings.database_url)
        .await?;
    sqlx::migrate!("../../migrations").run(&db_pool).await?;

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

    let state = Arc::new(AppState {
        db_pool,
        settings: settings.clone(),
        ingestion: Arc::new(RingBuffer::with_capacity(1024)),
        aggregator: RwLock::new(WindowAggregator::new(Duration::from_secs(1))),
        transform_registry: TransformRegistry::with_native_transforms(),
        stats_tx,
        gossip,
        election,
        checkpoint_repo,
    });

    tokio::spawn(consume_ingestion(state.clone()));
    tokio::spawn(broadcast_stats(state.clone()));
    tokio::spawn(persist_checkpoints(state.clone()));

    let app = routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(&settings.http_bind_addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn consume_ingestion(state: Arc<AppState>) {
    loop {
        if let Some(event) = state.ingestion.try_pop() {
            state.aggregator.write().await.ingest(&event);
        } else {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

async fn broadcast_stats(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tick.tick().await;
        let snapshot: Vec<((u64, domain::Window), engine::WindowStats)> =
            state.aggregator.read().await.finalize().into_iter().collect();
        if let Ok(json) = serde_json::to_string(&snapshot) {
            let _ = state.stats_tx.send(json);
        }
    }
}

/// Periodically upserts every in-memory window into `checkpoints`. A
/// failed write here is logged, not propagated — a transient DB outage
/// shouldn't take down ingestion, since the in-memory aggregator still
/// holds the data and the next tick will retry the write.
async fn persist_checkpoints(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let snapshot: Vec<((u64, domain::Window), engine::WindowStats)> =
            state.aggregator.read().await.finalize().into_iter().collect();
        for ((stream_id, window), stats) in snapshot {
            let stats_json = match serde_json::to_value(&stats) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to serialize window stats for checkpoint");
                    continue;
                }
            };
            if let Err(e) = state
                .checkpoint_repo
                .upsert(stream_id as i64, window.start_ms, window.end_ms, &stats_json)
                .await
            {
                tracing::warn!(error = %e, stream_id, "checkpoint upsert failed, will retry next tick");
            }
        }
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutdown signal received");
}