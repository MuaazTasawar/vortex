use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::AppState;

/// Deliberately unauthenticated, matching /cluster/status -- Prometheus
/// scrapers don't send bearer tokens by default, and in a real deployment
/// this endpoint would sit behind network policy rather than app-level auth.
pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut out = String::new();

    let ring_depth = state.ingestion.len();
    let _ = writeln!(
        out,
        "# HELP vortex_ring_buffer_depth Current number of events waiting in the ingestion ring buffer"
    );
    let _ = writeln!(out, "# TYPE vortex_ring_buffer_depth gauge");
    let _ = writeln!(out, "vortex_ring_buffer_depth {ring_depth}");

    let term = state.election.term();
    let _ = writeln!(
        out,
        "# HELP vortex_election_term Current Raft term for this node"
    );
    let _ = writeln!(out, "# TYPE vortex_election_term gauge");
    let _ = writeln!(out, "vortex_election_term {term}");

    let is_leader = if state.election.role().await == cluster::Role::Leader {
        1
    } else {
        0
    };
    let _ = writeln!(
        out,
        "# HELP vortex_election_is_leader 1 if this node is currently the elected leader, else 0"
    );
    let _ = writeln!(out, "# TYPE vortex_election_is_leader gauge");
    let _ = writeln!(out, "vortex_election_is_leader {is_leader}");

    let members = state.gossip.members().await;
    let alive = members
        .iter()
        .filter(|m| m.state == cluster::MemberState::Alive)
        .count();
    let suspect = members
        .iter()
        .filter(|m| m.state == cluster::MemberState::Suspect)
        .count();
    let dead = members
        .iter()
        .filter(|m| m.state == cluster::MemberState::Dead)
        .count();
    let _ = writeln!(
        out,
        "# HELP vortex_gossip_members Known cluster members by state"
    );
    let _ = writeln!(out, "# TYPE vortex_gossip_members gauge");
    let _ = writeln!(out, "vortex_gossip_members{{state=\"alive\"}} {alive}");
    let _ = writeln!(out, "vortex_gossip_members{{state=\"suspect\"}} {suspect}");
    let _ = writeln!(out, "vortex_gossip_members{{state=\"dead\"}} {dead}");

    let _ = writeln!(
        out,
        "# HELP vortex_ingest_accepted_total Total events successfully accepted via POST /ingest"
    );
    let _ = writeln!(out, "# TYPE vortex_ingest_accepted_total counter");
    let _ = writeln!(
        out,
        "vortex_ingest_accepted_total {}",
        state.metrics.ingest_accepted_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP vortex_ingest_rejected_total Total events rejected via POST /ingest"
    );
    let _ = writeln!(out, "# TYPE vortex_ingest_rejected_total counter");
    let _ = writeln!(
        out,
        "vortex_ingest_rejected_total {}",
        state.metrics.ingest_rejected_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP vortex_checkpoint_writes_total Total successful checkpoint upserts to Postgres"
    );
    let _ = writeln!(out, "# TYPE vortex_checkpoint_writes_total counter");
    let _ = writeln!(
        out,
        "vortex_checkpoint_writes_total {}",
        state
            .metrics
            .checkpoint_writes_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP vortex_checkpoint_write_failures_total Total failed checkpoint upserts to Postgres"
    );
    let _ = writeln!(out, "# TYPE vortex_checkpoint_write_failures_total counter");
    let _ = writeln!(
        out,
        "vortex_checkpoint_write_failures_total {}",
        state
            .metrics
            .checkpoint_write_failures_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP vortex_windows_evicted_total Total windows evicted from memory due to TTL expiry"
    );
    let _ = writeln!(out, "# TYPE vortex_windows_evicted_total counter");
    let _ = writeln!(
        out,
        "vortex_windows_evicted_total {}",
        state.metrics.windows_evicted_total.load(Ordering::Relaxed)
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        out,
    )
}
