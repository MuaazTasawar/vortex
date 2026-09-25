use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use crate::handlers::{auth, cluster, ingest, query};
use crate::middleware::request_id;
use crate::AppState;

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/ingest", post(ingest::ingest))
        .route("/query", get(query::query_windows))
        .route("/cluster/status", get(cluster::cluster_status))
        .route("/stream/ws", get(crate::ws::stream_ws))
        .layer(axum::middleware::from_fn(request_id))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}