use axum::Router;
use axum::routing::{get, post};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

use crate::AppState;
use crate::handlers::{auth, checkpoints, cluster, ingest, query};
use crate::middleware::rate_limit::{RateLimiterState, check_and_respond};
use crate::middleware::request_id;

pub fn build_router(state: Arc<AppState>) -> Router {
    let limiter = RateLimiterState::default();

    Router::new()
        .route("/auth/register", post(auth::register))
        .route("/auth/login", post(auth::login))
        .route("/ingest", post(ingest::ingest))
        .route("/query", get(query::query_windows))
        .route("/checkpoints", get(checkpoints::list_checkpoints))
        .route("/cluster/status", get(cluster::cluster_status))
        .route("/stream/ws", get(crate::ws::stream_ws))
        .layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let limiter = limiter.clone();
                async move { check_and_respond(limiter, req, next).await }
            },
        ))
        .layer(axum::middleware::from_fn(request_id))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
