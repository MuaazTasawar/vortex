use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::extractors::auth_user::AuthUser;
use crate::AppState;

#[derive(Deserialize)]
pub struct QueryParams {
    pub stream_id: Option<u64>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

#[derive(Serialize)]
pub struct WindowStatsDto {
    pub stream_id: u64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub count: usize,
    pub sum: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
}

pub async fn query_windows(
    State(state): State<Arc<AppState>>,
    _auth: AuthUser,
    Query(params): Query<QueryParams>,
) -> Json<Vec<WindowStatsDto>> {
    let aggregator = state.aggregator.read().await;
    let mut out: Vec<WindowStatsDto> = aggregator
        .finalize()
        .into_iter()
        .filter(|((sid, w), _)| {
            params.stream_id.map_or(true, |s| s == *sid)
                && params.start_ms.map_or(true, |s| w.start_ms >= s)
                && params.end_ms.map_or(true, |e| w.end_ms <= e)
        })
        .map(|((sid, w), s)| WindowStatsDto {
            stream_id: sid,
            start_ms: w.start_ms,
            end_ms: w.end_ms,
            count: s.count,
            sum: s.sum,
            mean: s.mean,
            min: s.min,
            max: s.max,
        })
        .collect();
    out.sort_by_key(|w| (w.stream_id, w.start_ms));
    Json(out)
}