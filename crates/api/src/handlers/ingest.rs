use axum::Json;
use axum::extract::State;
use base64::Engine;
use domain::Event;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::AppState;
use crate::error::ApiError;
use crate::extractors::auth_user::AuthUser;

#[derive(Deserialize)]
pub struct IngestRequest {
    pub stream_id: u64,
    pub timestamp_ms: i64,
    pub key: String,
    pub payload_b64: String,
    pub transform: Option<String>,
}

#[derive(Serialize)]
pub struct IngestResponse {
    pub accepted: bool,
}

pub async fn ingest(
    State(state): State<Arc<AppState>>,
    _auth: AuthUser,
    Json(req): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    let payload = match base64::engine::general_purpose::STANDARD.decode(&req.payload_b64) {
        Ok(p) => p,
        Err(e) => {
            state
                .metrics
                .ingest_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(ApiError::BadRequest(format!("invalid base64 payload: {e}")));
        }
    };

    let event = Event {
        stream_id: req.stream_id,
        timestamp_ms: req.timestamp_ms,
        key: Cow::Owned(req.key),
        payload: Cow::Owned(payload),
    };

    let event = match &req.transform {
        Some(name) => {
            let transform = match state.transform_registry.get(name) {
                Some(t) => t,
                None => {
                    state
                        .metrics
                        .ingest_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(ApiError::BadRequest(format!("unknown transform '{name}'")));
                }
            };
            let mut out = match transform.apply(&event) {
                Ok(o) => o,
                Err(e) => {
                    state
                        .metrics
                        .ingest_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(ApiError::BadRequest(e.to_string()));
                }
            };
            match out.pop() {
                Some(ev) => ev,
                None => {
                    state
                        .metrics
                        .ingest_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(ApiError::Internal("transform produced no output".into()));
                }
            }
        }
        None => event,
    };

    if state.ingestion.try_push(event).is_err() {
        state
            .metrics
            .ingest_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        return Err(ApiError::Internal("ingestion buffer full".into()));
    }

    state
        .metrics
        .ingest_accepted_total
        .fetch_add(1, Ordering::Relaxed);
    Ok(Json(IngestResponse { accepted: true }))
}
