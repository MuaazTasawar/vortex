use axum::extract::State;
use axum::Json;
use base64::Engine;
use domain::Event;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

use crate::error::ApiError;
use crate::extractors::auth_user::AuthUser;
use crate::AppState;

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
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&req.payload_b64)
        .map_err(|e| ApiError::BadRequest(format!("invalid base64 payload: {e}")))?;

    let event = Event {
        stream_id: req.stream_id,
        timestamp_ms: req.timestamp_ms,
        key: Cow::Owned(req.key),
        payload: Cow::Owned(payload),
    };

    let event = match &req.transform {
        Some(name) => {
            let transform = state
                .transform_registry
                .get(name)
                .ok_or_else(|| ApiError::BadRequest(format!("unknown transform '{name}'")))?;
            let mut out = transform
                .apply(&event)
                .map_err(|e| ApiError::BadRequest(e.to_string()))?;
            out.pop()
                .ok_or_else(|| ApiError::Internal("transform produced no output".into()))?
        }
        None => event,
    };

    state
        .ingestion
        .try_push(event)
        .map_err(|_| ApiError::Internal("ingestion buffer full".into()))?;

    Ok(Json(IngestResponse { accepted: true }))
}