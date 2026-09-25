use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use crate::error::ApiError;
use crate::extractors::auth_user::AuthUser;
use crate::AppState;
use infra::checkpoint_repo::CheckpointRow;

#[derive(Deserialize)]
pub struct CheckpointQuery {
    pub stream_id: i64,
}

pub async fn list_checkpoints(
    State(state): State<Arc<AppState>>,
    _auth: AuthUser,
    Query(params): Query<CheckpointQuery>,
) -> Result<Json<Vec<CheckpointRow>>, ApiError> {
    let rows = state.checkpoint_repo.list_for_stream(params.stream_id).await?;
    Ok(Json(rows))
}