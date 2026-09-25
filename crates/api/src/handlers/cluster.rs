use axum::extract::State;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;

use crate::AppState;

#[derive(Serialize)]
pub struct ClusterStatus {
    pub node_id: String,
    pub role: String,
    pub leader: Option<String>,
    pub members: Vec<String>,
}

pub async fn cluster_status(State(state): State<Arc<AppState>>) -> Json<ClusterStatus> {
    let role = format!("{:?}", state.election.role().await);
    let leader = state.election.current_leader().await;
    let members = state.gossip.members().await.into_iter().map(|m| m.id).collect();

    Json(ClusterStatus { node_id: state.gossip.id.clone(), role, leader, members })
}