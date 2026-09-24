use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub type NodeId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    Ping { from: NodeId },
    Ack { from: NodeId, members: Vec<MemberInfo> },
    PingReq { from: NodeId, target: NodeId },
    Join { from: NodeId, addr: SocketAddr },
    RequestVote { term: u64, candidate_id: NodeId },
    VoteResponse { term: u64, granted: bool, voter: NodeId },
    Heartbeat { term: u64, leader_id: NodeId },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: u64,
    pub state: MemberState,
}