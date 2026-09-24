//! Leader election only — no log replication (a deliberate scope cut;
//! see the project's Phase 3 discussion for why). A node is Follower,
//! Candidate, or Leader; it becomes Candidate on a randomized election
//! timeout with no heartbeat, requests votes from every gossip-known
//! peer, and becomes Leader on a majority.

use crate::gossip::Gossip;
use crate::message::{ClusterMessage, MemberState, NodeId};
use rand::RngExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

struct ElectionRound {
    term: u64,
    votes: usize,
    total: usize,
}

pub struct Election {
    id: NodeId,
    gossip: Arc<Gossip>,
    term: AtomicU64,
    role: RwLock<Role>,
    voted_for: RwLock<Option<(u64, NodeId)>>,
    leader_id: RwLock<Option<NodeId>>,
    current_round: RwLock<Option<ElectionRound>>,
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

fn random_election_timeout() -> Duration {
    Duration::from_millis(rand::rng().random_range(150..300))
}

impl Election {
    pub fn new(id: NodeId, gossip: Arc<Gossip>) -> Arc<Self> {
        Arc::new(Election {
            id,
            gossip,
            term: AtomicU64::new(0),
            role: RwLock::new(Role::Follower),
            voted_for: RwLock::new(None),
            leader_id: RwLock::new(None),
            current_round: RwLock::new(None),
        })
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.leader_id.read().await.clone()
    }

    pub async fn role(&self) -> Role {
        *self.role.read().await
    }

    pub async fn run(
        self: Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<(ClusterMessage, std::net::SocketAddr)>,
    ) {
        let mut last_reset = Instant::now();
        let mut deadline = random_election_timeout();

        loop {
            let role = self.role().await;
            let sleep_for = if role == Role::Leader {
                HEARTBEAT_INTERVAL
            } else {
                deadline.saturating_sub(last_reset.elapsed())
            };

            tokio::select! {
                _ = sleep(sleep_for) => {
                    if role == Role::Leader {
                        self.send_heartbeats().await;
                    } else {
                        self.start_election().await;
                        last_reset = Instant::now();
                        deadline = random_election_timeout();
                    }
                }
                Some((msg, from_addr)) = rx.recv() => {
                    if self.handle_message(msg, from_addr).await {
                        last_reset = Instant::now();
                        deadline = random_election_timeout();
                    }
                }
            }
        }
    }

    async fn start_election(&self) {
        let term = self.term.fetch_add(1, Ordering::SeqCst) + 1;
        *self.role.write().await = Role::Candidate;
        *self.voted_for.write().await = Some((term, self.id.clone()));
        tracing::info!(term, "starting election");

        let peers: Vec<_> = self
            .gossip
            .members()
            .await
            .into_iter()
            .filter(|m| m.id != self.id && m.state != MemberState::Dead)
            .collect();
        let total = peers.len() + 1;

        *self.current_round.write().await = Some(ElectionRound { term, votes: 1, total });

        if total == 1 {
            self.become_leader(term).await;
            return;
        }

        let msg = ClusterMessage::RequestVote { term, candidate_id: self.id.clone() };
        let bytes = serde_json::to_vec(&msg).expect("serialize");
        let socket = self.gossip.socket();
        for peer in &peers {
            let _ = socket.send_to(&bytes, peer.addr).await;
        }
    }

    async fn become_leader(&self, term: u64) {
        *self.role.write().await = Role::Leader;
        *self.leader_id.write().await = Some(self.id.clone());
        *self.current_round.write().await = None;
        tracing::info!(term, "elected leader");
        self.send_heartbeats().await;
    }

    async fn send_heartbeats(&self) {
        let term = self.term.load(Ordering::SeqCst);
        let msg = ClusterMessage::Heartbeat { term, leader_id: self.id.clone() };
        let bytes = serde_json::to_vec(&msg).expect("serialize");
        let socket = self.gossip.socket();
        for peer in self.gossip.members().await.into_iter().filter(|m| m.id != self.id) {
            let _ = socket.send_to(&bytes, peer.addr).await;
        }
    }

    async fn handle_message(&self, msg: ClusterMessage, from_addr: std::net::SocketAddr) -> bool {
        match msg {
            ClusterMessage::RequestVote { term, candidate_id } => {
                let current_term = self.term.load(Ordering::SeqCst);
                if term < current_term {
                    return false;
                }
                if term > current_term {
                    self.term.store(term, Ordering::SeqCst);
                    *self.role.write().await = Role::Follower;
                    *self.voted_for.write().await = None;
                }
                let mut voted_for = self.voted_for.write().await;
                let granted = match &*voted_for {
                    None => true,
                    Some((_, id)) => id == &candidate_id,
                };
                if granted {
                    *voted_for = Some((term, candidate_id.clone()));
                }
                drop(voted_for);

                let response = ClusterMessage::VoteResponse { term, granted, voter: self.id.clone() };
                let bytes = serde_json::to_vec(&response).expect("serialize");
                let _ = self.gossip.socket().send_to(&bytes, from_addr).await;
                granted
            }
            ClusterMessage::VoteResponse { term, granted, .. } => {
                if !granted || self.role().await != Role::Candidate {
                    return false;
                }
                let mut round = self.current_round.write().await;
                let became_leader = if let Some(r) = round.as_mut() {
                    if r.term == term {
                        r.votes += 1;
                        r.votes * 2 > r.total
                    } else {
                        false
                    }
                } else {
                    false
                };
                drop(round);
                if became_leader {
                    self.become_leader(term).await;
                }
                false
            }
            ClusterMessage::Heartbeat { term, leader_id } => {
                let current_term = self.term.load(Ordering::SeqCst);
                let current_role = self.role().await;

                if term < current_term {
                    return false;
                }
                // Deterministic tie-break for the case where two nodes
                // both self-elected in the same term before they had
                // gossiped enough to know about each other (possible at
                // bootstrap). Lower NodeId wins; the loser steps down.
                // This is what guarantees convergence on one leader
                // instead of nodes disagreeing based on message arrival
                // order.
                if term == current_term && current_role == Role::Leader && leader_id > self.id {
                    return false;
                }

                self.term.store(term, Ordering::SeqCst);
                *self.role.write().await = Role::Follower;
                *self.leader_id.write().await = Some(leader_id);
                true
            }
            _ => false,
        }
    }
}
