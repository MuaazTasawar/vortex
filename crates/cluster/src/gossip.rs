//! UDP membership gossip, SWIM-inspired: nodes periodically probe a
//! random peer and wait for an ack; a missed ack marks the peer
//! Suspect, and a Suspect that never recovers within SUSPECT_TIMEOUT
//! is marked Dead. Every Ack piggybacks the sender's full membership
//! view so the cluster's picture of itself converges without a
//! separate anti-entropy pass.
//!
//! Not implemented (deliberate scope cut, see Phase 3 notes): indirect
//! probing through k other members before declaring Suspect. Full SWIM
//! uses this to avoid false positives from a single lossy network path;
//! this version accepts that tradeoff to keep the piece under test
//! (leader election) the focus.

use crate::message::{ClusterMessage, MemberInfo, MemberState, NodeId};
use rand::RngExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{interval, timeout};

const PROBE_INTERVAL: Duration = Duration::from_millis(500);
const ACK_TIMEOUT: Duration = Duration::from_millis(200);
const SUSPECT_TIMEOUT: Duration = Duration::from_secs(3);

pub struct Gossip {
    pub id: NodeId,
    pub addr: SocketAddr,
    socket: Arc<UdpSocket>,
    members: Arc<RwLock<HashMap<NodeId, MemberInfo>>>,
    pending_acks: RwLock<HashMap<NodeId, oneshot::Sender<()>>>,
    election_tx: std::sync::OnceLock<mpsc::UnboundedSender<(ClusterMessage, SocketAddr)>>,
}

impl Gossip {
    pub async fn bind(id: NodeId, addr: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        let mut members = HashMap::new();
        members.insert(
            id.clone(),
            MemberInfo { id: id.clone(), addr: local_addr, incarnation: 0, state: MemberState::Alive },
        );
        Ok(Gossip {
            id,
            addr: local_addr,
            socket: Arc::new(socket),
            members: Arc::new(RwLock::new(members)),
            pending_acks: RwLock::new(HashMap::new()),
            election_tx: std::sync::OnceLock::new(),
        })
    }

    pub async fn join(&self, seed: SocketAddr) -> std::io::Result<()> {
        let msg = ClusterMessage::Join { from: self.id.clone(), addr: self.addr };
        self.send_to(seed, &msg).await
    }

    async fn send_to(&self, addr: SocketAddr, msg: &ClusterMessage) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(msg).expect("serialize cluster message");
        self.socket.send_to(&bytes, addr).await?;
        Ok(())
    }

    pub async fn members(&self) -> Vec<MemberInfo> {
        self.members.read().await.values().cloned().collect()
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    /// Spawns the probe loop and receive loop, and returns a channel
    /// that yields election-related messages (RequestVote, VoteResponse,
    /// Heartbeat) as they arrive — gossip and election share one socket,
    /// so the receive loop demultiplexes by message type rather than
    /// each subsystem racing to read the same UDP socket independently.
    pub fn spawn(
        self: Arc<Self>,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<(ClusterMessage, SocketAddr)>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = self.election_tx.set(tx);

        let probe_handle = {
            let this = self.clone();
            tokio::spawn(async move { this.probe_loop().await })
        };
        let recv_handle = {
            let this = self.clone();
            tokio::spawn(async move { this.recv_loop().await })
        };
        (probe_handle, recv_handle, rx)
    }

    async fn probe_loop(self: Arc<Self>) {
        let mut tick = interval(PROBE_INTERVAL);
        loop {
            tick.tick().await;

            let target = {
                let members = self.members.read().await;
                let candidates: Vec<_> = members
                    .values()
                    .filter(|m| m.id != self.id && m.state != MemberState::Dead)
                    .cloned()
                    .collect();
                if candidates.is_empty() {
                    None
                } else {
                    let idx = rand::rng().random_range(0..candidates.len());
                    Some(candidates[idx].clone())
                }
            };
            let Some(target) = target else { continue };

            let (tx, rx) = oneshot::channel();
            self.pending_acks.write().await.insert(target.id.clone(), tx);

            let ping = ClusterMessage::Ping { from: self.id.clone() };
            if self.send_to(target.addr, &ping).await.is_err() {
                self.pending_acks.write().await.remove(&target.id);
                continue;
            }

            if timeout(ACK_TIMEOUT, rx).await.is_err() {
                self.pending_acks.write().await.remove(&target.id);
                self.mark_suspect(&target.id).await;
            }
        }
    }

    async fn mark_suspect(&self, id: &NodeId) {
        {
            let mut members = self.members.write().await;
            if let Some(m) = members.get_mut(id) {
                if m.state == MemberState::Alive {
                    tracing::warn!(node = %id, "marking suspect");
                    m.state = MemberState::Suspect;
                    m.incarnation += 1;
                }
            }
        }
        let members_handle = self.members.clone();
        let id = id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SUSPECT_TIMEOUT).await;
            let mut members = members_handle.write().await;
            if let Some(m) = members.get_mut(&id) {
                if m.state == MemberState::Suspect {
                    tracing::warn!(node = %id, "marking dead");
                    m.state = MemberState::Dead;
                }
            }
        });
    }

    async fn recv_loop(self: Arc<Self>) {
        let mut buf = [0u8; 4096];
        loop {
            let Ok((len, from_addr)) = self.socket.recv_from(&mut buf).await else { continue };
            let Ok(msg) = serde_json::from_slice::<ClusterMessage>(&buf[..len]) else { continue };
            self.handle_message(msg, from_addr).await;
        }
    }

    async fn handle_message(&self, msg: ClusterMessage, from_addr: SocketAddr) {
        match msg {
            ClusterMessage::Join { from, addr } => {
                let mut members = self.members.write().await;
                members.entry(from.clone()).or_insert(MemberInfo {
                    id: from,
                    addr,
                    incarnation: 0,
                    state: MemberState::Alive,
                });
            }
            ClusterMessage::Ping { from } => {
                let snapshot = self.members().await;
                let ack = ClusterMessage::Ack { from: self.id.clone(), members: snapshot };
                let _ = self.send_to(from_addr, &ack).await;
                self.revive(&from, from_addr).await;
            }
            ClusterMessage::Ack { from, members: their_members } => {
                self.revive(&from, from_addr).await;
                self.merge(their_members).await;
                if let Some(tx) = self.pending_acks.write().await.remove(&from) {
                    let _ = tx.send(());
                }
            }
            ClusterMessage::PingReq { .. } => {
                // indirect-probe extension point; not wired up (see module docs).
            }
            other => {
                if let Some(tx) = self.election_tx.get() {
                    let _ = tx.send((other, from_addr));
                }
            }
        }
    }

    async fn revive(&self, id: &NodeId, addr: SocketAddr) {
        let mut members = self.members.write().await;
        members
            .entry(id.clone())
            .and_modify(|m| m.state = MemberState::Alive)
            .or_insert(MemberInfo { id: id.clone(), addr, incarnation: 0, state: MemberState::Alive });
    }

    async fn merge(&self, incoming: Vec<MemberInfo>) {
        let mut members = self.members.write().await;
        for m in incoming {
            match members.get(&m.id) {
                Some(existing) if existing.incarnation >= m.incarnation => {}
                _ => {
                    members.insert(m.id.clone(), m);
                }
            }
        }
    }
}
