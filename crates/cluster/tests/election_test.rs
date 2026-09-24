//! Integration test: spins up three real nodes on localhost UDP ports
//! and asserts the cluster converges on exactly one leader. This is
//! the actual proof the election logic works end to end — unit tests
//! alone can't catch a bootstrap split-brain like the one this module
//! documents fixing.

use cluster::{Election, Gossip};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

async fn spawn_node(id: &str, port: u16, seed: Option<SocketAddr>) -> Arc<Election> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let gossip = Arc::new(Gossip::bind(id.to_string(), addr).await.unwrap());
    let (_probe, _recv, election_rx) = gossip.clone().spawn();
    if let Some(seed) = seed {
        gossip.join(seed).await.unwrap();
    }
    let election = Election::new(id.to_string(), gossip);
    tokio::spawn(election.clone().run(election_rx));
    election
}

#[tokio::test]
async fn a_leader_is_elected_among_three_nodes() {
    let a = spawn_node("a", 17001, None).await;
    let seed: SocketAddr = "127.0.0.1:17001".parse().unwrap();
    let b = spawn_node("b", 17002, Some(seed)).await;
    let c = spawn_node("c", 17003, Some(seed)).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let la = a.current_leader().await;
    let lb = b.current_leader().await;
    let lc = c.current_leader().await;

    let distinct: std::collections::HashSet<_> = [&la, &lb, &lc].into_iter().flatten().collect();
    assert_eq!(
        distinct.len(),
        1,
        "expected exactly one leader across the cluster, got a={:?} b={:?} c={:?}",
        la, lb, lc
    );
}