//! Hand-rolled Prometheus text-exposition metrics -- not an external
//! crate. Same reasoning as the rate limiter (see middleware/rate_limit.rs):
//! the format itself is small and stable enough to own outright rather
//! than add a fourth dependency-drift risk to a project that's already
//! hit three.

use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct Metrics {
    pub ingest_accepted_total: AtomicU64,
    pub ingest_rejected_total: AtomicU64,
    pub checkpoint_writes_total: AtomicU64,
    pub checkpoint_write_failures_total: AtomicU64,
    pub windows_evicted_total: AtomicU64,
}
