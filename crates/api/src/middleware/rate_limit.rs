//! Hand-rolled fixed-window per-IP rate limiter — not an external crate.
//! After three separate dependency-drift incidents already this project
//! (argon2, jsonwebtoken, axum's ws feature), the last phase isn't the
//! place to gamble on a fourth; this is small enough to own outright.
//!
//! Fixed-window, not a token bucket: simpler, and "N requests per
//! rolling second per IP" is precise enough for this purpose. The known
//! limitation — the IP map only grows, never evicts idle entries — is a
//! real gap for a long-running production node and would need a TTL
//! sweep; noted rather than hidden.
//!
//! `ConnectInfo` is read directly out of `req.extensions()` rather than
//! taken as a typed extractor parameter — an `Option<ConnectInfo<_>>`
//! parameter on a `from_fn` closure doesn't satisfy axum 0.8.9's
//! generated `Service` bounds (a real rough edge, not a misunderstanding
//! of the API), so this sidesteps that code path entirely while doing
//! the same thing a typed extractor would do internally.

use axum::extract::{ConnectInfo, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const WINDOW: Duration = Duration::from_secs(1);
const MAX_REQUESTS_PER_WINDOW: u32 = 20;

#[derive(Default)]
struct Bucket {
    window_start: Option<Instant>,
    count: u32,
}

#[derive(Clone, Default)]
pub struct RateLimiterState {
    buckets: Arc<Mutex<HashMap<SocketAddr, Bucket>>>,
}

impl RateLimiterState {
    /// Pure decision logic, separated from the axum wiring below so it's
    /// directly unit-testable without constructing a real `Next`.
    async fn check(&self, addr: SocketAddr) -> bool {
        let mut buckets = self.buckets.lock().await;
        let bucket = buckets.entry(addr).or_default();
        let now = Instant::now();

        match bucket.window_start {
            Some(start) if now.duration_since(start) < WINDOW => {
                if bucket.count >= MAX_REQUESTS_PER_WINDOW {
                    false
                } else {
                    bucket.count += 1;
                    true
                }
            }
            _ => {
                bucket.window_start = Some(now);
                bucket.count = 1;
                true
            }
        }
    }
}

pub async fn check_and_respond(
    limiter: RateLimiterState,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Read straight out of extensions rather than as a typed extractor
    // parameter (see module docs). Real traffic always has this,
    // populated by `into_make_service_with_connect_info`; a Router
    // called directly via `.oneshot()` in tests won't, and in that case
    // limiting is skipped rather than rejected.
    let addr = req.extensions().get::<ConnectInfo<SocketAddr>>().map(|ci| ci.0);

    if let Some(addr) = addr {
        if !limiter.check(addr).await {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }

    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn allows_up_to_the_limit_then_rejects_within_the_same_window() {
        let limiter = RateLimiterState::default();
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        for _ in 0..MAX_REQUESTS_PER_WINDOW {
            assert!(limiter.check(addr).await, "should allow up to the limit");
        }
        assert!(!limiter.check(addr).await, "should reject once over the limit");
    }

    #[tokio::test]
    async fn different_ips_have_independent_limits() {
        let limiter = RateLimiterState::default();
        let a: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2".parse().unwrap();

        for _ in 0..MAX_REQUESTS_PER_WINDOW {
            assert!(limiter.check(a).await);
        }
        assert!(!limiter.check(a).await);
        assert!(limiter.check(b).await, "a different IP must not be affected by a's limit");
    }
}