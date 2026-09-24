use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl Window {
    pub fn covering(timestamp_ms: i64, size: Duration) -> Self {
        let size_ms = size.as_millis() as i64;
        let start_ms = (timestamp_ms / size_ms) * size_ms;
        Window { start_ms, end_ms: start_ms + size_ms }
    }

    pub fn contains(&self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_ms && timestamp_ms < self.end_ms
    }
}