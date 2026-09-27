//! Buckets events into fixed windows and computes per-window stats.
//! `sum`/`mean` use the SIMD path; `min`/`max` are plain scalar scans
//! (see simd_agg.rs module docs for why min/max weren't SIMD'd here).
//!
//! Windows are keyed by `(stream_id, Window)`, not just `Window` — an
//! earlier version keyed by window alone, which silently merged every
//! stream's events into the same bucket.
//!
//! Eviction is wall-clock TTL based: `evict_older_than(cutoff_ms)`
//! removes any window whose `end_ms` is before the cutoff. This assumes
//! `Event::timestamp_ms` is real epoch milliseconds — a demo/test event
//! with a small timestamp like `500` is "1970" as far as eviction is
//! concerned, and would be evicted almost immediately by a real running
//! eviction loop. That's expected, not a bug: test data with unrealistic
//! timestamps ages out fast; real ingested data (real epoch ms) doesn't.

use crate::simd_agg::sum_simd;
use domain::{Event, Window};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct WindowStats {
    pub count: usize,
    pub sum: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
}

pub struct WindowAggregator {
    window_size: Duration,
    windows: HashMap<(u64, Window), Vec<f64>>,
}

impl WindowAggregator {
    pub fn new(window_size: Duration) -> Self {
        WindowAggregator {
            window_size,
            windows: HashMap::new(),
        }
    }

    pub fn ingest(&mut self, event: &Event<'_>) {
        let Some(values) = event.as_f64_slice() else {
            return;
        };
        let window = Window::covering(event.timestamp_ms, self.window_size);
        self.windows
            .entry((event.stream_id, window))
            .or_default()
            .extend_from_slice(values);
    }

    pub fn finalize(&self) -> HashMap<(u64, Window), WindowStats> {
        self.windows
            .iter()
            .map(|((stream_id, window), values)| {
                let count = values.len();
                let sum = sum_simd(values);
                let mean = if count > 0 { sum / count as f64 } else { 0.0 };
                let min = values.iter().copied().fold(f64::INFINITY, f64::min);
                let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                (
                    (*stream_id, *window),
                    WindowStats {
                        count,
                        sum,
                        mean,
                        min,
                        max,
                    },
                )
            })
            .collect()
    }

    /// Removes every window whose `end_ms` is before `cutoff_ms`.
    /// Returns how many windows were removed, so callers can log it.
    /// Safe to call at any time — it never touches windows still within
    /// the retention period, only ones already aged past it.
    pub fn evict_older_than(&mut self, cutoff_ms: i64) -> usize {
        let before = self.windows.len();
        self.windows
            .retain(|(_, window), _| window.end_ms >= cutoff_ms);
        before - self.windows.len()
    }

    #[cfg(test)]
    pub fn window_count(&self) -> usize {
        self.windows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Event;

    #[test]
    fn aggregates_events_into_correct_window_with_correct_stats() {
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));

        let payload: Vec<u8> = 2.0f64
            .to_le_bytes()
            .into_iter()
            .chain(4.0f64.to_le_bytes())
            .collect();
        let event = Event::borrowed(1, 500, "k", &payload);
        agg.ingest(&event);

        let stats = agg.finalize();
        assert_eq!(stats.len(), 1);
        let ((stream_id, _window), s) = stats.iter().next().unwrap();
        assert_eq!(*stream_id, 1);
        assert_eq!(s.count, 2);
        assert_eq!(s.sum, 6.0);
        assert_eq!(s.mean, 3.0);
        assert_eq!(s.min, 2.0);
        assert_eq!(s.max, 4.0);
    }

    #[test]
    fn events_outside_window_size_land_in_separate_windows() {
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));
        let payload: Vec<u8> = 1.0f64.to_le_bytes().to_vec();

        agg.ingest(&Event::borrowed(1, 500, "k", &payload));
        agg.ingest(&Event::borrowed(1, 1500, "k", &payload));

        assert_eq!(agg.finalize().len(), 2);
    }

    #[test]
    fn events_from_different_streams_in_the_same_time_window_stay_separate() {
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));
        let payload: Vec<u8> = 10.0f64.to_le_bytes().to_vec();

        agg.ingest(&Event::borrowed(1, 500, "k", &payload));
        agg.ingest(&Event::borrowed(2, 500, "k", &payload));

        let stats = agg.finalize();
        assert_eq!(
            stats.len(),
            2,
            "expected two separate per-stream windows, not one merged window"
        );
        for (_, s) in stats.values().map(|s| ((), s)) {
            assert_eq!(s.count, 1);
            assert_eq!(s.sum, 10.0);
        }
    }

    #[test]
    fn evict_older_than_removes_only_windows_past_the_cutoff() {
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));
        let payload: Vec<u8> = 1.0f64.to_le_bytes().to_vec();

        // window [0, 1000) — old, should be evicted
        agg.ingest(&Event::borrowed(1, 500, "k", &payload));
        // window [10_000, 11_000) — recent, should survive
        agg.ingest(&Event::borrowed(1, 10_500, "k", &payload));

        assert_eq!(agg.window_count(), 2);

        let removed = agg.evict_older_than(5_000);
        assert_eq!(removed, 1, "only the [0, 1000) window should be evicted");
        assert_eq!(agg.window_count(), 1);

        let remaining = agg.finalize();
        let ((_, window), _) = remaining.iter().next().unwrap();
        assert_eq!(window.start_ms, 10_000);
    }

    #[test]
    fn evict_older_than_is_a_no_op_when_nothing_is_past_the_cutoff() {
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));
        let payload: Vec<u8> = 1.0f64.to_le_bytes().to_vec();
        agg.ingest(&Event::borrowed(1, 10_500, "k", &payload));

        let removed = agg.evict_older_than(0);
        assert_eq!(removed, 0);
        assert_eq!(agg.window_count(), 1);
    }
}
