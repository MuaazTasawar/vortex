//! Buckets events into fixed windows and computes per-window stats.
//! `sum`/`mean` use the SIMD path; `min`/`max` are plain scalar scans
//! (see simd_agg.rs module docs for why min/max weren't SIMD'd here).
//!
//! Windows are keyed by `(stream_id, Window)`, not just `Window` — an
//! earlier version keyed by window alone, which silently merged every
//! stream's events into the same bucket. That only became visible once
//! Phase 7 needed a `stream_id` to persist checkpoints by, which is
//! exactly the kind of bug a type-level shortcut like "just use the
//! window as the key" tends to hide until something downstream needs
//! the information that got discarded.

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
        WindowAggregator { window_size, windows: HashMap::new() }
    }

    pub fn ingest(&mut self, event: &Event<'_>) {
        let Some(values) = event.as_f64_slice() else { return };
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
                ((*stream_id, *window), WindowStats { count, sum, mean, min, max })
            })
            .collect()
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
        // The bug this test guards against: before the (stream_id, Window)
        // key, these two events would have landed in the same bucket and
        // been summed together, even though they belong to unrelated streams.
        let mut agg = WindowAggregator::new(Duration::from_millis(1000));
        let payload: Vec<u8> = 10.0f64.to_le_bytes().to_vec();

        agg.ingest(&Event::borrowed(1, 500, "k", &payload));
        agg.ingest(&Event::borrowed(2, 500, "k", &payload));

        let stats = agg.finalize();
        assert_eq!(stats.len(), 2, "expected two separate per-stream windows, not one merged window");
        for (_, s) in stats.iter() {
            assert_eq!(s.count, 1);
            assert_eq!(s.sum, 10.0);
        }
    }
}