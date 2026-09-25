//! Buckets events into fixed windows and computes per-window stats.
//! `sum`/`mean` use the SIMD path; `min`/`max` are plain scalar scans
//! (see simd_agg.rs module docs for why min/max weren't SIMD'd here).

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
    windows: HashMap<Window, Vec<f64>>,
}

impl WindowAggregator {
    pub fn new(window_size: Duration) -> Self {
        WindowAggregator { window_size, windows: HashMap::new() }
    }

    /// Silently drops events whose payload isn't a well-formed f64
    /// array (see `Event::as_f64_slice`) — non-numeric events simply
    /// aren't part of this aggregation.
    pub fn ingest(&mut self, event: &Event<'_>) {
        let Some(values) = event.as_f64_slice() else { return };
        let window = Window::covering(event.timestamp_ms, self.window_size);
        self.windows.entry(window).or_default().extend_from_slice(values);
    }

    pub fn finalize(&self) -> HashMap<Window, WindowStats> {
        self.windows
            .iter()
            .map(|(window, values)| {
                let count = values.len();
                let sum = sum_simd(values);
                let mean = if count > 0 { sum / count as f64 } else { 0.0 };
                let min = values.iter().copied().fold(f64::INFINITY, f64::min);
                let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                (*window, WindowStats { count, sum, mean, min, max })
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
        let (_, s) = stats.iter().next().unwrap();
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
}