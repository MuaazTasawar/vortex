use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A single ingested event. Borrows its payload from the ingestion buffer
/// wherever possible — `Cow` lets a transform mutate in place without an
/// upfront clone, while still letting owned data flow through when a
/// plugin needs to produce a brand-new event. Also `Serialize`/`Deserialize`
/// so an owned `Event<'static>` can cross the WASM plugin boundary as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<'a> {
    pub stream_id: u64,
    pub timestamp_ms: i64,
    pub key: Cow<'a, str>,
    pub payload: Cow<'a, [u8]>,
}

impl<'a> Event<'a> {
    pub fn borrowed(stream_id: u64, timestamp_ms: i64, key: &'a str, payload: &'a [u8]) -> Self {
        Event {
            stream_id,
            timestamp_ms,
            key: Cow::Borrowed(key),
            payload: Cow::Borrowed(payload),
        }
    }

    pub fn into_owned(self) -> Event<'static> {
        Event {
            stream_id: self.stream_id,
            timestamp_ms: self.timestamp_ms,
            key: Cow::Owned(self.key.into_owned()),
            payload: Cow::Owned(self.payload.into_owned()),
        }
    }

    pub fn as_f64_slice(&self) -> Option<&[f64]> {
        let bytes = &self.payload;
        if bytes.len() % 8 != 0 {
            return None;
        }
        if (bytes.as_ptr() as usize) % std::mem::align_of::<f64>() != 0 {
            return None;
        }
        let len = bytes.len() / 8;
        Some(unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f64, len) })
    }
}