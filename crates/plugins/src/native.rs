use domain::{DomainError, Event};
use macros::transform;
use std::borrow::Cow;

#[transform]
fn uppercase_key(event: &Event<'_>) -> Result<Vec<Event<'static>>, DomainError> {
    let owned = event.clone().into_owned();
    Ok(vec![Event {
        stream_id: owned.stream_id,
        timestamp_ms: owned.timestamp_ms,
        key: Cow::Owned(owned.key.to_uppercase()),
        payload: owned.payload,
    }])
}

#[cfg(test)]
mod tests {
    use crate::registry::TransformRegistry;
    use domain::Event;

    #[test]
    fn uppercase_key_native_transform_is_auto_registered_and_works() {
        let registry = TransformRegistry::with_native_transforms();
        let transform = registry
            .get("uppercase_key")
            .expect("uppercase_key should be auto-registered via #[transform] + inventory");

        let event = Event::borrowed(1, 0, "abc", b"payload");
        let out = transform.apply(&event).unwrap();

        assert_eq!(out[0].key, "ABC");
    }
}