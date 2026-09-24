use domain::{Transform, TransformRegistration};
use std::sync::Arc;

/// Holds every available transform — native (auto-discovered via
/// `inventory`) plus any WASM-backed transforms registered manually
/// after loading a guest module.
pub struct TransformRegistry {
    transforms: Vec<Arc<dyn Transform>>,
}

impl TransformRegistry {
    pub fn with_native_transforms() -> Self {
        let transforms = inventory::iter::<TransformRegistration>()
            .map(|reg| Arc::from((reg.factory)()))
            .collect();
        TransformRegistry { transforms }
    }

    pub fn register(&mut self, transform: Arc<dyn Transform>) {
        self.transforms.push(transform);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Transform>> {
        self.transforms.iter().find(|t| t.name() == name).cloned()
    }

    pub fn names(&self) -> Vec<&str> {
        self.transforms.iter().map(|t| t.name()).collect()
    }
}