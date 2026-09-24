use crate::transform::Transform;

pub struct TransformRegistration {
    pub name: &'static str,
    pub factory: fn() -> Box<dyn Transform>,
}

inventory::collect!(TransformRegistration);