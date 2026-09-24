use crate::errors::DomainError;
use crate::event::Event;

pub trait Transform: Send + Sync {
    fn name(&self) -> &str;
    fn apply<'a>(&self, event: &Event<'a>) -> Result<Vec<Event<'static>>, DomainError>;
}

pub struct Identity;

impl Transform for Identity {
    fn name(&self) -> &str {
        "identity"
    }

    fn apply<'a>(&self, event: &Event<'a>) -> Result<Vec<Event<'static>>, DomainError> {
        Ok(vec![event.clone().into_owned()])
    }
}