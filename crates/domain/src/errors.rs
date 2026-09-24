use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("transform '{0}' failed: {1}")]
    TransformFailed(String, String),

    #[error("malformed event payload: {0}")]
    MalformedPayload(String),

    #[error("window out of range")]
    WindowOutOfRange,
}