use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

/// Stashed in request extensions for any handler that wants to correlate
/// its own logs with the `x-request-id` response header. Not read by any
/// handler yet, which is why this carries an explicit dead-code
/// allowance rather than being silently unused.
#[allow(dead_code)]
#[derive(Clone)]
pub struct RequestId(pub String);

pub async fn request_id(mut req: Request, next: Next) -> Response {
    let id = Uuid::new_v4().to_string();
    req.extensions_mut().insert(RequestId(id.clone()));
    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}