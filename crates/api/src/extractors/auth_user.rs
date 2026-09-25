use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

pub struct AuthUser {
    pub user_id: String,
}

#[derive(Debug)]
pub struct AuthError(pub String);

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": self.0 }))).into_response()
    }
}

impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AuthError("missing Authorization header".into()))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| AuthError("expected Bearer token".into()))?;

        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(state.settings.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|e| AuthError(format!("invalid token: {e}")))?;

        Ok(AuthUser { user_id: data.claims.sub })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use axum::body::Body;
    use axum::extract::Request;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use tokio::sync::{broadcast, RwLock};

    /// Builds a real `AppState` without opening a database connection Ã¢â‚¬â€
    /// `connect_lazy` validates the URL but defers the actual socket
    /// connect until a query runs, which lets us exercise auth logic
    /// (which never touches the DB) without needing Postgres running.
    fn test_state(jwt_secret: &str) -> Arc<AppState> {
        let db_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/db")
            .expect("connect_lazy should not require a live connection");

        let settings = infra::config::Settings {
            database_url: "postgres://user:pass@localhost/db".into(),
            jwt_secret: jwt_secret.into(),
            gossip_bind_addr: "127.0.0.1:0".into(),
            gossip_seeds: vec![],
            http_bind_addr: "127.0.0.1:0".into(),
        };

        Arc::new(AppState {
            db_pool,
            settings,
            ingestion: Arc::new(engine::RingBuffer::with_capacity(2)),
            aggregator: RwLock::new(engine::WindowAggregator::new(std::time::Duration::from_secs(1))),
            transform_registry: plugins::TransformRegistry::with_native_transforms(),
            stats_tx: broadcast::channel(1).0,
            gossip: test_gossip(),
            election: test_election(),
            checkpoint_repo: infra::checkpoint_repo::CheckpointRepo::new(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://user:pass@localhost/db")
                    .expect("connect_lazy should not require a live connection"),
            ),
        })
    }

    // Gossip/Election aren't exercised by these tests, but AppState needs
    // real values to construct Ã¢â‚¬â€ bind on port 0 (OS-assigned) so tests
    // never collide with each other or a real running node.
    fn test_gossip() -> Arc<cluster::Gossip> {
        Arc::new(
            futures::executor::block_on(cluster::Gossip::bind(
                "test-node".to_string(),
                "127.0.0.1:0".parse().unwrap(),
            ))
            .expect("bind should succeed on port 0"),
        )
    }

    fn test_election() -> Arc<cluster::Election> {
        cluster::Election::new("test-node".to_string(), test_gossip())
    }

    fn make_token(secret: &str, user_id: &str) -> String {
        let claims = Claims {
            sub: user_id.to_string(),
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize,
        };
        encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
    }

    #[tokio::test]
    async fn valid_bearer_token_resolves_to_the_correct_user() {
        let state = test_state("test-secret");
        let token = make_token("test-secret", "user-123");

        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let (mut parts, _) = request.into_parts();

        let result = AuthUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().user_id, "user-123");
    }

    #[tokio::test]
    async fn missing_authorization_header_is_rejected() {
        let state = test_state("test-secret");
        let request = Request::builder().body(Body::empty()).unwrap();
        let (mut parts, _) = request.into_parts();

        assert!(AuthUser::from_request_parts(&mut parts, &state).await.is_err());
    }

    #[tokio::test]
    async fn token_signed_with_wrong_secret_is_rejected() {
        let state = test_state("real-secret");
        let token = make_token("wrong-secret", "user-123");

        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let (mut parts, _) = request.into_parts();

        assert!(AuthUser::from_request_parts(&mut parts, &state).await.is_err());
    }
}