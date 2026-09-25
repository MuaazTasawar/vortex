use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::State;
use axum::Json;
use jsonwebtoken::{encode, EncodingKey, Header};
use argon2::password_hash::rand_core::OsRng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::ApiError;
use crate::extractors::auth_user::Claims;
use crate::AppState;

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub token: String,
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: uuid::Uuid,
    password_hash: String,
}

pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(req.password.as_bytes(), &salt)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .to_string();

    let result: Result<(uuid::Uuid,), sqlx::Error> =
        sqlx::query_as("INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id")
            .bind(&req.username)
            .bind(&hash)
            .fetch_one(&state.db_pool)
            .await;

    let (user_id,) = result.map_err(|e| match &e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            ApiError::Conflict("username already taken".into())
        }
        _ => ApiError::from(e),
    })?;

    let token = issue_token(&user_id.to_string(), &state.settings.jwt_secret)?;
    Ok(Json(AuthResponse { token }))
}

pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let row: Option<UserRow> = sqlx::query_as("SELECT id, password_hash FROM users WHERE username = $1")
        .bind(&req.username)
        .fetch_optional(&state.db_pool)
        .await?;

    let row = row.ok_or_else(|| ApiError::Unauthorized("invalid username or password".into()))?;

    let parsed_hash =
        PasswordHash::new(&row.password_hash).map_err(|e| ApiError::Internal(e.to_string()))?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| ApiError::Unauthorized("invalid username or password".into()))?;

    let token = issue_token(&row.id.to_string(), &state.settings.jwt_secret)?;
    Ok(Json(AuthResponse { token }))
}

fn issue_token(user_id: &str, secret: &str) -> Result<String, ApiError> {
    let exp = (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp() as usize;
    let claims = Claims { sub: user_id.to_string(), exp };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes()))
        .map_err(|e| ApiError::Internal(e.to_string()))
}