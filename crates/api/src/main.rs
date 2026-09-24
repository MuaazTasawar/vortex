mod routes;
mod handlers;
mod middleware;
mod extractors;

use infra::config::Settings;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let settings = Settings::load()?;
    tracing::info!(addr = %settings.http_bind_addr, "starting vortex gateway");

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&settings.database_url)
        .await?;
    sqlx::migrate!("../../migrations").run(&db_pool).await?;

    let state = Arc::new(AppState { db_pool, settings: settings.clone() });

    let app = axum::Router::new()
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&settings.http_bind_addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

#[derive(Clone)]
pub struct AppState {
    pub db_pool: sqlx::PgPool,
    pub settings: Settings,
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutdown signal received");
}