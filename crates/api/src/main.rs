use infra::config::Settings;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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

    let state = api::build_state(settings.clone(), db_pool).await?;

    let cancel = CancellationToken::new();
    let h1 = tokio::spawn(api::consume_ingestion_loop(state.clone(), cancel.clone()));
    let h2 = tokio::spawn(api::broadcast_stats_loop(state.clone(), cancel.clone()));
    let h3 = tokio::spawn(api::persist_checkpoints_loop(state.clone(), cancel.clone()));

    let app = api::routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(&settings.http_bind_addr).await?;

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown_signal(cancel))
        .await?;

    tracing::info!("server stopped, waiting for background tasks to drain");
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = tokio::join!(h1, h2, h3);
    })
    .await;

    Ok(())
}

async fn shutdown_signal(cancel: CancellationToken) {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutdown signal received");
    cancel.cancel();
}