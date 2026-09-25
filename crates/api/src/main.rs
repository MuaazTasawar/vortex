use infra::config::Settings;

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

    tokio::spawn(api::consume_ingestion_loop(state.clone()));
    tokio::spawn(api::broadcast_stats_loop(state.clone()));
    tokio::spawn(api::persist_checkpoints_loop(state.clone()));

    let app = api::routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(&settings.http_bind_addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    tracing::info!("shutdown signal received");
}