use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub database_url: String,
    pub jwt_secret: String,
    pub gossip_bind_addr: String,
    pub gossip_seeds: Vec<String>,
    pub http_bind_addr: String,
    /// How long (in seconds) a window is kept in memory after it closes,
    /// before the eviction loop removes it. Must stay comfortably larger
    /// than the checkpoint-persist interval (5s, hardcoded in api::lib)
    /// so every window is guaranteed to reach Postgres before it's ever
    /// evicted from memory.
    pub window_retention_secs: u64,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let seeds = std::env::var("GOSSIP_SEEDS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();

        Ok(Settings {
            database_url: std::env::var("DATABASE_URL")?,
            jwt_secret: std::env::var("JWT_SECRET")?,
            gossip_bind_addr: std::env::var("GOSSIP_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:7946".to_string()),
            gossip_seeds: seeds,
            http_bind_addr: std::env::var("HTTP_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            window_retention_secs: std::env::var("WINDOW_RETENTION_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(600),
        })
    }
}
