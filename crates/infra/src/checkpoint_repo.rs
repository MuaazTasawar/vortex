//! Deliberately takes primitive/serializable data (`i64`, `serde_json::Value`),
//! not `domain::Window` / `engine::WindowStats` directly — `infra` stays a
//! leaf crate that only knows how to talk to Postgres, with no dependency
//! on the business-logic crates. `api` is the one place that understands
//! both persistence and domain types, and does the conversion.

use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct CheckpointRow {
    pub id: uuid::Uuid,
    pub stream_id: i64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub stats: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub struct CheckpointRepo {
    pool: PgPool,
}

impl CheckpointRepo {
    pub fn new(pool: PgPool) -> Self {
        CheckpointRepo { pool }
    }

    /// Insert-or-update on the `(stream_id, window_start_ms, window_end_ms)`
    /// unique constraint — re-checkpointing a window that's already been
    /// persisted (e.g. after more events arrive for it) updates in place
    /// rather than erroring.
    pub async fn upsert(
        &self,
        stream_id: i64,
        window_start_ms: i64,
        window_end_ms: i64,
        stats: &serde_json::Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO checkpoints (stream_id, window_start_ms, window_end_ms, stats)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (stream_id, window_start_ms, window_end_ms)
             DO UPDATE SET stats = EXCLUDED.stats, created_at = now()",
        )
        .bind(stream_id)
        .bind(window_start_ms)
        .bind(window_end_ms)
        .bind(stats)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_for_stream(&self, stream_id: i64) -> Result<Vec<CheckpointRow>, sqlx::Error> {
        sqlx::query_as::<_, CheckpointRow>(
            "SELECT id, stream_id, window_start_ms, window_end_ms, stats, created_at
             FROM checkpoints WHERE stream_id = $1 ORDER BY window_start_ms",
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Not a happy-path test (that needs a real DB — Phase 8's
    /// testcontainers work covers it). This proves a connection failure
    /// surfaces as a normal `Result::Err` rather than panicking, which
    /// matters because `upsert` runs inside a background task in `api`
    /// that must survive a transient DB outage.
    #[tokio::test]
    async fn upsert_against_unreachable_db_returns_error_not_panic() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@127.0.0.1:1/nonexistent")
            .expect("connect_lazy should not require a live connection");
        let repo = CheckpointRepo::new(pool);

        let result = repo
            .upsert(1, 0, 1000, &serde_json::json!({ "count": 0 }))
            .await;

        assert!(result.is_err());
    }
}