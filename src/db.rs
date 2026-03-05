use anyhow::Result;
use sqlx::PgPool;

pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS search;

        CREATE TABLE IF NOT EXISTS search.saved_searches (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            query_type  TEXT NOT NULL
                        CHECK (query_type IN ('all', 'contacts', 'deals', 'emails', 'notes')),
            query_params JSONB NOT NULL DEFAULT '{}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS search.search_log (
            id          TEXT PRIMARY KEY,
            query       TEXT NOT NULL,
            result_count INTEGER NOT NULL DEFAULT 0,
            searched_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_saved_searches_name ON search.saved_searches(name);
        CREATE INDEX IF NOT EXISTS idx_saved_searches_type ON search.saved_searches(query_type);
        CREATE INDEX IF NOT EXISTS idx_search_log_query ON search.search_log(query);
        CREATE INDEX IF NOT EXISTS idx_search_log_searched_at ON search.search_log(searched_at);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
