use crate::epub::EpubMeta;
use anyhow::Context;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Re-export the pool type used throughout the crate.
pub type DbPool = sqlx::SqlitePool;

/// A row from the `editions` table.
#[derive(Debug, Clone)]
pub struct EditionRow {
    pub id: i64,
    pub title: Option<String>,
    pub authors: Option<String>,
    pub isbn: Option<String>,
    pub series_name: Option<String>,
    pub series_position: Option<String>,
    pub publisher: Option<String>,
    pub publish_date: Option<String>,
    pub language: Option<String>,
    pub description: Option<String>,
    pub cover_image_path: Option<String>,
    pub source_path: String,
    pub work_id: Option<i64>,
    pub owned: i64,
    pub enriched_at: Option<String>,
    pub enrichment_attempted: i64,
}

/// Fields that an enrichment pass may update on an `editions` row.
#[derive(Debug, Default)]
pub struct EnrichmentUpdate {
    pub title: Option<String>,
    pub authors: Option<String>,
    pub publisher: Option<String>,
    pub publish_date: Option<String>,
    pub description: Option<String>,
    pub isbn: Option<String>,
    pub enriched_at: Option<String>,
    pub enrichment_attempted: i64,
    pub ol_work_id: Option<String>,
}

/// Resolve the default database path: `%APPDATA%\bookshelf\library.db` on
/// Windows. Falls back to `./bookshelf.db` if `dirs::data_dir()` returns
/// `None`.
pub fn default_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("bookshelf")
        .join("library.db")
}

/// Open (or create) the SQLite database at `path`, run pending migrations,
/// and return an active connection pool.
pub async fn open(path: &Path) -> anyhow::Result<DbPool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create DB directory {}", parent.display()))?;
    }

    let url = format!("sqlite:{}", path.to_string_lossy());
    let opts = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .connect_with(opts)
        .await
        .with_context(|| format!("cannot open database at {}", path.display()))?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("database migration failed")?;

    Ok(pool)
}

/// Insert an edition row if the `source_path` does not already exist.
/// Returns the `id` of the row (new or existing).
pub async fn upsert_edition(pool: &DbPool, meta: &EpubMeta) -> anyhow::Result<i64> {
    // Try to insert; silently ignore conflicts on source_path UNIQUE constraint.
    sqlx::query(
        r"INSERT OR IGNORE INTO editions
          (title, authors, isbn, series_name, series_position, publisher,
           publish_date, language, description, cover_image_path, source_path)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&meta.title)
    .bind(&meta.authors)
    .bind(&meta.isbn)
    .bind(&meta.series_name)
    .bind(&meta.series_position)
    .bind(&meta.publisher)
    .bind(&meta.publish_date)
    .bind(&meta.language)
    .bind(&meta.description)
    .bind(&meta.cover_image_path)
    .bind(&meta.source_path)
    .execute(pool)
    .await
    .context("upsert_edition INSERT")?;

    // Retrieve the id (works whether just inserted or already existed).
    let row = sqlx::query("SELECT id FROM editions WHERE source_path = ?")
        .bind(&meta.source_path)
        .fetch_one(pool)
        .await
        .context("upsert_edition SELECT id")?;

    Ok(row.try_get("id")?)
}

/// Find an existing `works` row by matching any edition's ISBN.
/// Returns the `work_id` of the first matching edition, or `None`.
pub async fn find_work_by_isbn(pool: &DbPool, isbn: &str) -> anyhow::Result<Option<i64>> {
    let row = sqlx::query(
        "SELECT work_id FROM editions WHERE isbn = ? AND work_id IS NOT NULL LIMIT 1",
    )
    .bind(isbn)
    .fetch_optional(pool)
    .await
    .context("find_work_by_isbn")?;

    Ok(row.and_then(|r| r.try_get::<Option<i64>, _>("work_id").ok().flatten()))
}

/// Find an existing `works` row by its OpenLibrary work ID.
/// Returns the `works.id`, or `None`.
pub async fn find_work_by_ol_id(pool: &DbPool, ol_id: &str) -> anyhow::Result<Option<i64>> {
    let row =
        sqlx::query("SELECT id FROM works WHERE openlibrary_work_id = ? LIMIT 1")
            .bind(ol_id)
            .fetch_optional(pool)
            .await
            .context("find_work_by_ol_id")?;

    Ok(row.and_then(|r| r.try_get::<i64, _>("id").ok()))
}

/// Insert a new `works` row and return its `id`.
pub async fn insert_work(
    pool: &DbPool,
    canonical_title: &str,
    canonical_authors: &str,
) -> anyhow::Result<i64> {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let result = sqlx::query(
        "INSERT INTO works (canonical_title, canonical_authors, created_at) VALUES (?, ?, ?)",
    )
    .bind(canonical_title)
    .bind(canonical_authors)
    .bind(&now)
    .execute(pool)
    .await
    .context("insert_work")?;

    Ok(result.last_insert_rowid())
}

/// Set the `work_id` foreign key on an `editions` row.
pub async fn set_edition_work_id(
    pool: &DbPool,
    edition_id: i64,
    work_id: i64,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET work_id = ? WHERE id = ?")
        .bind(work_id)
        .bind(edition_id)
        .execute(pool)
        .await
        .context("set_edition_work_id")?;
    Ok(())
}

/// Return all editions where `enrichment_attempted = 0`.
pub async fn editions_needing_enrichment(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows =
        sqlx::query("SELECT * FROM editions WHERE enrichment_attempted = 0")
            .fetch_all(pool)
            .await
            .context("editions_needing_enrichment")?;

    rows.into_iter().map(row_to_edition).collect()
}

/// Apply an enrichment update to one `editions` row. Only NULL columns are
/// updated (never overwrites existing data).
pub async fn apply_enrichment(
    pool: &DbPool,
    edition_id: i64,
    update: &EnrichmentUpdate,
) -> anyhow::Result<()> {
    sqlx::query(
        r"UPDATE editions SET
            title        = COALESCE(title,        ?),
            authors      = COALESCE(authors,      ?),
            publisher    = COALESCE(publisher,    ?),
            publish_date = COALESCE(publish_date, ?),
            description  = COALESCE(description,  ?),
            isbn         = COALESCE(isbn,         ?),
            enriched_at          = ?,
            enrichment_attempted = ?
          WHERE id = ?",
    )
    .bind(&update.title)
    .bind(&update.authors)
    .bind(&update.publisher)
    .bind(&update.publish_date)
    .bind(&update.description)
    .bind(&update.isbn)
    .bind(&update.enriched_at)
    .bind(update.enrichment_attempted)
    .bind(edition_id)
    .execute(pool)
    .await
    .context("apply_enrichment")?;
    Ok(())
}

/// Return all editions ordered by `id`.
pub async fn list_editions(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query("SELECT * FROM editions ORDER BY id")
        .fetch_all(pool)
        .await
        .context("list_editions")?;

    rows.into_iter().map(row_to_edition).collect()
}

/// Return one edition by `id`, or `None` if not found.
pub async fn get_edition(pool: &DbPool, id: i64) -> anyhow::Result<Option<EditionRow>> {
    let row = sqlx::query("SELECT * FROM editions WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
        .context("get_edition")?;

    row.map(row_to_edition).transpose()
}

/// Return all editions that have a `work_id` set (used during dedup scan).
pub async fn all_editions_for_dedup(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query(
        "SELECT * FROM editions WHERE work_id IS NOT NULL AND isbn IS NULL",
    )
    .fetch_all(pool)
    .await
    .context("all_editions_for_dedup")?;

    rows.into_iter().map(row_to_edition).collect()
}

/// Update the `openlibrary_work_id` on a `works` row. Also re-links any
/// other `works` rows that already have the same OL ID, merging editions
/// under the earliest work.
pub async fn update_work_ol_id(
    pool: &DbPool,
    work_id: i64,
    ol_id: &str,
) -> anyhow::Result<()> {
    // Check if another works row already has this OL ID.
    if let Some(existing_work_id) = find_work_by_ol_id(pool, ol_id).await? {
        if existing_work_id != work_id {
            // Merge: re-point editions from the newer work to the existing one.
            sqlx::query("UPDATE editions SET work_id = ? WHERE work_id = ?")
                .bind(existing_work_id)
                .bind(work_id)
                .execute(pool)
                .await
                .context("update_work_ol_id merge editions")?;
            // Delete the superseded works row.
            sqlx::query("DELETE FROM works WHERE id = ?")
                .bind(work_id)
                .execute(pool)
                .await
                .context("update_work_ol_id delete old work")?;
            return Ok(());
        }
    }

    // No conflict: just set the OL ID.
    sqlx::query("UPDATE works SET openlibrary_work_id = ? WHERE id = ?")
        .bind(ol_id)
        .bind(work_id)
        .execute(pool)
        .await
        .context("update_work_ol_id")?;
    Ok(())
}

/// Convert a raw `sqlx::sqlite::SqliteRow` into an `EditionRow`.
fn row_to_edition(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<EditionRow> {
    Ok(EditionRow {
        id: row.try_get("id")?,
        title: row.try_get("title")?,
        authors: row.try_get("authors")?,
        isbn: row.try_get("isbn")?,
        series_name: row.try_get("series_name")?,
        series_position: row.try_get("series_position")?,
        publisher: row.try_get("publisher")?,
        publish_date: row.try_get("publish_date")?,
        language: row.try_get("language")?,
        description: row.try_get("description")?,
        cover_image_path: row.try_get("cover_image_path")?,
        source_path: row.try_get("source_path")?,
        work_id: row.try_get("work_id")?,
        owned: row.try_get("owned")?,
        enriched_at: row.try_get("enriched_at")?,
        enrichment_attempted: row.try_get("enrichment_attempted")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    async fn open_temp_db() -> (DbPool, NamedTempFile) {
        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = open(tmp.path()).await.unwrap();
        (pool, tmp)
    }

    #[tokio::test]
    async fn test_open_creates_schema() {
        let (_pool, _tmp) = open_temp_db().await;
        // If we reach here without panic, schema was created successfully.
    }

    #[tokio::test]
    async fn test_upsert_edition_inserts_and_deduplicates() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Test Book".to_string()),
            authors: Some("Test Author".to_string()),
            isbn: Some("9781234567890".to_string()),
            source_path: "/tmp/test.epub".to_string(),
            ..Default::default()
        };
        let id1 = upsert_edition(&pool, &meta).await.unwrap();
        let id2 = upsert_edition(&pool, &meta).await.unwrap();
        assert_eq!(id1, id2, "second upsert should return same id");

        let editions = list_editions(&pool).await.unwrap();
        assert_eq!(editions.len(), 1);
    }

    #[tokio::test]
    async fn test_insert_work_and_find_by_isbn() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Book".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9781111111111".to_string()),
            source_path: "/tmp/book.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap();
        let work_id = insert_work(&pool, "Book", "Author").await.unwrap();
        set_edition_work_id(&pool, edition_id, work_id).await.unwrap();

        let found = find_work_by_isbn(&pool, "9781111111111").await.unwrap();
        assert_eq!(found, Some(work_id));
    }

    #[tokio::test]
    async fn test_apply_enrichment() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Book".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9781234500000".to_string()),
            source_path: "/tmp/enrich_test.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap();
        let update = EnrichmentUpdate {
            publisher: Some("Test Publisher".to_string()),
            enriched_at: Some("2026-01-01T00:00:00Z".to_string()),
            enrichment_attempted: 1,
            ..Default::default()
        };
        apply_enrichment(&pool, edition_id, &update).await.unwrap();

        let row = get_edition(&pool, edition_id).await.unwrap().unwrap();
        assert_eq!(row.publisher.as_deref(), Some("Test Publisher"));
        assert_eq!(row.enriched_at.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(row.enrichment_attempted, 1);
    }

    #[tokio::test]
    async fn test_default_db_path_contains_bookshelf() {
        let path = default_db_path();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains("bookshelf"),
            "DB path should contain 'bookshelf', got: {path_str}"
        );
        assert!(path_str.ends_with("library.db"));
    }
}
