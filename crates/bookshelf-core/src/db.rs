use crate::epub::EpubMeta;
use crate::fuzzy::normalize_isbn;
use anyhow::Context;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Re-export the pool type used throughout the crate.
pub type DbPool = sqlx::SqlitePool;

// ---------------------------------------------------------------------------
// Library statistics
// ---------------------------------------------------------------------------

/// Aggregated statistics returned by `library_stats`.
#[derive(Debug, Default, PartialEq)]
pub struct LibraryStats {
    // Library section
    pub books_in_library: i64,
    pub with_isbn: i64,
    pub in_a_series: i64,
    pub enriched: i64,
    // Want list section
    pub want_total: i64,
    pub want_with_isbn: i64,
    pub want_by_goodreads_csv: i64,
    pub want_by_manual: i64,
    pub want_by_openlibrary: i64,
    pub want_by_text_file: i64,
    // Grab list (not owned)
    pub grab_count: i64,
}

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
    // Normalize ISBN on storage so comparisons always work (Issue 1).
    let normalized_isbn = meta.isbn.as_deref().map(normalize_isbn);
    // Try to insert; silently ignore conflicts on source_path UNIQUE constraint.
    sqlx::query(
        r"INSERT OR IGNORE INTO editions
          (title, authors, isbn, series_name, series_position, publisher,
           publish_date, language, description, cover_image_path, source_path)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&meta.title)
    .bind(&meta.authors)
    .bind(&normalized_isbn)
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
    // Normalize ISBN on storage (Issue 1).
    let normalized_isbn = update.isbn.as_deref().map(normalize_isbn);
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
    .bind(&normalized_isbn)
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

// ---------------------------------------------------------------------------
// want_list table: struct, helpers, and CRUD functions
// ---------------------------------------------------------------------------

/// A row from the `want_list` table.
#[derive(Debug, Clone)]
pub struct WantRow {
    pub id: i64,
    pub title: String,
    pub author: Option<String>,
    pub isbn13: Option<String>,
    pub source: String,
    pub source_id: Option<String>,
    pub added_at: String,
    pub priority: i64,
    pub notes: Option<String>,
}

const VALID_SOURCES: &[&str] = &[
    "goodreads_csv",
    "openlibrary",
    "manual",
    "text_file",
    "author_follow",
    "series_fill",
];

/// Insert one row into `want_list`. Returns the `last_insert_rowid`.
#[allow(clippy::too_many_arguments)]
pub async fn insert_want(
    pool: &DbPool,
    title: &str,
    author: Option<&str>,
    isbn13: Option<&str>,
    source: &str,
    source_id: Option<&str>,
    priority: i64,
    notes: Option<&str>,
) -> anyhow::Result<i64> {
    if !VALID_SOURCES.contains(&source) {
        anyhow::bail!(
            "insert_want: invalid source {:?}; must be one of {:?}",
            source,
            VALID_SOURCES
        );
    }
    if !(1..=10).contains(&priority) {
        anyhow::bail!(
            "insert_want: priority {} is out of range; must be 1–10 inclusive",
            priority
        );
    }
    let added_at = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let result = sqlx::query(
        r"INSERT INTO want_list (title, author, isbn13, source, source_id, added_at, priority, notes)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(title)
    .bind(author)
    .bind(isbn13)
    .bind(source)
    .bind(source_id)
    .bind(&added_at)
    .bind(priority)
    .bind(notes)
    .execute(pool)
    .await
    .context("insert_want")?;

    Ok(result.last_insert_rowid())
}

/// Update `title`, `author`, `isbn13`, `source_id`, `priority`, and `notes` of an existing
/// `want_list` row.  Does NOT modify `added_at`.
#[allow(clippy::too_many_arguments)]
pub async fn update_want(
    pool: &DbPool,
    id: i64,
    title: &str,
    author: Option<&str>,
    isbn13: Option<&str>,
    source_id: Option<&str>,
    priority: i64,
    notes: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE want_list SET title = ?, author = ?, isbn13 = ?, source_id = ?, priority = ?, notes = ? WHERE id = ?",
    )
    .bind(title)
    .bind(author)
    .bind(isbn13)
    .bind(source_id)
    .bind(priority)
    .bind(notes)
    .bind(id)
    .execute(pool)
    .await
    .context("update_want")?;
    Ok(())
}

/// Update only `isbn13` on a `want_list` row (used by `want enrich`).
pub async fn update_want_isbn13(pool: &DbPool, id: i64, isbn13: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE want_list SET isbn13 = ? WHERE id = ?")
        .bind(isbn13)
        .bind(id)
        .execute(pool)
        .await
        .context("update_want_isbn13")?;
    Ok(())
}

/// Return all `want_list` rows, optionally filtered by `source`, ordered by `id`.
pub async fn list_want(
    pool: &DbPool,
    source_filter: Option<&str>,
) -> anyhow::Result<Vec<WantRow>> {
    let rows = if let Some(src) = source_filter {
        sqlx::query("SELECT * FROM want_list WHERE source = ? ORDER BY id")
            .bind(src)
            .fetch_all(pool)
            .await
            .context("list_want (filtered)")?
    } else {
        sqlx::query("SELECT * FROM want_list ORDER BY id")
            .fetch_all(pool)
            .await
            .context("list_want")?
    };

    rows.into_iter().map(row_to_want).collect()
}

/// Return one `want_list` row by `id`, or `None` if not found.
pub async fn get_want(pool: &DbPool, id: i64) -> anyhow::Result<Option<WantRow>> {
    let row = sqlx::query("SELECT * FROM want_list WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
        .context("get_want")?;

    row.map(row_to_want).transpose()
}

/// Return all `want_list` rows eligible for enrichment: `isbn13 IS NULL`.
/// Rows with NULL title or author are included so the caller can emit a
/// per-row warning and skip them; they must not be silently excluded here.
pub async fn want_entries_needing_enrichment(pool: &DbPool) -> anyhow::Result<Vec<WantRow>> {
    let rows = sqlx::query("SELECT * FROM want_list WHERE isbn13 IS NULL")
    .fetch_all(pool)
    .await
    .context("want_entries_needing_enrichment")?;

    rows.into_iter().map(row_to_want).collect()
}

/// Return the first `want_list` row with `isbn13 = ?`, or `None`.
/// The search value is normalized (hyphens/spaces stripped) before comparison.
pub async fn find_want_by_isbn13(
    pool: &DbPool,
    isbn13: &str,
) -> anyhow::Result<Option<WantRow>> {
    let normalized = normalize_isbn(isbn13);
    let row = sqlx::query("SELECT * FROM want_list WHERE isbn13 = ? LIMIT 1")
        .bind(&normalized)
        .fetch_optional(pool)
        .await
        .context("find_want_by_isbn13")?;

    row.map(row_to_want).transpose()
}

/// Delete one `want_list` row by `id`.
/// Returns `true` if a row was deleted, `false` if the id was not found.
pub async fn delete_want(pool: &DbPool, id: i64) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM want_list WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await
        .context("delete_want")?;
    Ok(result.rows_affected() > 0)
}

/// Return all rows from `want_list` (no filter); used by the grab command.
pub async fn all_want_entries(pool: &DbPool) -> anyhow::Result<Vec<WantRow>> {
    let rows = sqlx::query("SELECT * FROM want_list")
        .fetch_all(pool)
        .await
        .context("all_want_entries")?;

    rows.into_iter().map(row_to_want).collect()
}

/// Convert a raw `sqlx::sqlite::SqliteRow` into a `WantRow`.
fn row_to_want(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<WantRow> {
    Ok(WantRow {
        id: row.try_get("id")?,
        title: row.try_get("title")?,
        author: row.try_get("author")?,
        isbn13: row.try_get("isbn13")?,
        source: row.try_get("source")?,
        source_id: row.try_get("source_id")?,
        added_at: row.try_get("added_at")?,
        priority: row.try_get("priority")?,
        notes: row.try_get("notes")?,
    })
}

/// Collect library statistics using a series of COUNT queries in a single
/// read transaction.
pub async fn library_stats(pool: &DbPool) -> anyhow::Result<LibraryStats> {
    let mut tx = pool.begin().await.context("library_stats begin tx")?;

    macro_rules! count {
        ($q:expr) => {{
            sqlx::query($q)
                .fetch_one(&mut *tx)
                .await
                .context(concat!("library_stats query: ", $q))?
                .try_get::<i64, _>(0)?
        }};
    }

    let books_in_library =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1");
    let with_isbn =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND isbn IS NOT NULL");
    let in_a_series =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND series_name IS NOT NULL");
    let enriched =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND enriched_at IS NOT NULL");

    let want_total = count!("SELECT COUNT(*) FROM want_list");
    let want_with_isbn = count!("SELECT COUNT(*) FROM want_list WHERE isbn13 IS NOT NULL");
    let want_by_goodreads_csv =
        count!("SELECT COUNT(*) FROM want_list WHERE source = 'goodreads_csv'");
    let want_by_manual = count!("SELECT COUNT(*) FROM want_list WHERE source = 'manual'");
    let want_by_openlibrary =
        count!("SELECT COUNT(*) FROM want_list WHERE source = 'openlibrary'");
    let want_by_text_file = count!("SELECT COUNT(*) FROM want_list WHERE source = 'text_file'");

    // Grab count: want entries where isbn13 is NOT matched by any owned edition.
    let grab_count = sqlx::query(
        r"SELECT COUNT(*) FROM want_list w
          WHERE NOT EXISTS (
              SELECT 1 FROM editions e
              WHERE e.isbn = w.isbn13 AND e.owned = 1
          )",
    )
    .fetch_one(&mut *tx)
    .await
    .context("library_stats grab_count")?
    .try_get::<i64, _>(0)?;

    tx.rollback().await.context("library_stats rollback")?;

    Ok(LibraryStats {
        books_in_library,
        with_isbn,
        in_a_series,
        enriched,
        want_total,
        want_with_isbn,
        want_by_goodreads_csv,
        want_by_manual,
        want_by_openlibrary,
        want_by_text_file,
        grab_count,
    })
}

/// Public wrapper around `row_to_edition` for use in sibling modules.
pub fn row_to_edition_pub(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<EditionRow> {
    row_to_edition(row)
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

// ---------------------------------------------------------------------------
// followed_authors table: struct and CRUD functions
// ---------------------------------------------------------------------------

/// A row from the `followed_authors` table.
#[derive(Debug, Clone)]
pub struct FollowedAuthorRow {
    pub id: i64,
    pub name: String,
    pub ol_key: Option<String>,
    pub last_synced: Option<String>,
    pub added_at: String,
}

/// Convert a raw `sqlx::sqlite::SqliteRow` into a `FollowedAuthorRow`.
fn row_to_followed_author(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<FollowedAuthorRow> {
    Ok(FollowedAuthorRow {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        ol_key: row.try_get("ol_key")?,
        last_synced: row.try_get("last_synced")?,
        added_at: row.try_get("added_at")?,
    })
}

/// Insert a new row into `followed_authors`. Sets both `added_at` and
/// `last_synced` to the current UTC timestamp. Returns `last_insert_rowid`.
pub async fn insert_followed_author(
    pool: &DbPool,
    name: &str,
    ol_key: Option<&str>,
) -> anyhow::Result<i64> {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let result = sqlx::query(
        "INSERT INTO followed_authors (name, ol_key, last_synced, added_at) VALUES (?, ?, ?, ?)",
    )
    .bind(name)
    .bind(ol_key)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .context("insert_followed_author")?;

    Ok(result.last_insert_rowid())
}

/// Return the `followed_authors` row with `name = ?`, or `None`.
pub async fn find_followed_author_by_name(
    pool: &DbPool,
    name: &str,
) -> anyhow::Result<Option<FollowedAuthorRow>> {
    let row = sqlx::query("SELECT * FROM followed_authors WHERE name = ? LIMIT 1")
        .bind(name)
        .fetch_optional(pool)
        .await
        .context("find_followed_author_by_name")?;

    row.map(row_to_followed_author).transpose()
}

/// Delete the `followed_authors` row with `name = ?`.
/// Returns `true` if a row was deleted, `false` if not found.
pub async fn delete_followed_author(pool: &DbPool, name: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM followed_authors WHERE name = ?")
        .bind(name)
        .execute(pool)
        .await
        .context("delete_followed_author")?;
    Ok(result.rows_affected() > 0)
}

/// Return all rows from `followed_authors` ordered by `name`.
pub async fn list_followed_authors(pool: &DbPool) -> anyhow::Result<Vec<FollowedAuthorRow>> {
    let rows = sqlx::query("SELECT * FROM followed_authors ORDER BY name")
        .fetch_all(pool)
        .await
        .context("list_followed_authors")?;

    rows.into_iter().map(row_to_followed_author).collect()
}

/// Update `ol_key` and `last_synced` on an existing `followed_authors` row.
pub async fn update_followed_author_synced(
    pool: &DbPool,
    name: &str,
    ol_key: Option<&str>,
    last_synced: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE followed_authors SET ol_key = ?, last_synced = ? WHERE name = ?",
    )
    .bind(ol_key)
    .bind(last_synced)
    .bind(name)
    .execute(pool)
    .await
    .context("update_followed_author_synced")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Series DB query
// ---------------------------------------------------------------------------

/// Return all owned editions that have a non-NULL `series_name`, ordered by
/// `series_name` then `series_position`.
pub async fn editions_with_series(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query(
        "SELECT * FROM editions WHERE owned = 1 AND series_name IS NOT NULL ORDER BY series_name, series_position",
    )
    .fetch_all(pool)
    .await
    .context("editions_with_series")?;

    rows.into_iter().map(row_to_edition).collect()
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

    // -----------------------------------------------------------------------
    // Category 3: Phase 1 → Phase 2 migration correctness
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_migration_from_phase1_preserves_editions() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();

        // Step 1: Create a Phase-1-only DB by applying only 0001_initial.sql manually.
        {
            let url = format!("sqlite:{}", tmp.path().to_string_lossy());
            let opts = SqliteConnectOptions::from_str(&url)
                .unwrap()
                .create_if_missing(true)
                .foreign_keys(true);
            let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
            sqlx::query(include_str!("../migrations/0001_initial.sql"))
                .execute(&pool)
                .await
                .unwrap();

            // Step 2: Insert edition rows into the Phase-1 DB.
            sqlx::query(
                r"INSERT INTO editions (title, authors, source_path) VALUES ('Phase1 Book', 'Phase1 Author', '/tmp/phase1.epub')"
            )
            .execute(&pool)
            .await
            .unwrap();

            pool.close().await;
        }

        // Step 3: Open the DB via db::open() — this runs migrate! which applies 0002_want_list.sql.
        let pool = open(tmp.path()).await.unwrap();

        // Step 4: want_list table must now exist.
        let table_exists: bool = sqlx::query(
            "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name='want_list'",
        )
        .fetch_one(&pool)
        .await
        .map(|r| r.try_get::<i64, _>("cnt").unwrap_or(0) > 0)
        .unwrap_or(false);
        assert!(table_exists, "want_list table must exist after migration");

        // Step 4b: The Phase-1 edition row must still be intact.
        let editions = list_editions(&pool).await.unwrap();
        assert_eq!(editions.len(), 1, "Phase-1 edition must survive migration");
        assert_eq!(editions[0].title.as_deref(), Some("Phase1 Book"));

        // Step 5: Running migrate! again (idempotent) must not error.
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("second migrate run must be idempotent");
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

    // -----------------------------------------------------------------------
    // AC-52: unit tests for every previously-untested public function
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_find_work_by_ol_id_returns_none_when_absent() {
        let (pool, _tmp) = open_temp_db().await;
        let result = find_work_by_ol_id(&pool, "/works/OL_MISSING").await.unwrap();
        assert!(result.is_none(), "should return None when OL ID is not in works table");
    }

    #[tokio::test]
    async fn test_find_work_by_ol_id_returns_id_when_present() {
        let (pool, _tmp) = open_temp_db().await;
        let work_id = insert_work(&pool, "Test Work", "Test Author").await.unwrap();
        // Set the OL ID on this work row.
        sqlx::query("UPDATE works SET openlibrary_work_id = ? WHERE id = ?")
            .bind("/works/OL123W")
            .bind(work_id)
            .execute(&pool)
            .await
            .unwrap();

        let found = find_work_by_ol_id(&pool, "/works/OL123W").await.unwrap();
        assert_eq!(found, Some(work_id));
    }

    #[tokio::test]
    async fn test_set_edition_work_id_updates_row() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Work ID Test".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/work_id_test.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap();

        // work_id should be NULL initially
        let before = get_edition(&pool, edition_id).await.unwrap().unwrap();
        assert!(before.work_id.is_none());

        let work_id = insert_work(&pool, "Work ID Test", "Author").await.unwrap();
        set_edition_work_id(&pool, edition_id, work_id).await.unwrap();

        let after = get_edition(&pool, edition_id).await.unwrap().unwrap();
        assert_eq!(after.work_id, Some(work_id));
    }

    #[tokio::test]
    async fn test_editions_needing_enrichment_returns_unattempted_only() {
        let (pool, _tmp) = open_temp_db().await;

        let meta_a = EpubMeta {
            title: Some("Needs Enrichment".to_string()),
            authors: Some("Author A".to_string()),
            source_path: "/tmp/needs_enrich.epub".to_string(),
            ..Default::default()
        };
        let meta_b = EpubMeta {
            title: Some("Already Enriched".to_string()),
            authors: Some("Author B".to_string()),
            source_path: "/tmp/already_enriched.epub".to_string(),
            ..Default::default()
        };

        let id_a = upsert_edition(&pool, &meta_a).await.unwrap();
        let id_b = upsert_edition(&pool, &meta_b).await.unwrap();

        // Mark edition B as already attempted
        let update = EnrichmentUpdate {
            enrichment_attempted: 1,
            ..Default::default()
        };
        apply_enrichment(&pool, id_b, &update).await.unwrap();

        let needing = editions_needing_enrichment(&pool).await.unwrap();
        let ids: Vec<i64> = needing.iter().map(|r| r.id).collect();
        assert!(ids.contains(&id_a), "unattempted edition must be returned");
        assert!(!ids.contains(&id_b), "already-attempted edition must be excluded");
    }

    #[tokio::test]
    async fn test_list_editions_returns_all_ordered_by_id() {
        let (pool, _tmp) = open_temp_db().await;

        for i in 0..3u8 {
            let meta = EpubMeta {
                title: Some(format!("Book {i}")),
                authors: Some("Author".to_string()),
                source_path: format!("/tmp/list_test_{i}.epub"),
                ..Default::default()
            };
            upsert_edition(&pool, &meta).await.unwrap();
        }

        let editions = list_editions(&pool).await.unwrap();
        assert_eq!(editions.len(), 3);
        // Must be ordered by id ascending
        assert!(editions[0].id < editions[1].id);
        assert!(editions[1].id < editions[2].id);
    }

    #[tokio::test]
    async fn test_get_edition_returns_none_for_missing_id() {
        let (pool, _tmp) = open_temp_db().await;
        let result = get_edition(&pool, 9999).await.unwrap();
        assert!(result.is_none(), "should return None for non-existent id");
    }

    #[tokio::test]
    async fn test_get_edition_returns_row_for_existing_id() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Get Edition Test".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/get_edition_test.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap();
        let row = get_edition(&pool, edition_id).await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().title.as_deref(), Some("Get Edition Test"));
    }

    #[tokio::test]
    async fn test_all_editions_for_dedup_returns_only_work_id_set_no_isbn() {
        let (pool, _tmp) = open_temp_db().await;

        // Edition with work_id and no ISBN — should appear in results
        let meta_a = EpubMeta {
            title: Some("Dedup Candidate".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/dedup_a.epub".to_string(),
            ..Default::default()
        };
        let id_a = upsert_edition(&pool, &meta_a).await.unwrap();
        let work_id = insert_work(&pool, "Dedup Candidate", "Author").await.unwrap();
        set_edition_work_id(&pool, id_a, work_id).await.unwrap();

        // Edition with work_id AND isbn — must be excluded
        let meta_b = EpubMeta {
            title: Some("Has ISBN".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9781000000001".to_string()),
            source_path: "/tmp/dedup_b.epub".to_string(),
            ..Default::default()
        };
        let id_b = upsert_edition(&pool, &meta_b).await.unwrap();
        let work_id_b = insert_work(&pool, "Has ISBN", "Author").await.unwrap();
        set_edition_work_id(&pool, id_b, work_id_b).await.unwrap();

        // Edition with no work_id — must be excluded
        let meta_c = EpubMeta {
            title: Some("No Work".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/dedup_c.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta_c).await.unwrap();

        let dedup_rows = all_editions_for_dedup(&pool).await.unwrap();
        let ids: Vec<i64> = dedup_rows.iter().map(|r| r.id).collect();
        assert!(ids.contains(&id_a), "edition with work_id and no isbn must be returned");
        assert!(!ids.contains(&id_b), "edition with isbn must be excluded");
    }

    #[tokio::test]
    async fn test_update_work_ol_id_sets_id_when_no_conflict() {
        let (pool, _tmp) = open_temp_db().await;
        let work_id = insert_work(&pool, "Some Work", "Some Author").await.unwrap();

        update_work_ol_id(&pool, work_id, "/works/OL_NEW").await.unwrap();

        let found = find_work_by_ol_id(&pool, "/works/OL_NEW").await.unwrap();
        assert_eq!(found, Some(work_id));
    }

    // -----------------------------------------------------------------------
    // Category 7: library_stats tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_library_stats_known_counts() {
        let (pool, _tmp) = open_temp_db().await;

        // Insert 3 owned editions: 2 with ISBN, 1 in a series, 1 enriched.
        let meta1 = EpubMeta {
            title: Some("Book A".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9780000000001".to_string()),
            source_path: "/tmp/stats_a.epub".to_string(),
            series_name: Some("Series".to_string()),
            ..Default::default()
        };
        let id1 = upsert_edition(&pool, &meta1).await.unwrap();
        apply_enrichment(
            &pool,
            id1,
            &EnrichmentUpdate {
                enriched_at: Some("2026-01-01T00:00:00Z".to_string()),
                enrichment_attempted: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let meta2 = EpubMeta {
            title: Some("Book B".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9780000000002".to_string()),
            source_path: "/tmp/stats_b.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta2).await.unwrap();

        let meta3 = EpubMeta {
            title: Some("Book C".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/stats_c.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta3).await.unwrap();

        // Insert want list entries from different sources.
        insert_want(&pool, "Want A", None, Some("9781000000001"), "goodreads_csv", None, 5, None)
            .await
            .unwrap();
        insert_want(&pool, "Want B", None, None, "manual", None, 5, None)
            .await
            .unwrap();
        insert_want(&pool, "Want C", None, None, "openlibrary", None, 5, None)
            .await
            .unwrap();
        insert_want(&pool, "Want D", None, None, "text_file", None, 5, None)
            .await
            .unwrap();

        let stats = library_stats(&pool).await.unwrap();

        assert_eq!(stats.books_in_library, 3, "3 owned books");
        assert_eq!(stats.with_isbn, 2, "2 books with ISBN");
        assert_eq!(stats.in_a_series, 1, "1 book in a series");
        assert_eq!(stats.enriched, 1, "1 enriched book");
        assert_eq!(stats.want_total, 4, "4 want list entries");
        assert_eq!(stats.want_with_isbn, 1, "1 want entry with ISBN");
        assert_eq!(stats.want_by_goodreads_csv, 1);
        assert_eq!(stats.want_by_manual, 1);
        assert_eq!(stats.want_by_openlibrary, 1);
        assert_eq!(stats.want_by_text_file, 1);
        // Grab count: 4 want entries, none owned by ISBN match.
        assert_eq!(stats.grab_count, 4);
    }

    #[tokio::test]
    async fn test_delete_want_happy_path() {
        let (pool, _tmp) = open_temp_db().await;
        let id = insert_want(&pool, "Test Book", Some("Author"), None, "manual", None, 5, None)
            .await
            .unwrap();

        let deleted = delete_want(&pool, id).await.unwrap();
        assert!(deleted, "should return true when row is deleted");

        let row = get_want(&pool, id).await.unwrap();
        assert!(row.is_none(), "row should no longer exist after delete");
    }

    #[tokio::test]
    async fn test_delete_want_nonexistent_returns_false() {
        let (pool, _tmp) = open_temp_db().await;
        let deleted = delete_want(&pool, 99999).await.unwrap();
        assert!(!deleted, "should return false when id not found");
    }

    #[tokio::test]
    async fn test_delete_want_not_in_list_want_after_removal() {
        let (pool, _tmp) = open_temp_db().await;
        let id = insert_want(&pool, "Gone Book", Some("Author"), None, "manual", None, 5, None)
            .await
            .unwrap();

        delete_want(&pool, id).await.unwrap();

        let all = list_want(&pool, None).await.unwrap();
        assert!(
            !all.iter().any(|r| r.id == id),
            "deleted entry must not appear in list_want"
        );
    }

    #[tokio::test]
    async fn test_update_work_ol_id_merges_when_duplicate_ol_id() {
        let (pool, _tmp) = open_temp_db().await;

        // Two distinct work rows
        let work_a = insert_work(&pool, "Work A", "Author").await.unwrap();
        let work_b = insert_work(&pool, "Work B", "Author").await.unwrap();

        // Two editions, one per work
        let meta_a = EpubMeta {
            title: Some("Edition A".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/merge_a.epub".to_string(),
            ..Default::default()
        };
        let meta_b = EpubMeta {
            title: Some("Edition B".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/merge_b.epub".to_string(),
            ..Default::default()
        };
        let ed_a = upsert_edition(&pool, &meta_a).await.unwrap();
        let ed_b = upsert_edition(&pool, &meta_b).await.unwrap();
        set_edition_work_id(&pool, ed_a, work_a).await.unwrap();
        set_edition_work_id(&pool, ed_b, work_b).await.unwrap();

        let ol_id = "/works/OL_MERGE";
        // Set OL ID on first work — no conflict
        update_work_ol_id(&pool, work_a, ol_id).await.unwrap();
        // Set same OL ID on second work — merge triggered
        update_work_ol_id(&pool, work_b, ol_id).await.unwrap();

        // Both editions must share the same work_id
        let row_a = get_edition(&pool, ed_a).await.unwrap().unwrap();
        let row_b = get_edition(&pool, ed_b).await.unwrap().unwrap();
        assert_eq!(row_a.work_id, row_b.work_id, "editions must share work_id after merge");

        // Exactly one works row with this OL ID
        let surviving = find_work_by_ol_id(&pool, ol_id).await.unwrap();
        assert!(surviving.is_some());
    }
}
