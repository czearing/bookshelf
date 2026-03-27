use crate::epub::EpubMeta;
use crate::fuzzy::normalize_isbn;
use anyhow::Context;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::Row;
use std::collections::HashSet;
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
    pub want_by_author_follow: i64,
    pub want_by_series_fill: i64,
    // Grab list (approximate: ISBN exact match only — fuzzy/work-level not counted here)
    pub grab_count: i64,
    // Reading status
    pub status_read: i64,
    pub status_reading: i64,
    pub status_unread: i64,
    // Metadata completeness
    pub without_metadata: i64,
    pub not_yet_enriched: i64,
    // Language distribution (top 5 + "other")
    pub lang_counts: Vec<(String, i64)>,
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
    pub tagged_at: Option<String>,
    pub download_url: Option<String>,
    pub download_source: Option<String>,
    pub reading_status: Option<String>,
    pub reading_started_at: Option<String>,
    pub reading_finished_at: Option<String>,
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
    pub series_name: Option<String>,
    pub series_position: Option<String>,
    pub language: Option<String>,
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
        .foreign_keys(true)
        // WAL mode allows concurrent reads while the daemon writes — critical for
        // CLI commands running alongside a long-lived `bookshelf run` process.
        .journal_mode(SqliteJournalMode::Wal)
        // Rather than immediately returning "database is locked", wait up to 30 s
        // for the writer to finish. Covers the common case of a short enrichment
        // or organize write overlapping a `bookshelf list` invocation.
        .busy_timeout(std::time::Duration::from_secs(30));

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
/// Returns `(id, was_inserted)` — `was_inserted` is `true` only when a new row
/// was created, `false` when the path already existed in the DB.
pub async fn upsert_edition(pool: &DbPool, meta: &EpubMeta) -> anyhow::Result<(i64, bool)> {
    // Normalize ISBN on storage so comparisons always work (Issue 1).
    let normalized_isbn = meta.isbn.as_deref().map(normalize_isbn);
    // Try to insert; silently ignore conflicts on source_path UNIQUE constraint.
    let insert_result = sqlx::query(
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

    let was_inserted = insert_result.rows_affected() > 0;

    // Retrieve the id (works whether just inserted or already existed).
    let row = sqlx::query("SELECT id FROM editions WHERE source_path = ?")
        .bind(&meta.source_path)
        .fetch_one(pool)
        .await
        .context("upsert_edition SELECT id")?;

    Ok((row.try_get("id")?, was_inserted))
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

/// Set `tagged_at` on an `editions` row to the current UTC timestamp.
pub async fn set_tagged_at(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    sqlx::query("UPDATE editions SET tagged_at = ? WHERE id = ?")
        .bind(&now)
        .bind(id)
        .execute(pool)
        .await
        .context("set_tagged_at")?;
    Ok(())
}

/// Set `download_url` and `download_source` on an editions row identified
/// by `source_path`. Returns `Ok(true)` if a row was updated.
pub async fn set_download_provenance(
    pool: &DbPool,
    source_path: &str,
    download_url: &str,
    download_source: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE editions SET download_url = ?, download_source = ? WHERE source_path = ?",
    )
    .bind(download_url)
    .bind(download_source)
    .bind(source_path)
    .execute(pool)
    .await
    .context("set_download_provenance")?;
    Ok(result.rows_affected() > 0)
}

/// Backfill `title` and `authors` on a downloaded edition whose EPUB had no embedded
/// metadata. Only writes if the current values are NULL or empty. Matched by `source_path`.
pub async fn fill_edition_metadata_if_blank(
    pool: &DbPool,
    source_path: &str,
    title: &str,
    authors: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE editions SET \
         title   = CASE WHEN title   IS NULL OR title   = '' THEN ? ELSE title   END, \
         authors = CASE WHEN authors IS NULL OR authors = '' THEN ? ELSE authors END \
         WHERE source_path = ?",
    )
    .bind(title)
    .bind(authors)
    .bind(source_path)
    .execute(pool)
    .await
    .context("fill_edition_metadata_if_blank")?;
    Ok(result.rows_affected() > 0)
}

/// Always overwrite `title` and `authors` with the provided values. Used for
/// downloaded files where the source catalog title/author is authoritative and
/// the file's own embedded metadata (filename fallback) should not take precedence.
pub async fn overwrite_edition_metadata(
    pool: &DbPool,
    source_path: &str,
    title: &str,
    authors: &str,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE editions SET title = ?, authors = ? WHERE source_path = ?",
    )
    .bind(title)
    .bind(authors)
    .bind(source_path)
    .execute(pool)
    .await
    .context("overwrite_edition_metadata")?;
    Ok(result.rows_affected() > 0)
}

/// Return all editions where `owned = 1` and `enriched_at IS NOT NULL`, ordered by `id`.
pub async fn list_owned_enriched_editions(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query(
        "SELECT * FROM editions WHERE owned = 1 AND enriched_at IS NOT NULL ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .context("list_owned_enriched_editions")?;

    rows.into_iter().map(row_to_edition).collect()
}

/// Update `source_path` on an `editions` row.
/// Force-set the authors field, overwriting any existing value.
pub async fn force_set_authors(pool: &DbPool, id: i64, authors: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET authors = ? WHERE id = ?")
        .bind(authors)
        .bind(id)
        .execute(pool)
        .await
        .context("force_set_authors")?;
    Ok(())
}

/// Force-set the title field, overwriting any existing value.
pub async fn force_set_title(pool: &DbPool, id: i64, title: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET title = ? WHERE id = ?")
        .bind(title)
        .bind(id)
        .execute(pool)
        .await
        .context("force_set_title")?;
    Ok(())
}

/// Force-set the source_path field, overwriting any existing value.
pub async fn force_set_source_path(pool: &DbPool, id: i64, path: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET source_path = ? WHERE id = ?")
        .bind(path)
        .bind(id)
        .execute(pool)
        .await
        .context("force_set_source_path")?;
    Ok(())
}

/// Clear series_name and series_position (set to NULL).
pub async fn force_clear_series(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET series_name = NULL, series_position = NULL WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await
        .context("force_clear_series")?;
    Ok(())
}

/// Force-set the language field, overwriting any existing value.
pub async fn set_language(pool: &DbPool, id: i64, language: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET language = ? WHERE id = ?")
        .bind(language)
        .bind(id)
        .execute(pool)
        .await
        .context("set_language")?;
    Ok(())
}

pub async fn update_source_path(pool: &DbPool, id: i64, new_path: &str) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET source_path = ? WHERE id = ?")
        .bind(new_path)
        .bind(id)
        .execute(pool)
        .await
        .context("update_source_path")?;
    Ok(())
}

/// Return editions matching a list of IDs, ordered by id.
pub async fn get_editions_by_ids(pool: &DbPool, ids: &[i64]) -> anyhow::Result<Vec<EditionRow>> {
    if ids.is_empty() { return Ok(vec![]); }
    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let sql = format!("SELECT * FROM editions WHERE id IN ({}) ORDER BY id", placeholders);
    let mut q = sqlx::query(&sql);
    for id in ids { q = q.bind(*id); }
    let rows = q.fetch_all(pool).await.context("get_editions_by_ids")?;
    rows.into_iter().map(row_to_edition).collect()
}

/// Mark an edition as read (reading_status = 'read', sets reading_finished_at).
pub async fn mark_read(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    sqlx::query("UPDATE editions SET reading_status = 'read', reading_finished_at = ? WHERE id = ?")
        .bind(&now).bind(id).execute(pool).await.context("mark_read")?;
    Ok(())
}

/// Mark an edition as currently reading. Sets reading_started_at if not already set.
pub async fn mark_reading(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    sqlx::query("UPDATE editions SET reading_status = 'reading', reading_started_at = COALESCE(reading_started_at, ?), reading_finished_at = NULL WHERE id = ?")
        .bind(&now).bind(id).execute(pool).await.context("mark_reading")?;
    Ok(())
}

/// Clear reading status from an edition.
pub async fn mark_unread(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET reading_status = NULL, reading_started_at = NULL, reading_finished_at = NULL WHERE id = ?")
        .bind(id).execute(pool).await.context("mark_unread")?;
    Ok(())
}

/// Return one edition by `source_path`, or `None` if not found.
pub async fn get_edition_by_source_path(
    pool: &DbPool,
    path: &str,
) -> anyhow::Result<Option<EditionRow>> {
    let row = sqlx::query("SELECT * FROM editions WHERE source_path = ? LIMIT 1")
        .bind(path)
        .fetch_optional(pool)
        .await
        .context("get_edition_by_source_path")?;

    row.map(row_to_edition).transpose()
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
            title           = COALESCE(title,           ?),
            authors         = COALESCE(authors,         ?),
            publisher       = COALESCE(publisher,       ?),
            publish_date    = COALESCE(publish_date,    ?),
            description     = COALESCE(description,     ?),
            isbn            = COALESCE(isbn,            ?),
            series_name     = COALESCE(series_name,     ?),
            series_position = COALESCE(series_position, ?),
            language        = COALESCE(language,        ?),
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
    .bind(&update.series_name)
    .bind(&update.series_position)
    .bind(&update.language)
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

/// Return one edition by `id`, or `None` if not found. Alias kept separate
/// so call sites that were added for `--id` enrichment remain readable.
pub async fn get_edition_by_id(pool: &DbPool, id: i64) -> anyhow::Result<Option<EditionRow>> {
    get_edition(pool, id).await
}

/// Return editions matching optional filters, ordered by `sort`.
///
/// Filters:
/// - `language`: exact match on the `language` column
/// - `series`: LIKE `%pattern%` on `series_name`
/// - `author`: LIKE `%pattern%` on `authors`
/// - `tag`: post-fetch filter on the `tags` table (or ignored if no tags table)
/// - `not_enriched`: only rows where `enriched_at IS NULL`
/// - `status`: filter by reading_status; "unread" means WHERE reading_status IS NULL;
///   other values are exact matches on reading_status
///
/// `sort` is one of `"title"`, `"author"`, `"series"` (default: `"title"`).
pub async fn list_editions_filtered(
    pool: &DbPool,
    language: Option<&str>,
    series: Option<&str>,
    author: Option<&str>,
    tag: Option<&str>,
    not_enriched: bool,
    sort: &str,
    status: Option<&str>,
) -> anyhow::Result<Vec<EditionRow>> {
    let order_clause = match sort {
        "author" => "authors COLLATE NOCASE ASC",
        "series" => "series_name COLLATE NOCASE ASC, CAST(series_position AS REAL) ASC",
        _ => "title COLLATE NOCASE ASC",
    };

    // Build WHERE conditions dynamically. We use nullable binds: a NULL bind
    // means "no filter" for that column.
    let lang_pat = language;
    let series_pat = series.map(|s| format!("%{}%", s));
    let author_pat = author.map(|a| format!("%{}%", a));

    // Status filter: "unread" → IS NULL; other values → exact match
    let status_is_unread = status == Some("unread");
    let status_exact = if status_is_unread { None } else { status };

    let sql = format!(
        "SELECT * FROM editions \
         WHERE (? IS NULL OR language = ?) \
           AND (? IS NULL OR series_name LIKE ?) \
           AND (? IS NULL OR authors LIKE ?) \
           AND (? = 0 OR enriched_at IS NULL) \
           AND (? = 0 OR reading_status IS NULL) \
           AND (? IS NULL OR reading_status = ?) \
         ORDER BY {}",
        order_clause
    );

    let not_enriched_int: i64 = if not_enriched { 1 } else { 0 };
    let status_unread_int: i64 = if status_is_unread { 1 } else { 0 };

    let rows = sqlx::query(&sql)
        .bind(lang_pat)
        .bind(lang_pat)
        .bind(series_pat.as_deref())
        .bind(series_pat.as_deref())
        .bind(author_pat.as_deref())
        .bind(author_pat.as_deref())
        .bind(not_enriched_int)
        .bind(status_unread_int)
        .bind(status_exact)
        .bind(status_exact)
        .fetch_all(pool)
        .await
        .context("list_editions_filtered")?;

    let mut editions: Vec<EditionRow> = rows.into_iter().map(row_to_edition).collect::<anyhow::Result<Vec<_>>>()?;

    // Apply tag filter in Rust (post-fetch) — simpler than a dynamic subquery.
    if let Some(tag_str) = tag {
        let tag_lower = tag_str.to_lowercase();
        // Check if there is a tags table by looking for any edition's tags.
        // We use a best-effort query; if the table doesn't exist this returns empty.
        let tagged_ids: HashSet<i64> = sqlx::query(
            "SELECT edition_id FROM tags WHERE LOWER(name) = ?",
        )
        .bind(&tag_lower)
        .fetch_all(pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| r.try_get::<i64, _>("edition_id").ok())
        .collect();

        editions.retain(|e| tagged_ids.contains(&e.id));
    }

    Ok(editions)
}

/// Return all editions that have a `work_id` set (used during dedup scan).
///
/// Note: includes both ISBN and no-ISBN editions so that a newly-scanned
/// no-ISBN edition can fuzzy-match against an existing ISBN edition and share
/// its work_id rather than creating a duplicate works row (GAP-20 fix).
pub async fn all_editions_for_dedup(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query(
        "SELECT * FROM editions WHERE work_id IS NOT NULL",
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
    pub download_attempts: i64,
    pub last_attempted_at: Option<String>,
}

/// All recognised `want_list.source` values.  This is the single source of
/// truth — `main.rs` imports this constant rather than duplicating it.
pub const VALID_SOURCES: &[&str] = &[
    "goodreads_csv",
    "openlibrary",
    "manual",
    "text_file",
    "author_follow",
    "series_fill",
    "series_gap",
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
    if title.is_empty() {
        anyhow::bail!("insert_want: title must not be empty");
    }
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
/// Returns `Err` if `source_filter` is `Some` but not a recognised valid source.
///
/// `max_attempts`: when `Some(n)`, only returns entries with `download_attempts < n`.
/// Pass `None` to return all entries regardless of attempt count.
pub async fn list_want(
    pool: &DbPool,
    source_filter: Option<&str>,
    max_attempts: Option<i64>,
) -> anyhow::Result<Vec<WantRow>> {
    if let Some(src) = source_filter {
        if !VALID_SOURCES.contains(&src) {
            anyhow::bail!(
                "list_want: invalid source {:?}; must be one of {:?}",
                src,
                VALID_SOURCES
            );
        }
    }

    let rows = match (source_filter, max_attempts) {
        (Some(src), Some(max)) => {
            sqlx::query("SELECT * FROM want_list WHERE source = ? AND download_attempts < ? ORDER BY id")
                .bind(src).bind(max).fetch_all(pool).await.context("list_want (filtered+attempts)")?
        }
        (Some(src), None) => {
            sqlx::query("SELECT * FROM want_list WHERE source = ? ORDER BY id")
                .bind(src).fetch_all(pool).await.context("list_want (filtered)")?
        }
        (None, Some(max)) => {
            sqlx::query("SELECT * FROM want_list WHERE download_attempts < ? ORDER BY id")
                .bind(max).fetch_all(pool).await.context("list_want (attempts)")?
        }
        (None, None) => {
            sqlx::query("SELECT * FROM want_list ORDER BY id")
                .fetch_all(pool).await.context("list_want")?
        }
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

/// Return all `want_list` rows eligible for enrichment: `isbn13 IS NULL` and
/// `author IS NOT NULL`. Rows without an author cannot be looked up and are
/// excluded so they do not inflate the eligible count.
pub async fn want_entries_needing_enrichment(pool: &DbPool) -> anyhow::Result<Vec<WantRow>> {
    let rows = sqlx::query("SELECT * FROM want_list WHERE isbn13 IS NULL AND author IS NOT NULL")
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

/// Increment `download_attempts` and set `last_attempted_at` to now for a want entry.
pub async fn increment_want_attempts(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    sqlx::query(
        "UPDATE want_list SET download_attempts = download_attempts + 1, last_attempted_at = ? WHERE id = ?",
    )
    .bind(&now)
    .bind(id)
    .execute(pool)
    .await
    .context("increment_want_attempts")?;
    Ok(())
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
        download_attempts: row.try_get("download_attempts").unwrap_or(0),
        last_attempted_at: row.try_get("last_attempted_at").unwrap_or(None),
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
    let want_by_author_follow =
        count!("SELECT COUNT(*) FROM want_list WHERE source = 'author_follow'");
    let want_by_series_fill =
        count!("SELECT COUNT(*) FROM want_list WHERE source = 'series_fill'");

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

    let status_read =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND reading_status = 'read'");
    let status_reading =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND reading_status = 'reading'");
    let status_unread =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND reading_status IS NULL");
    let without_metadata =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND (title IS NULL OR title = '') AND (authors IS NULL OR authors = '')");
    let not_yet_enriched =
        count!("SELECT COUNT(*) FROM editions WHERE owned = 1 AND enrichment_attempted = 0 AND enriched_at IS NULL");

    // Language distribution
    let lang_rows = sqlx::query(
        "SELECT language, COUNT(*) as cnt \
         FROM editions \
         WHERE owned = 1 AND language IS NOT NULL AND language != '' \
         GROUP BY language \
         ORDER BY cnt DESC \
         LIMIT 6",
    )
    .fetch_all(&mut *tx)
    .await
    .context("library_stats lang_counts")?;

    let mut lang_raw: Vec<(String, i64)> = lang_rows
        .iter()
        .map(|r| {
            let lang: String = r.try_get("language").unwrap_or_default();
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            (lang, cnt)
        })
        .collect();

    // Take top 5, collapse the rest into "other"
    let lang_counts = if lang_raw.len() <= 5 {
        lang_raw
    } else {
        let other_sum: i64 = lang_raw.drain(5..).map(|(_, c)| c).sum();
        let mut out = lang_raw;
        if other_sum > 0 {
            out.push(("other".to_string(), other_sum));
        }
        out
    };

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
        want_by_author_follow,
        want_by_series_fill,
        grab_count,
        status_read,
        status_reading,
        status_unread,
        without_metadata,
        not_yet_enriched,
        lang_counts,
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
        tagged_at: row.try_get("tagged_at")?,
        download_url: row.try_get("download_url")?,
        download_source: row.try_get("download_source")?,
        reading_status: row.try_get("reading_status")?,
        reading_started_at: row.try_get("reading_started_at")?,
        reading_finished_at: row.try_get("reading_finished_at")?,
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

/// Return the `followed_authors` row with `name = ?` (case-insensitive), or `None`.
pub async fn find_followed_author_by_name(
    pool: &DbPool,
    name: &str,
) -> anyhow::Result<Option<FollowedAuthorRow>> {
    let row = sqlx::query(
        "SELECT * FROM followed_authors WHERE LOWER(name) = LOWER(?) LIMIT 1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .context("find_followed_author_by_name")?;

    row.map(row_to_followed_author).transpose()
}

/// Delete the `followed_authors` row with `name = ?`.
/// Returns `true` if a row was deleted, `false` if not found.
pub async fn delete_followed_author(pool: &DbPool, name: &str) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM followed_authors WHERE LOWER(name) = LOWER(?)")
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

/// Returns all owned editions that have both series_name and a numeric series_position.
pub async fn editions_in_series(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let rows = sqlx::query(
        "SELECT * FROM editions \
         WHERE owned = 1 \
           AND series_name IS NOT NULL AND series_name != '' \
           AND series_position IS NOT NULL AND series_position != '' \
         ORDER BY series_name, CAST(series_position AS REAL)",
    )
    .fetch_all(pool)
    .await
    .context("editions_in_series")?;
    rows.into_iter().map(row_to_edition).collect()
}

/// Full-text search across title, authors, and series_name in owned editions.
pub async fn search_editions(pool: &DbPool, query: &str) -> anyhow::Result<Vec<EditionRow>> {
    let pattern = format!("%{}%", query);
    let rows = sqlx::query(
        "SELECT * FROM editions WHERE owned = 1 AND (\
            title   LIKE ? OR \
            authors LIKE ? OR \
            series_name LIKE ?\
         ) ORDER BY title COLLATE NOCASE ASC",
    )
    .bind(&pattern)
    .bind(&pattern)
    .bind(&pattern)
    .fetch_all(pool)
    .await
    .context("search_editions")?;
    rows.into_iter().map(row_to_edition).collect()
}

/// Find groups of owned editions with the same normalized (title, authors).
/// Returns only groups with 2+ entries.
pub async fn find_duplicate_editions(pool: &DbPool) -> anyhow::Result<Vec<Vec<EditionRow>>> {
    use std::collections::HashMap;

    let all = sqlx::query(
        "SELECT * FROM editions \
         WHERE owned = 1 \
           AND title IS NOT NULL AND title != '' \
           AND authors IS NOT NULL AND authors != '' \
         ORDER BY title COLLATE NOCASE, authors COLLATE NOCASE, id",
    )
    .fetch_all(pool)
    .await
    .context("find_duplicate_editions")?;

    let mut groups: HashMap<(String, String), Vec<EditionRow>> = HashMap::new();
    for row in all.into_iter().map(row_to_edition).collect::<anyhow::Result<Vec<_>>>()? {
        let key = (
            row.title.as_deref().unwrap_or("").to_lowercase().trim().to_string(),
            row.authors.as_deref().unwrap_or("").to_lowercase().trim().to_string(),
        );
        groups.entry(key).or_default().push(row);
    }

    let mut result: Vec<Vec<EditionRow>> = groups
        .into_values()
        .filter(|g| g.len() >= 2)
        .collect();
    result.sort_by(|a, b| a[0].title.cmp(&b[0].title));
    Ok(result)
}

/// Mark an edition as having had enrichment attempted (without changing other fields).
/// Used for entries that are permanently unenrichable (no title, no ISBN) so they
/// don't clog the enrichment queue on every pipeline run.
pub async fn mark_enrichment_attempted(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE editions SET enrichment_attempted = 1 WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await
        .context("mark_enrichment_attempted")?;
    Ok(())
}

/// Delete one edition by `id`.
pub async fn delete_edition(pool: &DbPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM editions WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await
        .context("delete_edition")?;
    Ok(())
}

/// Create a clean, single-file backup of the database using `VACUUM INTO`.
///
/// Works correctly with WAL mode. Creates any missing parent directories.
/// The destination file must not already exist (SQLite will return an error
/// if it does — callers should supply a timestamped or versioned path).
pub async fn backup(pool: &DbPool, dest: &Path) -> anyhow::Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create backup directory: {}", parent.display()))?;
    }
    // VACUUM INTO path — escape single quotes in case the path contains them.
    let dest_str = dest.to_string_lossy();
    let escaped = dest_str.replace('\'', "''");
    sqlx::query(&format!("VACUUM INTO '{escaped}'"))
        .execute(pool)
        .await
        .with_context(|| format!("VACUUM INTO '{}' failed", dest.display()))?;
    Ok(())
}

/// Return all owned editions whose `source_path` file does not exist on disk.
/// Used by the recovery pipeline to detect books lost to file moves or the
/// dedup path-normalisation bug.
pub async fn list_missing_file_editions(pool: &DbPool) -> anyhow::Result<Vec<EditionRow>> {
    let all = list_editions(pool).await?;
    Ok(all
        .into_iter()
        .filter(|e| !std::path::Path::new(&e.source_path).exists())
        .collect())
}

/// Run `PRAGMA integrity_check`. Returns `true` if the database reports "ok",
/// `false` if any corruption is detected.
pub async fn integrity_check(pool: &DbPool) -> anyhow::Result<bool> {
    let result: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(pool)
        .await
        .context("integrity_check")?;
    Ok(result == "ok")
}

/// Check if a want already exists for the given title and author (case-insensitive).
/// Used to skip duplicate entries before inserting.
pub async fn want_exists_by_title_author(
    pool: &DbPool,
    title: &str,
    author: &str,
) -> anyhow::Result<bool> {
    let row = sqlx::query(
        "SELECT 1 FROM want_list WHERE LOWER(title) = LOWER(?) AND LOWER(COALESCE(author,'')) = LOWER(?) LIMIT 1",
    )
    .bind(title)
    .bind(author)
    .fetch_optional(pool)
    .await
    .context("want_exists_by_title_author")?;
    Ok(row.is_some())
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
        let id1 = upsert_edition(&pool, &meta).await.unwrap().0;
        let id2 = upsert_edition(&pool, &meta).await.unwrap().0;
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
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;
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
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;
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
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;

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

        let id_a = upsert_edition(&pool, &meta_a).await.unwrap().0;
        let id_b = upsert_edition(&pool, &meta_b).await.unwrap().0;

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
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;
        let row = get_edition(&pool, edition_id).await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().title.as_deref(), Some("Get Edition Test"));
    }

    #[tokio::test]
    async fn test_all_editions_for_dedup_returns_all_work_id_set_editions() {
        // GAP-20 fix: both ISBN and no-ISBN editions with work_id must be returned
        // so that a newly-scanned no-ISBN edition can fuzzy-match against an ISBN edition.
        let (pool, _tmp) = open_temp_db().await;

        // Edition with work_id and no ISBN — must be returned.
        let meta_a = EpubMeta {
            title: Some("Dedup Candidate".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/dedup_a.epub".to_string(),
            ..Default::default()
        };
        let id_a = upsert_edition(&pool, &meta_a).await.unwrap().0;
        let work_id = insert_work(&pool, "Dedup Candidate", "Author").await.unwrap();
        set_edition_work_id(&pool, id_a, work_id).await.unwrap();

        // Edition with work_id AND isbn — must ALSO be returned (GAP-20 fix).
        let meta_b = EpubMeta {
            title: Some("Has ISBN".to_string()),
            authors: Some("Author".to_string()),
            isbn: Some("9781000000001".to_string()),
            source_path: "/tmp/dedup_b.epub".to_string(),
            ..Default::default()
        };
        let id_b = upsert_edition(&pool, &meta_b).await.unwrap().0;
        let work_id_b = insert_work(&pool, "Has ISBN", "Author").await.unwrap();
        set_edition_work_id(&pool, id_b, work_id_b).await.unwrap();

        // Edition with no work_id — must be excluded.
        let meta_c = EpubMeta {
            title: Some("No Work".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/dedup_c.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta_c).await.unwrap();

        let dedup_rows = all_editions_for_dedup(&pool).await.unwrap();
        let ids: Vec<i64> = dedup_rows.iter().map(|r| r.id).collect();
        assert!(ids.contains(&id_a), "edition with work_id (no isbn) must be returned");
        assert!(ids.contains(&id_b), "edition with work_id AND isbn must also be returned (GAP-20)");
        // Edition C has no work_id — must be absent.
        assert_eq!(dedup_rows.len(), 2, "only editions with work_id must be returned");
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
        let id1 = upsert_edition(&pool, &meta1).await.unwrap().0;
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
        insert_want(&pool, "Want E", None, None, "author_follow", None, 5, None)
            .await
            .unwrap();
        insert_want(&pool, "Want F", None, None, "series_fill", None, 7, None)
            .await
            .unwrap();

        let stats = library_stats(&pool).await.unwrap();

        assert_eq!(stats.books_in_library, 3, "3 owned books");
        assert_eq!(stats.with_isbn, 2, "2 books with ISBN");
        assert_eq!(stats.in_a_series, 1, "1 book in a series");
        assert_eq!(stats.enriched, 1, "1 enriched book");
        assert_eq!(stats.want_total, 6, "6 want list entries");
        assert_eq!(stats.want_with_isbn, 1, "1 want entry with ISBN");
        assert_eq!(stats.want_by_goodreads_csv, 1);
        assert_eq!(stats.want_by_manual, 1);
        assert_eq!(stats.want_by_openlibrary, 1);
        assert_eq!(stats.want_by_text_file, 1);
        assert_eq!(stats.want_by_author_follow, 1);
        assert_eq!(stats.want_by_series_fill, 1);
        // Grab count: 6 want entries, none owned by ISBN match.
        assert_eq!(stats.grab_count, 6);
    }

    #[tokio::test]
    async fn test_library_stats_empty_db_all_zeros() {
        let (pool, _tmp) = open_temp_db().await;
        let stats = library_stats(&pool).await.unwrap();
        assert_eq!(stats.books_in_library, 0);
        assert_eq!(stats.with_isbn, 0);
        assert_eq!(stats.in_a_series, 0);
        assert_eq!(stats.enriched, 0);
        assert_eq!(stats.want_total, 0);
        assert_eq!(stats.want_with_isbn, 0);
        assert_eq!(stats.want_by_goodreads_csv, 0);
        assert_eq!(stats.want_by_manual, 0);
        assert_eq!(stats.want_by_openlibrary, 0);
        assert_eq!(stats.want_by_text_file, 0);
        assert_eq!(stats.want_by_author_follow, 0);
        assert_eq!(stats.want_by_series_fill, 0);
        assert_eq!(stats.grab_count, 0);
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

        let all = list_want(&pool, None, None).await.unwrap();
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
        let ed_a = upsert_edition(&pool, &meta_a).await.unwrap().0;
        let ed_b = upsert_edition(&pool, &meta_b).await.unwrap().0;
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

        // The superseded works row (work_b) must no longer exist.
        let deleted_work = sqlx::query("SELECT id FROM works WHERE id = ?")
            .bind(work_b)
            .fetch_optional(&pool)
            .await
            .unwrap();
        assert!(
            deleted_work.is_none(),
            "superseded works row must be deleted after merge"
        );

        // The surviving work must be work_a (the first one we assigned the OL ID to).
        assert_eq!(
            surviving.unwrap(),
            work_a,
            "surviving work_id must be the first one (work_a)"
        );
    }

    // -----------------------------------------------------------------------
    // GAP-01: apply_enrichment COALESCE must NOT overwrite pre-populated columns
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_apply_enrichment_does_not_overwrite_existing_columns() {
        let (pool, _tmp) = open_temp_db().await;
        // Insert an edition that already has a title, authors, and publisher.
        let meta = EpubMeta {
            title: Some("Original Title".to_string()),
            authors: Some("Original Author".to_string()),
            isbn: Some("9781234500001".to_string()),
            source_path: "/tmp/coalesce_test.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;

        // Set publisher directly so it is non-NULL before enrichment.
        sqlx::query("UPDATE editions SET publisher = 'Original Publisher' WHERE id = ?")
            .bind(edition_id)
            .execute(&pool)
            .await
            .unwrap();

        // Enrichment update that provides different values for all pre-populated fields.
        let update = EnrichmentUpdate {
            title: Some("Enriched Title".to_string()),
            authors: Some("Enriched Author".to_string()),
            publisher: Some("Enriched Publisher".to_string()),
            enriched_at: Some("2026-01-01T00:00:00Z".to_string()),
            enrichment_attempted: 1,
            ..Default::default()
        };
        apply_enrichment(&pool, edition_id, &update).await.unwrap();

        let row = get_edition(&pool, edition_id).await.unwrap().unwrap();
        // COALESCE(existing, new) — existing wins.
        assert_eq!(
            row.title.as_deref(),
            Some("Original Title"),
            "apply_enrichment must NOT overwrite an existing title"
        );
        assert_eq!(
            row.authors.as_deref(),
            Some("Original Author"),
            "apply_enrichment must NOT overwrite existing authors"
        );
        assert_eq!(
            row.publisher.as_deref(),
            Some("Original Publisher"),
            "apply_enrichment must NOT overwrite an existing publisher"
        );
        // enriched_at and enrichment_attempted are always set unconditionally.
        assert_eq!(row.enriched_at.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(row.enrichment_attempted, 1);
    }

    #[tokio::test]
    async fn test_apply_enrichment_fills_null_columns() {
        let (pool, _tmp) = open_temp_db().await;
        // Edition with no publisher or description.
        let meta = EpubMeta {
            title: Some("Sparse Book".to_string()),
            source_path: "/tmp/sparse.epub".to_string(),
            ..Default::default()
        };
        let edition_id = upsert_edition(&pool, &meta).await.unwrap().0;

        let update = EnrichmentUpdate {
            publisher: Some("New Publisher".to_string()),
            description: Some("A great description".to_string()),
            enrichment_attempted: 1,
            ..Default::default()
        };
        apply_enrichment(&pool, edition_id, &update).await.unwrap();

        let row = get_edition(&pool, edition_id).await.unwrap().unwrap();
        // NULL columns must be filled in.
        assert_eq!(
            row.publisher.as_deref(),
            Some("New Publisher"),
            "apply_enrichment must fill NULL publisher"
        );
        assert_eq!(
            row.description.as_deref(),
            Some("A great description"),
            "apply_enrichment must fill NULL description"
        );
    }

    // -----------------------------------------------------------------------
    // GAP-22: insert_want must reject empty-string title
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_insert_want_rejects_empty_title() {
        let (pool, _tmp) = open_temp_db().await;
        let result = insert_want(&pool, "", None, None, "manual", None, 5, None).await;
        assert!(result.is_err(), "insert_want must return Err for empty title");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("insert_want: title must not be empty"),
            "error message must say title must not be empty, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Phase-4 Order-1: set_tagged_at, list_owned_enriched_editions,
    // update_source_path, get_edition_by_source_path
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_set_tagged_at_writes_timestamp() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Tag Test".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/tag_test.epub".to_string(),
            ..Default::default()
        };
        let id = upsert_edition(&pool, &meta).await.unwrap().0;

        // tagged_at must be NULL before calling set_tagged_at.
        let before = get_edition(&pool, id).await.unwrap().unwrap();
        assert!(before.tagged_at.is_none(), "tagged_at must start as NULL");

        set_tagged_at(&pool, id).await.unwrap();

        let after = get_edition(&pool, id).await.unwrap().unwrap();
        let ts = after.tagged_at.expect("tagged_at must be set after set_tagged_at");
        // Must match %Y-%m-%dT%H:%M:%SZ format (20 chars, letter T at index 10, letter Z at end).
        assert_eq!(ts.len(), 20, "timestamp must be 20 chars, got: {ts}");
        assert_eq!(&ts[10..11], "T", "timestamp must contain T separator, got: {ts}");
        assert!(ts.ends_with('Z'), "timestamp must end with Z, got: {ts}");
    }

    #[tokio::test]
    async fn test_list_owned_enriched_editions_filters_correctly() {
        let (pool, _tmp) = open_temp_db().await;

        // Edition A: owned=1, enriched — must be returned.
        let meta_a = EpubMeta {
            title: Some("Owned Enriched".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/owned_enriched.epub".to_string(),
            ..Default::default()
        };
        let id_a = upsert_edition(&pool, &meta_a).await.unwrap().0;
        apply_enrichment(
            &pool,
            id_a,
            &EnrichmentUpdate {
                enriched_at: Some("2026-01-01T00:00:00Z".to_string()),
                enrichment_attempted: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Edition B: owned=1, NOT enriched — must be excluded.
        let meta_b = EpubMeta {
            title: Some("Owned Not Enriched".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/owned_not_enriched.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta_b).await.unwrap();

        // Edition C: owned=0, enriched — must be excluded.
        let meta_c = EpubMeta {
            title: Some("Unowned Enriched".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/unowned_enriched.epub".to_string(),
            ..Default::default()
        };
        let id_c = upsert_edition(&pool, &meta_c).await.unwrap().0;
        // Set owned=0
        sqlx::query("UPDATE editions SET owned = 0 WHERE id = ?")
            .bind(id_c)
            .execute(&pool)
            .await
            .unwrap();
        apply_enrichment(
            &pool,
            id_c,
            &EnrichmentUpdate {
                enriched_at: Some("2026-01-01T00:00:00Z".to_string()),
                enrichment_attempted: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let rows = list_owned_enriched_editions(&pool).await.unwrap();
        assert_eq!(rows.len(), 1, "only one row must match owned=1 AND enriched_at IS NOT NULL");
        assert_eq!(rows[0].id, id_a);
    }

    #[tokio::test]
    async fn test_update_source_path_changes_stored_path() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Path Test".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/old_path.epub".to_string(),
            ..Default::default()
        };
        let id = upsert_edition(&pool, &meta).await.unwrap().0;

        update_source_path(&pool, id, "/tmp/new_path.epub").await.unwrap();

        let row = get_edition(&pool, id).await.unwrap().unwrap();
        assert_eq!(row.source_path, "/tmp/new_path.epub");
    }

    #[tokio::test]
    async fn test_get_edition_by_source_path_returns_row_and_none() {
        let (pool, _tmp) = open_temp_db().await;
        let meta = EpubMeta {
            title: Some("Source Path Test".to_string()),
            authors: Some("Author".to_string()),
            source_path: "/tmp/source_path_test.epub".to_string(),
            ..Default::default()
        };
        upsert_edition(&pool, &meta).await.unwrap();

        // Must find by exact path.
        let found = get_edition_by_source_path(&pool, "/tmp/source_path_test.epub")
            .await
            .unwrap();
        assert!(found.is_some(), "must return Some for existing source_path");
        assert_eq!(
            found.unwrap().title.as_deref(),
            Some("Source Path Test")
        );

        // Must return None for unknown path.
        let missing = get_edition_by_source_path(&pool, "/tmp/does_not_exist.epub")
            .await
            .unwrap();
        assert!(missing.is_none(), "must return None for unknown source_path");
    }
}
