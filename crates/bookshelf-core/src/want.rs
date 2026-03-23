use crate::{db, db::WantRow, enrich, fuzzy};
use anyhow::Context;
use serde::Deserialize;
use sqlx::Row;
use std::path::Path;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Summary returned by all import functions.
#[derive(Debug, Default)]
pub struct ImportSummary {
    pub imported: usize,
    pub skipped_owned: usize,
    pub skipped_rows: usize, // malformed/unparseable (CSV only)
}

/// Result of a manual `want add` operation.
#[derive(Debug, PartialEq)]
pub enum AddResult {
    Inserted,
    AlreadyOwned,
    AlreadyInWantList,
}

// ---------------------------------------------------------------------------
// Private OL reading list deserialization structures
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlReadingListResponse {
    #[serde(rename = "numFound")]
    num_found: u32,
    reading_log_entries: Vec<OlReadingEntry>,
}

#[derive(Debug, Deserialize)]
struct OlReadingEntry {
    work: OlWork,
}

#[derive(Debug, Deserialize)]
struct OlWork {
    title: String,
    key: String,
    #[serde(default)]
    author_names: Vec<String>,
}

// ---------------------------------------------------------------------------
// Private helpers — ownership check and within-want-list dedup
// ---------------------------------------------------------------------------

/// Return `true` if the book is already owned (editions table, owned = 1).
/// Mirrors AC-41 matching order.
async fn is_already_owned(
    pool: &db::DbPool,
    title: &str,
    author: Option<&str>,
    isbn13: Option<&str>,
) -> anyhow::Result<bool> {
    // Strategy 1: ISBN-13 exact match.
    if let Some(isbn) = isbn13 {
        let row = sqlx::query(
            "SELECT 1 FROM editions WHERE isbn = ? AND owned = 1 LIMIT 1",
        )
        .bind(isbn)
        .fetch_optional(pool)
        .await
        .context("is_already_owned isbn check")?;
        if row.is_some() {
            return Ok(true);
        }
    }

    // Strategy 2: Fuzzy title+author match (skip when author is NULL — AC-52).
    if let Some(a) = author {
        let owned_editions = sqlx::query("SELECT * FROM editions WHERE owned = 1")
            .fetch_all(pool)
            .await
            .context("is_already_owned fuzzy fetch")?;
        for row in owned_editions {
            let ed_title: Option<String> = row.try_get("title").unwrap_or(None);
            let ed_authors: Option<String> = row.try_get("authors").unwrap_or(None);
            if fuzzy::is_same_work(
                title,
                a,
                ed_title.as_deref().unwrap_or(""),
                ed_authors.as_deref().unwrap_or(""),
            ) {
                return Ok(true);
            }
        }
    }

    // Strategy 3: Work-level match.
    if let Some(isbn) = isbn13 {
        let work_row = sqlx::query(
            "SELECT work_id FROM editions WHERE isbn = ? AND work_id IS NOT NULL LIMIT 1",
        )
        .bind(isbn)
        .fetch_optional(pool)
        .await
        .context("is_already_owned work lookup")?;
        if let Some(wr) = work_row {
            let work_id: Option<i64> = wr.try_get("work_id").unwrap_or(None);
            if let Some(wid) = work_id {
                let owned = sqlx::query(
                    "SELECT 1 FROM editions WHERE work_id = ? AND owned = 1 LIMIT 1",
                )
                .bind(wid)
                .fetch_optional(pool)
                .await
                .context("is_already_owned work owned check")?;
                if owned.is_some() {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

/// Return the first existing `want_list` row that matches by ISBN or fuzzy.
/// Resolves Q3: title-only entries dedup by exact case-insensitive title.
async fn find_existing_want(
    pool: &db::DbPool,
    title: &str,
    author: Option<&str>,
    isbn13: Option<&str>,
) -> anyhow::Result<Option<WantRow>> {
    // 1. ISBN exact match.
    if let Some(isbn) = isbn13 {
        if let Some(row) = db::find_want_by_isbn13(pool, isbn).await? {
            return Ok(Some(row));
        }
    }

    // 2. Fuzzy match (only when incoming author is Some).
    if let Some(a) = author {
        let all = db::list_want(pool, None).await?;
        for row in all {
            if let Some(ref row_author) = row.author {
                if fuzzy::is_same_work(title, a, &row.title, row_author) {
                    return Ok(Some(row));
                }
            }
        }
    } else {
        // 3. Both incoming and existing have NULL author: exact case-insensitive title.
        let all = db::list_want(pool, None).await?;
        for row in all {
            if row.author.is_none() && row.title.to_lowercase() == title.to_lowercase() {
                return Ok(Some(row));
            }
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Goodreads CSV import
// ---------------------------------------------------------------------------

/// Import books from a Goodreads CSV export file.
///
/// Returns an `ImportSummary` and prints progress/summary to stdout/stderr.
pub async fn import_goodreads_csv(
    pool: &db::DbPool,
    path: &Path,
) -> anyhow::Result<ImportSummary> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("cannot open CSV file: {}", path.display()))?;

    // Verify required columns exist.
    let headers = reader.headers().context("reading CSV headers")?.clone();
    for required in &["Title", "Author", "ISBN13", "Book Id"] {
        if !headers.iter().any(|h| h == *required) {
            anyhow::bail!("CSV is missing required column: {required}");
        }
    }

    let mut summary = ImportSummary::default();
    let mut row_num: usize = 1; // header is row 0

    for record in reader.records() {
        row_num += 1;
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                eprintln!("WARNING: skipping row {row_num}: {e}");
                summary.skipped_rows += 1;
                continue;
            }
        };

        let title = match record.get(headers.iter().position(|h| h == "Title").unwrap()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => {
                eprintln!("WARNING: skipping row {row_num}: empty Title");
                summary.skipped_rows += 1;
                continue;
            }
        };
        let author = record
            .get(headers.iter().position(|h| h == "Author").unwrap())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let raw_isbn = record
            .get(headers.iter().position(|h| h == "ISBN13").unwrap())
            .unwrap_or("")
            .to_string();
        let isbn13 = strip_goodreads_isbn(&raw_isbn);
        let book_id = record
            .get(headers.iter().position(|h| h == "Book Id").unwrap())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        if is_already_owned(pool, &title, author.as_deref(), isbn13.as_deref()).await? {
            println!("Already owned: {title}");
            summary.skipped_owned += 1;
            continue;
        }

        if let Some(existing) =
            find_existing_want(pool, &title, author.as_deref(), isbn13.as_deref()).await?
        {
            db::update_want(
                pool,
                existing.id,
                &title,
                author.as_deref(),
                isbn13.as_deref(),
                book_id.as_deref(),
                existing.priority,
                existing.notes.as_deref(),
            )
            .await?;
            summary.imported += 1;
        } else {
            db::insert_want(
                pool,
                &title,
                author.as_deref(),
                isbn13.as_deref(),
                "goodreads_csv",
                book_id.as_deref(),
                5,
                None,
            )
            .await?;
            summary.imported += 1;
        }
    }

    println!(
        "Imported {} books from Goodreads CSV. Skipped {} already owned.",
        summary.imported, summary.skipped_owned
    );

    Ok(summary)
}

/// Strip Goodreads `="..."` ISBN wrapper. Returns `None` when empty after stripping.
fn strip_goodreads_isbn(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    // Goodreads wraps with =" and " so the cell value after CSV parsing is ="9780..."
    let s = if s.starts_with("=\"") && s.ends_with('"') {
        &s[2..s.len() - 1]
    } else {
        s
    };
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

// ---------------------------------------------------------------------------
// OpenLibrary reading list import
// ---------------------------------------------------------------------------

/// Import books from an OpenLibrary want-to-read reading list.
pub async fn import_openlibrary(
    pool: &db::DbPool,
    client: &reqwest::Client,
    username: &str,
    base_url: &str,
) -> anyhow::Result<ImportSummary> {
    let mut summary = ImportSummary::default();
    let mut page: u32 = 1;

    loop {
        let url = format!(
            "{base_url}/people/{username}/books/want-to-read.json?page={page}"
        );

        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("network error fetching {url}"))?;

        let status = resp.status();
        if status.as_u16() == 404 {
            anyhow::bail!("Error: OpenLibrary user '{username}' not found.");
        }
        if !status.is_success() {
            anyhow::bail!(
                "OpenLibrary API returned HTTP {} for user '{username}'",
                status.as_u16()
            );
        }

        let body: OlReadingListResponse = resp
            .json()
            .await
            .context("deserializing OL reading list response")?;

        if body.reading_log_entries.is_empty() {
            if page == 1 {
                println!("No want-to-read entries found for user '{username}'.");
            }
            break;
        }

        for entry in &body.reading_log_entries {
            let title = entry.work.title.clone();
            let author = if entry.work.author_names.is_empty() {
                None
            } else {
                Some(entry.work.author_names.join(", "))
            };
            let source_id = Some(entry.work.key.clone());

            if is_already_owned(pool, &title, author.as_deref(), None).await? {
                println!("Already owned: {title}");
                summary.skipped_owned += 1;
                continue;
            }

            if let Some(existing) =
                find_existing_want(pool, &title, author.as_deref(), None).await?
            {
                db::update_want(
                    pool,
                    existing.id,
                    &title,
                    author.as_deref(),
                    None,
                    source_id.as_deref(),
                    existing.priority,
                    existing.notes.as_deref(),
                )
                .await?;
                summary.imported += 1;
            } else {
                db::insert_want(
                    pool,
                    &title,
                    author.as_deref(),
                    None,
                    "openlibrary",
                    source_id.as_deref(),
                    5,
                    None,
                )
                .await?;
                summary.imported += 1;
            }
        }

        // Stop if we've gone past the last page or entries were empty.
        let total_pages = if body.num_found == 0 {
            1
        } else {
            // Use 100 as the assumed page size (OL default); adjust defensively.
            let page_size = body.reading_log_entries.len() as u32;
            if page_size == 0 {
                break;
            }
            body.num_found.div_ceil(page_size)
        };

        if page >= total_pages {
            break;
        }
        page += 1;
    }

    println!(
        "Imported {} books from OpenLibrary. Skipped {} already owned.",
        summary.imported, summary.skipped_owned
    );

    Ok(summary)
}

// ---------------------------------------------------------------------------
// Plain text file import
// ---------------------------------------------------------------------------

/// Import books from a plain text file (one entry per line).
pub async fn import_text_file(
    pool: &db::DbPool,
    path: &Path,
) -> anyhow::Result<ImportSummary> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read file: {}", path.display()))?;

    let mut summary = ImportSummary::default();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (title, author) = parse_text_line(line);

        if is_already_owned(pool, &title, author.as_deref(), None).await? {
            println!("Already owned: {title}");
            summary.skipped_owned += 1;
            continue;
        }

        if let Some(existing) =
            find_existing_want(pool, &title, author.as_deref(), None).await?
        {
            db::update_want(
                pool,
                existing.id,
                &title,
                author.as_deref(),
                None,
                None,
                existing.priority,
                existing.notes.as_deref(),
            )
            .await?;
            summary.imported += 1;
        } else {
            db::insert_want(
                pool,
                &title,
                author.as_deref(),
                None,
                "text_file",
                None,
                5,
                None,
            )
            .await?;
            summary.imported += 1;
        }
    }

    println!(
        "Imported {} books from text file. Skipped {} already owned.",
        summary.imported, summary.skipped_owned
    );

    Ok(summary)
}

/// Parse a single text line using AC-20 priority rules.
fn parse_text_line(line: &str) -> (String, Option<String>) {
    // Rule 1: ` by ` delimiter (case-insensitive).
    if let Some(pos) = find_delimiter_ci(line, " by ") {
        let title = line[..pos].trim().to_string();
        let author = line[pos + 4..].trim().to_string();
        if !title.is_empty() && !author.is_empty() {
            return (title, Some(author));
        }
    }
    // Rule 2: ` - ` delimiter.
    if let Some(pos) = line.find(" - ") {
        let title = line[..pos].trim().to_string();
        let author = line[pos + 3..].trim().to_string();
        if !title.is_empty() && !author.is_empty() {
            return (title, Some(author));
        }
    }
    // Rule 3: whole line is title.
    (line.to_string(), None)
}

/// Find the byte position of `needle` in `haystack` (case-insensitive).
/// The returned offset is a valid byte index into `haystack` itself.
fn find_delimiter_ci(haystack: &str, needle: &str) -> Option<usize> {
    let lower_needle = needle.to_lowercase();
    // Walk each char boundary of the original string.  At each position,
    // check whether the suffix starting there, when lowercased, begins with
    // the lowercased needle.  This guarantees the returned offset is a valid
    // byte index into `haystack` even when lowercasing changes byte lengths.
    for (byte_pos, _) in haystack.char_indices() {
        let suffix_lower = haystack[byte_pos..].to_lowercase();
        if suffix_lower.starts_with(&lower_needle) {
            return Some(byte_pos);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Manual add
// ---------------------------------------------------------------------------

/// Manually add a book to the want list.
pub async fn add_manual(
    pool: &db::DbPool,
    title: &str,
    author: Option<&str>,
    isbn13: Option<&str>,
    priority: i64,
    notes: Option<&str>,
) -> anyhow::Result<AddResult> {
    if !(1..=10).contains(&priority) {
        anyhow::bail!("priority must be between 1 and 10, got {priority}");
    }

    if is_already_owned(pool, title, author, isbn13).await? {
        return Ok(AddResult::AlreadyOwned);
    }

    if let Some(existing) = find_existing_want(pool, title, author, isbn13).await? {
        db::update_want(pool, existing.id, title, author, isbn13, None, priority, notes).await?;
        return Ok(AddResult::AlreadyInWantList);
    }

    db::insert_want(pool, title, author, isbn13, "manual", None, priority, notes).await?;
    Ok(AddResult::Inserted)
}

// ---------------------------------------------------------------------------
// Want list enrichment
// ---------------------------------------------------------------------------

/// Resolve ISBN-13 for want_list entries that are missing it.
/// Returns `(enriched, eligible)`.
pub async fn enrich_want_list(
    pool: &db::DbPool,
    client: &reqwest::Client,
    base_url: &str,
) -> anyhow::Result<(usize, usize)> {
    let eligible = db::want_entries_needing_enrichment(pool).await?;
    let total = eligible.len();
    let mut enriched = 0usize;

    for row in eligible {
        // Guard: rows from the DB query always have title and author, but be defensive.
        if row.title.is_empty() || row.author.is_none() {
            eprintln!(
                "Skipping id={}: missing title or author for enrichment.",
                row.id
            );
            continue;
        }
        let author = row.author.as_deref().unwrap();

        match enrich::find_isbn_by_title_author(client, &row.title, author, base_url).await {
            Ok(Some(isbn)) => {
                db::update_want_isbn13(pool, row.id, &isbn).await?;
                enriched += 1;
            }
            Ok(None) => {
                // No result — leave unchanged, no error.
            }
            Err(e) => {
                eprintln!("WARNING: enrichment failed for id={}: {e}", row.id);
            }
        }
    }

    Ok((enriched, total))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_goodreads_isbn_normal() {
        assert_eq!(
            strip_goodreads_isbn("9780261102217"),
            Some("9780261102217".to_string())
        );
    }

    #[test]
    fn test_strip_goodreads_isbn_wrapped() {
        assert_eq!(
            strip_goodreads_isbn("=\"9780261102217\""),
            Some("9780261102217".to_string())
        );
    }

    #[test]
    fn test_strip_goodreads_isbn_empty() {
        assert_eq!(strip_goodreads_isbn(""), None);
        assert_eq!(strip_goodreads_isbn("=\"\""), None);
    }

    #[test]
    fn test_parse_text_line_by_delimiter() {
        let (title, author) = parse_text_line("The Hobbit by J.R.R. Tolkien");
        assert_eq!(title, "The Hobbit");
        assert_eq!(author.as_deref(), Some("J.R.R. Tolkien"));
    }

    #[test]
    fn test_parse_text_line_by_delimiter_case_insensitive() {
        let (title, author) = parse_text_line("Dune BY Frank Herbert");
        assert_eq!(title, "Dune");
        assert_eq!(author.as_deref(), Some("Frank Herbert"));
    }

    #[test]
    fn test_parse_text_line_dash_delimiter() {
        let (title, author) = parse_text_line("Dune - Frank Herbert");
        assert_eq!(title, "Dune");
        assert_eq!(author.as_deref(), Some("Frank Herbert"));
    }

    #[test]
    fn test_parse_text_line_title_only() {
        let (title, author) = parse_text_line("Just a Title");
        assert_eq!(title, "Just a Title");
        assert!(author.is_none());
    }

    /// `İ` (U+0130, 2 UTF-8 bytes) lowercases to `i` + combining dot above
    /// (3 UTF-8 bytes).  The byte offset returned by `find_delimiter_ci` must
    /// be valid for the *original* string, not the lowercased copy; otherwise
    /// the slice `line[..pos]` would land on a non-char boundary and panic.
    #[test]
    fn test_parse_text_line_non_ascii_multibyte_title() {
        // U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE — expands when lowercased.
        let (title, author) = parse_text_line("\u{130}stanbul by Orhan Pamuk");
        assert_eq!(title, "\u{130}stanbul");
        assert_eq!(author.as_deref(), Some("Orhan Pamuk"));
    }
}
