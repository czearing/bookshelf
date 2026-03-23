use crate::{db, db::WantRow, enrich, fuzzy};
use crate::fuzzy::normalize_isbn;
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
    #[serde(default)]
    title: Option<String>,
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
    // Strategy 1: ISBN-13 exact match (normalize before comparing — Issue 1).
    if let Some(isbn) = isbn13 {
        let normalized = normalize_isbn(isbn);
        let row = sqlx::query(
            "SELECT 1 FROM editions WHERE isbn = ? AND owned = 1 LIMIT 1",
        )
        .bind(&normalized)
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

    // Strategy 3: Work-level match (normalize ISBN — Issue 1).
    if let Some(isbn) = isbn13 {
        let normalized = normalize_isbn(isbn);
        let work_row = sqlx::query(
            "SELECT work_id FROM editions WHERE isbn = ? AND work_id IS NOT NULL LIMIT 1",
        )
        .bind(&normalized)
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
/// When `all_shelves` is `false` (the default), only rows where
/// `Exclusive Shelf == "to-read"` are imported.  Pass `true` to import all
/// shelves (e.g. with `--all-shelves` CLI flag).
///
/// Returns an `ImportSummary` and prints progress/summary to stdout/stderr.
pub async fn import_goodreads_csv(
    pool: &db::DbPool,
    path: &Path,
    all_shelves: bool,
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

    // Locate optional columns (may be absent in trimmed CSVs).
    let isbn10_col = headers.iter().position(|h| h == "ISBN");
    let shelf_col = headers.iter().position(|h| h == "Exclusive Shelf");

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

        // Skip blank rows (all fields empty).
        if record.iter().all(|f| f.trim().is_empty()) {
            continue;
        }

        let title = match record.get(headers.iter().position(|h| h == "Title").unwrap()) {
            Some(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => {
                eprintln!("WARNING: skipping row {row_num}: empty Title");
                summary.skipped_rows += 1;
                continue;
            }
        };

        // Issue 3 — shelf filter.
        if !all_shelves {
            let shelf = shelf_col
                .and_then(|col| record.get(col))
                .unwrap_or("")
                .trim();
            if shelf != "to-read" {
                eprintln!("Skipping '{title}': shelf is '{shelf}', not to-read.");
                summary.skipped_rows += 1;
                continue;
            }
        }

        let author = record
            .get(headers.iter().position(|h| h == "Author").unwrap())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let raw_isbn13 = record
            .get(headers.iter().position(|h| h == "ISBN13").unwrap())
            .unwrap_or("")
            .to_string();
        // Issue 1 — normalize ISBN-13 (strips hyphens / wrapper).
        let mut isbn13 = strip_goodreads_isbn(&raw_isbn13);

        // Issue 2 — ISBN-10 fallback when ISBN13 is blank.
        if isbn13.is_none() {
            if let Some(col) = isbn10_col {
                let raw_isbn10 = record.get(col).unwrap_or("").to_string();
                if let Some(isbn10_normalized) = strip_goodreads_isbn(&raw_isbn10) {
                    isbn13 = isbn10_to_isbn13(&isbn10_normalized);
                }
            }
        }

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

/// Strip Goodreads `="..."` ISBN wrapper and normalize (remove hyphens/spaces).
/// Returns `None` when empty after stripping.
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
        return None;
    }
    let normalized = normalize_isbn(s);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

/// Convert an ISBN-10 (exactly 10 digits, no hyphens) to ISBN-13.
/// Returns `None` if the input is not exactly 10 digit characters.
pub fn isbn10_to_isbn13(isbn10: &str) -> Option<String> {
    // After normalization, must be exactly 10 digits (or 9 digits + X).
    let digits = normalize_isbn(isbn10);
    if digits.len() != 10 {
        return None;
    }
    // Only purely numeric ISBN-10s can be converted (X check digit is position 10).
    // We drop the check digit (last char) and prepend "978".
    let base = format!("978{}", &digits[..9]);
    // Compute ISBN-13 check digit.
    let checksum: u32 = base
        .chars()
        .enumerate()
        .map(|(i, c)| {
            let d = c.to_digit(10)?;
            Some(if i % 2 == 0 { d } else { d * 3 })
        })
        .try_fold(0u32, |acc, v| v.map(|n| acc + n))?;
    let check_digit = (10 - (checksum % 10)) % 10;
    Some(format!("{base}{check_digit}"))
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
            // Skip entries with missing title (Category 4e).
            let title = match &entry.work.title {
                Some(t) if !t.is_empty() => t.clone(),
                _ => {
                    eprintln!(
                        "WARNING: skipping OL entry with missing or empty title (key={})",
                        entry.work.key
                    );
                    continue;
                }
            };
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
///
/// Pre-processing: strip leading number prefixes like `1. ` or `1) `.
/// Rule 1: ` by ` delimiter (case-insensitive) — use the LAST occurrence.
/// Rule 2: ` - ` delimiter — use the LAST occurrence.
/// Rule 3: whole line is title.
fn parse_text_line(line: &str) -> (String, Option<String>) {
    // Strip leading number prefix: `1. `, `2) `, etc.
    let line = strip_number_prefix(line);

    // Rule 1: ` by ` delimiter (case-insensitive) — use LAST occurrence.
    if let Some(pos) = find_last_delimiter_ci(line, " by ") {
        let title = line[..pos].trim().to_string();
        let author = line[pos + 4..].trim().to_string();
        if !title.is_empty() && !author.is_empty() {
            return (title, Some(author));
        }
    }
    // Rule 2: ` - ` delimiter — use LAST occurrence.
    if let Some(pos) = rfind_delimiter(line, " - ") {
        let title = line[..pos].trim().to_string();
        let author = line[pos + 3..].trim().to_string();
        if !title.is_empty() && !author.is_empty() {
            return (title, Some(author));
        }
    }
    // Rule 3: whole line is title.
    (line.to_string(), None)
}

/// Strip a leading number prefix of the form `N. ` or `N) ` where N is one
/// or more digits.  Returns a slice into the original string (no allocation
/// unless stripping occurs).
fn strip_number_prefix(s: &str) -> &str {
    // Find where the digits end.
    let digits_end = s
        .char_indices()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(i, _)| i + 1);

    if let Some(end) = digits_end {
        if end > 0 {
            let rest = &s[end..];
            if rest.starts_with(". ") || rest.starts_with(") ") {
                return rest[2..].trim_start();
            }
        }
    }
    s
}

/// Find the byte position of the LAST occurrence of `needle` in `haystack`
/// (case-insensitive).  Returns a valid byte index into `haystack`.
fn find_last_delimiter_ci(haystack: &str, needle: &str) -> Option<usize> {
    let lower_needle = needle.to_lowercase();
    let mut last_pos: Option<usize> = None;
    for (byte_pos, _) in haystack.char_indices() {
        let suffix_lower = haystack[byte_pos..].to_lowercase();
        if suffix_lower.starts_with(&lower_needle) {
            last_pos = Some(byte_pos);
        }
    }
    last_pos
}

/// Find the byte position of the LAST occurrence of `needle` in `haystack`
/// (exact, byte-level). Returns a valid byte index into `haystack`.
fn rfind_delimiter(haystack: &str, needle: &str) -> Option<usize> {
    // Walk backwards through all valid char boundaries.
    let needle_bytes = needle.as_bytes();
    let hay_bytes = haystack.as_bytes();
    if needle_bytes.len() > hay_bytes.len() {
        return None;
    }
    let mut last_pos: Option<usize> = None;
    for i in 0..=(hay_bytes.len() - needle_bytes.len()) {
        // Only check at char boundaries.
        if haystack.is_char_boundary(i) && &hay_bytes[i..i + needle_bytes.len()] == needle_bytes {
            last_pos = Some(i);
        }
    }
    last_pos
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
// Remove from want list
// ---------------------------------------------------------------------------

/// Remove one want list entry by `id`.
/// Returns `true` if a row was deleted, `false` if the id was not found.
pub async fn remove_want(pool: &db::DbPool, id: i64) -> anyhow::Result<bool> {
    db::delete_want(pool, id).await
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
    use tempfile::NamedTempFile;

    async fn open_temp_db() -> (db::DbPool, NamedTempFile) {
        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        (pool, tmp)
    }

    // -----------------------------------------------------------------------
    // Category 1: remove_want tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_remove_want_happy_path() {
        let (pool, _tmp) = open_temp_db().await;
        let id = db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
            .await
            .unwrap();

        let removed = remove_want(&pool, id).await.unwrap();
        assert!(removed, "should return true when row is deleted");
    }

    #[tokio::test]
    async fn test_remove_want_nonexistent_returns_false() {
        let (pool, _tmp) = open_temp_db().await;
        let removed = remove_want(&pool, 99999).await.unwrap();
        assert!(!removed, "should return false for nonexistent id");
    }

    #[tokio::test]
    async fn test_remove_want_entry_not_in_list_after_removal() {
        let (pool, _tmp) = open_temp_db().await;
        let id = db::insert_want(&pool, "Foundation", Some("Isaac Asimov"), None, "manual", None, 5, None)
            .await
            .unwrap();

        remove_want(&pool, id).await.unwrap();

        let all = db::list_want(&pool, None).await.unwrap();
        assert!(
            !all.iter().any(|r| r.id == id),
            "removed entry must not appear in list_want"
        );
    }

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

    // Issue 1 — strip_goodreads_isbn now normalizes (strips hyphens).
    #[test]
    fn test_strip_goodreads_isbn_normalizes_hyphens() {
        assert_eq!(
            strip_goodreads_isbn("978-0-441-01359-3"),
            Some("9780441013593".to_string())
        );
    }

    #[test]
    fn test_strip_goodreads_isbn_normalizes_hyphens_wrapped() {
        assert_eq!(
            strip_goodreads_isbn("=\"978-0-441-01359-3\""),
            Some("9780441013593".to_string())
        );
    }

    // Issue 2 — ISBN-10 to ISBN-13 conversion.
    #[test]
    fn test_isbn10_to_isbn13_conversion() {
        assert_eq!(
            isbn10_to_isbn13("0441013597"),
            Some("9780441013593".to_string())
        );
    }

    #[test]
    fn test_isbn10_to_isbn13_another() {
        assert_eq!(
            isbn10_to_isbn13("0553588941"),
            Some("9780553588941".to_string())
        );
    }

    #[test]
    fn test_isbn10_to_isbn13_rejects_wrong_length() {
        assert_eq!(isbn10_to_isbn13("123"), None);
        assert_eq!(isbn10_to_isbn13("97804410135930"), None);
    }

    // -----------------------------------------------------------------------
    // Category 4: OpenLibrary import edge cases (using wiremock)
    // -----------------------------------------------------------------------

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_ol_client() -> reqwest::Client {
        reqwest::Client::new()
    }

    // 4a: 404 → Err with username in message
    #[tokio::test]
    async fn test_import_openlibrary_404_returns_err_with_username() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people/unknownuser/books/want-to-read.json"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "unknownuser", &server.uri()).await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("unknownuser"),
            "error message must contain username, got: {msg}"
        );
    }

    // 4b: Empty list → 0 imported, no error
    #[tokio::test]
    async fn test_import_openlibrary_empty_list_imports_zero() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people/emptyuser/books/want-to-read.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 1,
                "numFound": 0,
                "reading_log_entries": []
            })))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "emptyuser", &server.uri()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().imported, 0);
    }

    // 4c: Pagination — 3 entries on page 1, 2 on page 2, empty page 3 → 5 total
    #[tokio::test]
    async fn test_import_openlibrary_pagination_two_pages() {
        let server = MockServer::start().await;

        let make_entry = |i: usize| serde_json::json!({
            "work": {
                "title": format!("Book {i}"),
                "key": format!("/works/OL{i}W"),
                "author_names": ["Author"]
            }
        });

        let page1_entries: Vec<_> = (1..=3).map(|i| make_entry(i)).collect();
        let page2_entries: Vec<_> = (4..=5).map(|i| make_entry(i)).collect();

        // Register in reverse priority order (LIFO — last registered = highest priority).
        Mock::given(method("GET"))
            .and(path("/people/paginateduser/books/want-to-read.json"))
            .and(query_param("page", "3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 3,
                "numFound": 5,
                "reading_log_entries": []
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/people/paginateduser/books/want-to-read.json"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 2,
                "numFound": 5,
                "reading_log_entries": page2_entries
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/people/paginateduser/books/want-to-read.json"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 1,
                "numFound": 5,
                "reading_log_entries": page1_entries
            })))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "paginateduser", &server.uri()).await;
        assert!(result.is_ok(), "pagination import failed: {:?}", result.err());
        assert_eq!(result.unwrap().imported, 5, "all 5 paginated entries must be imported");
    }

    // 4d: Empty author_names → author stored as NULL
    #[tokio::test]
    async fn test_import_openlibrary_empty_author_names_stores_null() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people/noauthor/books/want-to-read.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 1,
                "numFound": 1,
                "reading_log_entries": [{
                    "work": {
                        "title": "Authorless Book",
                        "key": "/works/OL999W",
                        "author_names": []
                    }
                }]
            })))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "noauthor", &server.uri()).await;
        assert!(result.is_ok());
        let all = db::list_want(&pool, None).await.unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].author.is_none(), "author must be NULL when author_names is empty");
    }

    // 4e: Missing title → row is skipped, not inserted
    #[tokio::test]
    async fn test_import_openlibrary_missing_title_skips_row() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people/notitle/books/want-to-read.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "page": 1,
                "numFound": 1,
                "reading_log_entries": [{
                    "work": {
                        "key": "/works/OL888W",
                        "author_names": ["Some Author"]
                    }
                }]
            })))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "notitle", &server.uri()).await;
        assert!(result.is_ok());
        let all = db::list_want(&pool, None).await.unwrap();
        assert_eq!(all.len(), 0, "row with missing title must be skipped");
    }

    // 4f: HTTP 429 → Err with descriptive message
    #[tokio::test]
    async fn test_import_openlibrary_rate_limit_returns_err() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people/ratelimited/books/want-to-read.json"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        let client = make_ol_client();
        let result = import_openlibrary(&pool, &client, "ratelimited", &server.uri()).await;
        assert!(result.is_err(), "429 must return Err");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("429") || msg.to_lowercase().contains("rate") || msg.contains("HTTP"),
            "error message must be descriptive, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Category 5: Text file import edge cases
    // -----------------------------------------------------------------------

    // 5a: "by" in title — use LAST occurrence
    #[test]
    fn test_parse_text_line_by_in_title_uses_last() {
        let (title, author) = parse_text_line("Driven by Data by Paul Bambrick-Santoyo");
        assert_eq!(title, "Driven by Data");
        assert_eq!(author.as_deref(), Some("Paul Bambrick-Santoyo"));
    }

    // 5b: " - " in title — use LAST occurrence
    #[test]
    fn test_parse_text_line_dash_in_title_uses_last() {
        let (title, author) = parse_text_line("Spider-Man - Brian Michael Bendis");
        assert_eq!(title, "Spider-Man");
        assert_eq!(author.as_deref(), Some("Brian Michael Bendis"));
    }

    // 5c: Numbered lines — strip number prefix
    #[test]
    fn test_parse_text_line_numbered_dot() {
        let (title, author) = parse_text_line("1. Dune by Frank Herbert");
        assert_eq!(title, "Dune");
        assert_eq!(author.as_deref(), Some("Frank Herbert"));
    }

    #[test]
    fn test_parse_text_line_numbered_paren() {
        let (title, author) = parse_text_line("1) Dune by Frank Herbert");
        assert_eq!(title, "Dune");
        assert_eq!(author.as_deref(), Some("Frank Herbert"));
    }

    // 5d: Lines starting with '#' are comments — skip silently
    #[tokio::test]
    async fn test_import_text_file_skips_comment_lines() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "# This is a comment").unwrap();
        writeln!(tmp, "Dune by Frank Herbert").unwrap();

        let db_tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(db_tmp.path()).await.unwrap();
        let result = import_text_file(&pool, tmp.path()).await.unwrap();
        assert_eq!(result.imported, 1, "comment line must not be imported");
    }

    // 5e: Whitespace-only lines are skipped
    #[tokio::test]
    async fn test_import_text_file_skips_whitespace_lines() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "   ").unwrap();
        writeln!(tmp, "\t").unwrap();
        writeln!(tmp, "Neuromancer by William Gibson").unwrap();

        let db_tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(db_tmp.path()).await.unwrap();
        let result = import_text_file(&pool, tmp.path()).await.unwrap();
        assert_eq!(result.imported, 1);
    }

    // 5f: Mixed formats in one file
    #[tokio::test]
    async fn test_import_text_file_mixed_formats() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "# comment").unwrap();
        writeln!(tmp, "1. Dune by Frank Herbert").unwrap();
        writeln!(tmp, "Foundation - Isaac Asimov").unwrap();
        writeln!(tmp, "Just A Title").unwrap();
        writeln!(tmp, "   ").unwrap();

        let db_tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(db_tmp.path()).await.unwrap();
        let result = import_text_file(&pool, tmp.path()).await.unwrap();
        assert_eq!(result.imported, 3, "3 valid lines must be imported");

        let all = db::list_want(&pool, None).await.unwrap();
        let titles: Vec<&str> = all.iter().map(|r| r.title.as_str()).collect();
        assert!(titles.contains(&"Dune"), "Dune must be imported");
        assert!(titles.contains(&"Foundation"), "Foundation must be imported");
        assert!(titles.contains(&"Just A Title"), "title-only must be imported");
    }

    // 5g: File doesn't exist → Err with path named
    #[tokio::test]
    async fn test_import_text_file_nonexistent_path_returns_err() {
        let db_tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(db_tmp.path()).await.unwrap();
        let result = import_text_file(&pool, std::path::Path::new("/nonexistent/file.txt")).await;
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("nonexistent") || msg.contains("file.txt"),
            "error must contain the path, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Category 9: Property-based fuzz tests for parse_text_line (quickcheck)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    mod fuzz {
        use super::super::parse_text_line;
        use quickcheck::quickcheck;

        // Property 1: never panics on any input string.
        // (Simply calling parse_text_line is the test — if it panics, the test fails.)
        quickcheck! {
            fn prop_never_panics(s: String) -> bool {
                let _ = parse_text_line(&s);
                true
            }
        }

        // Property 2: always returns either None author or a non-empty title.
        quickcheck! {
            fn prop_title_always_nonempty(s: String) -> bool {
                // Skip empty input — stripping the number prefix of "" still returns "".
                if s.trim().is_empty() {
                    return true;
                }
                let (title, _author) = parse_text_line(s.trim());
                !title.is_empty()
            }
        }

        // Property 3: deterministic — calling twice on the same input gives same result.
        quickcheck! {
            fn prop_deterministic(s: String) -> bool {
                let r1 = parse_text_line(&s);
                let r2 = parse_text_line(&s);
                r1 == r2
            }
        }
    }
}
