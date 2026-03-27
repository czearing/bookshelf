use crate::{db, fuzzy};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;
use std::collections::{HashMap, HashSet};

/// One entry in the grab list (a book wanted but not yet owned).
#[derive(Debug, Clone, Serialize)]
pub struct GrabEntry {
    pub id: i64,
    pub title: Option<String>,
    pub author: Option<String>,
    pub isbn13: Option<String>,
    pub priority: i64,
    pub source: String,
    pub notes: Option<String>,
}

/// Narrow owned-edition view used only for fuzzy matching — avoids loading all columns.
struct OwnedThin {
    title: Option<String>,
    authors: Option<String>,
}

/// First-word key for the fuzzy index: lowercase, leading-article stripped, first word.
/// Used to bucket owned editions so fuzzy matching is O(bucket_size) per want entry
/// instead of O(all_owned_editions).
fn first_word_key(title: &str) -> String {
    let lower = title.to_lowercase();
    let s = lower.trim_start();
    let s = s.strip_prefix("the ")
        .or_else(|| s.strip_prefix("a "))
        .or_else(|| s.strip_prefix("an "))
        .unwrap_or(s);
    s.split_whitespace().next().unwrap_or("").to_string()
}

/// Progressive backoff based on failed download attempts.
/// Returns true if enough time has passed since the last try.
///
/// Retry schedule:
///   0–4  failures → retry every cycle (no delay)
///   5–9  failures → retry after 1 hour
///   10–19 failures → retry after 6 hours
///   20+  failures → retry after 24 hours
fn should_retry(attempts: i64, last_attempted_at: Option<&str>) -> bool {
    let min_minutes: i64 = match attempts {
        0..=4  => return true, // always retry
        5..=9  => 60,
        10..=19 => 360,
        _      => 1440, // 24 hours
    };
    let Some(last) = last_attempted_at else { return true; };
    let Ok(last_dt) = last.parse::<DateTime<Utc>>() else { return true; };
    (Utc::now() - last_dt).num_minutes() >= min_minutes
}

/// Compute the grab list: want_list entries that are not matched by any owned edition.
///
/// All three ownership strategies are resolved with minimal work:
///   AC-41a — ISBN exact match: single SQL EXISTS query; results stored in a HashSet
///             for O(1) per-entry lookup (was O(n) Vec scan).
///   AC-41b — Fuzzy title+author: owned editions loaded with only `title, authors`
///             columns; a first-word HashMap reduces average scan from O(E) to
///             O(avg_bucket_size ≈ 1–5) per want entry.
///   AC-41c — Work-level match: resolved entirely in SQL via a JOIN between editions
///             rows sharing the same work_id; result stored as a HashSet<normalized_isbn>
///             for O(1) lookup, eliminating the earlier full `all_editions` table load.
///
/// `min_priority`: when `Some(n)`, only include entries with `priority >= n`.
///
/// Sorted by priority descending, then title ascending (case-insensitive).
pub async fn compute_grab_list(
    pool: &db::DbPool,
    min_priority: Option<i64>,
) -> anyhow::Result<Vec<GrabEntry>> {
    // ---------------------------------------------------------------------------
    // Step 1: AC-41a — ISBN exact match in SQL → HashSet<i64> for O(1) lookup.
    // ---------------------------------------------------------------------------
    let isbn_owned_ids: HashSet<i64> = {
        let rows = sqlx::query(
            r"SELECT w.id FROM want_list w
              WHERE w.isbn13 IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM editions e
                    WHERE e.isbn = w.isbn13 AND e.owned = 1
                )",
        )
        .fetch_all(pool)
        .await
        .context("compute_grab_list isbn EXISTS check")?;
        rows.into_iter()
            .filter_map(|r| r.try_get::<i64, _>("id").ok())
            .collect()
    };

    // ---------------------------------------------------------------------------
    // Step 2: AC-41c — Work-level owned ISBNs, fully resolved in SQL.
    // Finds every edition ISBN that shares a work_id with at least one owned
    // edition.  Stored as normalized ISBNs in a HashSet for O(1) lookup.
    // This replaces the earlier "load ALL editions into Rust, iterate" approach.
    // ---------------------------------------------------------------------------
    let work_owned_isbns: HashSet<String> = {
        let rows = sqlx::query(
            r"SELECT DISTINCT e1.isbn
              FROM editions e1
              JOIN editions e2 ON e2.work_id = e1.work_id AND e2.owned = 1
              WHERE e1.work_id IS NOT NULL AND e1.isbn IS NOT NULL",
        )
        .fetch_all(pool)
        .await
        .context("compute_grab_list work-level owned ISBNs")?;
        rows.into_iter()
            .filter_map(|r| r.try_get::<String, _>("isbn").ok())
            .map(|isbn| fuzzy::normalize_isbn(&isbn))
            .collect()
    };

    // ---------------------------------------------------------------------------
    // Step 3: Load want entries + owned editions (narrow columns only).
    // `SELECT *` is avoided — only the two columns needed for fuzzy matching.
    // ---------------------------------------------------------------------------
    let want_entries = db::all_want_entries(pool).await?;
    let owned: Vec<OwnedThin> = {
        let rows = sqlx::query("SELECT title, authors FROM editions WHERE owned = 1")
            .fetch_all(pool)
            .await
            .context("compute_grab_list load owned editions")?;
        rows.into_iter()
            .map(|r| OwnedThin {
                title: r.try_get::<Option<String>, _>("title").ok().flatten(),
                authors: r.try_get::<Option<String>, _>("authors").ok().flatten(),
            })
            .collect()
    };

    // ---------------------------------------------------------------------------
    // Step 4: Build first-word index over owned editions.
    // Key = first non-article lowercase word of the edition title.
    // Value = indices into `owned`.  Each want entry only checks the bucket
    // matching its own first word, reducing average fuzzy iterations dramatically.
    // ---------------------------------------------------------------------------
    let mut fuzzy_index: HashMap<String, Vec<usize>> = HashMap::with_capacity(owned.len());
    for (i, ed) in owned.iter().enumerate() {
        if let Some(ref t) = ed.title {
            let key = first_word_key(t);
            if !key.is_empty() {
                fuzzy_index.entry(key).or_default().push(i);
            }
        }
    }

    let mut grab: Vec<GrabEntry> = Vec::new();

    'want: for want in &want_entries {
        // Apply min_priority filter.
        if let Some(min) = min_priority {
            if want.priority < min {
                continue 'want;
            }
        }

        // Skip entries still within their backoff window.
        if !should_retry(want.download_attempts, want.last_attempted_at.as_deref()) {
            continue 'want;
        }

        // AC-41a: ISBN exact match — O(1) HashSet lookup.
        if isbn_owned_ids.contains(&want.id) {
            continue 'want;
        }

        // AC-41b: Fuzzy title+author — only check owned editions in the same
        // first-word bucket (skipped when author is NULL — AC-52).
        if let Some(ref author) = want.author {
            let key = first_word_key(&want.title);
            if let Some(candidates) = fuzzy_index.get(&key) {
                for &idx in candidates {
                    let ed = &owned[idx];
                    if fuzzy::is_same_work(
                        &want.title,
                        author,
                        ed.title.as_deref().unwrap_or(""),
                        ed.authors.as_deref().unwrap_or(""),
                    ) {
                        continue 'want;
                    }
                }
            }
        }

        // AC-41c: Work-level match — O(1) HashSet lookup (precomputed in SQL).
        if let Some(ref isbn) = want.isbn13 {
            let norm = fuzzy::normalize_isbn(isbn);
            if work_owned_isbns.contains(&norm) {
                continue 'want;
            }
        }

        grab.push(GrabEntry {
            id: want.id,
            title: Some(want.title.clone()),
            author: want.author.clone(),
            isbn13: want.isbn13.clone(),
            priority: want.priority,
            source: want.source.clone(),
            notes: want.notes.clone(),
        });
    }

    // Sort: priority DESC, then title ASC (case-insensitive).
    grab.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| {
                let ta = a.title.as_deref().unwrap_or("").to_lowercase();
                let tb = b.title.as_deref().unwrap_or("").to_lowercase();
                ta.cmp(&tb)
            })
    });

    Ok(grab)
}

// ---------------------------------------------------------------------------
// Output formatters
// ---------------------------------------------------------------------------

/// Format the grab list as a human-readable aligned table.
pub fn format_text(entries: &[GrabEntry]) -> String {
    if entries.is_empty() {
        return String::new();
    }

    // Determine column widths.
    let w_pri = "Priority".len().max(
        entries.iter().map(|e| e.priority.to_string().len()).max().unwrap_or(0),
    );
    let w_title = "Title".len().max(
        entries
            .iter()
            .map(|e| e.title.as_deref().unwrap_or("(none)").len())
            .max()
            .unwrap_or(0),
    );
    let w_author = "Author".len().max(
        entries
            .iter()
            .map(|e| e.author.as_deref().unwrap_or("(none)").len())
            .max()
            .unwrap_or(0),
    );
    let w_isbn = "ISBN13".len().max(
        entries
            .iter()
            .map(|e| e.isbn13.as_deref().unwrap_or("(none)").len())
            .max()
            .unwrap_or(0),
    );

    let mut out = format!(
        "{:<w_pri$}  {:<w_title$}  {:<w_author$}  {:<w_isbn$}  Source\n",
        "Priority",
        "Title",
        "Author",
        "ISBN13",
        w_pri = w_pri,
        w_title = w_title,
        w_author = w_author,
        w_isbn = w_isbn,
    );

    // Separator line.
    out.push_str(&"-".repeat(w_pri + 2 + w_title + 2 + w_author + 2 + w_isbn + 2 + 6));
    out.push('\n');

    for e in entries {
        out.push_str(&format!(
            "{:<w_pri$}  {:<w_title$}  {:<w_author$}  {:<w_isbn$}  {}\n",
            e.priority,
            e.title.as_deref().unwrap_or("(none)"),
            e.author.as_deref().unwrap_or("(none)"),
            e.isbn13.as_deref().unwrap_or("(none)"),
            e.source,
            w_pri = w_pri,
            w_title = w_title,
            w_author = w_author,
            w_isbn = w_isbn,
        ));
    }

    out
}

/// Serialize the grab list to a pretty-printed JSON array.
pub fn format_json(entries: &[GrabEntry]) -> anyhow::Result<String> {
    serde_json::to_string_pretty(entries).context("serializing grab list to JSON")
}

/// Serialize the grab list to CSV (RFC 4180), always including a header row.
pub fn format_csv(entries: &[GrabEntry]) -> anyhow::Result<String> {
    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record(["priority", "title", "author", "isbn13", "source", "notes"])
        .context("writing CSV header")?;

    for e in entries {
        wtr.write_record([
            e.priority.to_string().as_str(),
            e.title.as_deref().unwrap_or(""),
            e.author.as_deref().unwrap_or(""),
            e.isbn13.as_deref().unwrap_or(""),
            e.source.as_str(),
            e.notes.as_deref().unwrap_or(""),
        ])
        .context("writing CSV row")?;
    }

    let bytes = wtr.into_inner().context("finalizing CSV writer")?;
    String::from_utf8(bytes).context("CSV output is not valid UTF-8")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(priority: i64, title: &str) -> GrabEntry {
        GrabEntry {
            id: 0,
            title: Some(title.to_string()),
            author: Some("Author".to_string()),
            isbn13: None,
            priority,
            source: "manual".to_string(),
            notes: None,
        }
    }

    #[test]
    fn test_format_json_empty() {
        let json = format_json(&[]).unwrap();
        assert_eq!(json.trim(), "[]");
    }

    #[test]
    fn test_format_json_has_keys() {
        let entries = vec![sample_entry(5, "Dune")];
        let json = format_json(&entries).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = &parsed[0];
        assert_eq!(obj["priority"], 5);
        assert_eq!(obj["title"], "Dune");
        assert_eq!(obj["source"], "manual");
        assert!(obj["isbn13"].is_null());
    }

    #[test]
    fn test_format_csv_header_always_present() {
        let csv = format_csv(&[]).unwrap();
        assert!(csv.starts_with("priority,title,author,isbn13,source,notes"));
    }

    #[test]
    fn test_format_csv_rfc4180_quoting() {
        let entry = GrabEntry {
            id: 0,
            title: Some("Title, with comma".to_string()),
            author: None,
            isbn13: None,
            priority: 5,
            source: "manual".to_string(),
            notes: None,
        };
        let csv = format_csv(&[entry]).unwrap();
        assert!(csv.contains("\"Title, with comma\""));
    }

    #[test]
    fn test_format_text_null_rendered_as_none() {
        let entry = GrabEntry {
            id: 0,
            title: Some("A Book".to_string()),
            author: None,
            isbn13: None,
            priority: 7,
            source: "manual".to_string(),
            notes: None,
        };
        let text = format_text(&[entry]);
        assert!(text.contains("(none)"));
    }

    // -----------------------------------------------------------------------
    // Category 2: Performance test — 10K owned editions, 1K want entries, < 2s
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_compute_grab_list_performance() {
        use crate::db;
        use crate::epub::EpubMeta;
        use std::time::Instant;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();

        // Insert 10 000 owned editions with distinct ISBNs.
        for i in 0..10_000u64 {
            let isbn = format!("{:013}", 9780000000000u64 + i);
            let meta = EpubMeta {
                title: Some(format!("Perf Book {i}")),
                authors: Some("Perf Author".to_string()),
                isbn: Some(isbn.clone()),
                source_path: format!("/tmp/perf_{i}.epub"),
                ..Default::default()
            };
            db::upsert_edition(&pool, &meta).await.unwrap();
        }

        // Insert 1 000 want entries with ISBNs that do NOT match any owned edition.
        for i in 0..1_000u64 {
            let isbn = format!("{:013}", 9790000000000u64 + i);
            db::insert_want(
                &pool,
                &format!("Want Book {i}"),
                Some("Want Author"),
                Some(&isbn),
                "manual",
                None,
                5,
                None,
            )
            .await
            .unwrap();
        }

        let start = Instant::now();
        let entries = compute_grab_list(&pool, None).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(entries.len(), 1_000, "all 1 000 want entries should appear in grab list");
        assert!(
            elapsed.as_secs_f64() < 2.0,
            "compute_grab_list took {:.3}s, must complete in under 2 seconds",
            elapsed.as_secs_f64()
        );
    }

    // -----------------------------------------------------------------------
    // Category 8: min_priority filter test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_compute_grab_list_min_priority_filter() {
        use crate::db;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();

        // Insert want entries with priorities 3, 5, 7, 8.
        for (title, priority) in &[("Book3", 3i64), ("Book5", 5), ("Book7", 7), ("Book8", 8)] {
            db::insert_want(&pool, title, Some("Author"), None, "manual", None, *priority, None)
                .await
                .unwrap();
        }

        // With min_priority=6, only 7 and 8 should appear.
        let entries = compute_grab_list(&pool, Some(6)).await.unwrap();
        let titles: Vec<&str> = entries
            .iter()
            .filter_map(|e| e.title.as_deref())
            .collect();

        assert!(titles.contains(&"Book7"), "priority 7 must be included");
        assert!(titles.contains(&"Book8"), "priority 8 must be included");
        assert!(!titles.contains(&"Book3"), "priority 3 must be excluded");
        assert!(!titles.contains(&"Book5"), "priority 5 must be excluded");
        assert_eq!(entries.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Ownership strategy correctness tests (AC-41a / AC-41b / AC-41c / AC-52)
    // -----------------------------------------------------------------------

    /// Helper: open a fresh in-memory-backed temp DB.
    async fn fresh_pool() -> (tempfile::NamedTempFile, crate::db::DbPool) {
        let tmp = tempfile::NamedTempFile::with_suffix(".db").unwrap();
        let pool = crate::db::open(tmp.path()).await.unwrap();
        (tmp, pool)
    }

    /// Helper: insert a minimal owned edition and return its id.
    async fn insert_owned(
        pool: &crate::db::DbPool,
        title: &str,
        authors: &str,
        isbn: Option<&str>,
        path_suffix: &str,
    ) -> i64 {
        use crate::epub::EpubMeta;
        let meta = EpubMeta {
            title: Some(title.to_string()),
            authors: Some(authors.to_string()),
            isbn: isbn.map(|s| s.to_string()),
            source_path: format!("/tmp/owned_{path_suffix}.epub"),
            ..Default::default()
        };
        let (id, _) = crate::db::upsert_edition(pool, &meta).await.unwrap();
        id
    }

    // -----------------------------------------------------------------------
    // AC-41a: want entry with an ISBN that exactly matches an owned edition
    //         must be excluded from the grab list.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_ac41a_isbn_exact_match_excludes_entry() {
        let (_tmp, pool) = fresh_pool().await;
        let isbn = "9780441013593";

        insert_owned(&pool, "Dune", "Frank Herbert", Some(isbn), "dune").await;
        crate::db::insert_want(&pool, "Dune", Some("Frank Herbert"), Some(isbn), "manual", None, 5, None)
            .await
            .unwrap();

        let grab = compute_grab_list(&pool, None).await.unwrap();
        assert!(grab.is_empty(), "owned-by-ISBN entry must not appear in grab list");
    }

    // -----------------------------------------------------------------------
    // AC-41a negative: a want entry whose ISBN differs from all owned editions
    // must NOT be excluded.  Title/author are kept distinct to avoid triggering
    // the AC-41b fuzzy path.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_ac41a_different_isbn_not_excluded() {
        let (_tmp, pool) = fresh_pool().await;

        insert_owned(&pool, "Owned Book Alpha", "Author One", Some("9780441013593"), "owned").await;
        crate::db::insert_want(
            &pool,
            "Wanted Book Beta",
            Some("Author Two"),
            Some("9780441172711"),
            "manual",
            None,
            5,
            None,
        )
        .await
        .unwrap();

        let grab = compute_grab_list(&pool, None).await.unwrap();
        assert_eq!(grab.len(), 1, "different ISBN must still appear in grab list");
    }

    // -----------------------------------------------------------------------
    // AC-41b: want entry whose title+author fuzzy-matches an owned edition
    //         must be excluded even without an ISBN.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_ac41b_fuzzy_match_excludes_entry() {
        let (_tmp, pool) = fresh_pool().await;

        // Owned with no ISBN.
        insert_owned(&pool, "The Fellowship of the Ring", "J.R.R. Tolkien", None, "lotr").await;
        // Want entry with slight title variation (subtitle) and same author.
        crate::db::insert_want(
            &pool,
            "Fellowship of the Ring",
            Some("J.R.R. Tolkien"),
            None,
            "manual",
            None,
            5,
            None,
        )
        .await
        .unwrap();

        let grab = compute_grab_list(&pool, None).await.unwrap();
        assert!(grab.is_empty(), "fuzzy-matched entry must not appear in grab list");
    }

    // -----------------------------------------------------------------------
    // AC-52: when author is NULL the fuzzy check is skipped — entry must still
    //        appear in grab list even if the title matches an owned edition.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_ac52_null_author_skips_fuzzy_check() {
        let (_tmp, pool) = fresh_pool().await;

        insert_owned(&pool, "Dune", "Frank Herbert", None, "dune").await;
        // Want entry with same title but NULL author.
        crate::db::insert_want(&pool, "Dune", None, None, "manual", None, 5, None)
            .await
            .unwrap();

        let grab = compute_grab_list(&pool, None).await.unwrap();
        assert_eq!(
            grab.len(),
            1,
            "entry with null author must not be excluded by fuzzy check (AC-52)"
        );
    }

    // -----------------------------------------------------------------------
    // AC-41c: want entry whose ISBN appears on an edition that shares a work_id
    //         with an owned edition must be excluded (cross-edition ownership).
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_ac41c_work_level_match_excludes_entry() {
        let (_tmp, pool) = fresh_pool().await;

        // Create a shared work.
        let work_id = crate::db::insert_work(&pool, "Dune", "Frank Herbert").await.unwrap();

        // Edition A: owned, no ISBN (e.g. an EPUB rip without metadata).
        let owned_id = insert_owned(&pool, "Dune", "Frank Herbert", None, "dune_owned").await;
        crate::db::set_edition_work_id(&pool, owned_id, work_id).await.unwrap();

        // Edition B: not owned, but carries the ISBN we want.
        let want_isbn = "9780441013593";
        // Insert edition B directly via raw SQL (not owned, but has the ISBN + work_id).
        sqlx::query(
            "INSERT INTO editions (title, authors, isbn, source_path, owned, work_id)
             VALUES (?, ?, ?, ?, 0, ?)",
        )
        .bind("Dune")
        .bind("Frank Herbert")
        .bind(want_isbn)
        .bind("/tmp/dune_nonowned.epub")
        .bind(work_id)
        .execute(&pool)
        .await
        .unwrap();

        // Want entry referencing the ISBN carried by the non-owned edition.
        crate::db::insert_want(
            &pool, "Dune", Some("Frank Herbert"), Some(want_isbn), "manual", None, 5, None,
        )
        .await
        .unwrap();

        let grab = compute_grab_list(&pool, None).await.unwrap();
        assert!(
            grab.is_empty(),
            "entry owned via work-level match must not appear in grab list"
        );
    }

    // -----------------------------------------------------------------------
    // should_retry: verify the progressive backoff schedule.
    // -----------------------------------------------------------------------
    #[test]
    fn test_should_retry_always_retries_under_5_attempts() {
        for attempts in 0..=4 {
            assert!(
                should_retry(attempts, Some("2000-01-01T00:00:00Z")),
                "attempts={attempts} should always retry"
            );
        }
    }

    #[test]
    fn test_should_retry_no_timestamp_always_retries() {
        for attempts in [0, 5, 10, 25] {
            assert!(
                should_retry(attempts, None),
                "missing timestamp should always retry regardless of attempts"
            );
        }
    }

    #[test]
    fn test_should_retry_respects_hourly_backoff() {
        // 5–9 failures: retry only after 60 minutes.
        // Use a timestamp 30 minutes ago — should NOT retry.
        let thirty_min_ago = (chrono::Utc::now() - chrono::Duration::minutes(30))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            !should_retry(5, Some(&thirty_min_ago)),
            "5 failures, 30 min ago → must NOT retry yet"
        );

        // 61 minutes ago — should retry.
        let sixty_one_min_ago = (chrono::Utc::now() - chrono::Duration::minutes(61))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            should_retry(5, Some(&sixty_one_min_ago)),
            "5 failures, 61 min ago → must retry"
        );
    }

    #[test]
    fn test_should_retry_respects_six_hour_backoff() {
        // 10–19 failures: retry after 360 minutes.
        let three_hours_ago = (chrono::Utc::now() - chrono::Duration::minutes(180))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            !should_retry(10, Some(&three_hours_ago)),
            "10 failures, 3 h ago → must NOT retry yet"
        );

        let seven_hours_ago = (chrono::Utc::now() - chrono::Duration::minutes(421))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            should_retry(10, Some(&seven_hours_ago)),
            "10 failures, 7 h ago → must retry"
        );
    }

    #[test]
    fn test_should_retry_respects_daily_backoff() {
        // 20+ failures: retry after 1440 minutes (24 h).
        let twelve_hours_ago = (chrono::Utc::now() - chrono::Duration::minutes(720))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            !should_retry(20, Some(&twelve_hours_ago)),
            "20 failures, 12 h ago → must NOT retry yet"
        );

        let twenty_five_hours_ago = (chrono::Utc::now() - chrono::Duration::minutes(1501))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert!(
            should_retry(20, Some(&twenty_five_hours_ago)),
            "20 failures, 25 h ago → must retry"
        );
    }

    // -----------------------------------------------------------------------
    // first_word_key: edge cases
    // -----------------------------------------------------------------------
    #[test]
    fn test_first_word_key_strips_article() {
        assert_eq!(first_word_key("The Hobbit"), "hobbit");
        assert_eq!(first_word_key("A Game of Thrones"), "game");
        assert_eq!(first_word_key("An American Marriage"), "american");
    }

    #[test]
    fn test_first_word_key_no_article() {
        assert_eq!(first_word_key("Dune"), "dune");
        assert_eq!(first_word_key("Foundation"), "foundation");
    }

    #[test]
    fn test_first_word_key_empty() {
        assert_eq!(first_word_key(""), "");
        // "The" alone has no trailing space so strip_prefix("the ") fails → first word is "the"
        assert_eq!(first_word_key("The"), "the");
    }
}
