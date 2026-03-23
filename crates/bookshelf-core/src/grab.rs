use crate::{db, fuzzy};
use anyhow::Context;
use serde::Serialize;
use sqlx::Row;

/// One entry in the grab list (a book wanted but not yet owned).
#[derive(Debug, Clone, Serialize)]
pub struct GrabEntry {
    pub title: Option<String>,
    pub author: Option<String>,
    pub isbn13: Option<String>,
    pub priority: i64,
    pub source: String,
    pub notes: Option<String>,
}

/// Compute the grab list: want_list entries that are not matched by any owned edition.
///
/// ISBN exact-match is performed via a single SQL EXISTS query to avoid loading
/// all editions into memory (N+1 fix). Fuzzy and work-level checks are still
/// performed in Rust because they require fuzzy-matching logic.
///
/// `min_priority`: when `Some(n)`, only include entries with `priority >= n`.
///
/// Sorted by priority descending, then title ascending (case-insensitive).
pub async fn compute_grab_list(
    pool: &db::DbPool,
    min_priority: Option<i64>,
) -> anyhow::Result<Vec<GrabEntry>> {
    // ---------------------------------------------------------------------------
    // Step 1: ISBN exact match — resolved entirely in SQL.
    // Returns IDs of want_list rows that ARE already owned by ISBN match.
    // ---------------------------------------------------------------------------
    let isbn_owned_ids: Vec<i64> = {
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
            .map(|r| r.try_get::<i64, _>("id").unwrap_or(0))
            .collect()
    };

    // ---------------------------------------------------------------------------
    // Step 2: Load want entries and owned editions for fuzzy / work-level passes.
    // We only load editions needed for fuzzy: those that are owned.
    // ---------------------------------------------------------------------------
    let want_entries = db::all_want_entries(pool).await?;
    // Load only owned editions (for fuzzy and work-level matching).
    let owned_editions: Vec<db::EditionRow> = {
        let rows = sqlx::query("SELECT * FROM editions WHERE owned = 1")
            .fetch_all(pool)
            .await
            .context("compute_grab_list load owned editions")?;
        rows.into_iter()
            .map(db::row_to_edition_pub)
            .collect::<anyhow::Result<Vec<_>>>()?
    };

    let mut grab: Vec<GrabEntry> = Vec::new();

    'want: for want in &want_entries {
        // Apply min_priority filter.
        if let Some(min) = min_priority {
            if want.priority < min {
                continue 'want;
            }
        }

        // Strategy AC-41a: ISBN-13 exact match (already done in SQL).
        if isbn_owned_ids.contains(&want.id) {
            continue 'want; // owned by ISBN
        }

        // Strategy AC-41b: Fuzzy title+author (skip when author is NULL — AC-52).
        if let Some(ref author) = want.author {
            for ed in &owned_editions {
                if fuzzy::is_same_work(
                    &want.title,
                    author,
                    ed.title.as_deref().unwrap_or(""),
                    ed.authors.as_deref().unwrap_or(""),
                ) {
                    continue 'want; // owned
                }
            }
        }

        // Strategy AC-41c: Work-level match via shared work_id.
        if let Some(ref isbn) = want.isbn13 {
            let want_isbn_norm = crate::fuzzy::normalize_isbn(isbn);
            let work_row = owned_editions.iter().find(|e| {
                e.isbn.as_deref().map(crate::fuzzy::normalize_isbn) == Some(want_isbn_norm.clone())
                    && e.work_id.is_some()
            });
            if let Some(ed_with_work) = work_row {
                if let Some(wid) = ed_with_work.work_id {
                    let owned_shares_work = owned_editions.iter().any(|e| e.work_id == Some(wid));
                    if owned_shares_work {
                        continue 'want; // owned
                    }
                }
            }
        }

        grab.push(GrabEntry {
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
    // Category 2: Performance test — 500 EPUBs, 200 want entries, < 2 seconds
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_compute_grab_list_performance() {
        use crate::db;
        use crate::epub::EpubMeta;
        use std::time::Instant;
        use tempfile::NamedTempFile;

        let tmp = NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();

        // Insert 500 owned editions with distinct ISBNs.
        for i in 0..500u64 {
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

        // Insert 200 want entries with ISBNs that do NOT match any owned edition.
        for i in 0..200u64 {
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

        assert_eq!(entries.len(), 200, "all 200 want entries should appear in grab list");
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
}
