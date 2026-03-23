use crate::{db, fuzzy};
use anyhow::Context;
use serde::Serialize;

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
/// Sorted by priority descending, then title ascending (case-insensitive).
pub async fn compute_grab_list(pool: &db::DbPool) -> anyhow::Result<Vec<GrabEntry>> {
    let want_entries = db::all_want_entries(pool).await?;
    let all_editions = db::list_editions(pool).await?;

    // Filter to only owned editions.
    let owned: Vec<_> = all_editions.iter().filter(|e| e.owned == 1).collect();

    let mut grab: Vec<GrabEntry> = Vec::new();

    'want: for want in &want_entries {
        // Strategy AC-41a: ISBN-13 exact match.
        if let Some(ref isbn) = want.isbn13 {
            for ed in &owned {
                if ed.isbn.as_deref() == Some(isbn.as_str()) {
                    continue 'want; // owned
                }
            }
        }

        // Strategy AC-41b: Fuzzy title+author (skip when author is NULL — AC-52).
        if let Some(ref author) = want.author {
            for ed in &owned {
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
            // Find work_id for this ISBN in any edition.
            let work_row = all_editions.iter().find(|e| {
                e.isbn.as_deref() == Some(isbn.as_str()) && e.work_id.is_some()
            });
            if let Some(ed_with_work) = work_row {
                if let Some(wid) = ed_with_work.work_id {
                    // Check if any owned edition shares that work_id.
                    let owned_shares_work = owned
                        .iter()
                        .any(|e| e.work_id == Some(wid));
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
}
