/// Integration tests for Phase 2 grab list computation and output formatting.
use bookshelf_core::{
    db,
    epub::EpubMeta,
    grab,
};
use sqlx::Row;
use tempfile::NamedTempFile;

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

async fn insert_owned_edition(pool: &db::DbPool, title: &str, authors: &str, isbn: Option<&str>, source_path: &str) -> i64 {
    let meta = EpubMeta {
        title: Some(title.to_string()),
        authors: Some(authors.to_string()),
        isbn: isbn.map(|s| s.to_string()),
        source_path: source_path.to_string(),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap()
}

async fn insert_want(pool: &db::DbPool, title: &str, author: Option<&str>, isbn13: Option<&str>, priority: i64) -> i64 {
    db::insert_want(pool, title, author, isbn13, "manual", None, priority, None)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Grab list computation (AC-41a/b/c, AC-43, AC-44, AC-52)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_grab_isbn_match_owned() {
    let (pool, _tmp) = temp_pool().await;
    insert_owned_edition(&pool, "Dune", "Frank Herbert", Some("9780441013593"), "/tmp/dune.epub").await;
    insert_want(&pool, "Dune", Some("Frank Herbert"), Some("9780441013593"), 5).await;

    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(grab.is_empty(), "ISBN-matched owned book must not appear in grab list");
}

#[tokio::test]
async fn test_grab_fuzzy_match_owned() {
    let (pool, _tmp) = temp_pool().await;
    // Owned edition — no ISBN, matched by fuzzy.
    insert_owned_edition(&pool, "The Hobbit", "J.R.R. Tolkien", None, "/tmp/hobbit.epub").await;
    insert_want(&pool, "The Hobbit", Some("J.R.R. Tolkien"), None, 5).await;

    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(grab.is_empty(), "Fuzzy-matched owned book must not appear in grab list");
}

#[tokio::test]
async fn test_grab_work_level_match_owned() {
    let (pool, _tmp) = temp_pool().await;

    // Edition with isbn that has a work_id, and another edition that is owned and shares work_id.
    let work_id = db::insert_work(&pool, "Dune", "Frank Herbert").await.unwrap();

    let meta_isbn = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        isbn: Some("9780441013593".to_string()),
        source_path: "/tmp/dune_isbn.epub".to_string(),
        ..Default::default()
    };
    let ed_isbn = db::upsert_edition(&pool, &meta_isbn).await.unwrap();
    db::set_edition_work_id(&pool, ed_isbn, work_id).await.unwrap();

    let meta_owned = EpubMeta {
        title: Some("Dune (alternate edition)".to_string()),
        authors: Some("Frank Herbert".to_string()),
        source_path: "/tmp/dune_alt.epub".to_string(),
        ..Default::default()
    };
    let ed_owned = db::upsert_edition(&pool, &meta_owned).await.unwrap();
    db::set_edition_work_id(&pool, ed_owned, work_id).await.unwrap();

    // Want row with the ISBN that links to the work.
    insert_want(&pool, "Dune", Some("Frank Herbert"), Some("9780441013593"), 5).await;

    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(
        grab.is_empty(),
        "Work-level-matched owned book must not appear in grab list"
    );
}

#[tokio::test]
async fn test_grab_no_match_appears_in_list() {
    let (pool, _tmp) = temp_pool().await;
    insert_want(&pool, "Neuromancer", Some("William Gibson"), None, 8).await;

    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert_eq!(grab.len(), 1);
    assert_eq!(grab[0].title.as_deref(), Some("Neuromancer"));
    assert_eq!(grab[0].priority, 8);
}

#[tokio::test]
async fn test_grab_null_author_skips_fuzzy() {
    let (pool, _tmp) = temp_pool().await;

    // Owned edition with title "Dune" but different author — should NOT be matched via fuzzy
    // because want entry has no author (AC-52).
    insert_owned_edition(&pool, "Dune", "Frank Herbert", None, "/tmp/dune2.epub").await;
    // Want row: same title but NULL author.
    insert_want(&pool, "Dune", None, None, 5).await;

    // The want entry has no ISBN and no author, so fuzzy is skipped.
    // Fuzzy would have matched but must be skipped. The entry appears in grab list.
    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert_eq!(
        grab.len(),
        1,
        "want entry with NULL author must not be matched via fuzzy; it must appear in grab list"
    );
}

#[tokio::test]
async fn test_grab_empty_want_list() {
    let (pool, _tmp) = temp_pool().await;
    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(grab.is_empty());
}

#[tokio::test]
async fn test_grab_all_owned() {
    let (pool, _tmp) = temp_pool().await;
    insert_owned_edition(&pool, "Dune", "Frank Herbert", Some("9780441013593"), "/tmp/dune3.epub").await;
    insert_want(&pool, "Dune", Some("Frank Herbert"), Some("9780441013593"), 5).await;
    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(grab.is_empty(), "all owned → grab list must be empty");
}

#[tokio::test]
async fn test_grab_sort_order() {
    let (pool, _tmp) = temp_pool().await;
    insert_want(&pool, "Zebra Book", Some("Author Z"), None, 3).await;
    insert_want(&pool, "Apple Book", Some("Author A"), None, 7).await;
    insert_want(&pool, "Mango Book", Some("Author M"), None, 7).await;

    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert_eq!(grab.len(), 3);
    // Priority 7 items come first, sorted by title asc within same priority.
    assert_eq!(grab[0].priority, 7);
    assert_eq!(grab[0].title.as_deref(), Some("Apple Book"));
    assert_eq!(grab[1].priority, 7);
    assert_eq!(grab[1].title.as_deref(), Some("Mango Book"));
    assert_eq!(grab[2].priority, 3);
    assert_eq!(grab[2].title.as_deref(), Some("Zebra Book"));
}

// ---------------------------------------------------------------------------
// Output format tests (AC-42/45/46/47/48/49/50)
// ---------------------------------------------------------------------------

fn sample_grab_entry(title: &str, author: Option<&str>, isbn13: Option<&str>) -> grab::GrabEntry {
    grab::GrabEntry {
        title: Some(title.to_string()),
        author: author.map(|s| s.to_string()),
        isbn13: isbn13.map(|s| s.to_string()),
        priority: 5,
        source: "manual".to_string(),
        notes: None,
    }
}

#[tokio::test]
async fn test_grab_text_output_format() {
    let entry = sample_grab_entry("Dune", None, None);
    let text = grab::format_text(&[entry]);
    assert!(text.contains("Priority"), "text must include Priority header");
    assert!(text.contains("Title"), "text must include Title header");
    assert!(text.contains("(none)"), "NULL author must render as (none)");
    assert!(text.contains("Dune"));
}

#[tokio::test]
async fn test_grab_json_output_valid() {
    let entry = sample_grab_entry("Dune", Some("Frank Herbert"), None);
    let json = grab::format_json(&[entry]).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
    let obj = &parsed[0];
    assert_eq!(obj["title"], "Dune");
    assert_eq!(obj["author"], "Frank Herbert");
    assert!(obj["isbn13"].is_null());
    assert_eq!(obj["priority"], 5);
    assert_eq!(obj["source"], "manual");
}

#[tokio::test]
async fn test_grab_json_empty_list_is_array() {
    let json = grab::format_json(&[]).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
    assert_eq!(parsed.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_grab_csv_output_header_present() {
    let csv = grab::format_csv(&[]).unwrap();
    assert!(csv.starts_with("priority,title,author,isbn13,source,notes\n") ||
        csv.starts_with("priority,title,author,isbn13,source,notes\r\n"),
        "CSV must start with header row: {csv:?}");
}

#[tokio::test]
async fn test_grab_csv_empty_list_has_only_header() {
    let csv = grab::format_csv(&[]).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 1, "empty grab list CSV must have only header row");
}

#[tokio::test]
async fn test_grab_csv_rfc4180_quoting() {
    let entry = grab::GrabEntry {
        title: Some("Title, with comma".to_string()),
        author: Some("Author \"Nickname\" Name".to_string()),
        isbn13: None,
        priority: 5,
        source: "manual".to_string(),
        notes: None,
    };
    let csv = grab::format_csv(&[entry]).unwrap();
    // Title with comma must be quoted per RFC 4180.
    assert!(csv.contains("\"Title, with comma\""), "commas must be RFC 4180 quoted: {csv}");
    // Double-quotes inside fields must be escaped as "".
    assert!(csv.contains("\"\""), "internal double-quotes must be escaped as \"\": {csv}");
}

#[tokio::test]
async fn test_grab_csv_null_fields_are_empty() {
    let entry = sample_grab_entry("Dune", None, None);
    let csv = grab::format_csv(&[entry]).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 2, "header + 1 data row");
    // author and isbn13 columns should be empty (consecutive commas).
    let data = lines[1];
    // "5,Dune,,,manual,"
    assert!(
        data.contains(",,"),
        "NULL fields must produce empty (consecutive comma) in CSV: {data}"
    );
}

// ---------------------------------------------------------------------------
// Issue 5 — Work-level match E2E after enrichment merge
// ---------------------------------------------------------------------------

/// Two editions share a work_id (set via set_edition_work_id).  Edition A is
/// owned; Edition B is not owned but has a matching want entry via its ISBN.
/// The want entry must NOT appear in the grab list because the work is already
/// owned through Edition A.
#[tokio::test]
async fn test_grab_work_level_match_after_enrichment_merge() {
    let (pool, _tmp) = temp_pool().await;

    // Edition A: owned, ISBN 9780441013593, will be assigned to the shared work.
    let meta_a = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        isbn: Some("9780441013593".to_string()),
        source_path: "/tmp/dune_a.epub".to_string(),
        ..Default::default()
    };
    let ed_a = db::upsert_edition(&pool, &meta_a).await.unwrap();

    // Edition B: not owned (owned defaults to 0 after upsert — we just don't mark it).
    // In the test DB, editions from upsert_edition are owned=1 by default because
    // scan sets owned. We need to insert a non-owned edition manually.
    let meta_b = EpubMeta {
        title: Some("Dune (unowned edition)".to_string()),
        authors: Some("Frank Herbert".to_string()),
        isbn: Some("9780441013570".to_string()),
        source_path: "/tmp/dune_b_unowned.epub".to_string(),
        ..Default::default()
    };
    // Insert non-owned edition directly using low-level insert (owned=0 by default in schema).
    sqlx::query(
        r"INSERT INTO editions (title, authors, isbn, source_path, owned)
          VALUES (?, ?, ?, ?, 0)",
    )
    .bind(&meta_b.title)
    .bind(&meta_b.authors)
    .bind(&meta_b.isbn)
    .bind(&meta_b.source_path)
    .execute(&pool)
    .await
    .unwrap();

    let ed_b_row = sqlx::query("SELECT id FROM editions WHERE source_path = ?")
        .bind(&meta_b.source_path)
        .fetch_one(&pool)
        .await
        .unwrap();
    let ed_b: i64 = ed_b_row.try_get("id").unwrap();

    // Create a shared work and assign both editions to it.
    let work_id = db::insert_work(&pool, "Dune", "Frank Herbert").await.unwrap();
    db::set_edition_work_id(&pool, ed_a, work_id).await.unwrap();
    db::set_edition_work_id(&pool, ed_b, work_id).await.unwrap();

    // Want entry with Edition B's ISBN.
    insert_want(&pool, "Dune", Some("Frank Herbert"), Some("9780441013570"), 5).await;

    // Grab list: the want entry shares a work_id with owned Edition A → must be empty.
    let grab = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(
        grab.is_empty(),
        "want entry must not appear in grab list when work is already owned via a different edition; got {:?}",
        grab.iter().map(|e| &e.title).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Issue 6 — CSV output quotes special characters
// ---------------------------------------------------------------------------

/// A grab list entry with a comma in the title must produce RFC 4180-quoted
/// output; a title with an internal double-quote must use "" escaping.
#[tokio::test]
async fn test_grab_csv_output_quotes_comma_title() {
    let entry = grab::GrabEntry {
        title: Some("The Comma, A Story".to_string()),
        author: None,
        isbn13: None,
        priority: 5,
        source: "manual".to_string(),
        notes: None,
    };
    let csv = grab::format_csv(&[entry]).unwrap();
    assert!(
        csv.contains("\"The Comma, A Story\""),
        "comma in title must be RFC 4180 quoted: {csv}"
    );
}

/// A title containing a double-quote must be escaped as "" per RFC 4180.
#[tokio::test]
async fn test_grab_csv_output_escapes_internal_quotes() {
    let entry = grab::GrabEntry {
        title: Some("The \"Great\" Escape".to_string()),
        author: None,
        isbn13: None,
        priority: 5,
        source: "manual".to_string(),
        notes: None,
    };
    let csv = grab::format_csv(&[entry]).unwrap();
    // RFC 4180: field with quotes must be wrapped in quotes, internal quotes doubled.
    assert!(
        csv.contains("\"The \"\"Great\"\" Escape\""),
        "double-quotes in title must be escaped as \"\"\"\": {csv}"
    );
}
