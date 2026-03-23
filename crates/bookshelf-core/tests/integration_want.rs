/// Integration tests for Phase 2 want-list ingestion.
///
/// Each test that touches the DB gets its own temp file via `temp_pool()`.
/// HTTP-dependent tests use wiremock — no real API calls are made.
use bookshelf_core::{
    db,
    epub::EpubMeta,
    want,
};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// Write a temp CSV file and return its path + temp file guard.
fn write_temp_csv(content: &str) -> (std::path::PathBuf, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".csv").unwrap();
    std::fs::write(tmp.path(), content).unwrap();
    (tmp.path().to_path_buf(), tmp)
}

/// Write a temp text file and return its path + temp file guard.
fn write_temp_txt(content: &str) -> (std::path::PathBuf, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".txt").unwrap();
    std::fs::write(tmp.path(), content).unwrap();
    (tmp.path().to_path_buf(), tmp)
}

// ---------------------------------------------------------------------------
// Schema tests (AC-1/2/3/4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_want_list_table_exists_after_open() {
    let (pool, _tmp) = temp_pool().await;
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='want_list'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(row.is_some(), "want_list table must exist after db::open");
}

#[tokio::test]
async fn test_double_open_does_not_fail_or_alter_rows() {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool1 = db::open(tmp.path()).await.unwrap();
    db::insert_want(&pool1, "Test Book", Some("Author"), None, "manual", None, 5, None)
        .await
        .unwrap();
    drop(pool1);

    let pool2 = db::open(tmp.path()).await.unwrap();
    let rows = db::list_want(&pool2, None).await.unwrap();
    assert_eq!(rows.len(), 1, "row must survive second open");
    assert_eq!(rows[0].title, "Test Book");
}

#[tokio::test]
async fn test_priority_validation_rejects_out_of_range() {
    let (pool, _tmp) = temp_pool().await;
    let err0 = want::add_manual(&pool, "Book", None, None, 0, None).await;
    assert!(err0.is_err(), "priority 0 must be rejected");
    let err11 = want::add_manual(&pool, "Book", None, None, 11, None).await;
    assert!(err11.is_err(), "priority 11 must be rejected");
    let ok = want::add_manual(&pool, "Book", None, None, 5, None).await;
    assert!(ok.is_ok(), "priority 5 must be accepted");
}

#[tokio::test]
async fn test_added_at_format() {
    let (pool, _tmp) = temp_pool().await;
    let id = db::insert_want(&pool, "Title", None, None, "manual", None, 5, None)
        .await
        .unwrap();
    let row = db::get_want(&pool, id).await.unwrap().unwrap();
    // Format: YYYY-MM-DDTHH:MM:SSZ — 20 chars, ends with Z, contains T
    assert_eq!(row.added_at.len(), 20, "added_at must be 20 chars: {}", row.added_at);
    assert!(row.added_at.ends_with('Z'), "added_at must end with Z: {}", row.added_at);
    assert!(row.added_at.contains('T'), "added_at must contain T: {}", row.added_at);
}

// ---------------------------------------------------------------------------
// Goodreads CSV import (AC-5/6/7/8/9/10/11/12/13)
// ---------------------------------------------------------------------------

const GOODREADS_HEADER: &str =
    "Book Id,Title,Author,Author l-f,Additional Authors,ISBN,ISBN13,My Rating,Average Rating,Publisher,Binding,Number of Pages,Year Published,Original Publication Year,Date Read,Date Added,Bookshelves,Bookshelves with positions,Exclusive Shelf,My Review,Spoiler,Private Notes,Read Count,Owned Copies\n";

/// Build a standard Goodreads CSV row with 24 fields matching GOODREADS_HEADER.
/// Shelf defaults to "to-read" so the shelf filter passes.
/// Column layout (24 columns, 23 commas):
///   Book Id(0),Title(1),Author(2),Author l-f(3),Additional Authors(4),
///   ISBN(5),ISBN13(6),My Rating(7),Average Rating(8),Publisher(9),
///   Binding(10),Number of Pages(11),Year Published(12),
///   Original Publication Year(13),Date Read(14),Date Added(15),
///   Bookshelves(16),Bookshelves with positions(17),Exclusive Shelf(18),
///   My Review(19),Spoiler(20),Private Notes(21),Read Count(22),Owned Copies(23)
fn goodreads_row(book_id: &str, title: &str, author: &str, isbn13: &str) -> String {
    // author at col2, isbn13 at col6 (3 empty cols between), shelf at col18 (11 empty after isbn13)
    // trailing 5 empty cols after shelf.
    format!(
        "{book_id},{title},{author},,,,{isbn13},,,,,,,,,,,,to-read,,,,,\n"
    )
}

/// Build a Goodreads row with an explicit ISBN-10 at col5 and a custom shelf.
fn goodreads_row_with_isbn10_and_shelf(
    book_id: &str,
    title: &str,
    author: &str,
    isbn10: &str,
    isbn13: &str,
    shelf: &str,
) -> String {
    // isbn10 at col5: author(2), ""(3), ""(4), isbn10(5), isbn13(6)
    // Then 12 empty cols (7-17), shelf(18), 5 empty cols (19-23) = 23 commas total.
    format!(
        "{book_id},{title},{author},,,{isbn10},{isbn13},,,,,,,,,,,,{shelf},,,,,\n"
    )
}

#[tokio::test]
async fn test_goodreads_csv_imports_correctly() {
    let (pool, _tmp) = temp_pool().await;
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("123", "Dune", "Frank Herbert", "9780441013593")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 1);
    assert_eq!(summary.skipped_owned, 0);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Dune");
    assert_eq!(rows[0].author.as_deref(), Some("Frank Herbert"));
    assert_eq!(rows[0].isbn13.as_deref(), Some("9780441013593"));
    assert_eq!(rows[0].source, "goodreads_csv");
    assert_eq!(rows[0].source_id.as_deref(), Some("123"));
}

#[tokio::test]
async fn test_goodreads_csv_unwraps_isbn_format() {
    let (pool, _tmp) = temp_pool().await;
    // Goodreads wraps ISBN as ="9780261102217" in the CSV
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("1", "The Hobbit", "J.R.R. Tolkien", "=\"9780261102217\"")
    );
    let (path, _file) = write_temp_csv(&csv);
    want::import_goodreads_csv(&pool, &path, false).await.unwrap();

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows[0].isbn13.as_deref(), Some("9780261102217"));
}

#[tokio::test]
async fn test_goodreads_csv_null_isbn_stored_as_null() {
    let (pool, _tmp) = temp_pool().await;
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("2", "No ISBN Book", "Some Author", "")
    );
    let (path, _file) = write_temp_csv(&csv);
    want::import_goodreads_csv(&pool, &path, false).await.unwrap();

    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows[0].isbn13.is_none(), "empty ISBN13 must be stored as NULL");
}

#[tokio::test]
async fn test_goodreads_csv_file_not_found() {
    let (pool, _tmp) = temp_pool().await;
    let result = want::import_goodreads_csv(
        &pool,
        std::path::Path::new("/nonexistent/path/missing.csv"),
        false,
    )
    .await;
    assert!(result.is_err(), "must return Err for missing file");
}

#[tokio::test]
async fn test_goodreads_csv_missing_required_column() {
    let (pool, _tmp) = temp_pool().await;
    // CSV without "Book Id" column
    let csv = "Title,Author,ISBN13\nDune,Frank Herbert,9780441013593\n";
    let (path, _file) = write_temp_csv(csv);
    let result = want::import_goodreads_csv(&pool, &path, false).await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("Book Id") || msg.contains("column"),
        "error must mention missing column: {msg}"
    );
}

#[tokio::test]
async fn test_goodreads_csv_skips_already_owned() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition with matching ISBN.
    let meta = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        isbn: Some("9780441013593".to_string()),
        source_path: "/tmp/dune.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("1", "Dune", "Frank Herbert", "9780441013593")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.skipped_owned, 1);
    assert_eq!(summary.imported, 0);

    let want_rows = db::list_want(&pool, None).await.unwrap();
    assert!(want_rows.is_empty(), "already-owned book must not be in want_list");
}

#[tokio::test]
async fn test_goodreads_csv_deduplicates_within_want_list() {
    let (pool, _tmp) = temp_pool().await;

    // Pre-insert want row with same ISBN.
    db::insert_want(
        &pool,
        "Old Title",
        Some("Old Author"),
        Some("9780441013593"),
        "goodreads_csv",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("42", "Dune", "Frank Herbert", "9780441013593")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 1);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1, "must not insert a duplicate row");
    assert_eq!(rows[0].title, "Dune", "title must be updated");
    assert_eq!(rows[0].source_id.as_deref(), Some("42"));
}

// ---------------------------------------------------------------------------
// OL reading list import (AC-14/15/16/17/18/19)
// ---------------------------------------------------------------------------

fn ol_response_body(entries: &[(&str, &str, &str)]) -> serde_json::Value {
    // entries: (title, author, key)
    let log_entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|(title, author, key)| {
            serde_json::json!({
                "work": {
                    "title": title,
                    "key": key,
                    "author_names": if author.is_empty() { vec![] } else { vec![author] },
                    "first_publish_year": 2000
                },
                "logged_edition": null,
                "logged_date": "2026/01/01, 00:00:00"
            })
        })
        .collect();

    serde_json::json!({
        "page": 1,
        "numFound": entries.len(),
        "reading_log_entries": log_entries
    })
}

#[tokio::test]
async fn test_openlibrary_import_basic() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    let body = ol_response_body(&[("Dune", "Frank Herbert", "/works/OL102W")]);
    Mock::given(method("GET"))
        .and(path_regex(r"/people/testuser/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    // Second page returns empty.
    Mock::given(method("GET"))
        .and(path_regex(r"/people/testuser/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "page": 2,
            "numFound": 1,
            "reading_log_entries": []
        })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let summary = want::import_openlibrary(&pool, &client, "testuser", &server.uri())
        .await
        .unwrap();

    assert_eq!(summary.imported, 1);
    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Dune");
    assert_eq!(rows[0].author.as_deref(), Some("Frank Herbert"));
    assert_eq!(rows[0].source_id.as_deref(), Some("/works/OL102W"));
    assert_eq!(rows[0].source, "openlibrary");
}

#[tokio::test]
async fn test_openlibrary_import_null_isbn() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    let body = ol_response_body(&[("Dune", "Frank Herbert", "/works/OL102W")]);
    Mock::given(method("GET"))
        .and(path_regex(r"/people/u1/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    want::import_openlibrary(&pool, &client, "u1", &server.uri())
        .await
        .unwrap();

    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows[0].isbn13.is_none(), "isbn13 must be NULL when OL doesn't provide it");
}

#[tokio::test]
async fn test_openlibrary_import_404() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/people/nobody/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = want::import_openlibrary(&pool, &client, "nobody", &server.uri()).await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("nobody") && msg.contains("not found"),
        "error must mention username and not found: {msg}"
    );
    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows.is_empty(), "no rows must be inserted on 404");
}

#[tokio::test]
async fn test_openlibrary_import_http_error() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/people/erruser/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = want::import_openlibrary(&pool, &client, "erruser", &server.uri()).await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("500"), "error must include HTTP status code: {msg}");
}

#[tokio::test]
async fn test_openlibrary_import_empty_list() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/people/emptyuser/books/want-to-read\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "page": 1,
            "numFound": 0,
            "reading_log_entries": []
        })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let summary = want::import_openlibrary(&pool, &client, "emptyuser", &server.uri())
        .await
        .unwrap();
    assert_eq!(summary.imported, 0);
    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_openlibrary_import_pagination() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Page 1: one entry, numFound=2 so paginator knows there are more
    let body_p1 = serde_json::json!({
        "page": 1,
        "numFound": 2,
        "reading_log_entries": [{
            "work": {
                "title": "Neuromancer",
                "key": "/works/OL1W",
                "author_names": ["William Gibson"],
                "first_publish_year": 1984
            },
            "logged_edition": null,
            "logged_date": "2026/01/01, 00:00:00"
        }]
    });
    // Page 2: one entry
    let body_p2 = serde_json::json!({
        "page": 2,
        "numFound": 2,
        "reading_log_entries": [{
            "work": {
                "title": "Foundation",
                "key": "/works/OL2W",
                "author_names": ["Isaac Asimov"],
                "first_publish_year": 1951
            },
            "logged_edition": null,
            "logged_date": "2026/01/02, 00:00:00"
        }]
    });
    // Page 3: empty — stop signal (won't be reached but set up as fallback)
    let body_p3 = serde_json::json!({
        "page": 3,
        "numFound": 2,
        "reading_log_entries": []
    });

    // Register mocks in reverse priority order (wiremock uses LIFO matching).
    // Page 3 registered first (lowest priority), page 1 last (highest priority).
    Mock::given(method("GET"))
        .and(path_regex(r"/people/pageuser/books/want-to-read\.json"))
        .and(query_param("page", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body_p3))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/people/pageuser/books/want-to-read\.json"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body_p2))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/people/pageuser/books/want-to-read\.json"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body_p1))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let summary = want::import_openlibrary(&pool, &client, "pageuser", &server.uri())
        .await
        .unwrap();
    assert_eq!(summary.imported, 2, "both pages must be imported");

    let rows = db::list_want(&pool, None).await.unwrap();
    let titles: Vec<String> = rows.iter().map(|r| r.title.clone()).collect();
    assert!(titles.iter().any(|t| t == "Neuromancer"), "Neuromancer not found in {:?}", titles);
    assert!(titles.iter().any(|t| t == "Foundation"), "Foundation not found in {:?}", titles);
}

// ---------------------------------------------------------------------------
// Text file import (AC-20/21/22/23)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_text_import_by_format() {
    let (pool, _tmp) = temp_pool().await;
    let content = "The Hobbit by J.R.R. Tolkien\nDune - Frank Herbert\nNeuromancer\n";
    let (path, _file) = write_temp_txt(content);
    let summary = want::import_text_file(&pool, &path).await.unwrap();
    assert_eq!(summary.imported, 3);

    let rows = db::list_want(&pool, None).await.unwrap();
    let titles: Vec<&str> = rows.iter().map(|r| r.title.as_str()).collect();
    assert!(titles.contains(&"The Hobbit"), "by-delimiter title");
    assert!(titles.contains(&"Dune"), "dash-delimiter title");
    assert!(titles.contains(&"Neuromancer"), "title-only");

    let hobbit = rows.iter().find(|r| r.title == "The Hobbit").unwrap();
    assert_eq!(hobbit.author.as_deref(), Some("J.R.R. Tolkien"));
    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.author.as_deref(), Some("Frank Herbert"));
    let neuro = rows.iter().find(|r| r.title == "Neuromancer").unwrap();
    assert!(neuro.author.is_none());
}

#[tokio::test]
async fn test_text_import_skips_empty_and_comments() {
    let (pool, _tmp) = temp_pool().await;
    let content = "\n# This is a comment\n   \nReal Book by An Author\n";
    let (path, _file) = write_temp_txt(content);
    let summary = want::import_text_file(&pool, &path).await.unwrap();
    assert_eq!(summary.imported, 1);
    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Real Book");
}

#[tokio::test]
async fn test_text_import_file_not_found() {
    let (pool, _tmp) = temp_pool().await;
    let result =
        want::import_text_file(&pool, std::path::Path::new("/no/such/file.txt")).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Manual add (AC-24 through AC-31)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_want_add_title_only() {
    let (pool, _tmp) = temp_pool().await;
    let result = want::add_manual(&pool, "Just a Title", None, None, 5, None)
        .await
        .unwrap();
    assert_eq!(result, want::AddResult::Inserted);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.title, "Just a Title");
    assert!(row.author.is_none());
    assert!(row.isbn13.is_none());
    assert_eq!(row.priority, 5);
    assert!(row.notes.is_none());
    assert_eq!(row.source, "manual");
    assert!(row.source_id.is_none());
}

#[tokio::test]
async fn test_want_add_with_all_fields() {
    let (pool, _tmp) = temp_pool().await;
    let result = want::add_manual(
        &pool,
        "Full Book",
        Some("Full Author"),
        Some("9781234567890"),
        3,
        Some("A note"),
    )
    .await
    .unwrap();
    assert_eq!(result, want::AddResult::Inserted);

    let rows = db::list_want(&pool, None).await.unwrap();
    let row = &rows[0];
    assert_eq!(row.author.as_deref(), Some("Full Author"));
    assert_eq!(row.isbn13.as_deref(), Some("9781234567890"));
    assert_eq!(row.priority, 3);
    assert_eq!(row.notes.as_deref(), Some("A note"));
}

#[tokio::test]
async fn test_want_add_already_owned() {
    let (pool, _tmp) = temp_pool().await;

    let meta = EpubMeta {
        title: Some("Owned Book".to_string()),
        authors: Some("Known Author".to_string()),
        isbn: Some("9780000000001".to_string()),
        source_path: "/tmp/owned.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    let result = want::add_manual(
        &pool,
        "Owned Book",
        Some("Known Author"),
        Some("9780000000001"),
        5,
        None,
    )
    .await
    .unwrap();
    assert_eq!(result, want::AddResult::AlreadyOwned);
    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_want_add_already_in_want_list() {
    let (pool, _tmp) = temp_pool().await;

    // Pre-insert want row.
    db::insert_want(
        &pool,
        "Existing Book",
        Some("Existing Author"),
        Some("9780000000002"),
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    // Add with same ISBN — should update not insert.
    let result = want::add_manual(
        &pool,
        "Existing Book",
        Some("Existing Author"),
        Some("9780000000002"),
        7,
        Some("updated note"),
    )
    .await
    .unwrap();
    assert_eq!(result, want::AddResult::AlreadyInWantList);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1, "must not insert a duplicate");
    assert_eq!(rows[0].priority, 7, "priority must be updated to new value");
    assert_eq!(
        rows[0].notes.as_deref(),
        Some("updated note"),
        "notes must be updated to new value"
    );
}

// ---------------------------------------------------------------------------
// Want enrich (AC-35 through AC-40)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_want_enrich_finds_isbn() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    db::insert_want(
        &pool,
        "Dune",
        Some("Frank Herbert"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let search_body = serde_json::json!({
        "docs": [{ "isbn": ["9780441013593"] }]
    });
    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(search_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(eligible, 1);
    assert_eq!(enriched, 1);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows[0].isbn13.as_deref(), Some("9780441013593"));
}

#[tokio::test]
async fn test_want_enrich_skips_already_has_isbn() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Row already has isbn13 — should NOT be in the eligible set.
    db::insert_want(
        &pool,
        "Already Enriched",
        Some("Author"),
        Some("9780000000001"),
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(eligible, 0, "row with isbn13 must not be eligible");
    assert_eq!(enriched, 0);
}

#[tokio::test]
async fn test_want_enrich_skips_missing_author() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Row with NULL author — returned by the query but skipped by the guard
    // with a stderr warning; isbn13 must remain NULL.
    db::insert_want(&pool, "Title Only", None, None, "manual", None, 5, None)
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(eligible, 1, "row without author is returned but skipped");
    assert_eq!(enriched, 0);

    // isbn13 must still be NULL.
    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].isbn13.is_none(), "isbn13 must remain NULL for skipped row");
}

#[tokio::test]
async fn test_want_enrich_no_result_leaves_row_unchanged() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    db::insert_want(
        &pool,
        "Unknown Book",
        Some("Unknown Author"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    // Return empty docs from search.
    let empty_body = serde_json::json!({ "docs": [] });
    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(eligible, 1);
    assert_eq!(enriched, 0);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert!(rows[0].isbn13.is_none(), "row must remain unchanged when no result");
}

#[tokio::test]
async fn test_want_enrich_summary_counts() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // 3 eligible rows: 2 will get ISBNs (isbn search returns data), 1 won't.
    for (title, author) in &[
        ("Book A", "Author A"),
        ("Book B", "Author B"),
        ("Book C", "Author C"),
    ] {
        db::insert_want(&pool, title, Some(author), None, "manual", None, 5, None)
            .await
            .unwrap();
    }

    // Mock: A and B return ISBN, C returns empty.
    // We use a single mock that returns ISBN for any search (simpler).
    // For the third request we need different behavior, so use separate mocks with limited hits.
    let isbn_body = serde_json::json!({ "docs": [{ "isbn": ["9780000000099"] }] });
    let empty_body = serde_json::json!({ "docs": [] });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(isbn_body))
        .expect(2)
        .up_to_n_times(2)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(eligible, 3);
    assert_eq!(enriched, 2);
}

// ---------------------------------------------------------------------------
// Issue 1 — Hyphenated ISBN normalization (E2E)
// ---------------------------------------------------------------------------

/// An EPUB with isbn "978-0-441-01359-3" must match a want entry with
/// isbn13 "9780441013593" — the book must NOT appear on the grab list.
#[tokio::test]
async fn test_e2e_hyphenated_isbn_recognized_as_owned() {
    use bookshelf_core::grab;

    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition with hyphenated ISBN (as Calibre might generate).
    let meta = bookshelf_core::EpubMeta {
        title: Some("Neuromancer".to_string()),
        authors: Some("William Gibson".to_string()),
        isbn: Some("978-0-441-01359-3".to_string()),
        source_path: "/tmp/neuromancer_hyphen.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    // Want entry with normalized ISBN.
    db::insert_want(
        &pool,
        "Neuromancer",
        Some("William Gibson"),
        Some("9780441013593"),
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let grab_list = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(
        grab_list.is_empty(),
        "hyphenated EPUB ISBN must match normalized want entry; grab list must be empty, got {:?}",
        grab_list.iter().map(|e| &e.title).collect::<Vec<_>>()
    );
}

/// Import a Goodreads CSV where the want entry has a normalized ISBN13,
/// and the owned EPUB in DB has a hyphenated ISBN — they must match
/// (owned book must not appear on grab list).
#[tokio::test]
async fn test_goodreads_csv_normalized_isbn_matches_hyphenated_epub() {
    use bookshelf_core::grab;

    let (pool, _tmp) = temp_pool().await;

    // Owned edition with hyphenated ISBN (simulates Calibre-generated metadata).
    let meta = bookshelf_core::EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        isbn: Some("978-0-441-01359-3".to_string()),
        source_path: "/tmp/dune_hyphen.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    // Goodreads CSV with normalized ISBN13 (no hyphens).
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("1", "Dune", "Frank Herbert", "9780441013593")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();

    // The CSV import should recognize the ISBN match and skip_owned.
    assert_eq!(
        summary.skipped_owned, 1,
        "hyphenated EPUB ISBN must be recognized as owned during CSV import"
    );
    assert_eq!(summary.imported, 0);

    // Grab list must be empty.
    let grab_list = grab::compute_grab_list(&pool, None).await.unwrap();
    assert!(
        grab_list.is_empty(),
        "owned book with hyphenated ISBN must not appear on grab list"
    );
}

// ---------------------------------------------------------------------------
// Issue 2 — ISBN-10 fallback (E2E)
// ---------------------------------------------------------------------------

/// A CSV row with blank ISBN13 but a valid ISBN10 must be stored with the
/// converted ISBN-13 in want_list.
#[tokio::test]
async fn test_goodreads_csv_isbn10_fallback_stores_isbn13() {
    let (pool, _tmp) = temp_pool().await;

    // Build a CSV row where ISBN13 is empty but ISBN (col 5) = "0441013597".
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row_with_isbn10_and_shelf("1", "Dune", "Frank Herbert", "0441013597", "", "to-read")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 1, "ISBN10-only row must be imported");

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].isbn13.as_deref(),
        Some("9780441013593"),
        "ISBN-10 0441013597 must be converted to ISBN-13 9780441013593; got {:?}",
        rows[0].isbn13
    );
}

// ---------------------------------------------------------------------------
// Issue 3 — Exclusive Shelf filter
// ---------------------------------------------------------------------------

/// Only "to-read" shelf rows must be imported; "read" and "currently-reading"
/// rows must be skipped.
#[tokio::test]
async fn test_goodreads_csv_shelf_filter_only_to_read() {
    let (pool, _tmp) = temp_pool().await;

    // Three rows on different shelves.
    let csv = format!(
        "{GOODREADS_HEADER}{}{}{}",
        goodreads_row_with_isbn10_and_shelf("1", "Book A", "Author A", "", "", "to-read"),
        goodreads_row_with_isbn10_and_shelf("2", "Book B", "Author B", "", "", "read"),
        goodreads_row_with_isbn10_and_shelf("3", "Book C", "Author C", "", "", "currently-reading"),
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();

    // Only "Book A" should be imported.
    assert_eq!(summary.imported, 1, "only to-read shelf rows must be imported");
    // "read" and "currently-reading" are counted as skipped_rows.
    assert_eq!(summary.skipped_rows, 2, "non-to-read shelves must increment skipped_rows");

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "Book A");
}

/// With all_shelves=true, all shelf rows are imported.
#[tokio::test]
async fn test_goodreads_csv_all_shelves_imports_all() {
    let (pool, _tmp) = temp_pool().await;

    let csv = format!(
        "{GOODREADS_HEADER}{}{}{}",
        goodreads_row_with_isbn10_and_shelf("1", "Book A", "Author A", "", "", "to-read"),
        goodreads_row_with_isbn10_and_shelf("2", "Book B", "Author B", "", "", "read"),
        goodreads_row_with_isbn10_and_shelf("3", "Book C", "Author C", "", "", "currently-reading"),
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, true).await.unwrap();

    assert_eq!(summary.imported, 3, "all_shelves=true must import all rows");
    assert_eq!(summary.skipped_rows, 0);
}

// ---------------------------------------------------------------------------
// Issue 4a — Titles with commas
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goodreads_csv_title_with_comma() {
    let (pool, _tmp) = temp_pool().await;

    // RFC 4180 quoted title containing a comma — use the row helper which produces correct field count.
    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("1", "\"Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch\"", "Terry Pratchett", "9780060853983")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 1);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(
        rows[0].title,
        "Good Omens: The Nice and Accurate Prophecies of Agnes Nutter, Witch"
    );
}

// ---------------------------------------------------------------------------
// Issue 4b — Unicode author names
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goodreads_csv_unicode_author() {
    let (pool, _tmp) = temp_pool().await;

    let csv = format!(
        "{GOODREADS_HEADER}{}",
        goodreads_row("1", "The Master and Margarita", "Михаил Булгаков", "9780141180144")
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 1);

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows[0].author.as_deref(), Some("Михаил Булгаков"));
}

// ---------------------------------------------------------------------------
// Issue 4c — Empty rows in CSV are skipped without error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goodreads_csv_empty_rows_skipped() {
    let (pool, _tmp) = temp_pool().await;

    // A CSV with blank lines interspersed.
    let csv = format!(
        "{GOODREADS_HEADER}{}\n{}",
        goodreads_row("1", "Dune", "Frank Herbert", "9780441013593"),
        goodreads_row("2", "Foundation", "Isaac Asimov", "9780553382570"),
    );
    let (path, _file) = write_temp_csv(&csv);
    let summary = want::import_goodreads_csv(&pool, &path, false).await.unwrap();
    assert_eq!(summary.imported, 2, "blank lines must be skipped silently");
    assert_eq!(summary.skipped_rows, 0);
}

// ---------------------------------------------------------------------------
// Issue 4d — Windows CRLF line endings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goodreads_csv_crlf_line_endings() {
    let (pool, _tmp) = temp_pool().await;

    // Build CSV with explicit \r\n line endings — replace \n with \r\n.
    let row = goodreads_row("1", "Dune", "Frank Herbert", "9780441013593");
    let crlf_csv = format!("{GOODREADS_HEADER}{row}")
        .replace('\n', "\r\n");
    let csv_bytes = crlf_csv.into_bytes();

    let tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    std::fs::write(tmp.path(), &csv_bytes).unwrap();

    let summary = want::import_goodreads_csv(&pool, tmp.path(), false)
        .await
        .unwrap();
    assert_eq!(summary.imported, 1, "CRLF CSV must parse correctly");

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows[0].title, "Dune");
}

// ---------------------------------------------------------------------------
// Issue 4e — Additional Authors field is ignored (primary author used)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goodreads_csv_additional_authors_ignored() {
    let (pool, _tmp) = temp_pool().await;

    // Good Omens: Terry Pratchett is Author, Neil Gaiman is in Additional Authors (col 4).
    // Use the full-row helper with isbn10="" (col5 empty), isbn13="" (col6 empty),
    // but we need to put "Neil Gaiman" in col4. Build manually with correct 24 fields:
    // col0=1, col1=Good Omens, col2=Terry Pratchett, col3=Pratchett col Terry,
    // col4=Neil Gaiman, col5="" (ISBN), col6="" (ISBN13), cols 7-17 empty,
    // col18=to-read, cols 19-23 empty = 23 commas total.
    // col0=1, col1-4 as set, col5-17 empty (13 commas from col4 to col17), col18=to-read, col19-23 empty.
    let csv = format!(
        "{GOODREADS_HEADER}1,\"Good Omens\",\"Terry Pratchett\",\"Pratchett, Terry\",\"Neil Gaiman\",,,,,,,,,,,,,,to-read,,,,,\n"
    );
    let (path, _file) = write_temp_csv(&csv);
    want::import_goodreads_csv(&pool, &path, false).await.unwrap();

    let rows = db::list_want(&pool, None).await.unwrap();
    // The author stored must be "Terry Pratchett" only — Additional Authors is ignored.
    assert_eq!(
        rows[0].author.as_deref(),
        Some("Terry Pratchett"),
        "only primary Author column must be stored; Additional Authors must be ignored"
    );
}
