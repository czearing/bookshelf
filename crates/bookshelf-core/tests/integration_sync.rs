/// Integration tests for Phase 5 sync feature.
use bookshelf_core::{db, epub::EpubMeta, follow, sync, want};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

fn works_body(titles: &[&str]) -> serde_json::Value {
    let entries: Vec<serde_json::Value> = titles
        .iter()
        .enumerate()
        .map(|(i, t)| serde_json::json!({ "title": t, "key": format!("/works/OL{i}W") }))
        .collect();
    serde_json::json!({ "entries": entries })
}

fn works_empty() -> serde_json::Value {
    serde_json::json!({ "entries": [] })
}

/// Insert an owned edition (owned = 1 via upsert_edition default).
async fn insert_owned(pool: &db::DbPool, title: &str, author: &str, path_suffix: &str) {
    let meta = EpubMeta {
        title: Some(title.to_string()),
        authors: Some(author.to_string()),
        source_path: format!("/tmp/{path_suffix}.epub"),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap();
}

/// Write content to a temp CSV file and return (path, guard).
fn write_temp_csv(content: &str) -> (std::path::PathBuf, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".csv").unwrap();
    std::fs::write(tmp.path(), content).unwrap();
    (tmp.path().to_path_buf(), tmp)
}

// ---------------------------------------------------------------------------
// 1. sync_authors_detailed finds a new work and inserts it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_finds_new_work() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    db::insert_followed_author(&pool, "Frank Herbert", Some("/authors/OL26320A"))
        .await
        .unwrap();

    // Page 1: one new work.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["Dune"])))
        .mount(&server)
        .await;

    // Page 2: empty — stop pagination.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = follow::sync_authors_detailed(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert!(!outcome.had_error);
    assert_eq!(outcome.results.len(), 1);
    assert_eq!(outcome.results[0].works_queued, 1);

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "Dune");
    assert_eq!(wants[0].source, "author_follow");
    assert_eq!(wants[0].priority, 5);
}

// ---------------------------------------------------------------------------
// 2. sync_authors_detailed skips an already-owned work
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_skips_owned() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    insert_owned(&pool, "Dune", "Frank Herbert", "dune").await;

    db::insert_followed_author(&pool, "Frank Herbert", Some("/authors/OL26320A"))
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["Dune"])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = follow::sync_authors_detailed(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert!(!outcome.had_error);
    assert_eq!(outcome.results[0].works_queued, 0);

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "owned work must not be queued");
}

// ---------------------------------------------------------------------------
// 3. sync_authors_detailed skips a work already in the want list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_skips_already_in_want_list() {
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

    db::insert_followed_author(&pool, "Frank Herbert", Some("/authors/OL26320A"))
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["Dune"])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = follow::sync_authors_detailed(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert!(!outcome.had_error);
    assert_eq!(outcome.results[0].works_queued, 0);

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1, "no duplicate must be inserted");
}

// ---------------------------------------------------------------------------
// 4. sync_authors_detailed handles OL error gracefully
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_handles_ol_error() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    db::insert_followed_author(&pool, "Frank Herbert", Some("/authors/OL26320A"))
        .await
        .unwrap();

    // OL returns 500 for this author.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL26320A/works\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = follow::sync_authors_detailed(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert!(outcome.had_error, "had_error must be true on OL 500");
    assert_eq!(outcome.results.len(), 1);
    assert_eq!(outcome.results[0].works_queued, 0);
    assert_eq!(outcome.results[0].name, "Frank Herbert");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty());
}

// ---------------------------------------------------------------------------
// 5. sync_authors_detailed returns empty outcome when no authors followed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_no_followed_authors() {
    let (pool, _tmp) = temp_pool().await;
    let client = reqwest::Client::new();

    let outcome = follow::sync_authors_detailed(&pool, &client, "http://unused", None)
        .await
        .unwrap();

    assert!(!outcome.had_error);
    assert!(outcome.results.is_empty());
}

// ---------------------------------------------------------------------------
// 6. sync::sync_all delegates series step to series::series_fill
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_series_delegates_to_series_fill() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Insert an owned edition that belongs to a series so series_fill has something to check.
    let meta = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        series_name: Some("Dune Chronicles".to_string()),
        series_position: Some("1".to_string()),
        source_path: "/tmp/dune.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    // series_fill will call OL search.json — respond with empty docs.
    Mock::given(method("GET"))
        .and(path_regex(r".*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"docs":[]})))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = sync::sync_all(&pool, &client, &server.uri(), None, false, None).await;

    // series step must have run and returned Ok (even if 0 entries queued).
    let series_result = result.series.expect("series result must be present");
    assert!(series_result.is_ok(), "series_fill must not error on empty OL response");

    // authors step must have run and returned empty (no followed authors).
    let authors_result = result.authors.expect("authors result must be present");
    let outcome = authors_result.unwrap();
    assert!(outcome.results.is_empty());

    // goodreads must be None (no path given).
    assert!(result.goodreads.is_none());
}

// ---------------------------------------------------------------------------
// 7. sync::sync_all delegates goodreads step to want::import_goodreads_csv
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_goodreads_delegates_to_import() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    let csv_content = "\
Book Id,Title,Author l-f,Author,Additional Authors,ISBN,ISBN13,My Rating,Average Rating,Publisher,Binding,Number of Pages,Year Published,Original Publication Year,Date Read,Date Added,Bookshelves,Bookshelves with positions,Exclusive Shelf,My Review,Spoiler,Private Notes,Read Count,Recommended For,Recommended By,Owned Copies,Original Purchase Date,Original Purchase Location,Condition,Condition Description,BCID\n\
1,The Left Hand of Darkness,\"Le Guin, Ursula K.\",Ursula K. Le Guin,,=\"9780441478125\",,0,4.1,Ace,Paperback,286,1987,1969,,,to-read,,to-read,,,,0,,,,,,,,\n\
";
    let (csv_path, _csv_guard) = write_temp_csv(csv_content);

    // series_fill calls OL search.json — respond with empty docs.
    Mock::given(method("GET"))
        .and(path_regex(r".*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"docs":[]})))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result =
        sync::sync_all(&pool, &client, &server.uri(), Some(&csv_path), false, None).await;

    let goodreads = result.goodreads.expect("goodreads result must be present");
    let summary = goodreads.unwrap();
    assert_eq!(summary.imported, 1, "one CSV row must be imported");

    let wants = db::list_want(&pool, Some("goodreads_csv"), None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "The Left Hand of Darkness");
}

// ---------------------------------------------------------------------------
// 8. sync_authors_detailed queues 0 works when OL returns empty entries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_authors_empty_entries() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    db::insert_followed_author(&pool, "New Author", Some("/authors/OL99999A"))
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL99999A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let outcome = follow::sync_authors_detailed(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert!(!outcome.had_error);
    assert_eq!(outcome.results.len(), 1);
    assert_eq!(outcome.results[0].works_queued, 0);
    assert_eq!(outcome.results[0].name, "New Author");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty());
}
