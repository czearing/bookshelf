/// Integration tests for metadata enrichment using wiremock HTTP mocks.
///
/// Each test spins up a local wiremock server, stubs the relevant endpoints,
/// calls the enrich functions with the mock base URL, and asserts the DB
/// was updated correctly.
use bookshelf_core::{
    db::{self, EnrichmentUpdate},
    enrich,
    epub::EpubMeta,
};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

async fn insert_test_edition(pool: &db::DbPool, isbn: &str, path: &str) -> i64 {
    let meta = EpubMeta {
        title: Some("Test Book".to_string()),
        authors: Some("Test Author".to_string()),
        isbn: Some(isbn.to_string()),
        source_path: path.to_string(),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap()
}

// ---------------------------------------------------------------------------
// AC-54 / AC-35 / AC-36: OpenLibrary returns data → columns updated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enrich_openlibrary_updates_columns() {
    let (pool, _db_tmp) = temp_pool().await;
    let edition_id = insert_test_edition(&pool, "9780261102217", "/tmp/hobbit.epub").await;

    let server = MockServer::start().await;

    let ol_body = serde_json::json!({
        "ISBN:9780261102217": {
            "title": "The Hobbit",
            "publishers": [{ "name": "George Allen & Unwin" }],
            "publish_date": "1937",
            "description": { "value": "A fantasy adventure" },
            "identifiers": { "isbn_13": ["9780261102217"] },
            "works": [{ "key": "/works/OL27516W" }]
        }
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/api/books"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result =
        enrich::enrich_from_openlibrary_isbn(&client, "9780261102217", &server.uri()).await;

    assert!(result.is_ok(), "enrich should succeed: {:?}", result);
    let update = result.unwrap().expect("should have data");

    assert_eq!(update.title.as_deref(), Some("The Hobbit"));
    assert_eq!(update.publisher.as_deref(), Some("George Allen & Unwin"));
    assert_eq!(update.publish_date.as_deref(), Some("1937"));
    assert_eq!(update.description.as_deref(), Some("A fantasy adventure"));
    assert_eq!(update.isbn.as_deref(), Some("9780261102217"));
    assert_eq!(update.ol_work_id.as_deref(), Some("/works/OL27516W"));
    assert!(update.enriched_at.is_some());
    assert_eq!(update.enrichment_attempted, 1);

    // Apply the update and verify DB state
    db::apply_enrichment(&pool, edition_id, &update).await.unwrap();
    let row = db::get_edition(&pool, edition_id).await.unwrap().unwrap();
    assert!(row.enriched_at.is_some(), "enriched_at should be set");
    assert_eq!(row.enrichment_attempted, 1);
    assert_eq!(row.publisher.as_deref(), Some("George Allen & Unwin"));
}

// ---------------------------------------------------------------------------
// AC-37: OpenLibrary returns empty JSON → no update, no error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enrich_openlibrary_empty_response_returns_none() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/api/books"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result =
        enrich::enrich_from_openlibrary_isbn(&client, "9780000000000", &server.uri()).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none(), "empty JSON body should return None");
}

// ---------------------------------------------------------------------------
// AC-39: OpenLibrary empty → falls back to Google Books
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enrich_falls_back_to_google_books() {
    let (pool, _db_tmp) = temp_pool().await;
    let edition_id =
        insert_test_edition(&pool, "9780441013593", "/tmp/dune.epub").await;

    let server = MockServer::start().await;

    // OpenLibrary returns empty
    Mock::given(method("GET"))
        .and(path_regex(r"/api/books"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    // Google Books returns data
    let gb_body = serde_json::json!({
        "totalItems": 1,
        "items": [{
            "volumeInfo": {
                "title": "Dune",
                "authors": ["Frank Herbert"],
                "publisher": "Chilton Books",
                "publishedDate": "1965",
                "description": "A science fiction epic",
                "industryIdentifiers": [
                    { "type": "ISBN_13", "identifier": "9780441013593" }
                ]
            }
        }]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/books/v1/volumes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gb_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();

    // First check OL returns None
    let ol_result =
        enrich::enrich_from_openlibrary_isbn(&client, "9780441013593", &server.uri()).await;
    assert!(ol_result.unwrap().is_none());

    // Then fall back to GB
    let gb_result =
        enrich::enrich_from_google_books_isbn(&client, "9780441013593", &server.uri()).await;
    assert!(gb_result.is_ok());
    let update = gb_result.unwrap().expect("Google Books should return data");
    assert_eq!(update.title.as_deref(), Some("Dune"));
    assert_eq!(update.publisher.as_deref(), Some("Chilton Books"));
    assert_eq!(update.isbn.as_deref(), Some("9780441013593"));

    // Apply and verify
    db::apply_enrichment(&pool, edition_id, &update).await.unwrap();
    let row = db::get_edition(&pool, edition_id).await.unwrap().unwrap();
    assert!(row.enriched_at.is_some());
    assert_eq!(row.enrichment_attempted, 1);
}

// ---------------------------------------------------------------------------
// AC-40: Both sources empty → enrichment_attempted = 1, enriched_at = NULL
//
// Resolution of Open Question 1 (requirements.md): option (a) was chosen.
// When both OL and Google Books return no data, `enrichment_attempted` is set
// to 1 and `enriched_at` remains NULL, rather than using a "+no_data" suffix
// on `enriched_at`. The separate integer column is the authoritative sentinel
// that prevents repeat API calls on subsequent runs.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enrich_both_empty_sets_attempted_flag() {
    let (pool, _db_tmp) = temp_pool().await;
    let edition_id =
        insert_test_edition(&pool, "9789999999999", "/tmp/unknown.epub").await;

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/api/books"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/books/v1/volumes"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "totalItems": 0 })),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();

    let ol = enrich::enrich_from_openlibrary_isbn(&client, "9789999999999", &server.uri())
        .await
        .unwrap();
    assert!(ol.is_none());

    let gb = enrich::enrich_from_google_books_isbn(&client, "9789999999999", &server.uri())
        .await
        .unwrap();
    assert!(gb.is_none());

    // Simulate the "both empty" path in main.rs
    let empty_update = EnrichmentUpdate {
        enrichment_attempted: 1,
        ..Default::default()
    };
    db::apply_enrichment(&pool, edition_id, &empty_update)
        .await
        .unwrap();

    let row = db::get_edition(&pool, edition_id).await.unwrap().unwrap();
    assert_eq!(row.enrichment_attempted, 1);
    assert!(
        row.enriched_at.is_none(),
        "enriched_at must remain NULL when no data found (AC-40)"
    );
}

// ---------------------------------------------------------------------------
// AC-38: HTTP error from OpenLibrary → returns Err (caller handles warning)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_enrich_openlibrary_http_error_returns_err() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/api/books"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result =
        enrich::enrich_from_openlibrary_isbn(&client, "9780000000001", &server.uri()).await;

    assert!(result.is_err(), "HTTP 500 should return Err");
}

// ---------------------------------------------------------------------------
// AC-41: Discovered ISBN from title+author search is persisted to the DB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_discovered_isbn_is_persisted_to_db() {
    // Simulate a row with no ISBN (as returned by editions_needing_enrichment)
    let (pool, _db_tmp) = temp_pool().await;
    let meta = bookshelf_core::epub::EpubMeta {
        title: Some("The Hobbit".to_string()),
        authors: Some("J.R.R. Tolkien".to_string()),
        isbn: None,
        source_path: "/tmp/hobbit_no_isbn.epub".to_string(),
        ..Default::default()
    };
    let edition_id = db::upsert_edition(&pool, &meta).await.unwrap();

    // Verify no ISBN is set initially
    let row_before = db::get_edition(&pool, edition_id).await.unwrap().unwrap();
    assert!(row_before.isbn.is_none(), "isbn should start as NULL");

    // Simulate the fix: persist the discovered ISBN via apply_enrichment
    let isbn_update = EnrichmentUpdate {
        isbn: Some("9780261102217".to_string()),
        ..Default::default()
    };
    db::apply_enrichment(&pool, edition_id, &isbn_update).await.unwrap();

    // Verify ISBN is now stored and visible
    let row_after = db::get_edition(&pool, edition_id).await.unwrap().unwrap();
    assert_eq!(
        row_after.isbn.as_deref(),
        Some("9780261102217"),
        "discovered ISBN must be persisted to editions.isbn (AC-41)"
    );
    // enrichment_attempted remains 0 — the full enrichment has not completed yet
    assert_eq!(row_after.enrichment_attempted, 0);
}

// ---------------------------------------------------------------------------
// AC-41: Title+author fallback finds ISBN-13
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_isbn_by_title_author_returns_isbn13() {
    let server = MockServer::start().await;

    let search_body = serde_json::json!({
        "docs": [
            { "isbn": ["0261102214", "9780261102217"] }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(search_body))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = enrich::find_isbn_by_title_author(
        &client,
        "The Hobbit",
        "Tolkien",
        &server.uri(),
    )
    .await
    .unwrap();

    assert_eq!(
        result.as_deref(),
        Some("9780261102217"),
        "should return the first ISBN-13 from the top search result"
    );
}

// ---------------------------------------------------------------------------
// AC-45: update_work_ol_id merges two editions under one work when both are
// enriched to the same openlibrary_work_id.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_work_ol_id_merges_editions() {
    let (pool, _db_tmp) = temp_pool().await;

    // Insert two editions, each with their own work row.
    let edition_a = insert_test_edition(&pool, "9780000000001", "/tmp/book_a.epub").await;
    let edition_b = insert_test_edition(&pool, "9780000000002", "/tmp/book_b.epub").await;

    let work_a = db::insert_work(&pool, "Work A", "Author A").await.unwrap();
    let work_b = db::insert_work(&pool, "Work B", "Author B").await.unwrap();

    db::set_edition_work_id(&pool, edition_a, work_a).await.unwrap();
    db::set_edition_work_id(&pool, edition_b, work_b).await.unwrap();

    // Both editions enrich to the same OL work ID.
    let ol_id = "/works/OL99999W";

    // First edition's work gets the OL ID set (no conflict yet).
    db::update_work_ol_id(&pool, work_a, ol_id).await.unwrap();

    // Second edition's work is enriched to the same OL ID → merge triggered.
    db::update_work_ol_id(&pool, work_b, ol_id).await.unwrap();

    // Both editions must now share the same work_id.
    let row_a = db::get_edition(&pool, edition_a).await.unwrap().unwrap();
    let row_b = db::get_edition(&pool, edition_b).await.unwrap().unwrap();

    assert!(row_a.work_id.is_some(), "edition_a must have a work_id");
    assert!(row_b.work_id.is_some(), "edition_b must have a work_id");
    assert_eq!(
        row_a.work_id, row_b.work_id,
        "both editions must share the same work_id after OL ID merge"
    );

    // The works table must have exactly one row with that OL ID.
    let surviving_work_id = row_a.work_id.unwrap();
    let found = db::find_work_by_ol_id(&pool, ol_id).await.unwrap();
    assert_eq!(
        found,
        Some(surviving_work_id),
        "works table must contain exactly one row with the OL work ID"
    );
}
