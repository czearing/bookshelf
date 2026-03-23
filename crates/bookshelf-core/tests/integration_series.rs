/// Integration tests for Phase 3 series tracking and gap detection.
use bookshelf_core::{db, epub::EpubMeta, grab, series};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// Insert an owned edition with series metadata.
async fn insert_series_edition(
    pool: &db::DbPool,
    title: &str,
    series_name: &str,
    series_position: &str,
    path_suffix: &str,
) {
    let meta = EpubMeta {
        title: Some(title.to_string()),
        authors: Some("Series Author".to_string()),
        series_name: Some(series_name.to_string()),
        series_position: Some(series_position.to_string()),
        source_path: format!("/tmp/{path_suffix}.epub"),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap();
}

/// Insert an owned edition with series metadata and an ISBN (for exact dedup).
async fn insert_series_edition_with_isbn(
    pool: &db::DbPool,
    title: &str,
    series_name: &str,
    series_position: &str,
    isbn: &str,
    path_suffix: &str,
) {
    let meta = EpubMeta {
        title: Some(title.to_string()),
        authors: Some("Series Author".to_string()),
        isbn: Some(isbn.to_string()),
        series_name: Some(series_name.to_string()),
        series_position: Some(series_position.to_string()),
        source_path: format!("/tmp/{path_suffix}.epub"),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap();
}

// ---------------------------------------------------------------------------
// AC-23 / AC-38: no series in library
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_no_series_in_library() {
    let (pool, _tmp) = temp_pool().await;
    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);
    assert!(views.is_empty());
    let text = series::format_series_text(&views);
    assert!(text.contains("No series found"));

    let json = series::format_series_json(&views).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.as_array().unwrap().is_empty(), "JSON must be []");

    let csv = series::format_series_csv(&views).unwrap();
    // Only the header row.
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 1, "CSV with no series must have only header");
    assert!(lines[0].starts_with("series_name"));
}

// ---------------------------------------------------------------------------
// AC-24: gap detection — positions 1 and 3 produce gap at 2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_gap_detection_numeric() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "The Series", "1", "s1").await;
    insert_series_edition(&pool, "Book 3", "The Series", "3", "s3").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);

    assert_eq!(views.len(), 1);
    let view = &views[0];
    assert_eq!(view.series_name, "The Series");
    assert!(!view.non_numeric);

    let owned: Vec<_> = view.entries.iter().filter(|e| e.owned).collect();
    let gaps: Vec<_> = view.entries.iter().filter(|e| !e.owned).collect();

    assert_eq!(owned.len(), 2, "positions 1 and 3 must be owned");
    assert_eq!(gaps.len(), 1, "gap at position 2");
    assert_eq!(gaps[0].position, "2");
}

// ---------------------------------------------------------------------------
// AC-25: no network calls — compute_series_views is a pure function
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_no_network_calls() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "No-Net Series", "1", "nn1").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    // Call pure function with no client or base_url — proves no network.
    let views = series::compute_series_views(&editions);
    assert_eq!(views.len(), 1);
    let text = series::format_series_text(&views);
    assert!(text.contains("No-Net Series"));
}

// ---------------------------------------------------------------------------
// AC-26: editions with NULL series_name are excluded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_null_series_excluded() {
    let (pool, _tmp) = temp_pool().await;

    // Edition without series_name.
    db::upsert_edition(
        &pool,
        &EpubMeta {
            title: Some("No Series Book".to_string()),
            authors: Some("Authorless".to_string()),
            source_path: "/tmp/noseries.epub".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let editions = db::editions_with_series(&pool).await.unwrap();
    assert!(editions.is_empty(), "editions with NULL series_name must not be returned");
}

// ---------------------------------------------------------------------------
// AC-27: single entry — no gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_single_entry_no_gap() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Only Book", "Solo Series", "1", "solo1").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);

    assert_eq!(views.len(), 1);
    let view = &views[0];
    let gaps: Vec<_> = view.entries.iter().filter(|e| !e.owned).collect();
    assert!(gaps.is_empty(), "single entry series must have no gaps");
}

// ---------------------------------------------------------------------------
// AC-28: series_fill with no series returns Ok(0) without network call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_no_series() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let result = series::series_fill(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(result, 0);
    // Verify no requests were made to the mock server.
    let requests = server.received_requests().await.unwrap();
    assert!(requests.is_empty(), "no network requests must be made when no series");
}

// ---------------------------------------------------------------------------
// AC-29: series_fill queries OL search endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_queries_ol() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "Test Series", "1", "ts1").await;

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "docs": []
        })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    series::series_fill(&pool, &client, &server.uri()).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert!(!requests.is_empty(), "OL search endpoint must be queried");
}

// ---------------------------------------------------------------------------
// AC-30: series_fill inserts missing entry with source=series_fill, priority=7
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_inserts_missing() {
    let (pool, _tmp) = temp_pool().await;
    // Use ISBNs for exact dedup so fuzzy matching does not produce false positives.
    insert_series_edition_with_isbn(
        &pool, "Dune", "Dune Chronicles", "1", "9780441013593", "fs1",
    )
    .await;
    insert_series_edition_with_isbn(
        &pool, "Children of Dune", "Dune Chronicles", "3", "9780441104001", "fs3",
    )
    .await;

    let server = MockServer::start().await;

    // OL returns all 3; only the missing #2 should be inserted.
    // Use matching ISBNs for owned entries so is_already_owned catches them by ISBN.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Dune",
                "author_name": ["Frank Herbert"],
                "isbn": ["9780441013593"],
                "series": ["Dune Chronicles"],
                "series_number": "1",
                "key": "/works/OL100W"
            },
            {
                "title": "Dune Messiah",
                "author_name": ["Frank Herbert"],
                "isbn": [],
                "series": ["Dune Chronicles"],
                "series_number": "2",
                "key": "/works/OL101W"
            },
            {
                "title": "Children of Dune",
                "author_name": ["Frank Herbert"],
                "isbn": ["9780441104001"],
                "series": ["Dune Chronicles"],
                "series_number": "3",
                "key": "/works/OL102W"
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri()).await.unwrap();
    // Only "Dune Messiah" should be inserted (Dune and Children of Dune are owned by ISBN).
    assert_eq!(inserted, 1, "only the missing entry must be inserted");

    let wants = db::list_want(&pool, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "Dune Messiah");
    assert_eq!(wants[0].source, "series_fill");
    assert_eq!(wants[0].priority, 7);
}

// ---------------------------------------------------------------------------
// AC-31: series_fill inserts nothing when all owned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_no_insert_when_all_owned() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "All Owned", "1", "ao1").await;

    let server = MockServer::start().await;

    // OL returns Book 1 which is already owned.
    let ol_response = serde_json::json!({
        "docs": [{
            "title": "Book 1",
            "author_name": ["Series Author"],
            "isbn": [],
            "series": ["All Owned"],
            "series_number": "1",
            "key": "/works/OL200W"
        }]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(inserted, 0);
    let wants = db::list_want(&pool, None).await.unwrap();
    assert!(wants.is_empty());
}

// ---------------------------------------------------------------------------
// AC-32: series_fill handles empty OL docs array gracefully
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_empty_ol_response() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "Empty Response Series", "1", "er1").await;

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "docs": [] })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(inserted, 0);
}

// ---------------------------------------------------------------------------
// AC-33: series_fill OL error continues — returns Err when a series fails
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_ol_error_continues() {
    let (pool, _tmp) = temp_pool().await;
    // One series in the DB.
    insert_series_edition(&pool, "Error Test Book", "Error Series", "1", "et1").await;

    let server = MockServer::start().await;

    // OL returns 500 for this series.
    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = series::series_fill(&pool, &client, &server.uri()).await;
    // Must return Err when any series OL request fails.
    assert!(result.is_err(), "series_fill must return Err when OL returns HTTP error");

    // No want entries should have been inserted.
    let wants = db::list_want(&pool, None).await.unwrap();
    assert!(wants.is_empty(), "no want entries on error");
}

// ---------------------------------------------------------------------------
// AC-34: series list text matches series show
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_list_text_matches_series_show() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "Text Series", "1", "txt1").await;
    insert_series_edition(&pool, "Book 3", "Text Series", "3", "txt3").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);
    let text = series::format_series_text(&views);

    assert!(text.contains("Text Series"));
    assert!(text.contains("[owned]"));
    assert!(text.contains("[MISSING]"));
    assert!(text.contains("2"));
}

// ---------------------------------------------------------------------------
// AC-35: series list json output is valid and contains expected fields
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_list_json_valid() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "JSON Series", "1", "j1").await;
    insert_series_edition(&pool, "Book 3", "JSON Series", "3", "j3").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);
    let json = series::format_series_json(&views).unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let view = &arr[0];
    assert!(view["series_name"].is_string());
    assert!(view["entries"].is_array());
    assert!(!view["entries"].as_array().unwrap().is_empty());
    // Each entry has owned field.
    let first_entry = &view["entries"][0];
    assert!(first_entry["owned"].is_boolean());
    assert!(first_entry["position"].is_string());
}

// ---------------------------------------------------------------------------
// AC-36: series list csv is valid RFC 4180 with header
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_list_csv_valid() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Book 1", "CSV Series", "1", "c1").await;
    insert_series_edition(&pool, "Book 3", "CSV Series", "3", "c3").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);
    let csv = series::format_series_csv(&views).unwrap();

    let lines: Vec<&str> = csv.lines().collect();
    assert!(lines[0].starts_with("series_name,position,owned"));
    assert!(lines.len() > 1, "CSV must have data rows");
    // Verify owned column is 1 or 0.
    for line in lines.iter().skip(1) {
        let cols: Vec<&str> = line.split(',').collect();
        assert!(
            cols.last().map(|v| *v == "1" || *v == "0").unwrap_or(false),
            "owned column must be 1 or 0, got: {line}"
        );
    }
}

// ---------------------------------------------------------------------------
// AC-24/Q1: non-numeric positions — no gaps, non_numeric flag set
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_non_numeric_positions() {
    let (pool, _tmp) = temp_pool().await;
    insert_series_edition(&pool, "Part One", "Non-Numeric", "Book One", "nn1").await;
    insert_series_edition(&pool, "Part Two", "Non-Numeric", "Book Two", "nn2").await;

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);

    assert_eq!(views.len(), 1);
    let view = &views[0];
    assert!(view.non_numeric, "non_numeric must be true for non-parseable positions");
    let gaps: Vec<_> = view.entries.iter().filter(|e| !e.owned).collect();
    assert!(gaps.is_empty(), "no gap entries for non-numeric series");

    let text = series::format_series_text(&views);
    assert!(
        text.contains("non-numeric positions"),
        "text output must include non-numeric note"
    );
}

// ---------------------------------------------------------------------------
// E2E: series_fill then grab — AC-39, AC-40
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_series_fill_then_grab() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned #1 and #3 (skipping #2) of Foundation. Use ISBNs for exact dedup.
    insert_series_edition_with_isbn(
        &pool, "Foundation", "Foundation", "1", "9780553293357", "f1",
    )
    .await;
    insert_series_edition_with_isbn(
        &pool, "Second Foundation", "Foundation", "3", "9780553293364", "f3",
    )
    .await;

    let server = MockServer::start().await;

    // OL returns #1, #2, #3 for Foundation. Use matching ISBNs for owned entries.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Foundation",
                "author_name": ["Isaac Asimov"],
                "isbn": ["9780553293357"],
                "series": ["Foundation"],
                "series_number": "1",
                "key": "/works/OL400W"
            },
            {
                "title": "Foundation and Empire",
                "author_name": ["Isaac Asimov"],
                "isbn": [],
                "series": ["Foundation"],
                "series_number": "2",
                "key": "/works/OL401W"
            },
            {
                "title": "Second Foundation",
                "author_name": ["Isaac Asimov"],
                "isbn": ["9780553293364"],
                "series": ["Foundation"],
                "series_number": "3",
                "key": "/works/OL402W"
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri()).await.unwrap();
    assert_eq!(inserted, 1, "only the missing #2 should be inserted");

    let wants = db::list_want(&pool, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "Foundation and Empire");
    assert_eq!(wants[0].source, "series_fill");
    assert_eq!(wants[0].priority, 7);

    // Verify it appears in the grab list.
    let grab_entries = grab::compute_grab_list(&pool, None).await.unwrap();
    let grab_titles: Vec<&str> = grab_entries
        .iter()
        .filter_map(|e| e.title.as_deref())
        .collect();
    assert!(
        grab_titles.contains(&"Foundation and Empire"),
        "Foundation and Empire must appear in grab list"
    );
    let entry = grab_entries
        .iter()
        .find(|e| e.title.as_deref() == Some("Foundation and Empire"))
        .unwrap();
    assert_eq!(entry.source, "series_fill");
    assert_eq!(entry.priority, 7);
}
