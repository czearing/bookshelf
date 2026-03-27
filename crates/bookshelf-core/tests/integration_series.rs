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
    let result = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
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
    series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();

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
                "key": "/works/OL100W",
                "number_of_pages_median": 400,
                "edition_count": 10
            },
            {
                "title": "Dune Messiah",
                "author_name": ["Frank Herbert"],
                "isbn": [],
                "series": ["Dune Chronicles"],
                "series_number": "2",
                "key": "/works/OL101W",
                "number_of_pages_median": 300,
                "edition_count": 8
            },
            {
                "title": "Children of Dune",
                "author_name": ["Frank Herbert"],
                "isbn": ["9780441104001"],
                "series": ["Dune Chronicles"],
                "series_number": "3",
                "key": "/works/OL102W",
                "number_of_pages_median": 350,
                "edition_count": 7
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
    // Only "Dune Messiah" should be inserted (Dune and Children of Dune are owned by ISBN).
    assert_eq!(inserted, 1, "only the missing entry must be inserted");

    let wants = db::list_want(&pool, None, None).await.unwrap();
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
    // Use an ISBN so the ISBN-only owned check (no fuzzy) can suppress re-insertion.
    insert_series_edition_with_isbn(&pool, "Book 1", "All Owned", "1", "9780000000001", "ao1").await;

    let server = MockServer::start().await;

    // OL returns Book 1 with the same ISBN that was scanned — ISBN exact match suppresses it.
    let ol_response = serde_json::json!({
        "docs": [{
            "title": "Book 1",
            "author_name": ["Series Author"],
            "isbn": ["9780000000001"],
            "series": ["All Owned"],
            "series_number": "1",
            "key": "/works/OL200W",
            "number_of_pages_median": 300,
            "edition_count": 5
        }]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(inserted, 0);
    let wants = db::list_want(&pool, None, None).await.unwrap();
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
    let inserted = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
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
    let result = series::series_fill(&pool, &client, &server.uri(), None).await;
    // Must return Err when any series OL request fails.
    assert!(result.is_err(), "series_fill must return Err when OL returns HTTP error");

    // No want entries should have been inserted.
    let wants = db::list_want(&pool, None, None).await.unwrap();
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
                "key": "/works/OL400W",
                "number_of_pages_median": 244,
                "edition_count": 15
            },
            {
                "title": "Foundation and Empire",
                "author_name": ["Isaac Asimov"],
                "isbn": [],
                "series": ["Foundation"],
                "series_number": "2",
                "key": "/works/OL401W",
                "number_of_pages_median": 247,
                "edition_count": 12
            },
            {
                "title": "Second Foundation",
                "author_name": ["Isaac Asimov"],
                "isbn": ["9780553293364"],
                "series": ["Foundation"],
                "series_number": "3",
                "key": "/works/OL402W",
                "number_of_pages_median": 256,
                "edition_count": 14
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(inserted, 1, "only the missing #2 should be inserted");

    let wants = db::list_want(&pool, None, None).await.unwrap();
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

// ---------------------------------------------------------------------------
// Gap 1 — series_fill idempotency: calling twice must not create duplicates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_idempotent() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned Stormlight #1 and #3 (with ISBNs for exact dedup).
    insert_series_edition_with_isbn(
        &pool,
        "The Way of Kings",
        "The Stormlight Archive",
        "1",
        "9780765326355",
        "sl1",
    )
    .await;
    insert_series_edition_with_isbn(
        &pool,
        "Oathbringer",
        "The Stormlight Archive",
        "3",
        "9780765326386",
        "sl3",
    )
    .await;

    let server = MockServer::start().await;

    // OL returns #1, #2, #3. Only #2 is missing.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "The Way of Kings",
                "author_name": ["Brandon Sanderson"],
                "isbn": ["9780765326355"],
                "series": ["The Stormlight Archive"],
                "series_number": "1",
                "key": "/works/OL500W",
                "number_of_pages_median": 1007,
                "edition_count": 18
            },
            {
                "title": "Words of Radiance",
                "author_name": ["Brandon Sanderson"],
                "isbn": [],
                "series": ["The Stormlight Archive"],
                "series_number": "2",
                "key": "/works/OL501W",
                "number_of_pages_median": 1088,
                "edition_count": 14
            },
            {
                "title": "Oathbringer",
                "author_name": ["Brandon Sanderson"],
                "isbn": ["9780765326386"],
                "series": ["The Stormlight Archive"],
                "series_number": "3",
                "key": "/works/OL502W",
                "number_of_pages_median": 1248,
                "edition_count": 12
            }
        ]
    });

    // Register the mock to allow multiple calls.
    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .expect(2) // expect exactly 2 calls (one per series_fill invocation)
        .mount(&server)
        .await;

    let client = reqwest::Client::new();

    // First call — should insert 1 entry ("Words of Radiance").
    let inserted_first = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(inserted_first, 1, "first call must insert exactly 1 missing entry");

    let wants_after_first = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants_after_first.len(), 1, "want_list must have 1 entry after first fill");

    // Second call — same mock, same DB. Must insert 0 (idempotent).
    let inserted_second = series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(inserted_second, 0, "second call must insert 0 (idempotent)");

    // want_list count must be unchanged.
    let wants_after_second = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(
        wants_after_second.len(),
        1,
        "want_list count must be unchanged after second fill"
    );

    // The entries must be identical (same id, title, source).
    assert_eq!(wants_after_first[0].id, wants_after_second[0].id);
    assert_eq!(wants_after_first[0].title, wants_after_second[0].title);
    assert_eq!(wants_after_first[0].source, wants_after_second[0].source);
}

// ---------------------------------------------------------------------------
// Gap 4 — series_name set but series_position = NULL for all entries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_null_position_all_entries() {
    let (pool, _tmp) = temp_pool().await;

    // Insert 3 editions with series_name set but series_position = NULL.
    for (title, path) in &[
        ("The Eye of the World", "wot1"),
        ("The Great Hunt", "wot2"),
        ("The Dragon Reborn", "wot3"),
    ] {
        let meta = bookshelf_core::epub::EpubMeta {
            title: Some(title.to_string()),
            authors: Some("Robert Jordan".to_string()),
            series_name: Some("The Wheel of Time".to_string()),
            series_position: None, // NULL for all
            source_path: format!("/tmp/{path}.epub"),
            ..Default::default()
        };
        db::upsert_edition(&pool, &meta).await.unwrap();
    }

    let editions = db::editions_with_series(&pool).await.unwrap();
    assert_eq!(editions.len(), 3, "all 3 editions must be returned by editions_with_series");

    let views = series::compute_series_views(&editions);

    // The series must appear in output (not silently dropped).
    assert_eq!(views.len(), 1, "series must appear even when all positions are NULL");
    let view = &views[0];
    assert_eq!(view.series_name, "The Wheel of Time");

    // non_numeric must be true (NULL positions treated as non-numeric).
    assert!(
        view.non_numeric,
        "non_numeric must be true when all series_position are NULL"
    );

    // No gap entries must be generated (no false gaps).
    let gaps: Vec<_> = view.entries.iter().filter(|e| !e.owned).collect();
    assert!(gaps.is_empty(), "no gap entries must be generated for NULL-position series");

    // Text output must include the non-numeric note.
    let text = series::format_series_text(&views);
    assert!(
        text.contains("non-numeric positions"),
        "text output must include non-numeric note for NULL-position series, got: {text}"
    );

    // No crash on JSON/CSV either.
    series::format_series_json(&views).unwrap();
    series::format_series_csv(&views).unwrap();
}

// ---------------------------------------------------------------------------
// Gap 5 — Two series with same name, different authors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_same_name_different_authors() {
    let (pool, _tmp) = temp_pool().await;

    // "Book 1" by "Author A", series "The Chronicles", position "1"
    let meta_a = bookshelf_core::epub::EpubMeta {
        title: Some("Book 1".to_string()),
        authors: Some("Author A".to_string()),
        series_name: Some("The Chronicles".to_string()),
        series_position: Some("1".to_string()),
        source_path: "/tmp/chronicles_a1.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta_a).await.unwrap();

    // "Volume 1" by "Author B", series "The Chronicles", position "1"
    let meta_b = bookshelf_core::epub::EpubMeta {
        title: Some("Volume 1".to_string()),
        authors: Some("Author B".to_string()),
        series_name: Some("The Chronicles".to_string()),
        series_position: Some("1".to_string()),
        source_path: "/tmp/chronicles_b1.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta_b).await.unwrap();

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);

    // Current behavior: groups by series_name only — both books merge into ONE view.
    // Position "1" appears twice in the same series view. No gaps are generated
    // because there are no integer gaps between consecutive 1.0 values.
    assert_eq!(
        views.len(),
        1,
        "current behavior: same series_name merges into one view regardless of author"
    );

    let view = &views[0];
    assert_eq!(view.series_name, "The Chronicles");

    // Both entries should appear as owned (no false gaps from duplicate position 1).
    let owned: Vec<_> = view.entries.iter().filter(|e| e.owned).collect();
    assert_eq!(owned.len(), 2, "both position-1 entries must appear as owned");

    let gaps: Vec<_> = view.entries.iter().filter(|e| !e.owned).collect();
    assert!(
        gaps.is_empty(),
        "no false gap must be generated between duplicate position 1 values"
    );
}

// ---------------------------------------------------------------------------
// GAP-11: All-NULL series positions produce empty entries vec in series view
// ---------------------------------------------------------------------------

/// Two owned editions in the same series, both with series_position = NULL.
/// compute_series_views must set non_numeric = true and entries = [].
/// format_series_text must include the series name.
#[tokio::test]
async fn test_gap11_null_positions_produce_empty_entries() {
    let (pool, _tmp) = temp_pool().await;

    // Insert 2 editions with series_name but series_position = NULL.
    for (title, path) in &[("Null Pos Book 1", "gap11_1"), ("Null Pos Book 2", "gap11_2")] {
        let meta = bookshelf_core::epub::EpubMeta {
            title: Some(title.to_string()),
            authors: Some("Gap11 Author".to_string()),
            series_name: Some("Gap11 Series".to_string()),
            series_position: None,
            source_path: format!("/tmp/{path}.epub"),
            ..Default::default()
        };
        db::upsert_edition(&pool, &meta).await.unwrap();
    }

    let editions = db::editions_with_series(&pool).await.unwrap();
    let views = series::compute_series_views(&editions);

    assert_eq!(views.len(), 1, "one series view must be produced");
    let view = &views[0];
    assert_eq!(view.series_name, "Gap11 Series");
    assert!(view.non_numeric, "non_numeric must be true when all series_position are NULL");
    assert!(view.entries.is_empty(), "entries must be empty when all series_position are NULL");

    let text = series::format_series_text(&views);
    assert!(
        text.contains("Gap11 Series"),
        "format_series_text must include the series name, got: {text}"
    );
}

// ---------------------------------------------------------------------------
// GAP-09: series_fill URL-encodes special characters in series names
// ---------------------------------------------------------------------------

/// A series_name containing parentheses and spaces ("The Witcher (Series)")
/// must be URL-encoded before being used as the OL search query parameter.
/// The wiremock path_regex checks the raw path; the encoded form uses %28 for
/// '(' and %29 for ')'.
#[tokio::test]
async fn test_gap09_series_fill_url_encodes_series_name() {
    let (pool, _tmp) = temp_pool().await;

    // Insert an edition with a series name that contains special characters.
    let meta = bookshelf_core::epub::EpubMeta {
        title: Some("The Last Wish".to_string()),
        authors: Some("Andrzej Sapkowski".to_string()),
        series_name: Some("The Witcher (Series)".to_string()),
        series_position: Some("1".to_string()),
        source_path: "/tmp/gap09_witcher.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    let server = MockServer::start().await;

    // The URL must contain the percent-encoded form of "The Witcher (Series)".
    // urlencoding::encode replaces ' ' with '+' or '%20' and '(' with '%28', ')' with '%29'.
    // Match by checking the query string contains the encoded parentheses.
    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "docs": [] })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();

    // Verify the mock was called (URL encoding did not break the request).
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "OL search endpoint must be called exactly once");

    // Verify the query string contains the URL-encoded parentheses.
    let url = requests[0].url.as_str();
    assert!(
        url.contains("%28") || url.contains("%29"),
        "URL must contain percent-encoded parentheses (%28 / %29) for series name, got: {url}"
    );
}

// ---------------------------------------------------------------------------
// GAP-10: series_fill substring filter — false-positive prevention
// ---------------------------------------------------------------------------

/// OL response contains docs for two different series; only the doc whose
/// `series` field exactly matches the queried series name must be added.
/// Docs tagged with a completely different series name must be excluded.
///
/// Note: the current `contains` substring filter means that a series name
/// which is a *substring* of a different series name would incorrectly pass
/// (known GAP-10 limitation). This test exercises the basic exclusion path
/// using fully disjoint series names so no substring ambiguity exists.
#[tokio::test]
async fn test_series_fill_unrelated_series_excluded() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Own book 1 of "Mistborn".
    let meta = bookshelf_core::epub::EpubMeta {
        title: Some("The Final Empire".to_string()),
        authors: Some("Brandon Sanderson".to_string()),
        series_name: Some("Mistborn".to_string()),
        series_position: Some("1".to_string()),
        source_path: "/tmp/mistborn_gap10.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    // OL returns two docs:
    // - "The Well of Ascension" tagged ["Mistborn"] → correct match → must be added
    // - "Elantris" tagged ["Elantris"] → completely different series → must be excluded
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex(r"/search\.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "docs": [
                    {
                        "title": "The Well of Ascension",
                        "author_name": ["Brandon Sanderson"],
                        "series": ["Mistborn"],
                        "key": "/works/OL_WOA",
                        "number_of_pages_median": 763,
                        "edition_count": 12
                    },
                    {
                        "title": "Elantris",
                        "author_name": ["Brandon Sanderson"],
                        "series": ["Elantris"],
                        "key": "/works/OL_EL",
                        "number_of_pages_median": 496,
                        "edition_count": 10
                    }
                ]
            })),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    series::series_fill(&pool, &client, &server.uri(), None).await.unwrap();

    let wants = db::list_want(&pool, None, None).await.unwrap();
    let titles: Vec<_> = wants.iter().map(|w| w.title.as_str()).collect();

    assert!(
        titles.contains(&"The Well of Ascension"),
        "correct-series match must be added to want list; got: {titles:?}"
    );
    assert!(
        !titles.contains(&"Elantris"),
        "doc from a different series must NOT be added to want list; got: {titles:?}"
    );
}

// ---------------------------------------------------------------------------
// GAP series_fill_no_fuzzy_false_positive: "Dune Messiah" must be added even
// when "Dune" (same author, prefix-overlapping title) is already owned.
// ---------------------------------------------------------------------------

/// Regression test for the series_fill fuzzy false-positive bug.
///
/// Jaro-Winkler("dune", "dune messiah") ≈ 0.867, which is above DEDUP_THRESHOLD
/// (0.85). When `is_already_owned` used fuzzy matching, "Dune Messiah" was
/// incorrectly treated as already owned because it scored > threshold against the
/// owned "Dune" edition — causing series_fill to skip the sequel entirely.
///
/// After the fix, series_fill uses `series_is_already_owned` (ISBN/work_id only).
/// Since "Dune Messiah" has no ISBN overlap with owned "Dune", it must be added
/// to the want list.
#[tokio::test]
async fn test_series_fill_no_fuzzy_false_positive_dune_messiah() {
    let (pool, _tmp) = temp_pool().await;

    // Own "Dune" #1 with an ISBN.
    insert_series_edition_with_isbn(
        &pool,
        "Dune",
        "Dune Chronicles",
        "1",
        "9780441013593",
        "dune1",
    )
    .await;

    let server = MockServer::start().await;

    // OL returns #1 (owned) and #2 "Dune Messiah" (different ISBN — not owned).
    // Both have the same author so fuzzy would incorrectly suppress the sequel.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Dune",
                "author_name": ["Frank Herbert"],
                "isbn": ["9780441013593"],
                "series": ["Dune Chronicles"],
                "series_number": "1",
                "key": "/works/OL_DUNE1",
                "number_of_pages_median": 412,
                "edition_count": 20
            },
            {
                "title": "Dune Messiah",
                "author_name": ["Frank Herbert"],
                "isbn": ["9780441172696"],
                "series": ["Dune Chronicles"],
                "series_number": "2",
                "key": "/works/OL_DUNE2",
                "number_of_pages_median": 288,
                "edition_count": 15
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(
        inserted, 1,
        "series_fill must insert exactly 1 entry (Dune Messiah); got {inserted}"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();

    assert!(
        titles.contains(&"Dune Messiah"),
        "Dune Messiah must be added to the want list even though 'Dune' (same author, \
         title prefix overlap) is already owned; got: {titles:?}"
    );
    assert!(
        !titles.contains(&"Dune"),
        "owned 'Dune' must NOT appear in want list; got: {titles:?}"
    );
}

// ---------------------------------------------------------------------------
// GAP-26: format_series_text with a series where all entries are owned
// (no gaps) — ensure no crash and output makes sense
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_format_series_text_all_owned_no_gaps() {
    // Build a SeriesView manually with all entries owned.
    let view = series::SeriesView {
        series_name: "Dune Chronicles".to_string(),
        entries: vec![
            series::SeriesEntry {
                series_name: "Dune Chronicles".to_string(),
                position: "1".to_string(),
                owned: true,
            },
            series::SeriesEntry {
                series_name: "Dune Chronicles".to_string(),
                position: "2".to_string(),
                owned: true,
            },
        ],
        non_numeric: false,
    };

    let text = series::format_series_text(&[view]);
    assert!(text.contains("Dune Chronicles"), "series name must appear in output");
    assert!(text.contains("owned"), "owned entries must be marked");
    assert!(!text.contains("missing"), "no missing entries must appear when all are owned");
}

// ---------------------------------------------------------------------------
// series_fill Strategy C: owned edition without ISBN is NOT re-added when
// OL returns the same title WITHOUT an ISBN — fuzzy fallback suppresses it.
// ---------------------------------------------------------------------------

/// When an owned edition has no ISBN, Strategy C (fuzzy title+author within
/// same series_name) must detect that a no-ISBN OL doc matches the owned
/// edition and suppress the insertion.
///
/// Note: Strategy C only applies when the OL doc itself has NO ISBN.
/// If the OL doc has an ISBN that did not match Strategy A/B, it is treated
/// as a distinct edition (not suppressed by fuzzy) to prevent false positives
/// like "Dune Messiah" being suppressed because "Dune" is owned.
#[tokio::test]
async fn test_series_fill_no_isbn_ol_doc_suppressed_by_strategy_c() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned "Children of Dune" (series #3) WITHOUT an ISBN.
    // insert_series_edition sets authors = "Series Author".
    insert_series_edition(&pool, "Children of Dune", "Dune Chronicles", "3", "cod_no_isbn").await;

    let server = MockServer::start().await;

    // OL returns "Children of Dune" WITHOUT an ISBN — same situation as an OL
    // record that simply has no isbn data.  Strategy C must fuzzy-match the
    // title+author against the owned no-ISBN edition and suppress insertion.
    let ol_response = serde_json::json!({
        "docs": [{
            "title": "Children of Dune",
            "author_name": ["Series Author"],
            "series": ["Dune Chronicles"],
            "series_number": "3",
            "key": "/works/OL_COD",
            "number_of_pages_median": 350,
            "edition_count": 7
        }]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    // Strategy C: OL doc has no ISBN; fuzzy match against owned edition → suppressed.
    assert_eq!(
        inserted, 0,
        "no-ISBN OL doc matching owned no-ISBN edition must be suppressed by Strategy C"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "want list must be empty when owned edition is suppressed by fuzzy");
}

// ---------------------------------------------------------------------------
// New test: series_fill finds series members for books with no ISBN in the DB
// ---------------------------------------------------------------------------

/// Covers the Mistborn scenario: the owned edition has series_name and
/// series_position in the DB but NO ISBN.  The OL mock returns members via a
/// series-specific search (some docs have no `series` field populated).
///
/// Expected outcome:
/// - The owned book #1 is NOT re-added (Strategy C fuzzy fallback suppresses it).
/// - The missing book #2 IS added to the want list.
#[tokio::test]
async fn test_series_fill_finds_members_without_isbn() {
    let (pool, _tmp) = temp_pool().await;

    // Insert "The Final Empire" (Mistborn #1) WITHOUT an ISBN.
    // insert_series_edition sets authors = "Series Author".
    insert_series_edition(
        &pool,
        "The Final Empire",
        "Mistborn",
        "1",
        "mistborn1_no_isbn",
    )
    .await;

    let server = MockServer::start().await;

    // OL returns #1 and #2.  #1 has no `series` field (simulating an OL doc
    // that lacks the populated field but was returned by a series:"..." query).
    // #2 has the `series` field populated.  Both must pass the relaxed filter.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "The Final Empire",
                "author_name": ["Series Author"],
                // No `series` field — must pass relaxed filter (trusted query).
                "series_number": "1",
                "key": "/works/OL_MB1",
                "number_of_pages_median": 541,
                "edition_count": 16
            },
            {
                "title": "The Well of Ascension",
                "author_name": ["Series Author"],
                "series": ["Mistborn"],
                "series_number": "2",
                "key": "/works/OL_MB2",
                "number_of_pages_median": 763,
                "edition_count": 13
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let inserted = series::series_fill(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    // Only "The Well of Ascension" (#2) must be inserted.
    // "The Final Empire" (#1) is owned; Strategy C must suppress it via fuzzy match.
    assert_eq!(
        inserted, 1,
        "only the missing #2 must be inserted; owned #1 must be suppressed by fuzzy fallback"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();

    assert!(
        titles.contains(&"The Well of Ascension"),
        "missing #2 must appear in want list; got: {titles:?}"
    );
    assert!(
        !titles.contains(&"The Final Empire"),
        "owned #1 (no ISBN) must NOT appear in want list; got: {titles:?}"
    );
    assert_eq!(wants[0].source, "series_fill");
    assert_eq!(wants[0].priority, 7);
}

// ---------------------------------------------------------------------------
// AC-15: series_fill uses config_language as fallback when edition has no language
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_uses_config_language_fallback() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition with no language field (language = NULL).
    insert_series_edition(&pool, "Book 1", "Test Series Config Lang", "1", "cflang1").await;

    let server = MockServer::start().await;

    // OL returns 2 docs: one eng, one ger.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Book Two English",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series Config Lang"],
                "series_number": "2",
                "key": "/works/OL_CFL1W",
                "language": ["eng"]
            },
            {
                "title": "Buch Zwei Deutsch",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series Config Lang"],
                "series_number": "2",
                "key": "/works/OL_CFL2W",
                "language": ["ger"]
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // config_language = Some("eng") — used as fallback since edition has no language.
    let inserted = series::series_fill(&pool, &client, &server.uri(), Some("eng"))
        .await
        .unwrap();

    // Only the English doc should be inserted; the German doc is filtered out.
    assert_eq!(inserted, 1, "only the English-language work must be queued");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "Book Two English", "only the English work must be in want list");
}

// ---------------------------------------------------------------------------
// AC-15 (no filter): series_fill queues all docs when config_language is None
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_config_language_none_no_filter() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition with no language field (language = NULL).
    insert_series_edition(&pool, "Book 1", "Test Series No Filter", "1", "nofilt1").await;

    let server = MockServer::start().await;

    // OL returns 2 docs: one eng, one ger.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Book Two English",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series No Filter"],
                "series_number": "2",
                "key": "/works/OL_NF1W",
                "language": ["eng"]
            },
            {
                "title": "Buch Zwei Deutsch",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series No Filter"],
                "series_number": "2",
                "key": "/works/OL_NF2W",
                "language": ["ger"]
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // config_language = None — no language filter applied at all.
    let inserted = series::series_fill(&pool, &client, &server.uri(), None)
        .await
        .unwrap();

    // Both docs must be queued.
    assert_eq!(inserted, 2, "both works must be queued when config_language is None");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 2);
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"Book Two English"));
    assert!(titles.contains(&"Buch Zwei Deutsch"));
}

// ---------------------------------------------------------------------------
// AC-16: edition-derived language takes precedence over config_language
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_edition_language_overrides_config() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition with BCP-47 language "en" (converts to ISO 639-2 "eng").
    let meta = EpubMeta {
        title: Some("Book 1".to_string()),
        authors: Some("Series Author".to_string()),
        series_name: Some("Test Series".to_string()),
        series_position: Some("1".to_string()),
        language: Some("en".to_string()),
        source_path: "/tmp/ac16_book1.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta).await.unwrap();

    let server = MockServer::start().await;

    // OL returns 2 docs: one eng, one ger.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Book Two English",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series"],
                "series_number": "2",
                "key": "/works/OL_AC16_ENG",
                "language": ["eng"]
            },
            {
                "title": "Buch Zwei Deutsch",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series"],
                "series_number": "2",
                "key": "/works/OL_AC16_GER",
                "language": ["ger"]
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // config_language = Some("ger"), but the edition says "en" → "eng".
    // The or_else chain must use the edition-derived "eng" and ignore "ger".
    let inserted = series::series_fill(&pool, &client, &server.uri(), Some("ger"))
        .await
        .unwrap();

    // Only the English work must be queued; "ger" from config is ignored.
    assert_eq!(inserted, 1, "edition-derived language must override config_language");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(
        wants[0].title, "Book Two English",
        "only the English work must be in want list when edition language is 'en'"
    );
}

// ---------------------------------------------------------------------------
// AC-17: config_language is a valid ISO 639-2 code whose BCP-47 counterpart
// is not in the bcp47_to_iso639_2 table; series_fill must not error.
//
// The `bcp47_to_iso639_2` function is only used to convert edition language
// tags (BCP-47 from the DB).  `config_language` is already ISO 639-2 and is
// used directly as the filter value via the `or_else` fallback.  When
// `config_language = Some("jpn")` and the edition has no language tag,
// `target_lang` becomes `Some("jpn")`.  OL docs that carry no matching
// language code are filtered out; the result is 0 insertions.
// The important guarantee is that `series_fill` returns Ok (no crash).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series_fill_unmapped_config_language_no_filter() {
    let (pool, _tmp) = temp_pool().await;

    // Owned edition with no language field (language = NULL).
    insert_series_edition(&pool, "Book 1", "Test Series Unmapped Lang", "1", "unmaplang1").await;

    let server = MockServer::start().await;

    // OL returns 2 docs: one tagged "eng", one tagged "ger".
    // Neither matches "jpn", so both will be filtered by the language check.
    let ol_response = serde_json::json!({
        "docs": [
            {
                "title": "Book Two English",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series Unmapped Lang"],
                "series_number": "2",
                "key": "/works/OL_UML1W",
                "language": ["eng"]
            },
            {
                "title": "Buch Zwei Deutsch",
                "author_name": ["Series Author"],
                "isbn": [],
                "series": ["Test Series Unmapped Lang"],
                "series_number": "2",
                "key": "/works/OL_UML2W",
                "language": ["ger"]
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"/search\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ol_response))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // config_language = Some("jpn"): a valid ISO 639-2 code that is not a
    // BCP-47 primary subtag in the bcp47_to_iso639_2 table.  The function
    // must not panic or return Err — it must return Ok.
    let result = series::series_fill(&pool, &client, &server.uri(), Some("jpn")).await;
    assert!(
        result.is_ok(),
        "series_fill must not error when config_language is a valid ISO 639-2 code: {:?}",
        result
    );

    // "jpn" does not match any of the returned docs' language arrays, so
    // all docs are filtered out and nothing is inserted.
    let inserted = result.unwrap();
    assert_eq!(inserted, 0, "no works must be inserted when no OL doc matches the config language");

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 0, "want_list must be empty — no docs passed the language filter");
}
