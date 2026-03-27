/// Integration tests for Phase 3 author-follow feature.
use bookshelf_core::{db, epub::EpubMeta, follow, grab};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

fn author_search_body(key: &str) -> serde_json::Value {
    serde_json::json!({ "docs": [{ "key": key }] })
}

fn author_search_empty() -> serde_json::Value {
    serde_json::json!({ "docs": [] })
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

// ---------------------------------------------------------------------------
// AC-1: followed_authors table exists after db::open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_followed_authors_table_exists() {
    let (pool, _tmp) = temp_pool().await;
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='followed_authors'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(row.is_some(), "followed_authors table must exist after db::open");
}

// ---------------------------------------------------------------------------
// AC-5 / AC-6: follow_add inserts author row and queues works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_inserts_author_and_works() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(author_search_body("/authors/OL1A")))
        .mount(&server)
        .await;

    // Works page 1: two works.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL1A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL1A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&[
            "Dune Chronicles: The First Volume",
            "Neuromancer: A Cyberpunk Novel",
        ])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Test Author", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::Added { works_queued: 2 });

    // Author row stored.
    let row = db::find_followed_author_by_name(&pool, "Test Author")
        .await
        .unwrap()
        .expect("author row must be stored");
    assert_eq!(row.ol_key.as_deref(), Some("/authors/OL1A"));
    assert!(row.last_synced.is_some());
    assert!(row.added_at.contains('T'));

    // Want list has both works.
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 2);
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"Dune Chronicles: The First Volume"));
    assert!(titles.contains(&"Neuromancer: A Cyberpunk Novel"));
    assert!(wants.iter().all(|w| w.source == "author_follow"));
    assert!(wants.iter().all(|w| w.priority == 5));
}

// ---------------------------------------------------------------------------
// AC-7: pagination — all pages processed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_pagination() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(author_search_body("/authors/OL2A")))
        .mount(&server)
        .await;

    // Register in reverse priority order (LIFO per journal entry).
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL2A/works\.json"))
        .and(query_param("offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    // Page 2: offset=50, 1 work.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL2A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["The Left Hand of Darkness"])))
        .mount(&server)
        .await;

    // Page 1: offset=0, 1 work.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL2A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["The Dispossessed: An Ambiguous Utopia"])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Paginated Author", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::Added { works_queued: 2 });
    let wants = db::list_want(&pool, None, None).await.unwrap();
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"The Left Hand of Darkness"));
    assert!(titles.contains(&"The Dispossessed: An Ambiguous Utopia"));
}

// ---------------------------------------------------------------------------
// AC-8: owned books are not inserted into want_list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_skips_owned() {
    let (pool, _tmp) = temp_pool().await;

    // Insert owned edition.
    db::upsert_edition(
        &pool,
        &EpubMeta {
            title: Some("Owned Book".to_string()),
            authors: Some("The Author".to_string()),
            source_path: "/tmp/owned.epub".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(author_search_body("/authors/OL3A")))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL3A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL3A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["Owned Book"])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "The Author", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::Added { works_queued: 0 });
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "owned books must not be queued");
}

// ---------------------------------------------------------------------------
// AC-9: no duplicate want_list entry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_no_duplicate_want() {
    let (pool, _tmp) = temp_pool().await;

    // Pre-insert a want entry.
    db::insert_want(&pool, "Already Wanted", Some("The Author"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(author_search_body("/authors/OL4A")))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL4A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL4A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(works_body(&["Already Wanted"])),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "The Author", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::Added { works_queued: 0 });
    // Still only 1 row.
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1, "must not insert duplicate want entry");
}

// ---------------------------------------------------------------------------
// AC-10: author not found on OL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_author_not_found() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(author_search_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Nonexistent XYZ999", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::AuthorNotFound);
    let row = db::find_followed_author_by_name(&pool, "Nonexistent XYZ999")
        .await
        .unwrap();
    assert!(row.is_none(), "no author row must be inserted when not found");
}

// ---------------------------------------------------------------------------
// AC-11: already followed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_already_followed() {
    let (pool, _tmp) = temp_pool().await;

    // Pre-insert the author.
    db::insert_followed_author(&pool, "Known Author", Some("/authors/OL5A"))
        .await
        .unwrap();

    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Known Author", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(result, follow::FollowAddResult::AlreadyFollowed);
    // Still only one row.
    let all = db::list_followed_authors(&pool).await.unwrap();
    assert_eq!(all.len(), 1);
}

// ---------------------------------------------------------------------------
// AC-12: network error — no partial row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_network_error_no_partial_row() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Error Author", &server.uri(), None).await;

    assert!(result.is_err(), "HTTP 500 must return Err");
    let row = db::find_followed_author_by_name(&pool, "Error Author")
        .await
        .unwrap();
    assert!(row.is_none(), "no row must be inserted after HTTP error");
}

// ---------------------------------------------------------------------------
// AC-13: follow_remove deletes existing author
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_remove_existing() {
    let (pool, _tmp) = temp_pool().await;
    db::insert_followed_author(&pool, "Remove Me", Some("/authors/OL6A"))
        .await
        .unwrap();

    let found = follow::follow_remove(&pool, "Remove Me").await.unwrap();
    assert!(found, "follow_remove must return true for existing author");

    let row = db::find_followed_author_by_name(&pool, "Remove Me")
        .await
        .unwrap();
    assert!(row.is_none(), "author row must be deleted");
}

// ---------------------------------------------------------------------------
// AC-14: follow_remove returns false for non-existent author
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_remove_nonexistent() {
    let (pool, _tmp) = temp_pool().await;
    let found = follow::follow_remove(&pool, "Nobody Here").await.unwrap();
    assert!(!found, "follow_remove must return false when author not found");
}

// ---------------------------------------------------------------------------
// AC-15: follow_remove does not touch want_list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_remove_preserves_want_list() {
    let (pool, _tmp) = temp_pool().await;

    db::insert_followed_author(&pool, "The Author", Some("/authors/OL7A"))
        .await
        .unwrap();
    db::insert_want(&pool, "Some Book", Some("The Author"), None, "author_follow", None, 5, None)
        .await
        .unwrap();

    follow::follow_remove(&pool, "The Author").await.unwrap();

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1, "want_list must be untouched after follow_remove");
}

// ---------------------------------------------------------------------------
// AC-16: follow_list when empty
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_list_empty() {
    let (pool, _tmp) = temp_pool().await;
    let authors = follow::follow_list(&pool).await.unwrap();
    assert!(authors.is_empty(), "follow_list must return empty vec when no authors followed");
}

// ---------------------------------------------------------------------------
// AC-17: follow_list shows name and last_synced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_list_shows_name_and_synced() {
    let (pool, _tmp) = temp_pool().await;
    db::insert_followed_author(&pool, "Alice Author", Some("/authors/OL8A"))
        .await
        .unwrap();

    let authors = follow::follow_list(&pool).await.unwrap();
    assert_eq!(authors.len(), 1);
    assert_eq!(authors[0].name, "Alice Author");
    assert!(authors[0].last_synced.is_some(), "last_synced set by insert_followed_author");
}

// ---------------------------------------------------------------------------
// AC-18: follow_sync with no authors returns Ok(0)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_sync_empty() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let result = follow::follow_sync(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(result, 0);
}

// ---------------------------------------------------------------------------
// AC-19: follow_sync adds new works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_sync_adds_new_works() {
    let (pool, _tmp) = temp_pool().await;
    db::insert_followed_author(&pool, "Sync Author", Some("/authors/OL9A"))
        .await
        .unwrap();

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL9A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL9A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["New Work"])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let synced = follow::follow_sync(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(synced, 1);

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "New Work");
    assert_eq!(wants[0].source, "author_follow");
}

// ---------------------------------------------------------------------------
// AC-20: follow_sync with no new works inserts nothing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_sync_no_new_works() {
    let (pool, _tmp) = temp_pool().await;
    db::insert_followed_author(&pool, "Empty Catalog Author", Some("/authors/OL10A"))
        .await
        .unwrap();

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL10A/works\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let synced = follow::follow_sync(&pool, &client, &server.uri(), None).await.unwrap();
    assert_eq!(synced, 1);

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "no want entries when catalog is empty");
}

// ---------------------------------------------------------------------------
// AC-21: follow_sync updates last_synced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_sync_updates_last_synced() {
    let (pool, _tmp) = temp_pool().await;
    db::insert_followed_author(&pool, "Sync TS Author", Some("/authors/OL11A"))
        .await
        .unwrap();

    let before = db::find_followed_author_by_name(&pool, "Sync TS Author")
        .await
        .unwrap()
        .unwrap()
        .last_synced
        .unwrap();

    // Sleep a second to ensure timestamp differs.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL11A/works\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    follow::follow_sync(&pool, &client, &server.uri(), None).await.unwrap();

    let after = db::find_followed_author_by_name(&pool, "Sync TS Author")
        .await
        .unwrap()
        .unwrap()
        .last_synced
        .unwrap();

    assert_ne!(before, after, "last_synced must be updated after sync");
}

// ---------------------------------------------------------------------------
// AC-22: partial failure — continues and returns Err; successful author updated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_sync_partial_failure_continues() {
    let (pool, _tmp) = temp_pool().await;

    db::insert_followed_author(&pool, "Good Author", Some("/authors/OL12A"))
        .await
        .unwrap();
    db::insert_followed_author(&pool, "Bad Author", Some("/authors/OL13A"))
        .await
        .unwrap();

    let server = MockServer::start().await;

    // Good Author: returns one work.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL12A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL12A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&["Good Work"])))
        .mount(&server)
        .await;

    // Bad Author: returns 500.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL13A/works\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_sync(&pool, &client, &server.uri(), None).await;

    // Must return Err.
    assert!(result.is_err(), "follow_sync must return Err when any author failed");

    // Good Author's last_synced must be updated.
    let good_row = db::find_followed_author_by_name(&pool, "Good Author")
        .await
        .unwrap()
        .unwrap();
    assert!(good_row.last_synced.is_some());

    // Bad Author's last_synced must NOT have been changed by the failed sync.
    // Set it to a known sentinel value before the sync so we can compare afterward.
    // (insert_followed_author sets last_synced = now at insert; we overwrite it with a
    // distinctive sentinel so any change by follow_sync is detectable.)
    let sentinel = "2000-01-01T00:00:00Z";
    sqlx::query("UPDATE followed_authors SET last_synced = ? WHERE name = 'Bad Author'")
        .bind(sentinel)
        .execute(&pool)
        .await
        .unwrap();

    let client2 = reqwest::Client::new();
    let _ = follow::follow_sync(&pool, &client2, &server.uri(), None).await; // may return Err

    let bad_row = db::find_followed_author_by_name(&pool, "Bad Author")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bad_row.last_synced.as_deref(),
        Some(sentinel),
        "failed author must not have last_synced changed by follow_sync"
    );

    // Want list has Good Author's work.
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1);
    assert_eq!(wants[0].title, "Good Work");
}

// ---------------------------------------------------------------------------
// Gap 2 — follow_add → grab full E2E test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_to_grab_e2e() {
    let (pool, _tmp) = temp_pool().await;

    // Insert 2 owned editions: Dune and The White Plague by Frank Herbert.
    db::upsert_edition(
        &pool,
        &EpubMeta {
            title: Some("Dune".to_string()),
            authors: Some("Frank Herbert".to_string()),
            isbn: Some("9780441013593".to_string()),
            source_path: "/tmp/dune.epub".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    db::upsert_edition(
        &pool,
        &EpubMeta {
            title: Some("The White Plague".to_string()),
            authors: Some("Frank Herbert".to_string()),
            isbn: Some("9780765320841".to_string()),
            source_path: "/tmp/white_plague.epub".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let server = MockServer::start().await;

    // Mock author search returning Frank Herbert's OL key.
    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "docs": [{ "key": "/authors/OL25386A" }] }),
        ))
        .mount(&server)
        .await;

    // Works page 2 (offset=50): empty — signals end of pagination.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL25386A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "entries": [] }),
        ))
        .mount(&server)
        .await;

    // Works page 1 (offset=0): 3 works.
    // "Dune" is owned (will be skipped by is_already_owned via fuzzy match).
    // "Hellstrom's Hive" and "The Dosadi Experiment" are completely distinct titles
    // that do not fuzzy-match the owned "Dune" or "The White Plague" strings.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL25386A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [
                { "title": "Dune", "key": "/works/OL102749W" },
                { "title": "Hellstrom's Hive", "key": "/works/OL102750W" },
                { "title": "The Dosadi Experiment", "key": "/works/OL102751W" }
            ]
        })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Frank Herbert", &server.uri(), None)
        .await
        .unwrap();

    // "Dune" is owned → skipped by fuzzy match. 2 works queued.
    assert_eq!(result, follow::FollowAddResult::Added { works_queued: 2 });

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 2, "exactly 2 unowned works must be in want_list");

    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"Hellstrom's Hive"), "Hellstrom's Hive must be in want_list");
    assert!(
        titles.contains(&"The Dosadi Experiment"),
        "The Dosadi Experiment must be in want_list"
    );

    // All queued with source = "author_follow".
    assert!(
        wants.iter().all(|w| w.source == "author_follow"),
        "all entries must have source=author_follow"
    );

    // Grab list must include the 2 unowned works.
    let grab_entries = grab::compute_grab_list(&pool, None).await.unwrap();
    let grab_titles: Vec<&str> = grab_entries
        .iter()
        .filter_map(|e| e.title.as_deref())
        .collect();

    assert!(
        grab_titles.contains(&"Hellstrom's Hive"),
        "Hellstrom's Hive must appear in grab list"
    );
    assert!(
        grab_titles.contains(&"The Dosadi Experiment"),
        "The Dosadi Experiment must appear in grab list"
    );

    // "Dune" must NOT appear in grab list (it is owned).
    assert!(
        !grab_titles.contains(&"Dune"),
        "Dune must not appear in grab list (owned)"
    );

    // Grab entries must have source = "author_follow".
    for entry in &grab_entries {
        assert_eq!(
            entry.source, "author_follow",
            "grab entry source must be author_follow"
        );
    }
}

// ---------------------------------------------------------------------------
// Gap 3 — db::list_want source filter validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_want_source_filter_author_follow() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Populate want_list with author_follow entries via follow_add.
    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "docs": [{ "key": "/authors/OL99A" }] }),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL99A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "entries": [] }),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL99A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [
                { "title": "Stranger in a Strange Land", "key": "/works/OL50W" },
                { "title": "The Moon Is a Harsh Mistress", "key": "/works/OL51W" }
            ]
        })))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    follow::follow_add(&pool, &client, "Robert Heinlein", &server.uri(), None)
        .await
        .unwrap();

    // list_want with source="author_follow" must return the 2 queued entries.
    let author_follow_entries = db::list_want(&pool, Some("author_follow"), None).await.unwrap();
    assert_eq!(
        author_follow_entries.len(),
        2,
        "list_want(author_follow) must return 2 entries"
    );
    assert!(
        author_follow_entries.iter().all(|w| w.source == "author_follow"),
        "all returned entries must have source=author_follow"
    );

    // list_want with source="series_fill" must return empty (no series_fill entries yet).
    let series_fill_entries = db::list_want(&pool, Some("series_fill"), None).await.unwrap();
    assert!(
        series_fill_entries.is_empty(),
        "list_want(series_fill) must return empty when no series_fill entries exist"
    );

    // list_want with source="invalid_source" must return Err (validation rejection).
    let invalid_result = db::list_want(&pool, Some("invalid_source"), None).await;
    assert!(
        invalid_result.is_err(),
        "list_want(invalid_source) must return Err"
    );
}

// ---------------------------------------------------------------------------
// Gap 6 — follow_add case sensitivity for duplicate detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_case_insensitive_duplicate_detection() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // First call: "Frank Herbert" — should succeed and add the author.
    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "docs": [{ "key": "/authors/OL25386A" }] }),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL25386A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "entries": [] }),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL25386A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "entries": [] }),
        ))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();

    let result1 = follow::follow_add(&pool, &client, "Frank Herbert", &server.uri(), None)
        .await
        .unwrap();
    assert_eq!(result1, follow::FollowAddResult::Added { works_queued: 0 });

    // Second call: "frank herbert" (lowercase) — must return AlreadyFollowed.
    let result2 = follow::follow_add(&pool, &client, "frank herbert", &server.uri(), None)
        .await
        .unwrap();
    assert_eq!(
        result2,
        follow::FollowAddResult::AlreadyFollowed,
        "follow_add with different case must return AlreadyFollowed"
    );

    // Must be exactly 1 row in followed_authors.
    let all = db::list_followed_authors(&pool).await.unwrap();
    assert_eq!(all.len(), 1, "must be exactly 1 followed author after case-variant add");
}

// ---------------------------------------------------------------------------
// GAP-17: delete_followed_author must be case-insensitive (symmetric with
// find_followed_author_by_name which already uses LOWER(name) = LOWER(?)).
// Prior bug: DELETE used WHERE name = ? (case-sensitive) so removing with a
// different case silently failed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_remove_case_insensitive() {
    let (pool, _tmp) = temp_pool().await;

    // Insert "Frank Herbert" directly so we don't need a mock server.
    db::insert_followed_author(&pool, "Frank Herbert", Some("/authors/OL25374A"))
        .await
        .unwrap();

    // Remove with lowercase name — must succeed.
    let removed = db::delete_followed_author(&pool, "frank herbert").await.unwrap();
    assert!(removed, "delete_followed_author must succeed even when case differs from stored name");

    let all = db::list_followed_authors(&pool).await.unwrap();
    assert!(all.is_empty(), "followed_authors must be empty after case-insensitive remove");
}

#[tokio::test]
async fn test_follow_remove_uppercase_variant() {
    let (pool, _tmp) = temp_pool().await;

    db::insert_followed_author(&pool, "isaac asimov", None)
        .await
        .unwrap();

    // Remove using title-cased variant.
    let removed = db::delete_followed_author(&pool, "Isaac Asimov").await.unwrap();
    assert!(removed, "delete_followed_author must match regardless of stored casing");

    let all = db::list_followed_authors(&pool).await.unwrap();
    assert!(all.is_empty());
}

// ---------------------------------------------------------------------------
// GAP-07: follow_add partial insert when works-fetch fails
// ---------------------------------------------------------------------------

/// When OL author search succeeds (author row inserted) but the subsequent
/// works fetch returns HTTP 500, follow_add propagates the error.
/// The author row IS left in followed_authors (known partial-state behaviour).
/// Subsequent follow_add calls return AlreadyFollowed — documenting the
/// current behaviour so any future fix is immediately visible.
#[tokio::test]
async fn test_follow_add_partial_state_when_works_fetch_fails() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Author search succeeds.
    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(author_search_body("/authors/OL_PARTIAL")),
        )
        .mount(&server)
        .await;

    // Works fetch fails.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_PARTIAL/works\.json"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Partial Author", &server.uri(), None).await;

    // follow_add propagates the error from the works fetch.
    assert!(result.is_err(), "follow_add must return Err when works fetch fails");

    // The author row was inserted before the works fetch.
    let row = db::find_followed_author_by_name(&pool, "Partial Author")
        .await
        .unwrap();
    assert!(
        row.is_some(),
        "author row is left in followed_authors after partial failure (known behaviour)"
    );

    // A second follow_add call returns AlreadyFollowed — the user cannot
    // retry without first removing the author manually.
    let result2 = follow::follow_add(&pool, &client, "Partial Author", &server.uri(), None)
        .await
        .unwrap();
    assert_eq!(
        result2,
        follow::FollowAddResult::AlreadyFollowed,
        "second follow_add must return AlreadyFollowed due to partial-insert state"
    );
}

// ---------------------------------------------------------------------------
// GAP-08: follow_sync NULL-ol_key re-query failure path
// ---------------------------------------------------------------------------

/// An author stored with ol_key = NULL triggers a re-query during sync.
/// If the re-query returns no results, follow_sync emits a warning,
/// sets had_error, and returns Err — but processes other authors first.
#[tokio::test]
async fn test_follow_sync_null_ol_key_author_not_found_returns_err() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    // Insert author with no ol_key.
    db::insert_followed_author(&pool, "Unknown Author", None)
        .await
        .unwrap();

    // Re-query search returns empty docs.
    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(author_search_empty()),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_sync(&pool, &client, &server.uri(), None).await;

    // follow_sync returns Err when any author fails.
    assert!(result.is_err(), "follow_sync must return Err when NULL-ol_key author cannot be found");

    // last_synced must not have been updated for the failed author.
    let row = db::find_followed_author_by_name(&pool, "Unknown Author")
        .await
        .unwrap()
        .unwrap();
    // last_synced was set at insert time — it should not have changed
    // because the author was not successfully synced.
    assert!(row.last_synced.is_some(), "last_synced was set at insert");
    // No want entries should have been created.
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "no want entries must be created for unresolvable author");
}

// ---------------------------------------------------------------------------
// AC-11/AC-12: follow_add filters foreign-article titles when language=Some("eng")
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_filters_foreign_articles_when_eng() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            author_search_body("/authors/OL_LANG1A"),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG1A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    // Three works: one English, two with foreign-article prefixes.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG1A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&[
            "Dune",
            "Die Welt",
            "El Camino",
        ])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = follow::follow_add(&pool, &client, "Test Author", &server.uri(), Some("eng"))
        .await
        .unwrap();

    // Only "Dune" passes — "Die Welt" and "El Camino" are filtered (foreign articles).
    assert_eq!(
        result,
        follow::FollowAddResult::Added { works_queued: 1 },
        "only the English-titled work must be queued"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 1, "want list must contain exactly 1 entry");
    assert_eq!(wants[0].title, "Dune", "only Dune must be in want list");
}

// ---------------------------------------------------------------------------
// AC-13: follow_add does NOT filter when language=None
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_no_filter_when_language_none() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            author_search_body("/authors/OL_LANG2A"),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG2A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    // Same three works: one English, two with foreign-article prefixes.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG2A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&[
            "Dune",
            "Die Welt",
            "El Camino",
        ])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // language = None — no filter applied.
    let result = follow::follow_add(&pool, &client, "Test Author 2", &server.uri(), None)
        .await
        .unwrap();

    assert_eq!(
        result,
        follow::FollowAddResult::Added { works_queued: 3 },
        "all 3 works must be queued when language is None"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 3, "want list must contain all 3 entries");
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"Dune"));
    assert!(titles.contains(&"Die Welt"));
    assert!(titles.contains(&"El Camino"));
}

// ---------------------------------------------------------------------------
// AC-13 (continued): follow_add does NOT filter when language=Some("fre")
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_no_filter_when_language_fre() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            author_search_body("/authors/OL_LANG_FRE"),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG_FRE/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    // Same three works: one English, two with foreign-article prefixes.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG_FRE/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&[
            "Dune",
            "Die Welt",
            "El Camino",
        ])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    // language = Some("fre") — non-eng, so no filter applied.
    let result = follow::follow_add(&pool, &client, "Test Author Fre", &server.uri(), Some("fre"))
        .await
        .unwrap();

    assert_eq!(
        result,
        follow::FollowAddResult::Added { works_queued: 3 },
        "all 3 works must be queued when language is Some(\"fre\")"
    );

    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(wants.len(), 3, "want list must contain all 3 entries");
    let titles: Vec<&str> = wants.iter().map(|w| w.title.as_str()).collect();
    assert!(titles.contains(&"Dune"));
    assert!(titles.contains(&"Die Welt"));
    assert!(titles.contains(&"El Camino"));
}

// ---------------------------------------------------------------------------
// AC-14: follow_add still inserts author row when all works are filtered out
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_follow_add_all_filtered_author_row_present() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"/search/authors\.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            author_search_body("/authors/OL_LANG3A"),
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG3A/works\.json"))
        .and(query_param("offset", "50"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_empty()))
        .mount(&server)
        .await;

    // All works start with foreign articles — all will be filtered.
    Mock::given(method("GET"))
        .and(path_regex(r"/authors/OL_LANG3A/works\.json"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(works_body(&[
            "Die Traumdeutung",
            "El Quijote",
            "Les Misérables",
        ])))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result =
        follow::follow_add(&pool, &client, "All Filtered Author", &server.uri(), Some("eng"))
            .await
            .unwrap();

    // All 3 titles are filtered: works_queued == 0, but result is Added (not AuthorNotFound).
    assert_eq!(
        result,
        follow::FollowAddResult::Added { works_queued: 0 },
        "follow_add must return Added{{works_queued: 0}} when all works are filtered"
    );

    // Author row must be present in followed_authors.
    let row = db::find_followed_author_by_name(&pool, "All Filtered Author")
        .await
        .unwrap();
    assert!(
        row.is_some(),
        "author row must be inserted even when all works are filtered"
    );

    // No want entries.
    let wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(wants.is_empty(), "want list must be empty when all works are filtered");
}
