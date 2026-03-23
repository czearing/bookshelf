/// QA-only tests for AC-2, AC-3, AC-4: insert_want source validation.
use bookshelf_core::db;
use tempfile::NamedTempFile;

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// AC-2: insert_want accepts source = "author_follow" without error.
#[tokio::test]
async fn test_insert_want_source_author_follow_accepted() {
    let (pool, _tmp) = temp_pool().await;
    let result = db::insert_want(
        &pool,
        "Test Book",
        Some("Test Author"),
        None,
        "author_follow",
        None,
        5,
        None,
    )
    .await;
    assert!(result.is_ok(), "source 'author_follow' must be accepted, got: {:?}", result);
}

/// AC-3: insert_want accepts source = "series_fill" without error.
#[tokio::test]
async fn test_insert_want_source_series_fill_accepted() {
    let (pool, _tmp) = temp_pool().await;
    let result = db::insert_want(
        &pool,
        "Test Book",
        Some("Test Author"),
        None,
        "series_fill",
        None,
        7,
        None,
    )
    .await;
    assert!(result.is_ok(), "source 'series_fill' must be accepted, got: {:?}", result);
}

/// AC-4: insert_want rejects an unknown source value.
#[tokio::test]
async fn test_insert_want_invalid_source_rejected() {
    let (pool, _tmp) = temp_pool().await;
    let result = db::insert_want(
        &pool,
        "Test Book",
        Some("Test Author"),
        None,
        "invalid_source_xyz",
        None,
        5,
        None,
    )
    .await;
    assert!(result.is_err(), "invalid source must return Err");
}

/// AC-4 edge: all six valid sources are accepted.
#[tokio::test]
async fn test_insert_want_all_valid_sources_accepted() {
    let (pool, _tmp) = temp_pool().await;
    let valid_sources = &[
        "goodreads_csv",
        "openlibrary",
        "manual",
        "text_file",
        "author_follow",
        "series_fill",
    ];
    for source in valid_sources {
        let result = db::insert_want(
            &pool,
            &format!("Book for {source}"),
            Some("Author"),
            None,
            source,
            None,
            5,
            None,
        )
        .await;
        assert!(result.is_ok(), "source '{source}' must be accepted, got: {:?}", result);
    }
}
