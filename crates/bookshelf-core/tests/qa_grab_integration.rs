/// QA-only tests for AC-39 and AC-40: grab integration with author_follow and series_fill sources.
use bookshelf_core::{db, grab};
use tempfile::NamedTempFile;

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// AC-39: grab includes author_follow and series_fill entries with correct source column.
#[tokio::test]
async fn test_grab_includes_author_follow_and_series_fill_sources() {
    let (pool, _tmp) = temp_pool().await;

    db::insert_want(&pool, "Author Follow Book", Some("Author A"), None, "author_follow", None, 5, None)
        .await
        .unwrap();
    db::insert_want(&pool, "Series Fill Book", Some("Author B"), None, "series_fill", None, 7, None)
        .await
        .unwrap();

    let entries = grab::compute_grab_list(&pool, None).await.unwrap();
    let author_follow_entry = entries.iter().find(|e| e.title.as_deref() == Some("Author Follow Book"));
    let series_fill_entry = entries.iter().find(|e| e.title.as_deref() == Some("Series Fill Book"));

    assert!(author_follow_entry.is_some(), "author_follow entry must appear in grab list");
    assert_eq!(author_follow_entry.unwrap().source, "author_follow");

    assert!(series_fill_entry.is_some(), "series_fill entry must appear in grab list");
    assert_eq!(series_fill_entry.unwrap().source, "series_fill");
}

/// AC-40: grab --min-priority 8 excludes series_fill (priority 7) and author_follow (priority 5).
#[tokio::test]
async fn test_grab_min_priority_8_excludes_author_follow_and_series_fill() {
    let (pool, _tmp) = temp_pool().await;

    db::insert_want(&pool, "Author Follow Book", Some("Author A"), None, "author_follow", None, 5, None)
        .await
        .unwrap();
    db::insert_want(&pool, "Series Fill Book", Some("Author B"), None, "series_fill", None, 7, None)
        .await
        .unwrap();
    // Also insert a high-priority manual entry that should be included.
    db::insert_want(&pool, "High Priority Book", Some("Author C"), None, "manual", None, 9, None)
        .await
        .unwrap();

    let entries = grab::compute_grab_list(&pool, Some(8)).await.unwrap();
    let titles: Vec<&str> = entries.iter().filter_map(|e| e.title.as_deref()).collect();

    assert!(!titles.contains(&"Author Follow Book"), "author_follow (priority 5) must be excluded at --min-priority 8");
    assert!(!titles.contains(&"Series Fill Book"), "series_fill (priority 7) must be excluded at --min-priority 8");
    assert!(titles.contains(&"High Priority Book"), "manual (priority 9) must be included at --min-priority 8");
}
