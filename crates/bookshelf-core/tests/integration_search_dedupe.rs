/// Integration tests for Feature 2 (search_editions) and Feature 3 (find_duplicate_editions).
use bookshelf_core::{db, epub::EpubMeta};
use tempfile::NamedTempFile;

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

async fn insert_edition(pool: &db::DbPool, title: &str, authors: &str, series_name: Option<&str>, path_suffix: &str) -> i64 {
    let meta = EpubMeta {
        title: Some(title.to_string()),
        authors: Some(authors.to_string()),
        series_name: series_name.map(|s| s.to_string()),
        source_path: format!("/tmp/{path_suffix}.epub"),
        ..Default::default()
    };
    db::upsert_edition(pool, &meta).await.unwrap().0
}

// ---------------------------------------------------------------------------
// Feature 2: search_editions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_search_finds_by_partial_title() {
    let (pool, _tmp) = temp_pool().await;
    insert_edition(&pool, "The Great Gatsby", "F. Scott Fitzgerald", None, "gatsby").await;
    insert_edition(&pool, "A Farewell to Arms", "Ernest Hemingway", None, "farewell").await;
    insert_edition(&pool, "Gatsby's Return", "Other Author", None, "gatsby2").await;

    let results = db::search_editions(&pool, "Gatsby").await.unwrap();
    assert_eq!(results.len(), 2, "should find 2 books matching 'Gatsby'");
    let titles: Vec<&str> = results.iter().map(|e| e.title.as_deref().unwrap_or("")).collect();
    assert!(titles.contains(&"The Great Gatsby"), "must include 'The Great Gatsby'");
    assert!(titles.contains(&"Gatsby's Return"), "must include 'Gatsby's Return'");
}

#[tokio::test]
async fn test_search_finds_by_author() {
    let (pool, _tmp) = temp_pool().await;
    insert_edition(&pool, "For Whom the Bell Tolls", "Ernest Hemingway", None, "bell").await;
    insert_edition(&pool, "The Sun Also Rises", "Ernest Hemingway", None, "sun").await;
    insert_edition(&pool, "Moby Dick", "Herman Melville", None, "moby").await;

    let results = db::search_editions(&pool, "Hemingway").await.unwrap();
    assert_eq!(results.len(), 2, "should find 2 Hemingway books");
}

#[tokio::test]
async fn test_search_finds_by_series() {
    let (pool, _tmp) = temp_pool().await;
    insert_edition(&pool, "The Final Empire", "Brandon Sanderson", Some("Mistborn"), "mistborn1").await;
    insert_edition(&pool, "The Well of Ascension", "Brandon Sanderson", Some("Mistborn"), "mistborn2").await;
    insert_edition(&pool, "The Way of Kings", "Brandon Sanderson", Some("Stormlight"), "stormlight1").await;

    let results = db::search_editions(&pool, "Mistborn").await.unwrap();
    assert_eq!(results.len(), 2, "should find 2 Mistborn books via series_name");
}

#[tokio::test]
async fn test_search_empty_result() {
    let (pool, _tmp) = temp_pool().await;
    insert_edition(&pool, "Some Book", "Some Author", None, "some").await;

    let results = db::search_editions(&pool, "zzznomatch").await.unwrap();
    assert!(results.is_empty(), "should return empty for no match");
}

// ---------------------------------------------------------------------------
// Feature 3: find_duplicate_editions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_duplicates_detects_exact_match() {
    let (pool, _tmp) = temp_pool().await;
    // Insert two copies with the same title+authors.
    let meta1 = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        source_path: "/tmp/dune_copy1.epub".to_string(),
        ..Default::default()
    };
    let meta2 = EpubMeta {
        title: Some("Dune".to_string()),
        authors: Some("Frank Herbert".to_string()),
        source_path: "/tmp/dune_copy2.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta1).await.unwrap();
    db::upsert_edition(&pool, &meta2).await.unwrap();

    let groups = db::find_duplicate_editions(&pool).await.unwrap();
    assert_eq!(groups.len(), 1, "should find 1 duplicate group");
    assert_eq!(groups[0].len(), 2, "group should have 2 entries");
}

#[tokio::test]
async fn test_find_duplicates_case_insensitive() {
    let (pool, _tmp) = temp_pool().await;
    let meta1 = EpubMeta {
        title: Some("dune".to_string()),
        authors: Some("frank herbert".to_string()),
        source_path: "/tmp/dune_lower.epub".to_string(),
        ..Default::default()
    };
    let meta2 = EpubMeta {
        title: Some("DUNE".to_string()),
        authors: Some("FRANK HERBERT".to_string()),
        source_path: "/tmp/dune_upper.epub".to_string(),
        ..Default::default()
    };
    db::upsert_edition(&pool, &meta1).await.unwrap();
    db::upsert_edition(&pool, &meta2).await.unwrap();

    let groups = db::find_duplicate_editions(&pool).await.unwrap();
    assert_eq!(groups.len(), 1, "case-insensitive match should produce 1 group");
}

#[tokio::test]
async fn test_find_duplicates_no_duplicates() {
    let (pool, _tmp) = temp_pool().await;
    insert_edition(&pool, "Dune", "Frank Herbert", None, "dune1").await;
    insert_edition(&pool, "Foundation", "Isaac Asimov", None, "foundation1").await;

    let groups = db::find_duplicate_editions(&pool).await.unwrap();
    assert!(groups.is_empty(), "distinct books should not be grouped as duplicates");
}

#[tokio::test]
async fn test_find_duplicates_three_copies() {
    let (pool, _tmp) = temp_pool().await;
    for i in 1..=3 {
        let meta = EpubMeta {
            title: Some("Neuromancer".to_string()),
            authors: Some("William Gibson".to_string()),
            source_path: format!("/tmp/neuro_copy{i}.epub"),
            ..Default::default()
        };
        db::upsert_edition(&pool, &meta).await.unwrap();
    }

    let groups = db::find_duplicate_editions(&pool).await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), 3);
}
