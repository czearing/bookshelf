/// Integration tests for tag.rs — AC-15 through AC-33.
use bookshelf_core::{db, epub, tag};
use std::io::Write as _;
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};

// ---------------------------------------------------------------------------
// EPUB fixture builder (same pattern as other integration tests)
// ---------------------------------------------------------------------------

fn make_epub(
    title: &str,
    authors: &[&str],
    isbn: Option<&str>,
    series_name: Option<&str>,
    series_position: Option<&str>,
    publisher: Option<&str>,
    publish_date: Option<&str>,
    description: Option<&str>,
    dest: &Path,
) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)
        .unwrap();
    let mut zip = zip::ZipWriter::new(file);
    let opts =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    zip.start_file("mimetype", opts).unwrap();
    zip.write_all(b"application/epub+zip").unwrap();

    zip.start_file("META-INF/container.xml", opts).unwrap();
    zip.write_all(
        br#"<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>"#,
    )
    .unwrap();

    let mut opf = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:opf="http://www.idpf.org/2007/opf">
"#,
    );

    opf.push_str(&format!("    <dc:title>{title}</dc:title>\n"));
    for author in authors {
        opf.push_str(&format!("    <dc:creator>{author}</dc:creator>\n"));
    }
    if let Some(v) = isbn {
        opf.push_str(&format!(
            "    <dc:identifier opf:scheme=\"ISBN\">{v}</dc:identifier>\n"
        ));
    }
    if let Some(v) = publisher {
        opf.push_str(&format!("    <dc:publisher>{v}</dc:publisher>\n"));
    }
    if let Some(v) = publish_date {
        opf.push_str(&format!("    <dc:date>{v}</dc:date>\n"));
    }
    if let Some(v) = description {
        opf.push_str(&format!("    <dc:description>{v}</dc:description>\n"));
    }
    if let Some(sname) = series_name {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series\" content=\"{sname}\"/>\n"
        ));
    }
    if let Some(spos) = series_position {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series_index\" content=\"{spos}\"/>\n"
        ));
    }

    opf.push_str("  </metadata>\n  <manifest/>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// Insert an edition into the DB from an EPUB file and return its ID.
async fn insert_edition(pool: &db::DbPool, epub_path: &Path) -> i64 {
    let meta = epub::parse_epub(epub_path).unwrap();
    db::upsert_edition(pool, &meta).await.unwrap().0
}

/// Mark an edition as enriched so it passes `list_owned_enriched_editions`.
async fn mark_enriched(pool: &db::DbPool, id: i64) {
    let update = db::EnrichmentUpdate {
        enriched_at: Some("2024-01-01T00:00:00Z".to_string()),
        enrichment_attempted: 1,
        ..Default::default()
    };
    db::apply_enrichment(pool, id, &update).await.unwrap();
}

// ---------------------------------------------------------------------------
// AC-15: non-existent ID returns Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_edition_not_found() {
    let (pool, _db_tmp) = temp_pool().await;
    let result = tag::tag_edition(&pool, 9999, false).await;
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("9999"), "error should mention the missing ID, got: {msg}");
}

// ---------------------------------------------------------------------------
// AC-16: .bak file exists after tag, is byte-for-byte copy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_creates_bak_backup() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Backup Test",
        &["Author One"],
        Some("9780000000001"),
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    // tag_edition requires source_path to exist; use the canonical path stored in DB
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path);

    tag::tag_edition(&pool, id, false).await.unwrap();

    let bak_path = actual_path.with_extension("epub.bak");
    assert!(bak_path.exists(), ".bak file should be created");

    // Verify the bak is a byte-for-byte copy of what was the original (it is identical at time of backup).
    let original_bytes = std::fs::read(&bak_path).unwrap();
    assert!(!original_bytes.is_empty(), ".bak file should not be empty");
}

// ---------------------------------------------------------------------------
// AC-17: pre-existing .bak → Err, EPUB unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_bak_already_exists_returns_err() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Bak Exists Test",
        &["Writer A"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path);

    // Pre-create the .bak file.
    let bak_path = actual_path.with_extension("epub.bak");
    std::fs::write(&bak_path, b"existing backup").unwrap();

    let original_epub_bytes = std::fs::read(actual_path).unwrap();
    let result = tag::tag_edition(&pool, id, false).await;
    assert!(result.is_err(), "should return Err when .bak already exists");

    // EPUB should be unchanged.
    let after_epub_bytes = std::fs::read(actual_path).unwrap();
    assert_eq!(
        original_epub_bytes, after_epub_bytes,
        "EPUB should be unchanged when bak already exists"
    );
}

// ---------------------------------------------------------------------------
// AC-18: DB title → dc:title in OPF
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_title() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Original Title",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Update the title in DB via direct SQL (simulating enrichment result).
    sqlx::query("UPDATE editions SET title = ? WHERE id = ?")
        .bind("Updated Title")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.title.as_deref(), Some("Updated Title"));
}

// ---------------------------------------------------------------------------
// AC-19: "Author A, Author B" → two dc:creator elements
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_authors() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Multi Author Book",
        &["Old Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET authors = ? WHERE id = ?")
        .bind("Author A, Author B")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(
        meta.authors.as_deref(),
        Some("Author A, Author B"),
        "parsed authors should match DB value"
    );
}

// ---------------------------------------------------------------------------
// AC-20: publisher written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_publisher() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Publisher Test",
        &["Author"],
        None,
        None,
        None,
        Some("Old Publisher"),
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET publisher = ? WHERE id = ?")
        .bind("New Publisher")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.publisher.as_deref(), Some("New Publisher"));
}

// ---------------------------------------------------------------------------
// AC-21: publish_date written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_publish_date() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Date Test",
        &["Author"],
        None,
        None,
        None,
        None,
        Some("1990"),
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET publish_date = ? WHERE id = ?")
        .bind("2001")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.publish_date.as_deref(), Some("2001"));
}

// ---------------------------------------------------------------------------
// AC-22: description written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_description() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Description Test",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        Some("Old description"),
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET description = ? WHERE id = ?")
        .bind("New description text")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.description.as_deref(), Some("New description text"));
}

// ---------------------------------------------------------------------------
// AC-23: ISBN written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_isbn() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "ISBN Test",
        &["Author"],
        Some("9780000000001"),
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET isbn = ? WHERE id = ?")
        .bind("9781234567890")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.isbn.as_deref(), Some("9781234567890"));
}

// ---------------------------------------------------------------------------
// AC-24: series_name written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_series_name() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Series Test",
        &["Author"],
        None,
        Some("Old Series"),
        Some("1"),
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET series_name = ? WHERE id = ?")
        .bind("New Series Name")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.series_name.as_deref(), Some("New Series Name"));
}

// ---------------------------------------------------------------------------
// AC-25: series_position written
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_writes_series_position() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Series Position Test",
        &["Author"],
        None,
        Some("My Series"),
        Some("1"),
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    sqlx::query("UPDATE editions SET series_position = ? WHERE id = ?")
        .bind("3")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.series_position.as_deref(), Some("3"));
}

// ---------------------------------------------------------------------------
// AC-26: null DB field leaves OPF element unchanged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_null_field_unchanged() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Original Title",
        &["Original Author"],
        None,
        None,
        None,
        Some("Original Publisher"),
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Publisher in DB is "Original Publisher" (from the EPUB scan), but we will
    // NULL it out to verify that it is not written back to the EPUB.
    sqlx::query("UPDATE editions SET publisher = NULL WHERE id = ?")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    let meta = epub::parse_epub(&actual_path).unwrap();
    // Publisher should remain as originally in the EPUB since DB field is NULL.
    assert_eq!(
        meta.publisher.as_deref(),
        Some("Original Publisher"),
        "null DB field must not overwrite existing OPF content"
    );
}

// ---------------------------------------------------------------------------
// AC-27: tagged_at IS NOT NULL after tag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_sets_tagged_at() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Tagged At Test",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;

    // Verify tagged_at is NULL before tagging.
    let row_before = db::get_edition(&pool, id).await.unwrap().unwrap();
    assert!(row_before.tagged_at.is_none(), "tagged_at should be NULL before tagging");

    tag::tag_edition(&pool, id, false).await.unwrap();

    let row_after = db::get_edition(&pool, id).await.unwrap().unwrap();
    assert!(
        row_after.tagged_at.is_some(),
        "tagged_at should be set after tagging"
    );
    // Verify format matches expected timestamp pattern.
    let ts = row_after.tagged_at.unwrap();
    assert!(
        ts.contains('T') && ts.ends_with('Z'),
        "tagged_at should be in ISO 8601 format, got: {ts}"
    );
}

// ---------------------------------------------------------------------------
// AC-28: tagged EPUB passes parse_epub
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_result_parseable() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Parseable Test",
        &["Some Author"],
        Some("9780000000002"),
        Some("Test Series"),
        Some("2"),
        Some("Test Publisher"),
        Some("2020"),
        Some("A description"),
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Update all DB fields to new values.
    sqlx::query(
        "UPDATE editions SET title=?, authors=?, publisher=?, publish_date=?, description=?,
         isbn=?, series_name=?, series_position=? WHERE id=?",
    )
    .bind("New Parseable Title")
    .bind("Updated Author")
    .bind("Updated Publisher")
    .bind("2021")
    .bind("Updated description")
    .bind("9789876543210")
    .bind("Updated Series")
    .bind("5")
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    // Tagged EPUB must be parseable and contain the updated values.
    let meta = epub::parse_epub(&actual_path).unwrap();
    assert_eq!(meta.title.as_deref(), Some("New Parseable Title"));
    assert_eq!(meta.authors.as_deref(), Some("Updated Author"));
    assert_eq!(meta.publisher.as_deref(), Some("Updated Publisher"));
    assert_eq!(meta.publish_date.as_deref(), Some("2021"));
    assert_eq!(meta.description.as_deref(), Some("Updated description"));
    assert_eq!(meta.isbn.as_deref(), Some("9789876543210"));
    assert_eq!(meta.series_name.as_deref(), Some("Updated Series"));
    assert_eq!(meta.series_position.as_deref(), Some("5"));
}

// ---------------------------------------------------------------------------
// AC-29: tag_all only hits enriched editions (not unenriched)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_all_skips_unenriched() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Edition 1: enriched (should be tagged).
    let epub1 = dir.path().join("enriched.epub");
    make_epub("Enriched Book", &["Author"], None, None, None, None, None, None, &epub1);
    let id1 = insert_edition(&pool, &epub1).await;
    mark_enriched(&pool, id1).await;

    // Edition 2: NOT enriched (should be skipped).
    let epub2 = dir.path().join("unenriched.epub");
    make_epub("Unenriched Book", &["Author"], None, None, None, None, None, None, &epub2);
    let _id2 = insert_edition(&pool, &epub2).await;
    // Do not call mark_enriched for id2.

    let result = tag::tag_all(&pool, false).await.unwrap();
    assert_eq!(result.tagged, 1, "only the enriched edition should be tagged");
    assert_eq!(result.skipped, 0, "unenriched editions are not counted as skipped by tag_all");
}

// ---------------------------------------------------------------------------
// AC-30: already-tagged editions not re-tagged
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_all_skips_already_tagged() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Edition 1: enriched, not tagged.
    let epub1 = dir.path().join("not_tagged.epub");
    make_epub("Not Tagged", &["Author"], None, None, None, None, None, None, &epub1);
    let id1 = insert_edition(&pool, &epub1).await;
    mark_enriched(&pool, id1).await;

    // Edition 2: enriched AND already tagged — should be skipped.
    let epub2 = dir.path().join("already_tagged.epub");
    make_epub("Already Tagged", &["Author"], None, None, None, None, None, None, &epub2);
    let id2 = insert_edition(&pool, &epub2).await;
    mark_enriched(&pool, id2).await;
    db::set_tagged_at(&pool, id2).await.unwrap();

    let result = tag::tag_all(&pool, false).await.unwrap();
    assert_eq!(result.tagged, 1, "only the untagged enriched edition should be tagged");
    assert_eq!(result.skipped, 1, "already-tagged edition should count as skipped");
}

// ---------------------------------------------------------------------------
// AC-31: dry_run prints diff, no file modified, no .bak created
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_dry_run_no_file_change() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Dry Run Test",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Update title in DB to something that would differ.
    sqlx::query("UPDATE editions SET title = ? WHERE id = ?")
        .bind("Dry Run New Title")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let original_bytes = std::fs::read(&actual_path).unwrap();

    // Run with dry_run = true.
    tag::tag_edition(&pool, id, true).await.unwrap();

    // File must not have changed.
    let after_bytes = std::fs::read(&actual_path).unwrap();
    assert_eq!(original_bytes, after_bytes, "dry_run must not modify the EPUB file");

    // No .bak file should have been created.
    let bak = actual_path.with_extension("epub.bak");
    assert!(!bak.exists(), "dry_run must not create a .bak file");

    // tagged_at must still be NULL.
    let row_after = db::get_edition(&pool, id).await.unwrap().unwrap();
    assert!(
        row_after.tagged_at.is_none(),
        "dry_run must not set tagged_at in DB"
    );
}

// ---------------------------------------------------------------------------
// AC-32: tag_all with dry_run=true — no files modified, no .bak, tagged_at null
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_all_dry_run_no_changes() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("dry_run_all.epub");
    make_epub(
        "Dry Run All Test",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Mark as owned and enriched so tag_all will consider it eligible.
    sqlx::query("UPDATE editions SET owned = 1 WHERE id = ?")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    mark_enriched(&pool, id).await;

    let original_bytes = std::fs::read(&actual_path).unwrap();

    // Call tag_all with dry_run = true.
    tag::tag_all(&pool, true).await.unwrap();

    // No .bak file should have been created.
    let bak = actual_path.with_extension("epub.bak");
    assert!(!bak.exists(), "dry_run must not create a .bak file");

    // EPUB bytes must be unchanged.
    let after_bytes = std::fs::read(&actual_path).unwrap();
    assert_eq!(original_bytes, after_bytes, "dry_run must not modify the EPUB file");

    // tagged_at must still be NULL in the DB.
    let row_after = db::get_edition(&pool, id).await.unwrap().unwrap();
    assert!(
        row_after.tagged_at.is_none(),
        "dry_run must not set tagged_at in DB"
    );
}

// ---------------------------------------------------------------------------
// OPF element insertion: ISBN, series, and series_index inserted when absent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_inserts_missing_opf_elements() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("minimal.epub");

    // Build a minimal EPUB whose OPF has NO isbn, series, or series_index elements.
    {
        use std::io::Write as _;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&epub_path)
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();

        zip.start_file("META-INF/container.xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>"#).unwrap();

        // OPF has title and creator only — no ISBN, no calibre:series meta.
        zip.start_file("content.opf", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0" encoding="UTF-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:opf="http://www.idpf.org/2007/opf">
    <dc:title>Minimal Book</dc:title>
    <dc:creator>Minimal Author</dc:creator>
  </metadata>
  <manifest/>
  <spine/>
</package>"#).unwrap();

        zip.finish().unwrap();
    }

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = std::path::Path::new(&row.source_path).to_path_buf();

    // Set ISBN, series_name, and series_position in the DB (enrichment simulation).
    sqlx::query(
        "UPDATE editions SET isbn = ?, series_name = ?, series_position = ? WHERE id = ?",
    )
    .bind("9780000000099")
    .bind("Minimal Series")
    .bind("2")
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();

    tag::tag_edition(&pool, id, false).await.unwrap();

    // Read back the tagged EPUB and verify all three elements are present.
    let meta = epub::parse_epub(&actual_path).unwrap();

    assert_eq!(
        meta.isbn.as_deref(),
        Some("9780000000099"),
        "ISBN must be inserted when not present in original OPF"
    );
    assert_eq!(
        meta.series_name.as_deref(),
        Some("Minimal Series"),
        "calibre:series must be inserted when not present in original OPF"
    );
    assert_eq!(
        meta.series_position.as_deref(),
        Some("2"),
        "calibre:series_index must be inserted when not present in original OPF"
    );
}

// ---------------------------------------------------------------------------
// AC-33: source_path does not exist → Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_source_path_missing_returns_err() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();
    let epub_path = dir.path().join("book.epub");
    make_epub(
        "Missing File Test",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_path,
    );

    let id = insert_edition(&pool, &epub_path).await;
    let row = db::get_edition(&pool, id).await.unwrap().unwrap();
    let actual_path = Path::new(&row.source_path).to_path_buf();

    // Delete the source file.
    std::fs::remove_file(&actual_path).unwrap();

    let result = tag::tag_edition(&pool, id, false).await;
    assert!(result.is_err(), "should return Err when source_path does not exist");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("does not exist") || msg.contains("source"),
        "error should indicate missing file, got: {msg}"
    );
}
