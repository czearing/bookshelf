/// Integration tests for rename.rs — AC-35 through AC-46.
use bookshelf_core::{db, rename};
use std::io::Write as _;
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};

// ---------------------------------------------------------------------------
// EPUB fixture builder (same pattern as integration_scan.rs)
// ---------------------------------------------------------------------------

fn make_epub(
    title: &str,
    authors: &[&str],
    isbn: Option<&str>,
    series_name: Option<&str>,
    series_position: Option<&str>,
    publisher: Option<&str>,
    publish_date: Option<&str>,
    language: Option<&str>,
    description: Option<&str>,
    cover_item_id: Option<&str>,
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
    if let Some(v) = language {
        opf.push_str(&format!("    <dc:language>{v}</dc:language>\n"));
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
    if let Some(cover_id) = cover_item_id {
        opf.push_str(&format!(
            "    <meta name=\"cover\" content=\"{cover_id}\"/>\n"
        ));
    }

    opf.push_str("  </metadata>\n  <manifest>\n  </manifest>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Helper: open a temp DB
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

// ---------------------------------------------------------------------------
// Helper: insert a minimal EditionRow directly into the DB so tests can
// control every field without going through parse_epub (which reads real files).
// We upsert via scan then UPDATE individual columns as needed.
// ---------------------------------------------------------------------------

/// Insert an edition with a real EPUB file, then optionally mark it owned.
async fn insert_edition(
    pool: &db::DbPool,
    source_path: &Path,
    owned: i64,
) -> i64 {
    // Use scan to insert the row.
    bookshelf_core::scan::scan_directory(pool, source_path.parent().unwrap(), &["epub"])
        .await
        .unwrap();

    let editions = db::list_editions(pool).await.unwrap();
    let row = editions
        .iter()
        .find(|e| {
            Path::new(&e.source_path)
                .file_name()
                == source_path.file_name()
        })
        .expect("edition not found after scan");

    let id = row.id;

    // Set owned flag.
    sqlx::query("UPDATE editions SET owned = ? WHERE id = ?")
        .bind(owned)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();

    id
}

// ---------------------------------------------------------------------------
// AC-35: default pattern, no series — "Frank Herbert - Dune.epub"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_default_pattern_no_series() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dune_original.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1, "should have renamed 1 file");
    assert_eq!(result.skipped, 0);

    let expected = dir.path().join("Frank Herbert - Dune.epub");
    assert!(expected.exists(), "renamed file should exist at {}", expected.display());
    assert!(
        !src.exists(),
        "original file should no longer exist at {}",
        src.display()
    );

    // DB should reflect new path.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1);
    let stored = Path::new(&editions[0].source_path);
    assert_eq!(
        stored.file_name().and_then(|n| n.to_str()),
        Some("Frank Herbert - Dune.epub")
    );
}

// ---------------------------------------------------------------------------
// AC-36: default pattern with series — "{Author} - {Series} {Position} - {Title}.epub"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_default_pattern_with_series() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dune_series.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        Some("Dune Chronicles"),
        Some("1"),
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(
        &pool,
        "{author} - {series} {position} - {title}",
        false,
        true,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.renamed, 1);

    let expected = dir.path().join("Frank Herbert - Dune Chronicles 1 - Dune.epub");
    assert!(expected.exists(), "renamed file must exist: {}", expected.display());
}

// ---------------------------------------------------------------------------
// AC-37: null author → "Unknown" when unknown_fallback=true
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_null_author_uses_unknown() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("no_author.epub");
    // Make an EPUB with no dc:creator element — parse_epub will store authors=None.
    // We write it manually to avoid the make_epub helper inserting an empty author.
    use std::io::Write as _;
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&src)
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();

        zip.start_file("META-INF/container.xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#).unwrap();

        zip.start_file("content.opf", opts).unwrap();
        zip.write_all(
            br#"<?xml version="1.0"?><package version="2.0" xmlns="http://www.idpf.org/2007/opf" unique-identifier="uid"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>Title Only Book</dc:title></metadata><manifest/><spine/></package>"#,
        )
        .unwrap();
        zip.finish().unwrap();
    }

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1);

    let expected = dir.path().join("Unknown - Title Only Book.epub");
    assert!(expected.exists(), "expected: {}", expected.display());
}

// ---------------------------------------------------------------------------
// AC-38: null title → "Unknown" when unknown_fallback=true
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_null_title_uses_unknown() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("no_title.epub");
    {
        use std::io::Write as _;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&src)
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();

        zip.start_file("META-INF/container.xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#).unwrap();

        zip.start_file("content.opf", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><package version="2.0" xmlns="http://www.idpf.org/2007/opf" unique-identifier="uid"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:creator>Frank Herbert</dc:creator></metadata><manifest/><spine/></package>"#).unwrap();
        zip.finish().unwrap();
    }

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1);

    let expected = dir.path().join("Frank Herbert - Unknown.epub");
    assert!(expected.exists(), "expected: {}", expected.display());
}

// ---------------------------------------------------------------------------
// AC-39: file already correctly named → skipped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_skips_already_matching() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Name the file exactly what the rename pipeline would produce.
    let src = dir.path().join("Frank Herbert - Dune.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 0, "already-correct file must not be renamed");
    assert_eq!(result.skipped, 1, "already-matching file must be counted as skipped (AC-39)");
    // File must still exist at original path.
    assert!(src.exists());
}

// ---------------------------------------------------------------------------
// AC-40: result has correct renamed/skipped counts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_summary_format() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Two editions: one needs rename, one already correct.
    let src1 = dir.path().join("dune_raw.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src1,
    );

    let src2 = dir.path().join("Foundation - Isaac Asimov.epub");
    make_epub(
        "Foundation",
        &["Isaac Asimov"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src2,
    );

    insert_edition(&pool, &src1, 1).await;
    insert_edition(&pool, &src2, 1).await;

    let result = rename::rename_editions(&pool, "{title} - {author}", false, true, None)
        .await
        .unwrap();

    // src1 → "Dune - Frank Herbert.epub" (rename)
    // src2 → "Foundation - Isaac Asimov.epub" (already matches → skipped)
    assert_eq!(result.renamed, 1);
    assert_eq!(result.skipped, 1);
}

// ---------------------------------------------------------------------------
// AC-41: custom pattern with publish_date → "Dune (1965).epub"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_custom_pattern() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dune_year.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        Some("1965-01-01"),
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{title} ({year})", false, false, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1);

    let expected = dir.path().join("Dune (1965).epub");
    assert!(expected.exists(), "expected: {}", expected.display());
}

// ---------------------------------------------------------------------------
// AC-41: custom pattern with null field → empty string (not "Unknown")
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_custom_pattern_null_tokens_empty() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dune_nodate.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None, // no publish_date
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    // unknown_fallback = false → null fields expand to ""
    let result = rename::rename_editions(&pool, "{title} ({year})", false, false, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1);

    let expected = dir.path().join("Dune ().epub");
    assert!(expected.exists(), "expected empty-year expansion: {}", expected.display());
}

// ---------------------------------------------------------------------------
// AC-42: invalid token returns Err before any rename
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_invalid_token_returns_err() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("book.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{badtoken} - {title}", false, true, None).await;

    assert!(result.is_err(), "bad token must return Err");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("badtoken"),
        "error message must name the bad token; got: {msg}"
    );

    // File must NOT have been renamed.
    assert!(src.exists(), "file must still exist when token validation fails");
}

// ---------------------------------------------------------------------------
// AC-43: dry run — files not renamed, source_path unchanged in DB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_dry_run_no_changes() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dry_run_book.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let original_path = {
        let eds = db::list_editions(&pool).await.unwrap();
        eds[0].source_path.clone()
    };

    let result = rename::rename_editions(&pool, "{author} - {title}", true, true, None)
        .await
        .unwrap();

    // Dry run: renamed=0, skipped=plan.len().
    assert_eq!(result.renamed, 0);
    assert_eq!(result.skipped, 1);

    // File must not have moved on disk.
    assert!(src.exists(), "dry run must not move file");

    // DB must not have changed.
    let eds = db::list_editions(&pool).await.unwrap();
    assert_eq!(eds[0].source_path, original_path);

    // AC-25: dry run must expose the planned rename (old → new paths).
    assert_eq!(result.planned.len(), 1, "dry run must record 1 planned rename");
    let (_, new_path) = &result.planned[0];
    assert!(
        new_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            == "Frank Herbert - Dune.epub",
        "planned new path must be 'Frank Herbert - Dune.epub', got: {}",
        new_path.display()
    );
}

// ---------------------------------------------------------------------------
// AC-44: collision — computed name already exists at different path → skipped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_collision_skips() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("collision_src.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    // Pre-create the target file so it already exists.
    let target = dir.path().join("Frank Herbert - Dune.epub");
    std::fs::write(&target, b"already exists").unwrap();

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 0, "collision must not rename");
    assert_eq!(result.skipped, 1, "collision must increment skipped");

    // Original file must still exist.
    assert!(src.exists());
}

// ---------------------------------------------------------------------------
// AC-45: after rename, db::get_edition shows new path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_updates_source_path() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("path_update_test.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    let id = insert_edition(&pool, &src, 1).await;

    rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    let row = db::get_edition(&pool, id).await.unwrap().expect("edition must exist");
    let stored = Path::new(&row.source_path);
    assert_eq!(
        stored.file_name().and_then(|n| n.to_str()),
        Some("Frank Herbert - Dune.epub"),
        "DB source_path must be updated after rename"
    );
}

// ---------------------------------------------------------------------------
// AC-46: non-owned edition (owned=0) not renamed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_only_owned() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("unowned.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    // Insert then mark owned=0.
    insert_edition(&pool, &src, 0).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 0, "non-owned editions must not be renamed");

    // File must still be at original path.
    assert!(src.exists());
}

// ---------------------------------------------------------------------------
// AC-36: series_pattern applied to series editions, plain pattern to others
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_series_pattern_applied_to_series_editions() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Edition with series info — should use the series pattern.
    let src_series = dir.path().join("series_book.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        Some("Dune Chronicles"),
        Some("1"),
        None,
        None,
        None,
        None,
        None,
        &src_series,
    );

    // Edition without series info — should use the plain pattern.
    let src_plain = dir.path().join("foundation_plain.epub");
    make_epub(
        "Foundation",
        &["Isaac Asimov"],
        None,
        None, // no series_name
        None, // no series_position
        None,
        None,
        None,
        None,
        None,
        &src_plain,
    );

    insert_edition(&pool, &src_series, 1).await;
    insert_edition(&pool, &src_plain, 1).await;

    let result = rename::rename_editions(
        &pool,
        "{author} - {title}",
        false,
        true,
        Some("{author} - {series} {position} - {title}"),
    )
    .await
    .unwrap();

    assert_eq!(result.renamed, 2, "both editions should be renamed");

    // Series edition should use the series pattern.
    let expected_series = dir.path().join("Frank Herbert - Dune Chronicles 1 - Dune.epub");
    assert!(
        expected_series.exists(),
        "series edition must use series pattern: {}",
        expected_series.display()
    );

    // Non-series edition should use the plain pattern.
    let expected_plain = dir.path().join("Isaac Asimov - Foundation.epub");
    assert!(
        expected_plain.exists(),
        "plain edition must use plain pattern: {}",
        expected_plain.display()
    );
}

// ---------------------------------------------------------------------------
// AC-22: subdirectory pattern — "{author}/{title}" creates a subdir
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_subdirectory_pattern_creates_dir() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("dune_subdir.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src,
    );

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author}/{title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1, "should have renamed 1 file");
    assert_eq!(result.skipped, 0);

    // Subdirectory must exist.
    let subdir = dir.path().join("Frank Herbert");
    assert!(subdir.exists(), "subdirectory must be created: {}", subdir.display());
    assert!(subdir.is_dir(), "Frank Herbert must be a directory");

    // File must be at the new path.
    let expected = subdir.join("Dune.epub");
    assert!(expected.exists(), "renamed file must exist at {}", expected.display());

    // Original file must be gone.
    assert!(!src.exists(), "original file must not exist after rename");

    // DB must reflect the new path.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1);
    let new_path = std::path::Path::new(&editions[0].source_path);
    assert_eq!(
        new_path.file_name().and_then(|n| n.to_str()),
        Some("Dune.epub"),
        "DB file name must be Dune.epub"
    );
    assert_eq!(
        new_path.parent().and_then(|p| p.file_name()).and_then(|n| n.to_str()),
        Some("Frank Herbert"),
        "DB parent directory must be Frank Herbert"
    );
}

// ---------------------------------------------------------------------------
// AC-23: both author AND title null → "Unknown - Unknown.epub"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_unknown_both_null() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    let src = dir.path().join("no_author_no_title.epub");
    {
        use std::io::Write as _;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&src)
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();

        zip.start_file("META-INF/container.xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#).unwrap();

        // OPF has no dc:title and no dc:creator — both fields will be None after parsing.
        zip.start_file("content.opf", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><package version="2.0" xmlns="http://www.idpf.org/2007/opf" unique-identifier="uid"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"></metadata><manifest/><spine/></package>"#).unwrap();
        zip.finish().unwrap();
    }

    insert_edition(&pool, &src, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    assert_eq!(result.renamed, 1, "should have renamed 1 file");

    let expected = dir.path().join("Unknown - Unknown.epub");
    assert!(expected.exists(), "expected both-null fallback: {}", expected.display());
}

// ---------------------------------------------------------------------------
// Gap 5: collision between two DB editions resolving to the same target name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rename_collision_between_db_editions() {
    let (pool, _db_tmp) = temp_pool().await;
    let dir = TempDir::new().unwrap();

    // Two editions with identical author and title — both resolve to the same target.
    let src1 = dir.path().join("dune_copy1.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src1,
    );

    let src2 = dir.path().join("dune_copy2.epub");
    make_epub(
        "Dune",
        &["Frank Herbert"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &src2,
    );

    insert_edition(&pool, &src1, 1).await;
    insert_edition(&pool, &src2, 1).await;

    let result = rename::rename_editions(&pool, "{author} - {title}", false, true, None)
        .await
        .unwrap();

    // One is renamed, one is skipped due to collision.
    assert_eq!(result.renamed, 1, "first edition should be renamed");
    assert_eq!(result.skipped, 1, "second edition should be skipped due to collision");

    // The target file exists.
    let target = dir.path().join("Frank Herbert - Dune.epub");
    assert!(target.exists(), "target file must exist: {}", target.display());

    // One of the two originals still exists (the one that was skipped).
    let src1_exists = src1.exists();
    let src2_exists = src2.exists();
    assert!(
        src1_exists || src2_exists,
        "the skipped edition's original file must still exist"
    );
    assert!(
        !(src1_exists && src2_exists),
        "only one of the originals should remain — one was renamed to the target"
    );
}
