/// E2E scan integration tests.
///
/// Fixture EPUBs are generated programmatically using the zip crate so
/// no binary blobs need to be committed.
use bookshelf_core::{db, epub, scan};
use std::io::Write as _;
use tempfile::{NamedTempFile, TempDir};

// ---------------------------------------------------------------------------
// EPUB fixture builder (duplicated here so it is available without
// `cfg(test)` on the inner module — same logic as epub.rs tests helper).
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
    dest: &std::path::Path,
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
        opf.push_str(&format!("    <dc:identifier opf:scheme=\"ISBN\">{v}</dc:identifier>\n"));
    }
    if let Some(v) = publisher {
        opf.push_str(&format!("    <dc:publisher>{}</dc:publisher>\n", epub::xml_escape(v)));
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

    opf.push_str("  </metadata>\n  <manifest>\n");

    if let Some(cover_id) = cover_item_id {
        opf.push_str(&format!(
            "    <item id=\"{cover_id}\" href=\"cover.jpg\" media-type=\"image/jpeg\"/>\n"
        ));
    }

    opf.push_str("  </manifest>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

// ---------------------------------------------------------------------------
// AC-53: E2E scan against fixture dir asserts row count and field values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_scan_fixture_epubs() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    // Create fixture: The Hobbit with full metadata
    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        Some("9780261102217"),
        Some("The Lord of the Rings"),
        Some("0"),
        Some("George Allen & Unwin"),
        Some("1937"),
        Some("en"),
        Some("A fantasy adventure"),
        Some("cover-img"),
        &epub_dir.path().join("the_hobbit.epub"),
    );

    // Create fixture: Fellowship of the Ring
    make_epub(
        "The Fellowship of the Ring",
        &["J.R.R. Tolkien"],
        Some("9780261102354"),
        Some("The Lord of the Rings"),
        Some("1"),
        Some("George Allen & Unwin"),
        Some("1954"),
        Some("en"),
        Some("The first part of LOTR"),
        None,
        &epub_dir.path().join("fellowship.epub"),
    );

    let result = scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    assert_eq!(result.scanned, 2, "should have scanned 2 epub files");
    assert_eq!(result.inserted, 2, "should have inserted 2 new books");
    assert_eq!(result.errors.len(), 0);

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 2);

    // Find the Hobbit row
    let hobbit = editions
        .iter()
        .find(|r| r.title.as_deref() == Some("The Hobbit"))
        .expect("Hobbit row missing");

    assert_eq!(hobbit.authors.as_deref(), Some("J.R.R. Tolkien"));
    assert_eq!(hobbit.isbn.as_deref(), Some("9780261102217"));
    assert_eq!(hobbit.series_name.as_deref(), Some("The Lord of the Rings"));
    assert_eq!(hobbit.series_position.as_deref(), Some("0"));
    assert_eq!(hobbit.publisher.as_deref(), Some("George Allen & Unwin"));
    assert_eq!(hobbit.publish_date.as_deref(), Some("1937"));
    assert_eq!(hobbit.language.as_deref(), Some("en"));
    assert_eq!(hobbit.description.as_deref(), Some("A fantasy adventure"));
    assert_eq!(hobbit.cover_image_path.as_deref(), Some("cover.jpg"));
    assert_eq!(hobbit.owned, 1, "owned should default to 1 (AC-48)");
    assert!(hobbit.work_id.is_some(), "Hobbit should have a work_id");
}

// ---------------------------------------------------------------------------
// AC-8, AC-26: Re-scan does not create duplicate records
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rescan_no_duplicates() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    make_epub(
        "Dune",
        &["Frank Herbert"],
        Some("9780441013593"),
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_dir.path().join("dune.epub"),
    );

    scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    scan::scan_directory(&pool, epub_dir.path()).await.unwrap();

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1, "second scan must not create duplicate rows");
}

// ---------------------------------------------------------------------------
// AC-55: Fuzzy dedup assigns same work_id to near-identical no-ISBN books
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fuzzy_dedup_assigns_same_work_id() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    // Two files, no ISBN, nearly identical titles, same author
    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        None, // no ISBN
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_dir.path().join("hobbit1.epub"),
    );

    make_epub(
        "The Hobbit, or There and Back Again",
        &["J.R.R. Tolkien"],
        None, // no ISBN
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_dir.path().join("hobbit2.epub"),
    );

    scan::scan_directory(&pool, epub_dir.path()).await.unwrap();

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 2, "should have 2 distinct edition rows");

    let work_ids: Vec<_> = editions.iter().map(|r| r.work_id).collect();
    assert!(
        work_ids[0].is_some() && work_ids[1].is_some(),
        "both editions should have a work_id"
    );
    assert_eq!(
        work_ids[0], work_ids[1],
        "fuzzy-matched books should share the same work_id (AC-55)"
    );
}

// ---------------------------------------------------------------------------
// AC-5: Non-EPUB files are not inserted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scan_ignores_non_epub_files() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    std::fs::write(epub_dir.path().join("readme.txt"), b"text").unwrap();
    std::fs::write(epub_dir.path().join("cover.jpg"), b"img").unwrap();
    std::fs::write(epub_dir.path().join("doc.pdf"), b"pdf").unwrap();

    let result = scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    assert_eq!(result.scanned, 0);
    assert_eq!(result.inserted, 0);
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 0);
}

// ---------------------------------------------------------------------------
// AC-4: Deeply nested EPUB files are discovered
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scan_discovers_deeply_nested_epub() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    // Create 5 levels of nested directories
    let nested = epub_dir.path().join("a").join("b").join("c").join("d").join("e");
    std::fs::create_dir_all(&nested).unwrap();

    make_epub(
        "Deep Book",
        &["Deep Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &nested.join("deep.epub"),
    );

    let result = scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    assert_eq!(result.scanned, 1);
    assert_eq!(result.inserted, 1);
}

// ---------------------------------------------------------------------------
// AC-44: Two EPUBs with the same ISBN share a work_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_same_isbn_editions_share_work_id() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        Some("9780261102217"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_dir.path().join("hobbit_a.epub"),
    );

    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        Some("9780261102217"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_dir.path().join("hobbit_b.epub"),
    );

    scan::scan_directory(&pool, epub_dir.path()).await.unwrap();

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 2);

    let wids: Vec<_> = editions.iter().map(|r| r.work_id).collect();
    assert_eq!(wids[0], wids[1], "same ISBN should share work_id (AC-44)");
    assert!(wids[0].is_some());
}

// ---------------------------------------------------------------------------
// AC-20: Corrupt EPUB produces a warning but scan continues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_corrupt_epub_skipped_with_warning() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    // Valid epub
    make_epub(
        "Valid Book",
        &["Valid Author"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &epub_dir.path().join("valid.epub"),
    );

    // Corrupt epub (not a valid ZIP)
    std::fs::write(epub_dir.path().join("corrupt.epub"), b"not a zip archive").unwrap();

    let result = scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    assert_eq!(result.scanned, 2);
    assert_eq!(result.inserted, 1, "only the valid epub should be inserted");
    assert_eq!(result.errors.len(), 1, "one error for the corrupt file");
}
