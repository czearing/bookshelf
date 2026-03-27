/// Integration tests for import.rs — AC-1 through AC-14.
use bookshelf_core::{db, import};
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

    opf.push_str("  </metadata>\n  <manifest/>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Helper: open a fresh in-memory-style DB using a temp file.
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

// ---------------------------------------------------------------------------
// AC-1: import one file — 1 imported, 0 skipped, file in library dir, edition in DB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_single_epub_file() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("hobbit.epub");
    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        Some("9780261102217"),
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    let result = import::import_path(&pool, &epub_path, library_dir.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1, "should have imported 1 file");
    assert_eq!(result.skipped, 0, "should have skipped 0 files");
    assert!(result.errors.is_empty());

    // File must be in library dir.
    assert!(
        library_dir.path().join("hobbit.epub").exists(),
        "file must exist in library dir"
    );

    // Edition must be in DB.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1, "one edition in DB");
    assert_eq!(editions[0].title.as_deref(), Some("The Hobbit"));
}

// ---------------------------------------------------------------------------
// AC-2: import same file twice — second time 0 imported, 1 skipped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_skips_already_registered() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("dune.epub");
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
        &epub_path,
    );

    // First import.
    let r1 = import::import_path(&pool, &epub_path, library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(r1.imported, 1);
    assert_eq!(r1.skipped, 0);

    // Second import of the same file — dest already in DB.
    let r2 = import::import_path(&pool, &epub_path, library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(r2.imported, 0, "second import must not import again");
    assert_eq!(r2.skipped, 1, "second import must skip the duplicate");

    // Still only one edition in DB.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1, "no duplicate editions");
}

// ---------------------------------------------------------------------------
// AC-3: import directory with 2 EPUBs — 2 imported
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_directory() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    make_epub(
        "Book One",
        &["Author A"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book1.epub"),
    );
    make_epub(
        "Book Two",
        &["Author B"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book2.epub"),
    );

    let result = import::import_path(&pool, source_dir.path(), library_dir.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 2, "should import both EPUBs");
    assert_eq!(result.skipped, 0);

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 2);
}

// ---------------------------------------------------------------------------
// AC-4: directory with 1 already registered, 1 new — 1 imported, 1 skipped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_directory_with_existing() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    make_epub(
        "Book A",
        &["Author A"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book_a.epub"),
    );
    make_epub(
        "Book B",
        &["Author B"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book_b.epub"),
    );

    // First import registers both.
    let r1 = import::import_path(&pool, source_dir.path(), library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(r1.imported, 2);

    // Add a third book.
    make_epub(
        "Book C",
        &["Author C"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book_c.epub"),
    );

    // Second import: 2 already exist (skipped), 1 new.
    let r2 = import::import_path(&pool, source_dir.path(), library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(r2.imported, 1, "only the new book should be imported");
    assert_eq!(r2.skipped, 2, "the two existing books should be skipped");
}

// ---------------------------------------------------------------------------
// AC-5: --move removes source file after successful import
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_move_removes_source() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("mover.epub");
    make_epub(
        "Move Me",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    let result = import::import_path(&pool, &epub_path, library_dir.path(), true)
        .await
        .unwrap();

    assert_eq!(result.imported, 1);
    assert!(
        !epub_path.exists(),
        "source file must be removed after --move"
    );
    assert!(
        library_dir.path().join("mover.epub").exists(),
        "file must exist in library dir"
    );
}

// ---------------------------------------------------------------------------
// AC-6: without --move, source file is kept
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_no_move_keeps_source() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("keeper.epub");
    make_epub(
        "Keep Me",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    let result = import::import_path(&pool, &epub_path, library_dir.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1);
    assert!(
        epub_path.exists(),
        "source file must still exist without --move"
    );
}

// ---------------------------------------------------------------------------
// AC-9: library dir doesn't exist — must be created
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_creates_library_dir() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let base_dir = TempDir::new().unwrap();

    // Point at a subdirectory that doesn't exist yet.
    let library_dir = base_dir.path().join("nested").join("library");
    assert!(
        !library_dir.exists(),
        "library dir must not exist before import"
    );

    let epub_path = source_dir.path().join("book.epub");
    make_epub(
        "Any Book",
        &["Any Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    let result = import::import_path(&pool, &epub_path, &library_dir, false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1);
    assert!(library_dir.exists(), "library dir must be created");
}

// ---------------------------------------------------------------------------
// AC-10: directory with epub + txt + pdf — only epub imported, others invisible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_ignores_non_epub_files() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    make_epub(
        "The Only Book",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("book.epub"),
    );
    std::fs::write(source_dir.path().join("notes.txt"), b"text file").unwrap();
    std::fs::write(source_dir.path().join("document.pdf"), b"pdf file").unwrap();

    let result = import::import_path(&pool, source_dir.path(), library_dir.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1, "only the epub should be imported");
    assert_eq!(result.skipped, 0, "non-epub files are not counted anywhere");

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1);
}

// ---------------------------------------------------------------------------
// AC-11: non-existent path returns Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_nonexistent_path_returns_err() {
    let (pool, _db_tmp) = temp_pool().await;
    let library_dir = TempDir::new().unwrap();

    let result = import::import_path(
        &pool,
        Path::new("/nonexistent/path/does_not_exist.epub"),
        library_dir.path(),
        false,
    )
    .await;

    assert!(result.is_err(), "non-existent source path must return Err");
}

// ---------------------------------------------------------------------------
// AC-12: corrupted epub → skipped; valid epub → imported
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_invalid_epub_increments_skipped() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    // Valid epub.
    make_epub(
        "Valid Book",
        &["Valid Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &source_dir.path().join("valid.epub"),
    );

    // Corrupted epub (not a valid ZIP).
    std::fs::write(
        source_dir.path().join("corrupt.epub"),
        b"this is not a zip archive",
    )
    .unwrap();

    let result = import::import_path(&pool, source_dir.path(), library_dir.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1, "valid epub must be imported");
    assert_eq!(result.skipped, 1, "corrupt epub must be skipped");
    assert_eq!(result.errors.len(), 1, "one error entry for corrupt epub");

    // Corrupt file must not remain in library dir.
    assert!(
        !library_dir.path().join("corrupt.epub").exists(),
        "corrupt epub must be cleaned up from library dir"
    );

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1, "only the valid epub in DB");
}

// ---------------------------------------------------------------------------
// Self-import guard: source IS the library-dir file, do_move=true → not deleted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_self_import_guard_does_not_delete_source() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    // Step 1: import from an external source dir into the library dir.
    let epub_src = source_dir.path().join("self_guard.epub");
    make_epub(
        "Self Guard Book",
        &["Guard Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_src,
    );

    let r1 = import::import_path(&pool, &epub_src, library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(r1.imported, 1, "initial import must succeed");

    let epub_in_lib = library_dir.path().join("self_guard.epub");
    assert!(epub_in_lib.exists(), "file must be in library dir after first import");

    // Step 2: try to import the already-in-library file again with do_move=true.
    // The guard in import.rs (source_path != &dest) means it will NOT delete the file.
    let r2 = import::import_path(&pool, &epub_in_lib, library_dir.path(), true)
        .await
        .unwrap();

    // Should be skipped (already registered in DB by dest path).
    assert_eq!(r2.skipped, 1, "second import of in-library file must be skipped (already registered)");

    // The file must still exist — the self-import guard must have prevented deletion.
    assert!(
        epub_in_lib.exists(),
        "file must not be deleted when source == dest (self-import guard)"
    );
}

// ---------------------------------------------------------------------------
// --library-dir routing: import_path respects explicit library_dir
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_respects_explicit_library_dir() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let custom_lib = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("routed.epub");
    make_epub(
        "Routed Book",
        &["Route Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    // Import using a custom (non-default) library dir.
    let result = import::import_path(&pool, &epub_path, custom_lib.path(), false)
        .await
        .unwrap();

    assert_eq!(result.imported, 1);

    // File must be in the CUSTOM library dir, not in the default.
    assert!(
        custom_lib.path().join("routed.epub").exists(),
        "file must land in the custom library dir"
    );

    // It must NOT be in any default location (we just verify it IS in custom_lib,
    // which is a distinct temp dir different from default_library_dir()).
    let default_lib = import::default_library_dir();
    assert!(
        !default_lib.join("routed.epub").exists(),
        "file must not appear in the default library dir when a custom dir is passed"
    );

    // DB must reflect the custom library dir path.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1);
    let stored = std::path::Path::new(&editions[0].source_path);
    // Canonicalize both paths to handle Windows extended-path prefixes (\\?\...).
    let stored_canon = stored
        .canonicalize()
        .unwrap_or_else(|_| stored.to_path_buf());
    let lib_canon = custom_lib
        .path()
        .canonicalize()
        .unwrap_or_else(|_| custom_lib.path().to_path_buf());
    assert!(
        stored_canon.starts_with(&lib_canon),
        "DB source_path must be under the custom library dir, got: {}",
        stored.display()
    );
}

// ---------------------------------------------------------------------------
// AC-14: imported edition has owned=1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_import_edition_is_owned() {
    let (pool, _db_tmp) = temp_pool().await;
    let source_dir = TempDir::new().unwrap();
    let library_dir = TempDir::new().unwrap();

    let epub_path = source_dir.path().join("owned.epub");
    make_epub(
        "Owned Book",
        &["Author"],
        None,
        None,
        None,
        None,
        None,
        Some("en"),
        None,
        None,
        &epub_path,
    );

    let result = import::import_path(&pool, &epub_path, library_dir.path(), false)
        .await
        .unwrap();
    assert_eq!(result.imported, 1);

    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1);
    assert_eq!(editions[0].owned, 1, "imported edition must have owned=1 (AC-14)");
}
