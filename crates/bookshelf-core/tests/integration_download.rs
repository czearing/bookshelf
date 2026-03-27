/// Integration tests for download.rs (AC-47 through AC-56).
use bookshelf_core::{db, download};
use std::io::Write as _;
use tempfile::{NamedTempFile, TempDir};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// Build minimal valid EPUB ZIP bytes for "title" / "author".
fn make_epub_bytes(title: &str, author: &str) -> Vec<u8> {
    let buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(buf);
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

    let opf = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title>
    <dc:creator>{author}</dc:creator>
  </metadata>
  <manifest/>
  <spine/>
</package>
"#
    );

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();

    zip.finish().unwrap().into_inner()
}

/// Minimal valid SE OPDS feed (no next link).
fn se_feed_single(title: &str, author: &str, epub_href: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opds="http://opds-spec.org/2010/catalog">
  <entry>
    <title>{title}</title>
    <author><name>{author}</name></author>
    <link rel="http://opds-spec.org/acquisition" href="{epub_href}" type="application/epub+zip"/>
  </entry>
</feed>"#
    )
}

/// SE feed with a rel="next" link pointing to next_href.
fn se_feed_with_next(title: &str, author: &str, epub_href: &str, next_href: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opds="http://opds-spec.org/2010/catalog">
  <link rel="next" href="{next_href}"/>
  <entry>
    <title>{title}</title>
    <author><name>{author}</name></author>
    <link rel="http://opds-spec.org/acquisition" href="{epub_href}" type="application/epub+zip"/>
  </entry>
</feed>"#
    )
}

/// Empty SE feed (no entries, no next link).
fn se_feed_empty() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opds="http://opds-spec.org/2010/catalog">
</feed>"#
        .to_string()
}

/// Gutenberg JSON response with one matching result.
fn gutenberg_result(title: &str, author: &str, epub_href: &str) -> serde_json::Value {
    serde_json::json!({
        "count": 1,
        "results": [{
            "title": title,
            "authors": [{ "name": author }],
            "formats": { "application/epub+zip": epub_href }
        }]
    })
}

/// Gutenberg JSON response with no results.
fn gutenberg_empty() -> serde_json::Value {
    serde_json::json!({ "count": 0, "results": [] })
}

// ---------------------------------------------------------------------------
// AC-47: empty grab list → 0 downloaded, 0 not found
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_empty_grab_list() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 0);
    assert_eq!(result.not_found, 0);
}

// ---------------------------------------------------------------------------
// AC-48: SE returns matching entry → downloaded and imported
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_standard_ebooks_match() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    // Insert a want entry for "Dune" by Frank Herbert.
    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let epub_bytes = make_epub_bytes("Dune", "Frank Herbert");

    // SE OPDS feed mock.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("Dune", "Frank Herbert", "/epub/dune.epub")),
        )
        .mount(&server)
        .await;

    // EPUB download mock.
    Mock::given(method("GET"))
        .and(path("/epub/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 1, "expected 1 downloaded");
    assert_eq!(result.not_found, 0);

    // Verify EPUB is now in library_dir.
    let epub_files: Vec<_> = std::fs::read_dir(lib_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "epub")
                .unwrap_or(false)
        })
        .collect();
    assert!(!epub_files.is_empty(), "EPUB must be copied to library_dir");
}

// ---------------------------------------------------------------------------
// AC-49: SE returns no match, Gutenberg returns match → downloaded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_gutenberg_match() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let epub_bytes = make_epub_bytes("Dune", "Frank Herbert");

    // SE returns nothing relevant.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(se_feed_empty()))
        .mount(&server)
        .await;

    // Gutenberg search returns a match.
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(gutenberg_result(
                "Dune",
                "Frank Herbert",
                "/files/dune.epub",
            )),
        )
        .mount(&server)
        .await;

    // EPUB download from Gutenberg - epub_href is "/files/dune.epub" so URL will be
    // "{base_url_gutenberg}/files/dune.epub".
    Mock::given(method("GET"))
        .and(path("/files/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 1, "gutenberg match must be downloaded");
    assert_eq!(result.not_found, 0);
}

// ---------------------------------------------------------------------------
// AC-50: neither SE nor Gutenberg has a match → not_found=1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_no_match() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "Extremely Obscure Unknown Book XYZ",
        Some("No Such Author"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(se_feed_empty()))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gutenberg_empty()))
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 0);
    assert_eq!(result.not_found, 1);
}

// ---------------------------------------------------------------------------
// AC-51: summary format "N downloaded, N not found" (via printed output)
// The function returns the DownloadResult that the caller uses to print.
// We verify that DownloadResult fields are correctly populated for display.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_summary() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    // Two want entries: one will match SE (downloaded), one won't match (not_found).
    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();
    db::insert_want(
        &pool,
        "Extremely Obscure Unknown Book XYZ2",
        Some("No Such Author"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("Dune", "Frank Herbert");

    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("Dune", "Frank Herbert", "/epub/dune.epub")),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/epub/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gutenberg_empty()))
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    // Caller would print: "{} downloaded, {} not found"
    let summary = format!("{} downloaded, {} not found", result.downloaded, result.not_found);
    assert_eq!(summary, "1 downloaded, 1 not found");
}

// ---------------------------------------------------------------------------
// AC-52: SE pagination — page 1 has rel="next", page 2 doesn't
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_pagination() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    // Want entry that only appears on page 2.
    db::insert_want(&pool, "Neuromancer", Some("William Gibson"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let epub_bytes = make_epub_bytes("Neuromancer", "William Gibson");

    // Page 1: served exactly once via up_to_n_times(1), so the second request
    // (for page=2) falls through to the page-2 mock (FIFO quota exhaustion pattern).
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(se_feed_with_next(
            "Dune",
            "Frank Herbert",
            "/epub/dune.epub",
            "/feeds/opds/all?page=2",
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2: fallback mock (served after page 1 quota is exhausted).
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(se_feed_single(
                "Neuromancer",
                "William Gibson",
                "/epub/neuromancer.epub",
            )),
        )
        .mount(&server)
        .await;

    // EPUB for Neuromancer.
    Mock::given(method("GET"))
        .and(path("/epub/neuromancer.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    // Neuromancer found on page 2 — must be downloaded.
    assert_eq!(result.downloaded, 1, "Neuromancer must be found on page 2");
    assert_eq!(result.not_found, 0);
}

// ---------------------------------------------------------------------------
// AC-53: SE returns 500, Gutenberg returns 500 → not_found, no crash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_http_error_continues() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();

    // SE catalog returns 500.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    // Gutenberg search returns 500.
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    // Should not panic, should return Ok with not_found=1.
    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 0);
    // not_found because gutenberg also failed for Dune
    assert!(result.not_found >= 1, "must have at least 1 not_found on HTTP errors");
}

// ---------------------------------------------------------------------------
// AC-54: downloaded file fails parse_epub → not counted as downloaded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_invalid_epub_not_counted() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();

    // SE returns matching entry but serves invalid EPUB bytes.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("Dune", "Frank Herbert", "/epub/dune.epub")),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/epub/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"this is not a valid epub zip file".to_vec())
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    // Gutenberg: no match.
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gutenberg_empty()))
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 0, "invalid EPUB must not count as downloaded");
}

// ---------------------------------------------------------------------------
// AC-56: when SE matches, Gutenberg endpoint not called
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_checked_before_gutenberg() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Dune", Some("Frank Herbert"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let epub_bytes = make_epub_bytes("Dune", "Frank Herbert");

    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("Dune", "Frank Herbert", "/epub/dune.epub")),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/epub/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    // Gutenberg mock: set up but must NOT be called.
    // wiremock will panic if a mock registered with expect(0) is hit.
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gutenberg_empty()))
        .expect(0) // Must NOT be called when SE matches.
        .mount(&server)
        .await;

    let result =
        download::download_grab_list(&pool, &client, lib_dir.path(), &server.uri(), &server.uri())
            .await
            .unwrap();

    assert_eq!(result.downloaded, 1);
    assert_eq!(result.not_found, 0);
    // wiremock will verify expect(0) is satisfied when `server` is dropped.
}
