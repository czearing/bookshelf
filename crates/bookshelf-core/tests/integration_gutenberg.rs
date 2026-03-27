/// Integration tests for GutenbergSource and 3-source download prioritization.
use bookshelf_core::{
    db,
    download::{self, DownloadEvent},
    download_sources::{DownloadCandidate, DownloadSource, GutenbergSource},
};
use std::io::Write as _;
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

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

fn gutenberg_search_response(title: &str, author_last_first: &str, epub_url: &str) -> serde_json::Value {
    serde_json::json!({
        "count": 1,
        "next": null,
        "previous": null,
        "results": [{
            "id": 1,
            "title": title,
            "authors": [{ "name": author_last_first, "birth_year": null, "death_year": null }],
            "formats": {
                "application/epub+zip": epub_url,
                "text/html": "http://example.com/book.html"
            }
        }]
    })
}

fn gutenberg_empty_response() -> serde_json::Value {
    serde_json::json!({ "count": 0, "next": null, "previous": null, "results": [] })
}

// ---------------------------------------------------------------------------
// GutenbergSource::find — happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_find_returns_candidate() {
    let server = MockServer::start().await;
    let epub_url = format!("{}/files/1/book.epub", server.uri());

    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(gutenberg_search_response(
                    "The Adventures of Tom Sawyer",
                    "Twain, Mark",
                    &epub_url,
                ))
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let result = source.find("The Adventures of Tom Sawyer", "Mark Twain").await.unwrap();
    assert!(result.is_some(), "find must return Some for a matching book");

    let candidate = result.unwrap();
    assert_eq!(candidate.source_name, "gutenberg");
    assert_eq!(candidate.download_url, epub_url);
    assert_eq!(candidate.format, "epub");
}

// ---------------------------------------------------------------------------
// GutenbergSource::find — no match (empty results)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_find_returns_none_when_no_results() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(gutenberg_empty_response()))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let result = source.find("A Copyrighted Modern Novel", "Some Author").await.unwrap();
    assert!(result.is_none(), "find must return None when no results match");
}

// ---------------------------------------------------------------------------
// GutenbergSource::find — author name normalized from "Last, First"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_find_normalizes_author_name() {
    let server = MockServer::start().await;
    let epub_url = format!("{}/files/2/moby.epub", server.uri());

    // Gutenberg returns "Melville, Herman" — find is called with "Herman Melville"
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(gutenberg_search_response(
                    "Moby Dick",
                    "Melville, Herman",
                    &epub_url,
                ))
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let result = source.find("Moby Dick", "Herman Melville").await.unwrap();
    assert!(result.is_some(), "inverted author name must be normalized and matched");
}

// ---------------------------------------------------------------------------
// GutenbergSource::find — fuzzy mismatch: results exist but none match
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_find_returns_none_on_fuzzy_mismatch() {
    let server = MockServer::start().await;
    let epub_url = format!("{}/files/3/book.epub", server.uri());

    // Search returns a completely different book
    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(gutenberg_search_response(
                    "An Entirely Different Book",
                    "Someone, Else",
                    &epub_url,
                ))
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let result = source.find("The Odyssey", "Homer").await.unwrap();
    assert!(result.is_none(), "fuzzy mismatch must return None");
}

// ---------------------------------------------------------------------------
// GutenbergSource::find — HTTP error propagates as Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_find_http_error_returns_err() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/books/"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let result = source.find("Hamlet", "Shakespeare").await;
    assert!(result.is_err(), "HTTP 503 must propagate as Err");
}

// ---------------------------------------------------------------------------
// GutenbergSource::fetch — downloads bytes to dest path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_fetch_writes_epub_to_dest() {
    let server = MockServer::start().await;
    let epub_bytes = make_epub_bytes("Tom Sawyer", "Mark Twain");

    Mock::given(method("GET"))
        .and(path("/files/1/book.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes.clone())
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let candidate = DownloadCandidate {
        source_name: "gutenberg".to_string(),
        title: "Tom Sawyer".to_string(),
        authors: "Mark Twain".to_string(),
        download_url: format!("{}/files/1/book.epub", server.uri()),
        format: "epub".to_string(),
    };

    let tmp = NamedTempFile::new().unwrap();
    source.fetch(&candidate, tmp.path()).await.unwrap();

    let written = std::fs::read(tmp.path()).unwrap();
    assert_eq!(written, epub_bytes, "fetched bytes must match what the server sent");
}

// ---------------------------------------------------------------------------
// GutenbergSource::fetch — HTTP error propagates as Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gutenberg_fetch_http_error_returns_err() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/files/1/book.epub"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let source = GutenbergSource::new(client, server.uri());

    let candidate = DownloadCandidate {
        source_name: "gutenberg".to_string(),
        title: "Missing Book".to_string(),
        authors: "Nobody".to_string(),
        download_url: format!("{}/files/1/book.epub", server.uri()),
        format: "epub".to_string(),
    };

    let tmp = NamedTempFile::new().unwrap();
    let result = source.fetch(&candidate, tmp.path()).await;
    assert!(result.is_err(), "HTTP 404 must propagate as Err");
}

// ---------------------------------------------------------------------------
// Priority tests: SE > Gutenberg > IA
//
// Uses simple MockSource stubs (no real HTTP) to isolate the priority logic.
// ---------------------------------------------------------------------------

struct StubSource {
    name: String,
    returns: Option<DownloadCandidate>,
    epub_bytes: Vec<u8>,
}

impl StubSource {
    fn hit(name: &str) -> Self {
        let candidate = DownloadCandidate {
            source_name: name.to_string(),
            title: "Test Book".to_string(),
            authors: "Test Author".to_string(),
            download_url: "http://example.com/book.epub".to_string(),
            format: "epub".to_string(),
        };
        Self {
            name: name.to_string(),
            returns: Some(candidate),
            epub_bytes: make_epub_bytes("Test Book", "Test Author"),
        }
    }

    fn miss(name: &str) -> Self {
        Self {
            name: name.to_string(),
            returns: None,
            epub_bytes: vec![],
        }
    }
}

#[async_trait]
impl DownloadSource for StubSource {
    fn name(&self) -> &str { &self.name }

    async fn find(&self, _title: &str, _authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        Ok(self.returns.clone())
    }

    async fn fetch(&self, _candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        std::fs::write(dest, &self.epub_bytes)?;
        Ok(())
    }
}

// SE matches → SE wins, Gutenberg and IA never tried
#[tokio::test]
async fn test_priority_se_wins_when_se_matches() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();
    db::insert_want(&pool, "Test Book", Some("Test Author"), None, "manual", None, 5, None).await.unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(StubSource::hit("standard-ebooks")),
        Box::new(StubSource::hit("gutenberg")),
        Box::new(StubSource::hit("internet-archive")),
    ];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, true).await.unwrap();

    assert_eq!(result.would_download, 1);
    match &result.events[0] {
        DownloadEvent::WouldDownload { source_name, .. } => {
            assert_eq!(source_name, "standard-ebooks", "SE must win when it matches");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

// SE misses, Gutenberg matches → Gutenberg wins
#[tokio::test]
async fn test_priority_gutenberg_wins_when_se_misses() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();
    db::insert_want(&pool, "Test Book", Some("Test Author"), None, "manual", None, 5, None).await.unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(StubSource::miss("standard-ebooks")),
        Box::new(StubSource::hit("gutenberg")),
        Box::new(StubSource::hit("internet-archive")),
    ];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, true).await.unwrap();

    assert_eq!(result.would_download, 1);
    match &result.events[0] {
        DownloadEvent::WouldDownload { source_name, .. } => {
            assert_eq!(source_name, "gutenberg", "Gutenberg must win when SE misses");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

// SE and Gutenberg both miss, IA matches → IA wins
#[tokio::test]
async fn test_priority_ia_wins_when_se_and_gutenberg_miss() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();
    db::insert_want(&pool, "Test Book", Some("Test Author"), None, "manual", None, 5, None).await.unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(StubSource::miss("standard-ebooks")),
        Box::new(StubSource::miss("gutenberg")),
        Box::new(StubSource::hit("internet-archive")),
    ];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, true).await.unwrap();

    assert_eq!(result.would_download, 1);
    match &result.events[0] {
        DownloadEvent::WouldDownload { source_name, .. } => {
            assert_eq!(source_name, "internet-archive", "IA must win when SE and Gutenberg miss");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

// All sources miss → not_found, attempts incremented
#[tokio::test]
async fn test_priority_not_found_when_all_miss() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();
    db::insert_want(&pool, "Test Book", Some("Test Author"), None, "manual", None, 5, None).await.unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(StubSource::miss("standard-ebooks")),
        Box::new(StubSource::miss("gutenberg")),
        Box::new(StubSource::miss("internet-archive")),
    ];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false).await.unwrap();

    assert_eq!(result.not_found, 1);
    assert_eq!(result.downloaded, 0);

    // Verify download_attempts was incremented in the DB
    let want_list = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(want_list[0].download_attempts, 1, "download_attempts must be incremented on not-found");
}

// Multiple entries: mix of source wins to verify prioritization across the full list
#[tokio::test]
async fn test_priority_mixed_sources_across_multiple_entries() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    // Three entries, each "found" by a different source
    db::insert_want(&pool, "Book SE", Some("Author A"), None, "manual", None, 5, None).await.unwrap();
    db::insert_want(&pool, "Book Gut", Some("Author B"), None, "manual", None, 5, None).await.unwrap();
    db::insert_want(&pool, "Book IA", Some("Author C"), None, "manual", None, 5, None).await.unwrap();

    // Each source only matches its own book
    struct TitledStub { name: String, match_title: String }

    #[async_trait]
    impl DownloadSource for TitledStub {
        fn name(&self) -> &str { &self.name }
        async fn find(&self, title: &str, _: &str) -> anyhow::Result<Option<DownloadCandidate>> {
            if title == self.match_title {
                Ok(Some(DownloadCandidate {
                    source_name: self.name.clone(),
                    title: title.to_string(),
                    authors: String::new(),
                    download_url: "http://example.com/book.epub".to_string(),
                    format: "epub".to_string(),
                }))
            } else {
                Ok(None)
            }
        }
        async fn fetch(&self, _: &DownloadCandidate, _: &Path) -> anyhow::Result<()> { Ok(()) }
    }

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(TitledStub { name: "standard-ebooks".to_string(), match_title: "Book SE".to_string() }),
        Box::new(TitledStub { name: "gutenberg".to_string(),       match_title: "Book Gut".to_string() }),
        Box::new(TitledStub { name: "internet-archive".to_string(), match_title: "Book IA".to_string() }),
    ];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, true).await.unwrap();

    assert_eq!(result.would_download, 3, "all three entries must be found");
    assert_eq!(result.not_found, 0);

    // Collect source names per title
    let mut found_by: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for event in &result.events {
        if let DownloadEvent::WouldDownload { title, source_name } = event {
            found_by.insert(title.clone(), source_name.clone());
        }
    }

    assert_eq!(found_by["Book SE"],  "standard-ebooks");
    assert_eq!(found_by["Book Gut"], "gutenberg");
    assert_eq!(found_by["Book IA"],  "internet-archive");
}
