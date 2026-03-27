/// Integration tests for download_want_list (trait-based pluggable download system).
use async_trait::async_trait;
use bookshelf_core::{
    db,
    download::{self, DownloadEvent},
    download_sources::{DownloadCandidate, DownloadSource},
};
use std::io::Write as _;
use std::path::Path;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use tempfile::{NamedTempFile, TempDir};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers (same as integration_download.rs)
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

/// IA advanced search response with one result.
fn ia_search_response(identifier: &str, title: &str, creator: &str) -> serde_json::Value {
    serde_json::json!({
        "response": {
            "docs": [{
                "identifier": identifier,
                "title": title,
                "creator": creator
            }]
        }
    })
}

/// IA metadata response that passes the public-domain filter (year < 1928).
fn ia_metadata_response(epub_filename: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "date": "1900",
            "subject": null
        },
        "files": [
            { "name": epub_filename }
        ]
    })
}

/// SE feed with a rel="next" link but NO entries (page has no match).
fn se_feed_empty_with_next(next_href: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:opds="http://opds-spec.org/2010/catalog">
  <link rel="next" href="{next_href}"/>
</feed>"#
    )
}

/// IA advanced search response with two results.
fn ia_search_response_two(
    id1: &str, title1: &str, creator1: &str,
    id2: &str, title2: &str, creator2: &str,
) -> serde_json::Value {
    serde_json::json!({
        "response": {
            "docs": [
                { "identifier": id1, "title": title1, "creator": creator1 },
                { "identifier": id2, "title": title2, "creator": creator2 }
            ]
        }
    })
}

// ---------------------------------------------------------------------------
// Mock DownloadSource for unit-style tests
// ---------------------------------------------------------------------------

struct MockSource {
    name: String,
    candidate: Option<DownloadCandidate>,
    epub_bytes: Vec<u8>,
    find_call_count: Arc<AtomicUsize>,
}

impl MockSource {
    fn new(name: &str, candidate: Option<DownloadCandidate>, epub_bytes: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            candidate,
            epub_bytes,
            find_call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl DownloadSource for MockSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn find(
        &self,
        _title: &str,
        _authors: &str,
    ) -> anyhow::Result<Option<DownloadCandidate>> {
        self.find_call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.candidate.clone())
    }

    async fn fetch(&self, _candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        std::fs::write(dest, &self.epub_bytes)?;
        Ok(())
    }
}

struct ErrorSource {
    name: String,
}

#[async_trait]
impl DownloadSource for ErrorSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn find(
        &self,
        _title: &str,
        _authors: &str,
    ) -> anyhow::Result<Option<DownloadCandidate>> {
        Err(anyhow::anyhow!("network error"))
    }

    async fn fetch(&self, _candidate: &DownloadCandidate, _dest: &Path) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("network error"))
    }
}

fn make_candidate(source_name: &str, title: &str, download_url: &str) -> DownloadCandidate {
    DownloadCandidate {
        source_name: source_name.to_string(),
        title: title.to_string(),
        authors: "Test Author".to_string(),
        download_url: download_url.to_string(),
        format: "epub".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Test: source priority order — first source wins
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_source_priority_order() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Odyssey by Homer",
        Some("Homer"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_a = make_epub_bytes("The Odyssey by Homer", "Homer");
    let epub_b = make_epub_bytes("The Odyssey by Homer", "Homer");

    let candidate_a = make_candidate("source-a", "The Odyssey by Homer", "http://example.com/a.epub");
    let candidate_b = make_candidate("source-b", "The Odyssey by Homer", "http://example.com/b.epub");

    let source_a = MockSource::new("source-a", Some(candidate_a.clone()), epub_a);
    let source_b = MockSource::new("source-b", Some(candidate_b), epub_b);
    let count_b = source_b.find_call_count.clone();

    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(source_a),
        Box::new(source_b),
    ];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "expected 1 downloaded");
    assert_eq!(result.not_found, 0);

    // Verify source-a was used: the downloaded event must name source-a.
    let downloaded_event = result.events.iter().find(|e| {
        matches!(e, DownloadEvent::Downloaded { source_name, .. } if source_name == "source-a")
    });
    assert!(
        downloaded_event.is_some(),
        "source-a must be the winning source, not source-b"
    );

    // AC 6: source-b must NOT have been consulted (short-circuit after source-a matched).
    assert_eq!(
        count_b.load(Ordering::SeqCst),
        0,
        "source-b find() must not be called when source-a returns Some"
    );
}

// ---------------------------------------------------------------------------
// Test: StandardEbooksSource with wiremock finds a match
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_source_finds_match() {
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

    let se_source = bookshelf_core::download_sources::StandardEbooksSource::new(
        client,
        server.uri(),
        None,
    );
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(se_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "expected 1 downloaded");
    assert_eq!(result.not_found, 0);

    // Want list should now be empty.
    let want = db::list_want(&pool, None, None).await.unwrap();
    assert!(want.is_empty(), "want list must be empty after download");
}

// ---------------------------------------------------------------------------
// Test: InternetArchiveSource with wiremock finds a match
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_ia_source_finds_match() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Iliad",
        Some("Homer"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("The Iliad", "Homer");

    // IA advanced search — matches any request to /advancedsearch.php
    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_search_response("iliad00homer", "The Iliad", "Homer")),
        )
        .mount(&server)
        .await;

    // IA metadata
    Mock::given(method("GET"))
        .and(path("/metadata/iliad00homer"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_metadata_response("iliad.epub")),
        )
        .mount(&server)
        .await;

    // IA EPUB download
    Mock::given(method("GET"))
        .and(path("/download/iliad00homer/iliad.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let ia_source = bookshelf_core::download_sources::InternetArchiveSource::new(
        client,
        server.uri(),
        vec!["epub".to_string()],
    );
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(ia_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "expected 1 downloaded via IA");
    assert_eq!(result.not_found, 0);
}

// ---------------------------------------------------------------------------
// Test: dry_run=true — would_download=1, downloaded=0, want list intact
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_dry_run() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "Frankenstein",
        Some("Mary Shelley"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("Frankenstein", "Mary Shelley");
    let candidate = make_candidate("mock-source", "Frankenstein", "http://example.com/frank.epub");

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(MockSource::new(
        "mock-source",
        Some(candidate),
        epub_bytes,
    ))];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, true)
        .await
        .unwrap();

    assert_eq!(result.would_download, 1, "dry run must report 1 would_download");
    assert_eq!(result.downloaded, 0, "dry run must not actually download");
    assert_eq!(result.not_found, 0);

    // Want list must still have the entry.
    let want = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(want.len(), 1, "want list must be intact after dry run");

    // No files written to lib_dir.
    let files: Vec<_> = std::fs::read_dir(lib_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(files.is_empty(), "no files must be written during dry run");

    // Events must contain WouldDownload.
    let would_download_event = result.events.iter().find(|e| {
        matches!(e, DownloadEvent::WouldDownload { .. })
    });
    assert!(
        would_download_event.is_some(),
        "events must contain WouldDownload"
    );
}

// ---------------------------------------------------------------------------
// Test: source returns Err → not_found, no crash, want entry stays
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_source_error_is_definitive() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Time Machine",
        Some("H.G. Wells"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(ErrorSource {
        name: "error-source".to_string(),
    })];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.not_found, 1, "source error must count as not_found");
    assert_eq!(result.downloaded, 0);

    // Want list entry must still be present.
    let want = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(want.len(), 1, "want entry must remain when source errors");
}

// ---------------------------------------------------------------------------
// Test: source returns Ok(None) → not_found=1, want entry stays
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_not_found() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "Extremely Obscure Book ZZZZZ99",
        Some("No Such Author"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let sources: Vec<Box<dyn DownloadSource>> =
        vec![Box::new(MockSource::new("null-source", None, vec![]))];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.not_found, 1, "Ok(None) must count as not_found");
    assert_eq!(result.downloaded, 0);

    let want = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(want.len(), 1, "want entry must remain when not found");
}

// ---------------------------------------------------------------------------
// Test: successful download removes entry from want list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_removes_from_want_list_on_success() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "War and Peace",
        Some("Leo Tolstoy"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("War and Peace", "Leo Tolstoy");
    let candidate = make_candidate(
        "mock-source",
        "War and Peace",
        "http://example.com/warandpeace.epub",
    );

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(MockSource::new(
        "mock-source",
        Some(candidate),
        epub_bytes,
    ))];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1);

    // Want list must be empty.
    let want = db::list_want(&pool, None, None).await.unwrap();
    assert!(
        want.is_empty(),
        "want list must be empty after successful download"
    );
}

// ---------------------------------------------------------------------------
// Test: download sets download_source provenance on the edition row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_sets_provenance() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Adventures of Huckleberry Finn",
        Some("Mark Twain"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("The Adventures of Huckleberry Finn", "Mark Twain");
    let candidate = make_candidate(
        "provenance-source",
        "The Adventures of Huckleberry Finn",
        "http://example.com/huck.epub",
    );

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(MockSource::new(
        "provenance-source",
        Some(candidate),
        epub_bytes,
    ))];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "expected 1 downloaded");

    // Read back the editions and find the one with a non-null download_source.
    let editions = db::list_editions(&pool).await.unwrap();
    let provenance_edition = editions
        .iter()
        .find(|e| e.download_source.is_some());

    assert!(
        provenance_edition.is_some(),
        "at least one edition must have download_source set"
    );

    let ed = provenance_edition.unwrap();
    assert_eq!(
        ed.download_source.as_deref(),
        Some("provenance-source"),
        "download_source must match source name"
    );
    assert_eq!(
        ed.download_url.as_deref(),
        Some("http://example.com/huck.epub"),
        "download_url must equal the candidate URL"
    );
}

// ---------------------------------------------------------------------------
// Issue 1 & 2: AC 7 — second source used when first returns None; AC 6 call counting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_source_fallback_when_first_returns_none() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Brothers Karamazov",
        Some("Fyodor Dostoevsky"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("The Brothers Karamazov", "Fyodor Dostoevsky");
    let candidate_b = make_candidate(
        "source-b",
        "The Brothers Karamazov",
        "http://example.com/karamazov.epub",
    );

    // source-a returns Ok(None) — no match.
    let source_a = MockSource::new("source-a", None, vec![]);
    let count_a = source_a.find_call_count.clone();

    // source-b returns a valid candidate.
    let source_b = MockSource::new("source-b", Some(candidate_b), epub_bytes);

    let sources: Vec<Box<dyn DownloadSource>> =
        vec![Box::new(source_a), Box::new(source_b)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "source-b must be used when source-a returns None");
    assert_eq!(result.not_found, 0);

    // AC 7: source-a was consulted (returned None), so count must be 1.
    assert_eq!(
        count_a.load(Ordering::SeqCst),
        1,
        "source-a find() must be called exactly once"
    );

    // The Downloaded event must name source-b.
    let downloaded_event = result.events.iter().find(|e| {
        matches!(e, DownloadEvent::Downloaded { source_name, .. } if source_name == "source-b")
    });
    assert!(
        downloaded_event.is_some(),
        "Downloaded event must name source-b"
    );
}

// ---------------------------------------------------------------------------
// Issue 3: AC 11 — StandardEbooksSource paginates (entry on page 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_pagination() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    // Want entry that only appears on page 2.
    db::insert_want(
        &pool,
        "Neuromancer",
        Some("William Gibson"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("Neuromancer", "William Gibson");

    // Page 1: empty feed (no matching entry) with a rel="next" link pointing to page 2.
    // Served exactly once via up_to_n_times(1) so the second request falls through to
    // the page-2 fallback mock (FIFO quota-exhaustion pattern from journal).
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_empty_with_next("/feeds/opds/all?page=2")),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2: feed with the matching entry and no next link.
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

    // EPUB download.
    Mock::given(method("GET"))
        .and(path("/epub/neuromancer.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let se_source =
        bookshelf_core::download_sources::StandardEbooksSource::new(client, server.uri(), None);
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(se_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(
        result.downloaded, 1,
        "Neuromancer must be found on page 2 via StandardEbooksSource pagination"
    );
    assert_eq!(result.not_found, 0);
}

// ---------------------------------------------------------------------------
// Issue 4: AC 18 — IA public-domain filter: second doc used when first fails filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_ia_public_domain_filter() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Iliad",
        Some("Homer"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("The Iliad", "Homer");

    // IA search: two results — bad-id first, good-id second.
    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ia_search_response_two(
                "bad-id", "The Iliad", "Homer",
                "good-id", "The Iliad", "Homer",
            )),
        )
        .mount(&server)
        .await;

    // bad-id metadata: access-restricted-item = "true" → fails filter.
    Mock::given(method("GET"))
        .and(path("/metadata/bad-id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "metadata": { "access-restricted-item": "true" },
                "files": [{ "name": "iliad.epub" }]
            })),
        )
        .expect(1)
        .mount(&server)
        .await;

    // good-id metadata: no access-restricted-item → freely downloadable, passes filter.
    Mock::given(method("GET"))
        .and(path("/metadata/good-id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "metadata": {},
                "files": [{ "name": "iliad.epub" }]
            })),
        )
        .expect(1)
        .mount(&server)
        .await;

    // EPUB download from good-id.
    Mock::given(method("GET"))
        .and(path("/download/good-id/iliad.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    let ia_source =
        bookshelf_core::download_sources::InternetArchiveSource::new(client, server.uri(), vec!["epub".to_string()]);
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(ia_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(
        result.downloaded, 1,
        "good-id must be used after bad-id fails the public-domain filter"
    );
    assert_eq!(result.not_found, 0);
    // wiremock verifies expect(1) on both metadata mocks when `server` is dropped.
}

// ---------------------------------------------------------------------------
// Issue 5: AC 36 — --source isolation: only SE source, IA path never called
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_source_flag_isolation() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "Dune",
        Some("Frank Herbert"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("Dune", "Frank Herbert");

    // SE OPDS feed mock — returns a matching entry.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("Dune", "Frank Herbert", "/epub/dune.epub")),
        )
        .mount(&server)
        .await;

    // SE EPUB download.
    Mock::given(method("GET"))
        .and(path("/epub/dune.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    // IA search mock: must NOT be called when only SE source is used.
    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "response": { "docs": [] }
        })))
        .expect(0)
        .mount(&server)
        .await;

    // Only SE source is passed — IA is excluded.
    let se_source =
        bookshelf_core::download_sources::StandardEbooksSource::new(client, server.uri(), None);
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(se_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "SE must download when IA is excluded");
    assert_eq!(result.not_found, 0);
    // wiremock verifies expect(0) on the IA mock when `server` is dropped.
}

// ---------------------------------------------------------------------------
// AC 37 — --source isolation: only IA source, SE OPDS path never called
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_ia_only_does_not_call_se() {
    let (pool, _tmp) = temp_pool().await;
    let server = MockServer::start().await;
    let client = reqwest::Client::new();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(
        &pool,
        "The Iliad",
        Some("Homer"),
        None,
        "manual",
        None,
        5,
        None,
    )
    .await
    .unwrap();

    let epub_bytes = make_epub_bytes("The Iliad", "Homer");

    // SE OPDS feed: must NOT be called when only IA source is used.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom"/>"#,
        ))
        .expect(0)
        .mount(&server)
        .await;

    // IA advanced search — returns a matching result.
    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_search_response("iliad00homer", "The Iliad", "Homer")),
        )
        .mount(&server)
        .await;

    // IA metadata — passes public-domain filter (year 1900).
    Mock::given(method("GET"))
        .and(path("/metadata/iliad00homer"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_metadata_response("iliad.epub")),
        )
        .mount(&server)
        .await;

    // IA EPUB download.
    Mock::given(method("GET"))
        .and(path("/download/iliad00homer/iliad.epub"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(epub_bytes)
                .insert_header("content-type", "application/epub+zip"),
        )
        .mount(&server)
        .await;

    // Only IA source is passed — SE is excluded.
    let ia_source =
        bookshelf_core::download_sources::InternetArchiveSource::new(client, server.uri(), vec!["epub".to_string()]);
    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(ia_source)];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.downloaded, 1, "IA must download when SE is excluded");
    assert_eq!(result.not_found, 0);
    // wiremock verifies expect(0) on the SE OPDS mock when `server` is dropped.
}

// ---------------------------------------------------------------------------
// AC 5: empty sources slice — all want entries counted as not_found, no panic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_empty_sources_all_not_found() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Book One", Some("Author A"), None, "manual", None, 5, None)
        .await
        .unwrap();
    db::insert_want(&pool, "Book Two", Some("Author B"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![];

    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.not_found, 2, "both entries must be not_found when sources is empty");
    assert_eq!(result.downloaded, 0);

    // Both want entries must still be present.
    let want = db::list_want(&pool, None, None).await.unwrap();
    assert_eq!(want.len(), 2, "both want entries must remain when no sources are provided");
}

// ---------------------------------------------------------------------------
// AC 13: StandardEbooksSource returns Err on non-2xx HTTP response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_http_error_returns_err() {
    let server = MockServer::start().await;
    let client = reqwest::Client::new();

    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let se_source =
        bookshelf_core::download_sources::StandardEbooksSource::new(client, server.uri(), None);

    let result = se_source.find("Any Title", "Any Author").await;

    assert!(result.is_err(), "SE source must return Err on HTTP 500, not Ok(None)");
}

// ---------------------------------------------------------------------------
// AC 25: InternetArchiveSource returns Err on non-2xx search response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_ia_search_http_error_returns_err() {
    let server = MockServer::start().await;
    let client = reqwest::Client::new();

    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let ia_source =
        bookshelf_core::download_sources::InternetArchiveSource::new(client, server.uri(), vec!["epub".to_string()]);

    let result = ia_source.find("Any Title", "Any Author").await;

    assert!(result.is_err(), "IA source must return Err on HTTP 503, not Ok(None)");
}

// ---------------------------------------------------------------------------
// AC 14: StandardEbooksSource fetch returns Err on non-2xx download response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_se_fetch_http_error_returns_err() {
    let server = MockServer::start().await;
    let client = reqwest::Client::new();

    // OPDS feed returns a single matching entry.
    Mock::given(method("GET"))
        .and(path("/feeds/opds/all"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(se_feed_single("The Title", "The Author", "/epub/book.epub")),
        )
        .mount(&server)
        .await;

    // Download URL returns 403.
    Mock::given(method("GET"))
        .and(path("/epub/book.epub"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let source =
        bookshelf_core::download_sources::StandardEbooksSource::new(client, server.uri(), None);

    let candidate = source.find("The Title", "The Author").await;
    assert!(candidate.is_ok(), "find must succeed");
    let candidate = candidate.unwrap();
    assert!(candidate.is_some(), "find must return Some candidate");
    let candidate = candidate.unwrap();

    let tmp = NamedTempFile::new().unwrap();
    let result = source.fetch(&candidate, tmp.path()).await;

    assert!(result.is_err(), "SE fetch must return Err on HTTP 403");
}

// ---------------------------------------------------------------------------
// AC 24: InternetArchiveSource fetch returns Err on non-2xx download response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_ia_fetch_http_error_returns_err() {
    let server = MockServer::start().await;
    let client = reqwest::Client::new();

    // IA search returns one doc.
    Mock::given(method("GET"))
        .and(path("/advancedsearch.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_search_response("book00auth", "Moby-Dick", "Herman Melville")),
        )
        .mount(&server)
        .await;

    // Metadata passes the public-domain filter and lists book.epub.
    Mock::given(method("GET"))
        .and(path("/metadata/book00auth"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ia_metadata_response("book.epub")),
        )
        .mount(&server)
        .await;

    // Download URL returns 403.
    Mock::given(method("GET"))
        .and(path("/download/book00auth/book.epub"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let source =
        bookshelf_core::download_sources::InternetArchiveSource::new(client, server.uri(), vec!["epub".to_string()]);

    let candidate = source.find("Moby-Dick", "Herman Melville").await;
    assert!(candidate.is_ok(), "find must succeed");
    let candidate = candidate.unwrap();
    assert!(candidate.is_some(), "find must return Some candidate");
    let candidate = candidate.unwrap();

    let tmp = NamedTempFile::new().unwrap();
    let result = source.fetch(&candidate, tmp.path()).await;

    assert!(result.is_err(), "IA fetch must return Err on HTTP 403");
}

// ---------------------------------------------------------------------------
// Test: parallel find preserves entry→candidate mapping (buffered ordering)
//
// Two want entries: "Book Alpha" and "Book Beta".
// Source A matches only "Book Alpha" but sleeps 50 ms before returning.
// Source B matches only "Book Beta" and returns immediately.
//
// With buffer_unordered (wrong), "Book Beta"'s fast result would arrive first
// and be zipped to "Book Alpha" — giving the wrong source name in the event.
// With buffered (correct), results stay in submission order regardless of speed.
// ---------------------------------------------------------------------------

/// A source whose find() sleeps a configurable duration before returning.
/// Returns Some(candidate) only when the input title exactly matches `match_title`.
struct DelayedSource {
    name: String,
    match_title: String,
    delay_ms: u64,
}

#[async_trait]
impl DownloadSource for DelayedSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn find(
        &self,
        title: &str,
        _authors: &str,
    ) -> anyhow::Result<Option<DownloadCandidate>> {
        if self.delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        }
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

    async fn fetch(&self, _candidate: &DownloadCandidate, _dest: &Path) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_parallel_find_preserves_entry_order() {
    let (pool, _tmp) = temp_pool().await;
    let lib_dir = TempDir::new().unwrap();

    // Insert want entries in order: Alpha first, Beta second.
    db::insert_want(&pool, "Book Alpha", Some("Author"), None, "manual", None, 5, None)
        .await
        .unwrap();
    db::insert_want(&pool, "Book Beta", Some("Author"), None, "manual", None, 5, None)
        .await
        .unwrap();

    // Source for Alpha: 50 ms delay (would finish second if ordering is wrong).
    // Source for Beta:  no delay  (finishes first — would scramble with buffer_unordered).
    let sources: Vec<Box<dyn DownloadSource>> = vec![
        Box::new(DelayedSource {
            name: "source-alpha".to_string(),
            match_title: "Book Alpha".to_string(),
            delay_ms: 50,
        }),
        Box::new(DelayedSource {
            name: "source-beta".to_string(),
            match_title: "Book Beta".to_string(),
            delay_ms: 0,
        }),
    ];

    let result = bookshelf_core::download::download_want_list(
        &pool, lib_dir.path(), &sources, true, // dry_run
    )
    .await
    .unwrap();

    assert_eq!(result.would_download, 2, "both entries must be found");
    assert_eq!(result.not_found, 0);

    // Verify each WouldDownload event carries the source name that matched its title.
    let events: Vec<_> = result.events.iter().collect();
    assert_eq!(events.len(), 2);

    match &events[0] {
        bookshelf_core::download::DownloadEvent::WouldDownload { title, source_name } => {
            assert_eq!(title, "Book Alpha", "first event must be Book Alpha");
            assert_eq!(source_name, "source-alpha", "Book Alpha must match source-alpha");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    match &events[1] {
        bookshelf_core::download::DownloadEvent::WouldDownload { title, source_name } => {
            assert_eq!(title, "Book Beta", "second event must be Book Beta");
            assert_eq!(source_name, "source-beta", "Book Beta must match source-beta");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}
