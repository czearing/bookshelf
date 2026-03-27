/// Integration tests for ArxivSource — Atom XML parsing, two-query fallback,
/// author normalization, fuzzy matching, fetch, and end-to-end PDF pipeline.
use async_trait::async_trait;
use bookshelf_core::{
    db,
    download::{self, DownloadEvent},
    download_sources::{ArxivSource, DownloadCandidate, DownloadSource},
};
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal arXiv Atom feed with one entry.
fn atom_feed(title: &str, authors: &[&str], pdf_href: &str) -> String {
    let author_xml: String = authors
        .iter()
        .map(|a| format!("    <author><name>{a}</name></author>\n"))
        .collect();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2301.00001v1</id>
    <title>{title}</title>
{author_xml}    <link href="http://arxiv.org/abs/2301.00001v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="{pdf_href}" rel="related" type="application/pdf"/>
    <summary>Abstract text.</summary>
  </entry>
</feed>"#
    )
}

fn empty_feed() -> &'static str {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
</feed>"#
}

// ---------------------------------------------------------------------------
// find — happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_returns_candidate() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/2301.00001", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(atom_feed("Attention Is All You Need", &["Vaswani, Ashish"], &pdf_url))
        )
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("Attention Is All You Need", "Ashish Vaswani").await.unwrap();

    assert!(result.is_some(), "find must return Some for a matching paper");
    let c = result.unwrap();
    assert_eq!(c.source_name, "arxiv");
    assert_eq!(c.download_url, pdf_url);
    assert_eq!(c.format, "pdf");
}

// ---------------------------------------------------------------------------
// find — empty feed returns None, then fallback query also returns None
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_returns_none_when_no_results() {
    let server = MockServer::start().await;

    // Both the primary and fallback queries return empty feeds.
    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(empty_feed()))
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("A Completely Unknown Paper", "Nobody").await.unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// find — results exist but none fuzzy-match → None
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_returns_none_on_fuzzy_mismatch() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/9999.00001", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(atom_feed("Completely Unrelated Paper", &["Smith, John"], &pdf_url))
        )
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("Quantum Computing Survey", "Alice Researcher").await.unwrap();
    assert!(result.is_none(), "fuzzy mismatch must return None");
}

// ---------------------------------------------------------------------------
// find — author name normalized from "Lastname, Firstname"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_normalizes_inverted_author_name() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/1706.03762", server.uri());

    // Feed returns "LeCun, Yann" — find is called with "Yann LeCun"
    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(atom_feed("Deep Learning", &["LeCun, Yann"], &pdf_url))
        )
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("Deep Learning", "Yann LeCun").await.unwrap();
    assert!(result.is_some(), "inverted author must be normalized and matched");
}

// ---------------------------------------------------------------------------
// find — multi-author paper matches on first author
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_multi_author_paper() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/1706.03762", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(atom_feed(
                    "Attention Is All You Need",
                    &["Vaswani, Ashish", "Shazeer, Noam", "Parmar, Niki"],
                    &pdf_url,
                ))
        )
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    // Search with just the lead author — should still match.
    let result = source.find("Attention Is All You Need", "Ashish Vaswani").await.unwrap();
    assert!(result.is_some(), "multi-author paper must match on lead author");
}

// ---------------------------------------------------------------------------
// find — title with extra whitespace/newlines gets cleaned up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_cleans_title_whitespace() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/0000.00001", server.uri());

    // Title with embedded newlines (common in arXiv Atom feeds for long titles).
    let feed = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/0000.00001v1</id>
    <title>Generative Adversarial
    Networks</title>
    <author><name>Goodfellow, Ian</name></author>
    <link title="pdf" href="{pdf_url}" rel="related" type="application/pdf"/>
    <summary>Abstract.</summary>
  </entry>
</feed>"#
    );

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed))
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("Generative Adversarial Networks", "Ian Goodfellow").await.unwrap();
    assert!(result.is_some(), "title with embedded newlines must still match after whitespace normalization");
}

// ---------------------------------------------------------------------------
// find — HTTP error propagates as Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_http_error_returns_err() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source.find("Some Paper", "Some Author").await;
    assert!(result.is_err(), "HTTP 503 must propagate as Err");
}

// ---------------------------------------------------------------------------
// fetch — writes PDF bytes to dest
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_fetch_writes_pdf_to_dest() {
    let server = MockServer::start().await;
    let fake_pdf = b"%PDF-1.4 fake pdf content for testing";

    Mock::given(method("GET"))
        .and(path("/pdf/2301.00001"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(fake_pdf.as_slice())
        )
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let candidate = DownloadCandidate {
        source_name: "arxiv".to_string(),
        title: "Some Paper".to_string(),
        authors: "Some Author".to_string(),
        download_url: format!("{}/pdf/2301.00001", server.uri()),
        format: "pdf".to_string(),
    };

    let tmp = NamedTempFile::new().unwrap();
    source.fetch(&candidate, tmp.path()).await.unwrap();

    let written = std::fs::read(tmp.path()).unwrap();
    assert_eq!(written, fake_pdf.as_slice());
}

// ---------------------------------------------------------------------------
// fetch — HTTP error propagates as Err
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_fetch_http_error_returns_err() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/pdf/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let candidate = DownloadCandidate {
        source_name: "arxiv".to_string(),
        title: "Missing Paper".to_string(),
        authors: "Nobody".to_string(),
        download_url: format!("{}/pdf/missing", server.uri()),
        format: "pdf".to_string(),
    };

    let tmp = NamedTempFile::new().unwrap();
    let result = source.fetch(&candidate, tmp.path()).await;
    assert!(result.is_err(), "HTTP 404 must propagate as Err");
}

// ---------------------------------------------------------------------------
// find — fallback to title-only query when title+author returns empty
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_arxiv_find_uses_title_only_fallback() {
    let server = MockServer::start().await;
    let pdf_url = format!("{}/pdf/1234.56789", server.uri());

    // First call (title+author) → empty; second call (title only) → match.
    let mut call_count = std::sync::atomic::AtomicUsize::new(0);

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(empty_feed()))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(atom_feed("BERT: Pre-training of Deep Bidirectional Transformers", &["Devlin, Jacob"], &pdf_url))
        )
        .mount(&server)
        .await;

    let _ = call_count.get_mut(); // silence unused warning
    let source = ArxivSource::new(reqwest::Client::new(), server.uri());
    let result = source
        .find("BERT: Pre-training of Deep Bidirectional Transformers", "Jacob Devlin")
        .await
        .unwrap();

    assert!(result.is_some(), "title-only fallback must find the paper when title+author returns empty");
    assert_eq!(result.unwrap().source_name, "arxiv");
}

// ---------------------------------------------------------------------------
// End-to-end PDF pipeline test
//
// Verifies the full chain for a PDF source:
//   find() → fetch() → %PDF- header check → copy to library →
//   upsert_edition → overwrite_edition_metadata → delete_want
//
// The want-list title/authors must win over whatever meta_from_pdf_path
// extracted from the file, and the want entry must be removed from the DB.
// ---------------------------------------------------------------------------

/// A stub source that writes a minimal valid PDF when fetch() is called.
struct PdfStub {
    candidate: DownloadCandidate,
}

#[async_trait]
impl DownloadSource for PdfStub {
    fn name(&self) -> &str { "arxiv" }

    async fn find(&self, _title: &str, _authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        Ok(Some(self.candidate.clone()))
    }

    async fn fetch(&self, _candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        // Minimal PDF that passes the %PDF- header check.
        // The Info dict here intentionally has different Title/Author than the want
        // entry so we can verify overwrite_edition_metadata wins.
        let pdf_bytes = b"%PDF-1.4\n\
            1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n\
            2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n\
            xref\n0 3\n0000000000 65535 f \n\
            trailer\n<< /Size 3 /Root 1 0 R\n\
            /Info << /Title (PDF Embedded Title) /Author (PDF Embedded Author) >>\n>>\n\
            startxref\n9\n%%EOF";
        std::fs::write(dest, pdf_bytes)?;
        Ok(())
    }
}

#[tokio::test]
async fn test_arxiv_e2e_pdf_downloaded_and_metadata_correct() {
    let tmp_db = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp_db.path()).await.unwrap();
    let lib_dir = TempDir::new().unwrap();

    // Insert a want entry with specific title and author.
    db::insert_want(
        &pool,
        "Attention Is All You Need",
        Some("Ashish Vaswani"),
        None,
        "manual",
        None,
        8,
        None,
    )
    .await
    .unwrap();

    let candidate = DownloadCandidate {
        source_name: "arxiv".to_string(),
        title: "Attention Is All You Need".to_string(),
        authors: "Ashish Vaswani".to_string(),
        download_url: "https://arxiv.org/pdf/1706.03762".to_string(),
        format: "pdf".to_string(),
    };

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(PdfStub { candidate })];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    // 1. Download was counted.
    assert_eq!(result.downloaded, 1, "must count 1 downloaded");
    assert_eq!(result.not_found, 0);

    // 2. DownloadEvent recorded.
    assert!(
        matches!(&result.events[0], DownloadEvent::Downloaded { source_name, .. } if source_name == "arxiv"),
        "event must be Downloaded with source_name=arxiv"
    );

    // 3. Want entry removed.
    let remaining_wants = db::list_want(&pool, None, None).await.unwrap();
    assert!(remaining_wants.is_empty(), "want entry must be deleted after successful download");

    // 4. Edition inserted into DB.
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 1, "exactly one edition must be in the DB");

    let ed = &editions[0];

    // 5. Title and author come from the want list, NOT from the embedded PDF metadata.
    assert_eq!(
        ed.title.as_deref(),
        Some("Attention Is All You Need"),
        "edition title must match want-list title, not PDF-embedded title"
    );
    assert_eq!(
        ed.authors.as_deref(),
        Some("Ashish Vaswani"),
        "edition authors must match want-list authors, not PDF-embedded author"
    );

    // 6. Provenance recorded.
    assert_eq!(ed.download_source.as_deref(), Some("arxiv"));
    assert!(
        ed.download_url.as_deref().unwrap_or("").contains("arxiv.org"),
        "download_url must be set"
    );

    // 7. File exists on disk.
    let pdf_path = std::path::Path::new(ed.source_path.as_str());
    assert!(pdf_path.exists(), "PDF file must exist at source_path");
    assert!(ed.source_path.ends_with(".pdf"), "source_path must have .pdf extension");
}

// ---------------------------------------------------------------------------
// PDF validation: bytes that don't start with %PDF- must be rejected.
// ---------------------------------------------------------------------------

/// A stub that writes garbage bytes (no %PDF- header).
struct GarbagePdfStub;

#[async_trait]
impl DownloadSource for GarbagePdfStub {
    fn name(&self) -> &str { "arxiv" }
    async fn find(&self, _: &str, _: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        Ok(Some(DownloadCandidate {
            source_name: "arxiv".to_string(),
            title: "Garbage".to_string(),
            authors: "Nobody".to_string(),
            download_url: "https://arxiv.org/pdf/0000.00000".to_string(),
            format: "pdf".to_string(),
        }))
    }
    async fn fetch(&self, _: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        std::fs::write(dest, b"this is not a pdf")?;
        Ok(())
    }
}

#[tokio::test]
async fn test_arxiv_e2e_invalid_pdf_header_counts_as_not_found() {
    let tmp_db = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp_db.path()).await.unwrap();
    let lib_dir = TempDir::new().unwrap();

    db::insert_want(&pool, "Garbage", Some("Nobody"), None, "manual", None, 5, None)
        .await
        .unwrap();

    let sources: Vec<Box<dyn DownloadSource>> = vec![Box::new(GarbagePdfStub)];
    let result = download::download_want_list(&pool, lib_dir.path(), &sources, false)
        .await
        .unwrap();

    assert_eq!(result.not_found, 1, "invalid PDF header must count as not_found");
    assert_eq!(result.downloaded, 0);

    // No edition should have been inserted.
    let editions = db::list_editions(&pool).await.unwrap();
    assert!(editions.is_empty(), "no edition must be inserted for an invalid PDF");
}
