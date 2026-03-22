use crate::db::EnrichmentUpdate;
use anyhow::Context;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// OpenLibrary jscmd=data response structures
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlBook {
    title: Option<String>,
    publishers: Option<Vec<OlPublisher>>,
    publish_date: Option<String>,
    description: Option<OlDescription>,
    identifiers: Option<OlIdentifiers>,
    works: Option<Vec<OlWorkRef>>,
}

#[derive(Debug, Deserialize)]
struct OlPublisher {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OlDescription {
    Object { value: String },
    Text(String),
}

impl OlDescription {
    fn into_string(self) -> String {
        match self {
            OlDescription::Object { value } => value,
            OlDescription::Text(s) => s,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OlIdentifiers {
    isbn_13: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct OlWorkRef {
    key: String,
}

// ---------------------------------------------------------------------------
// OpenLibrary search.json structures
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlSearchResponse {
    docs: Vec<OlSearchDoc>,
}

#[derive(Debug, Deserialize)]
struct OlSearchDoc {
    isbn: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Google Books structures
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GbResponse {
    #[serde(rename = "totalItems")]
    total_items: u32,
    items: Option<Vec<GbItem>>,
}

#[derive(Debug, Deserialize)]
struct GbItem {
    #[serde(rename = "volumeInfo")]
    volume_info: GbVolumeInfo,
}

#[derive(Debug, Deserialize)]
struct GbVolumeInfo {
    title: Option<String>,
    authors: Option<Vec<String>>,
    publisher: Option<String>,
    #[serde(rename = "publishedDate")]
    published_date: Option<String>,
    description: Option<String>,
    #[serde(rename = "industryIdentifiers")]
    industry_identifiers: Option<Vec<GbIdentifier>>,
}

#[derive(Debug, Deserialize)]
struct GbIdentifier {
    #[serde(rename = "type")]
    id_type: String,
    identifier: String,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Base URLs — configurable for testing via wiremock.
pub const OPENLIBRARY_BASE: &str = "https://openlibrary.org";
pub const GOOGLE_BOOKS_BASE: &str = "https://www.googleapis.com";

/// Query OpenLibrary by ISBN. Returns `Some(EnrichmentUpdate)` with data if
/// the API returned a non-empty body, `None` if the body was an empty JSON
/// object (triggers Google Books fallback), or `Err` on network/HTTP errors.
pub async fn enrich_from_openlibrary_isbn(
    client: &reqwest::Client,
    isbn: &str,
    base_url: &str,
) -> anyhow::Result<Option<EnrichmentUpdate>> {
    let url = format!(
        "{base_url}/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("HTTP request failed: {url}"))?;

    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(
            "OpenLibrary returned HTTP {} for ISBN {}",
            resp.status(),
            isbn
        ));
    }

    let body: HashMap<String, serde_json::Value> = resp
        .json()
        .await
        .context("OpenLibrary response is not valid JSON")?;

    let key = format!("ISBN:{isbn}");
    let book_value = match body.get(&key) {
        Some(v) => v,
        None => return Ok(None), // Empty map — no data for this ISBN
    };

    let book: OlBook = serde_json::from_value(book_value.clone())
        .context("failed to parse OpenLibrary book data")?;

    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut update = EnrichmentUpdate {
        enriched_at: Some(now),
        enrichment_attempted: 1,
        ..Default::default()
    };

    update.title = book.title;
    update.publisher = book
        .publishers
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0).name) });
    update.publish_date = book.publish_date;
    update.description = book.description.map(|d| d.into_string());
    update.isbn = book
        .identifiers
        .and_then(|id| id.isbn_13)
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) });
    update.ol_work_id = book
        .works
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0).key) });

    Ok(Some(update))
}

/// Query Google Books by ISBN. Returns `Some(EnrichmentUpdate)` if items
/// were returned, `None` if there were no results, or `Err` on HTTP/network
/// errors.
pub async fn enrich_from_google_books_isbn(
    client: &reqwest::Client,
    isbn: &str,
    base_url: &str,
) -> anyhow::Result<Option<EnrichmentUpdate>> {
    let url = format!(
        "{base_url}/books/v1/volumes?q=isbn:{isbn}"
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("HTTP request failed: {url}"))?;

    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(
            "Google Books returned HTTP {} for ISBN {}",
            resp.status(),
            isbn
        ));
    }

    let gb: GbResponse = resp
        .json()
        .await
        .context("Google Books response is not valid JSON")?;

    if gb.total_items == 0 {
        return Ok(None);
    }

    let items = match gb.items {
        Some(i) if !i.is_empty() => i,
        _ => return Ok(None),
    };

    let vi = &items[0].volume_info;
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let isbn13 = vi
        .industry_identifiers
        .as_ref()
        .and_then(|ids| {
            ids.iter()
                .find(|id| id.id_type == "ISBN_13")
                .map(|id| id.identifier.clone())
        });

    let update = EnrichmentUpdate {
        title: vi.title.clone(),
        authors: vi.authors.as_ref().map(|a| a.join(", ")),
        publisher: vi.publisher.clone(),
        publish_date: vi.published_date.clone(),
        description: vi.description.clone(),
        isbn: isbn13,
        enriched_at: Some(now),
        enrichment_attempted: 1,
        ol_work_id: None,
    };

    Ok(Some(update))
}

/// Query OpenLibrary `search.json` by title and author to find an ISBN-13.
/// Returns the first ISBN-13 from the top result, or `None`.
pub async fn find_isbn_by_title_author(
    client: &reqwest::Client,
    title: &str,
    author: &str,
    base_url: &str,
) -> anyhow::Result<Option<String>> {
    let url = format!(
        "{base_url}/search.json?title={}&author={}",
        urlencoding::encode(title),
        urlencoding::encode(author)
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("HTTP request failed: {url}"))?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let search: OlSearchResponse = resp
        .json()
        .await
        .context("OpenLibrary search response is not valid JSON")?;

    let isbn13 = search
        .docs
        .into_iter()
        .next()
        .and_then(|doc| doc.isbn)
        .and_then(|isbns| {
            isbns
                .into_iter()
                .find(|s| s.chars().filter(|c| c.is_ascii_digit()).count() == 13)
        });

    Ok(isbn13)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ol_description_text_variant() {
        let d = OlDescription::Text("A great book".to_string());
        assert_eq!(d.into_string(), "A great book");
    }

    #[test]
    fn test_ol_description_object_variant() {
        let d = OlDescription::Object {
            value: "A great book".to_string(),
        };
        assert_eq!(d.into_string(), "A great book");
    }

    #[test]
    fn test_enrichment_update_default() {
        let u = EnrichmentUpdate::default();
        assert!(u.title.is_none());
        assert_eq!(u.enrichment_attempted, 0);
    }
}
