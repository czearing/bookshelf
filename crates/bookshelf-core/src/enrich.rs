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
    authors: Option<Vec<OlAuthorRef>>,
    publishers: Option<Vec<OlPublisher>>,
    publish_date: Option<String>,
    description: Option<OlDescription>,
    identifiers: Option<OlIdentifiers>,
    works: Option<Vec<OlWorkRef>>,
}

#[derive(Debug, Deserialize)]
struct OlAuthorRef {
    name: Option<String>,
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
pub const OL_BASE_URL: &str = "https://openlibrary.org";
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
    update.authors = book.authors.and_then(|v| {
        let names: Vec<String> = v.into_iter().filter_map(|a| a.name).collect();
        if names.is_empty() { None } else { Some(names.join(", ")) }
    });
    update.publisher = book
        .publishers
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0).name) });
    update.publish_date = book.publish_date;
    update.description = book.description
        .map(|d| d.into_string())
        .filter(|d| description_looks_english(d));
    update.isbn = book
        .identifiers
        .and_then(|id| id.isbn_13)
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) });
    update.ol_work_id = book
        .works
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0).key) });

    Ok(Some(update))
}

/// Fetch the description from an OpenLibrary Work record.
///
/// OL edition records (`jscmd=data`) often lack descriptions; the canonical
/// description lives on the Work. Call this after `enrich_from_openlibrary_isbn`
/// when the returned update has `ol_work_id` but no `description`.
///
/// `work_key` is the raw OL key string, e.g. `"/works/OL45804W"`.
pub async fn fetch_ol_work_description(
    client: &reqwest::Client,
    work_key: &str,
    base_url: &str,
) -> anyhow::Result<Option<String>> {
    // Normalise: strip leading slash if present, then build URL.
    let key = work_key.trim_start_matches('/');
    let url = format!("{base_url}/{key}.json");

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("HTTP request failed: {url}"))?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    #[derive(Deserialize)]
    struct WorkDoc {
        description: Option<OlDescription>,
    }

    let doc: WorkDoc = match resp.json().await {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };

    Ok(doc.description
        .map(|d| d.into_string())
        .filter(|d| description_looks_english(d)))
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
        description: vi.description.clone().filter(|d| description_looks_english(d)),
        isbn: isbn13,
        enriched_at: Some(now),
        enrichment_attempted: 1,
        ol_work_id: None,
        ..Default::default()
    };

    Ok(Some(update))
}

/// Fetch a description from Google Books using a title+author query.
///
/// Used as a last-resort fallback when OL has no English description.
pub async fn fetch_description_from_google_books(
    client: &reqwest::Client,
    title: &str,
    author: &str,
    base_url: &str,
) -> anyhow::Result<Option<String>> {
    let query = if author.is_empty() {
        format!("intitle:{}", urlencoding::encode(title))
    } else {
        format!("intitle:{}+inauthor:{}", urlencoding::encode(title), urlencoding::encode(author))
    };
    let url = format!("{base_url}/books/v1/volumes?q={query}&maxResults=1");

    let resp = client.get(&url).send().await
        .with_context(|| format!("HTTP request failed: {url}"))?;
    if !resp.status().is_success() { return Ok(None); }

    let gb: GbResponse = match resp.json().await {
        Ok(g) => g,
        Err(_) => return Ok(None),
    };
    if gb.total_items == 0 { return Ok(None); }

    Ok(gb.items
        .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) })
        .and_then(|item| item.volume_info.description)
        .filter(|d| description_looks_english(d)))
}

/// Query OpenLibrary `search.json` by title and author to find an ISBN-13.
/// Returns the first ISBN-13 from the top result, or `None`.
///
/// Tries up to three title forms to handle OL indexing quirks:
/// 1. Full title as-is
/// 2. Short title (text before first `:` or ` - `) — handles long subtitles
/// 3. Title without leading article ("The ", "A ", "An ") — handles article drops
pub async fn find_isbn_by_title_author(
    client: &reqwest::Client,
    title: &str,
    author: &str,
    base_url: &str,
) -> anyhow::Result<Option<String>> {
    find_isbn_by_title_author_lang(client, title, author, base_url, None).await
}

pub async fn find_isbn_by_title_author_lang(
    client: &reqwest::Client,
    title: &str,
    author: &str,
    base_url: &str,
    language: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let short_title: Option<String> = title
        .split_once(':')
        .or_else(|| title.split_once(" - "))
        .map(|(before, _)| before.trim().to_string())
        .filter(|s| !s.is_empty() && s != title);

    let sans_article: Option<String> = ["The ", "A ", "An "]
        .iter()
        .find_map(|art| title.strip_prefix(art))
        .map(|s| s.to_string());

    let mut candidates: Vec<&str> = vec![title];
    if let Some(ref s) = short_title {
        candidates.push(s.as_str());
    }
    if let Some(ref s) = sans_article {
        if !candidates.contains(&s.as_str()) {
            candidates.push(s.as_str());
        }
    }

    for candidate in candidates {
        let lang_param = language
            .map(|l| format!("&language={}", urlencoding::encode(l)))
            .unwrap_or_default();
        let url = format!(
            "{base_url}/search.json?title={}&author={}&fields=isbn{}",
            urlencoding::encode(candidate),
            urlencoding::encode(author),
            lang_param
        );

        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("HTTP request failed: {url}"))?;

        if !resp.status().is_success() {
            continue;
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

        if isbn13.is_some() {
            return Ok(isbn13);
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// OpenLibrary search.json enrichment structures (title+author search)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlSearchEnrichDoc {
    key: Option<String>,
    title: Option<String>,
    author_name: Option<Vec<String>>,
    language: Option<Vec<String>>,
    series: Option<Vec<String>>,
    series_number: Option<String>,
    series_statement: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OlSearchEnrichResponse {
    docs: Vec<OlSearchEnrichDoc>,
}

/// Query OpenLibrary `search.json` by title and author, returning a full
/// `EnrichmentUpdate` with series and language data populated where available.
///
/// Tries two candidate title forms: the full title, then the text before the
/// first `:` or ` - ` separator. Returns `Ok(None)` if no results found.
pub async fn enrich_from_search(
    client: &reqwest::Client,
    title: &str,
    authors: &str,
    base_url: &str,
) -> anyhow::Result<Option<EnrichmentUpdate>> {
    enrich_from_search_lang(client, title, authors, base_url, None).await
}

pub async fn enrich_from_search_lang(
    client: &reqwest::Client,
    title: &str,
    authors: &str,
    base_url: &str,
    language: Option<&str>,
) -> anyhow::Result<Option<EnrichmentUpdate>> {
    // Build candidate titles to try
    let short_title: Option<String> = title
        .split_once(':')
        .or_else(|| title.split_once(" - "))
        .map(|(before, _)| before.trim().to_string())
        .filter(|s| !s.is_empty() && s != title);

    let mut candidates: Vec<&str> = vec![title];
    if let Some(ref s) = short_title {
        candidates.push(s.as_str());
    }

    for candidate in candidates {
        let lang_param = language
            .map(|l| format!("&language={}", urlencoding::encode(l)))
            .unwrap_or_default();
        let url = format!(
            "{}/search.json?title={}&author={}&fields=key,title,author_name,language,series,series_number,series_statement&limit=1{}",
            base_url,
            urlencoding::encode(candidate),
            urlencoding::encode(authors),
            lang_param
        );

        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("HTTP request failed: {url}"))?;

        if !resp.status().is_success() {
            continue;
        }

        let search: OlSearchEnrichResponse = resp
            .json()
            .await
            .context("OpenLibrary search enrich response is not valid JSON")?;

        let doc = match search.docs.into_iter().next() {
            Some(d) => d,
            None => continue,
        };

        let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Normalize author names from the search result
        let enriched_authors = doc.author_name.map(|names| {
            names
                .iter()
                .map(|n| crate::author::normalize_author(n))
                .collect::<Vec<_>>()
                .join(", ")
        });

        // Language: prefer the filtered language if it appears in the list,
        // otherwise take the first element. OL's language list spans all editions
        // so "dut" may come before "eng" even for an English query.
        let language = doc.language.and_then(|langs| {
            if let Some(pref) = language {
                if langs.iter().any(|l| l == pref) {
                    return Some(pref.to_string());
                }
            }
            langs.into_iter().next()
        });

        // Series name: first element of series array, or parse from series_statement
        let series_name = doc.series.and_then(|v| v.into_iter().next()).or_else(|| {
            doc.series_statement
                .as_deref()
                .and_then(|s| s.split(" ; ").next())
                .map(|s| s.to_string())
        });

        // Series position: series_number, or second part of series_statement
        let series_position = doc.series_number.or_else(|| {
            doc.series_statement
                .as_deref()
                .and_then(|s| s.split(" ; ").nth(1))
                .map(|s| s.trim().to_string())
        });

        let update = EnrichmentUpdate {
            title: doc.title,
            authors: enriched_authors,
            language,
            series_name,
            series_position,
            ol_work_id: doc.key.clone(),
            enrichment_attempted: 1,
            enriched_at: Some(now),
            ..Default::default()
        };

        return Ok(Some(update));
    }

    Ok(None)
}

/// Heuristic: does this description text appear to be English?
///
/// Rejects:
/// - Non-Latin-script text (Cyrillic, Greek, CJK, etc.)
/// - Heavy diacritic use typical of Romance/other European languages
///   (Portuguese ã, Spanish ñ, French ç/œ, etc.) — more than 4% of
///   non-whitespace characters carrying diacritics is a strong signal
///   that the text is not English.
fn description_looks_english(text: &str) -> bool {
    if text.is_empty() { return false; }
    let non_ws: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    let total = non_ws.len();
    if total == 0 { return false; }

    // Reject non-Latin script (Cyrillic, Greek, CJK, Arabic, etc.)
    let non_latin = non_ws.iter().filter(|&&c| {
        !c.is_ascii()
            && !matches!(c, '\u{00C0}'..='\u{024F}')
            && !c.is_ascii_punctuation()
    }).count();
    if non_latin * 100 / total >= 10 { return false; }

    // Reject heavy diacritic use (Romance languages)
    // Diacritics: À-ÿ range excluding the Latin-1 supplement misc symbols
    let diacritics = non_ws.iter().filter(|&&c| {
        matches!(c,
            '\u{00C0}'..='\u{00D6}' | '\u{00D8}'..='\u{00F6}' | '\u{00F8}'..='\u{00FF}'
        )
    }).count();
    // More than 2% diacritics → likely not English
    diacritics * 100 / total < 2
}

/// Search OL for a work by title+author and return its description.
///
/// Uses `search.json` to find the work key, then fetches the Work record.
/// Pass `language` (e.g. `Some("eng")`) to bias results toward a language.
/// Returns `None` if no match or the work has no description.
pub async fn fetch_description_by_title_author(
    client: &reqwest::Client,
    title: &str,
    author: &str,
    base_url: &str,
    language: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let lang_param = language
        .map(|l| format!("&language={}", urlencoding::encode(l)))
        .unwrap_or_default();
    let url = format!(
        "{}/search.json?title={}&author={}&fields=key&limit=1{}",
        base_url,
        urlencoding::encode(title),
        urlencoding::encode(author),
        lang_param,
    );

    let resp = client.get(&url).send().await
        .with_context(|| format!("HTTP request failed: {url}"))?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    #[derive(Deserialize)]
    struct KeyDoc { key: Option<String> }
    #[derive(Deserialize)]
    struct KeyResp { docs: Vec<KeyDoc> }

    let resp_body: KeyResp = match resp.json().await {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };

    let work_key = match resp_body.docs.into_iter().next().and_then(|d| d.key) {
        Some(k) => k,
        None => return Ok(None),
    };

    let desc = fetch_ol_work_description(client, &work_key, base_url).await?;
    Ok(desc.filter(|d| description_looks_english(d)))
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
