//! Bibliographic providers behind one typed query interface. Google Books keeps
//! its own client; OpenLibrary is keyless and is mapped onto the same `Volume`
//! model so the matcher's corroboration rules apply identically to both.

use crate::google::{Google, Identifier, ImageLinks, SaleInfo, Volume, VolumeInfo};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;

/// Live HTTP requests made by this process (cache hits are not counted).
pub static REQUESTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

const OL_SEARCH: &str = "https://openlibrary.org/search.json";
const OL_FIELDS: &str = "key,title,subtitle,author_name,author_alternative_name,first_publish_year,language,cover_i,ebook_access,number_of_pages_median";
const MAX_RESPONSE: usize = 8 * 1024 * 1024;
const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// What the matcher asks a provider for. Providers translate; callers never
/// build provider-specific query strings.
#[derive(Clone, Copy, Debug)]
pub enum Query<'a> {
    TitleAuthor {
        title: &'a str,
        author: &'a str,
    },
    Isbn(&'a str),
    /// A passage of the book's own text, for a full-text index.
    Passage(&'a str),
    /// The second-attempt query. `lang` (ISO 639-1) restricts results to the book's
    /// own language, so editions in other languages cannot crowd it out.
    LooseTitle {
        title: &'a str,
        author: &'a str,
        lang: Option<&'a str>,
    },
}

impl Query<'_> {
    /// Google's field operators (intitle:, inauthor:) currently return nothing for
    /// any book, so a lookup is plain words, then the same words with the title
    /// as an exact phrase. An ISBN keeps its `isbn:` form (only sent when enabled).
    fn google(&self) -> String {
        use crate::matching::query_text;
        match self {
            Query::TitleAuthor { title, author } => {
                format!("{} {}", query_text(title), query_text(author))
            }
            Query::Isbn(isbn) => format!("isbn:{isbn}"),
            Query::Passage(text) => format!("\"{}\"", crate::matching::query_text(text)),
            Query::LooseTitle { title, author, .. } => {
                format!("\"{}\" {}", query_text(title), query_text(author))
            }
        }
    }
}

#[derive(Clone)]
pub enum Provider {
    Google(Google),
    OpenLibrary(OpenLibrary),
}

impl Provider {
    pub fn name(&self) -> &'static str {
        match self {
            Provider::Google(_) => "google",
            Provider::OpenLibrary(_) => "openlibrary",
        }
    }

    pub async fn search(&self, query: Query<'_>) -> Result<Vec<Volume>> {
        self.search_page(query, 0).await
    }

    pub async fn search_page(&self, query: Query<'_>, start: u32) -> Result<Vec<Volume>> {
        match self {
            Provider::Google(g) => {
                if matches!(query, Query::Isbn(_)) && !g.isbn_search() {
                    return Ok(Vec::new());
                }
                let lang = match query {
                    Query::LooseTitle { lang, .. } => lang,
                    _ => None,
                };
                g.search_page_lang(&query.google(), start, lang).await
            }
            Provider::OpenLibrary(o) => o.search_page(query, start).await,
        }
    }

    pub async fn volume(&self, id: &str) -> Result<Volume> {
        match self {
            Provider::Google(g) => g.volume(id).await,
            Provider::OpenLibrary(o) => o.volume(id).await,
        }
    }

    pub async fn cover(&self, url: &str) -> Result<crate::cover::Signature> {
        match self {
            Provider::Google(g) => g.cover(url).await,
            Provider::OpenLibrary(o) => o.cover(url).await,
        }
    }

    pub async fn cover_bytes(&self, url: &str) -> Result<Vec<u8>> {
        match self {
            Provider::Google(g) => g.cover_bytes(url).await,
            Provider::OpenLibrary(o) => o.cover_bytes(url).await,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OlResponse {
    #[serde(default)]
    docs: Vec<OlDoc>,
}

#[derive(Debug, Deserialize)]
struct OlDoc {
    key: String,
    title: Option<String>,
    subtitle: Option<String>,
    #[serde(default)]
    author_name: Vec<String>,
    #[serde(default)]
    author_alternative_name: Vec<String>,
    first_publish_year: Option<i32>,
    #[serde(default)]
    language: Vec<String>,
    cover_i: Option<u64>,
    ebook_access: Option<String>,
    number_of_pages_median: Option<u32>,
}

#[derive(Clone)]
pub struct OpenLibrary {
    client: Client,
    cache: PathBuf,
    offline: bool,
    refresh: bool,
    next_request: Arc<Mutex<tokio::time::Instant>>,
    interval: Duration,
    known: Arc<Mutex<HashMap<String, Volume>>>,
}

fn approved_ol_host(host: &str) -> bool {
    host == "covers.openlibrary.org" || host == "archive.org" || host.ends_with(".archive.org")
}

impl OpenLibrary {
    pub fn new(cache: PathBuf, offline: bool, refresh: bool, rps: u32) -> Result<Self> {
        ensure!(rps > 0, "Request rate must be positive");
        let redirects = reqwest::redirect::Policy::custom(|attempt| {
            let allowed = attempt.url().scheme() == "https"
                && attempt.url().host_str().is_some_and(approved_ol_host)
                && attempt.previous().len() < 3;
            if allowed {
                attempt.follow()
            } else {
                attempt.stop()
            }
        });
        Ok(Self {
            client: Client::builder()
                .user_agent(concat!(
                    "book-manager/",
                    env!("CARGO_PKG_VERSION"),
                    " (local EPUB library tool)"
                ))
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .redirect(redirects)
                .build()?,
            cache,
            offline,
            refresh,
            next_request: Arc::new(Mutex::new(tokio::time::Instant::now())),
            interval: Duration::from_secs_f64(1.0 / f64::from(rps)),
            known: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn search_page(&self, query: Query<'_>, start: u32) -> Result<Vec<Volume>> {
        ensure!(
            matches!(start, 0 | 40),
            "Search pagination is limited to two pages"
        );
        let mut params: Vec<(&str, String)> = match query {
            Query::Passage(text) => return self.passage_search(text).await,
            Query::TitleAuthor { title, author } => {
                vec![("title", title.to_owned()), ("author", author.to_owned())]
            }
            Query::Isbn(isbn) => vec![("isbn", isbn.to_owned())],
            Query::LooseTitle {
                title,
                author,
                lang,
            } => {
                let mut params = vec![("q", format!("{title} {author}"))];
                if let Some(three) = lang
                    .and_then(isolang::Language::from_639_1)
                    .map(|l| l.to_639_3())
                {
                    params.push(("language", three.to_owned()));
                }
                params
            }
        };
        params.push(("fields", OL_FIELDS.to_owned()));
        params.push(("limit", "40".to_owned()));
        params.push(("offset", start.to_string()));
        let key = format!("ol-v4\n{params:?}");
        let name = format!("{:x}", Sha256::digest(key));
        // Sharded by the first two hex digits: a large library makes hundreds of
        // thousands of entries, far too many for one folder.
        let path = self.cache.join(&name[..2]).join(format!("{name}.json"));
        let data = match self.read(&path, MAX_RESPONSE).await? {
            Some(data) => data,
            None => {
                ensure!(
                    !self.offline,
                    "No fresh cached OpenLibrary result (offline mode)"
                );
                let request = self.client.get(OL_SEARCH).query(&params);
                let data = self.fetch(request, MAX_RESPONSE).await?;
                let (p, d) = (path.clone(), data.clone());
                tokio::task::spawn_blocking(move || write_cache(&p, &d)).await??;
                data
            }
        };
        let response: OlResponse =
            serde_json::from_slice(&data).context("Malformed OpenLibrary response")?;
        let isbn = match query {
            Query::Isbn(isbn) => Some(isbn),
            _ => None,
        };
        let volumes: Vec<Volume> = response
            .docs
            .into_iter()
            .filter_map(|doc| volume_from(doc, isbn))
            .collect();
        let mut known = self.known.lock().await;
        for volume in &volumes {
            known.insert(volume.id.clone(), volume.clone());
        }
        Ok(volumes)
    }

    /// Full-text search over Internet Archive scans ("search inside"): editions whose
    /// text contains the passage, as records with a title and authors. No key, no
    /// daily quota; slower and with less coverage than Google's text search.
    async fn passage_search(&self, text: &str) -> Result<Vec<Volume>> {
        let phrase = format!("\"{}\"", crate::matching::query_text(text));
        let name = format!(
            "{:x}",
            Sha256::digest(format!(
                "ol-inside-v1
{phrase}"
            ))
        );
        let path = self.cache.join(&name[..2]).join(format!("{name}.json"));
        let data = match self.read(&path, MAX_RESPONSE).await? {
            Some(data) => data,
            None => {
                ensure!(
                    !self.offline,
                    "No fresh cached OpenLibrary text search (offline mode)"
                );
                let request = self
                    .client
                    .get("https://openlibrary.org/search/inside.json")
                    .query(&[("q", phrase.as_str()), ("limit", "20")]);
                let data = self.fetch(request, MAX_RESPONSE).await?;
                let (p, d) = (path, data.clone());
                tokio::task::spawn_blocking(move || write_cache(&p, &d)).await??;
                data
            }
        };
        let value: serde_json::Value =
            serde_json::from_slice(&data).context("Malformed OpenLibrary text search")?;
        let mut volumes = Vec::new();
        for hit in value["hits"]["hits"].as_array().into_iter().flatten() {
            let edition = &hit["edition"];
            let (Some(key), Some(title)) = (edition["key"].as_str(), edition["title"].as_str())
            else {
                continue;
            };
            // Only the exact shape /books/OL123M is an edition key.
            let Some(id) = key
                .strip_prefix("/books/OL")
                .map(|rest| format!("OL{rest}"))
            else {
                continue;
            };
            if id.len() < 4 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
                continue;
            }
            let authors: Vec<String> = edition["authors"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|a| a["name"].as_str().map(str::to_owned))
                .collect();
            volumes.push(Volume {
                id: format!("inside{id}"),
                info: VolumeInfo {
                    title: title.to_owned(),
                    authors,
                    ..VolumeInfo::default()
                },
                sale_info: SaleInfo::default(),
            });
        }
        Ok(volumes)
    }

    /// OpenLibrary search documents already carry everything the matcher uses,
    /// so detail loading returns the record seen in search.
    pub async fn volume(&self, id: &str) -> Result<Volume> {
        self.known
            .lock()
            .await
            .get(id)
            .cloned()
            .with_context(|| format!("OpenLibrary record {id} was not returned by a search"))
    }

    pub async fn cover(&self, raw: &str) -> Result<crate::cover::Signature> {
        let bytes = self.cover_bytes(raw).await?;
        tokio::task::spawn_blocking(move || crate::cover::signature(&bytes)).await?
    }

    pub async fn cover_bytes(&self, raw: &str) -> Result<Vec<u8>> {
        let url = reqwest::Url::parse(raw).context("Invalid OpenLibrary cover URL")?;
        ensure!(
            url.scheme() == "https"
                && url.host_str() == Some("covers.openlibrary.org")
                && url.username().is_empty()
                && url.password().is_none()
                && url.port().is_none(),
            "Cover URL is not an approved OpenLibrary host"
        );
        let name = format!(
            "{:x}",
            Sha256::digest(format!(
                "ol-cover-v1
{url}"
            ))
        );
        let path = self
            .cache
            .join("covers")
            .join(&name[..2])
            .join(format!("{name}.image"));
        if let Some(bytes) = self.read(&path, crate::cover::MAX_BYTES).await? {
            return Ok(bytes);
        }
        ensure!(
            !self.offline,
            "No fresh cached OpenLibrary cover (offline mode)"
        );
        let bytes = self
            .fetch(self.client.get(url), crate::cover::MAX_BYTES)
            .await?;
        crate::cover::signature(&bytes)?;
        let (p, b) = (path, bytes.clone());
        tokio::task::spawn_blocking(move || write_cache(&p, &b)).await??;
        Ok(bytes)
    }

    async fn read(&self, path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
        if self.refresh {
            return Ok(None);
        }
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || read_cache(&path, limit)).await?
    }

    async fn fetch(&self, request: reqwest::RequestBuilder, max_bytes: usize) -> Result<Vec<u8>> {
        for attempt in 0..3 {
            {
                let mut next = self.next_request.lock().await;
                tokio::time::sleep_until(*next).await;
                *next = tokio::time::Instant::now() + self.interval;
            }
            REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let sent = request
                .try_clone()
                .context("Cannot clone OpenLibrary request")?
                .send()
                .await;
            let response = match sent {
                Ok(response) => response,
                // A timeout or dropped connection is routine over a long run: wait
                // and try again, within the same three-attempt budget.
                Err(error) if (error.is_timeout() || error.is_connect()) && attempt < 2 => {
                    tokio::time::sleep(Duration::from_secs(2 << attempt)).await;
                    continue;
                }
                Err(error) => {
                    return Err(reqwest::Error::without_url(error))
                        .context("OpenLibrary request failed");
                }
            };
            let status = response.status();
            if status.is_success() {
                ensure!(
                    response
                        .content_length()
                        .is_none_or(|n| n <= max_bytes as u64),
                    "OpenLibrary response exceeds size limit"
                );
                let mut response = response;
                let mut data = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(reqwest::Error::without_url)?
                {
                    ensure!(
                        data.len() + chunk.len() <= max_bytes,
                        "OpenLibrary response exceeds size limit"
                    );
                    data.extend_from_slice(&chunk);
                }
                return Ok(data);
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                if attempt == 2 {
                    bail!("OpenLibrary HTTP {status} after 3 attempts; retry later");
                }
                tokio::time::sleep(Duration::from_secs(2 << attempt)).await;
                continue;
            }
            bail!("OpenLibrary HTTP {status}");
        }
        unreachable!("bounded retry loop")
    }
}

fn volume_from(doc: OlDoc, queried_isbn: Option<&str>) -> Option<Volume> {
    let id = doc.key.strip_prefix("/works/")?.to_owned();
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    let title = doc.title.filter(|t| !t.trim().is_empty())?;
    Some(Volume {
        id,
        info: VolumeInfo {
            title,
            subtitle: doc.subtitle,
            authors: doc.author_name,
            author_aliases: doc.author_alternative_name,
            industry_identifiers: queried_isbn
                .map(|isbn| {
                    vec![Identifier {
                        kind: "ISBN_13".into(),
                        identifier: isbn.to_owned(),
                    }]
                })
                .unwrap_or_default(),
            // Many editions list many languages; only a single one is unambiguous.
            language: match doc.language.as_slice() {
                [only] => Some(only.clone()),
                _ => None,
            },
            published_date: doc.first_publish_year.map(|y| y.to_string()),
            page_count: doc.number_of_pages_median,
            publisher: None,
            image_links: ImageLinks {
                thumbnail: doc.cover_i.map(|id| {
                    format!("https://covers.openlibrary.org/b/id/{id}-L.jpg?default=false")
                }),
                ..ImageLinks::default()
            },
        },
        sale_info: SaleInfo {
            is_ebook: doc.ebook_access.map(|a| a != "no_ebook"),
        },
    })
}

pub(crate) fn read_cache(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    let info = match std::fs::metadata(path) {
        Ok(info) => info,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("Cannot inspect OpenLibrary cache"),
    };
    if SystemTime::now().duration_since(info.modified()?)? > CACHE_TTL {
        return Ok(None);
    }
    ensure!(
        info.len() <= limit as u64,
        "Cached response exceeds size limit"
    );
    let mut data = Vec::new();
    std::fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut data)?;
    ensure!(
        data.len() <= limit,
        "Cached response grew beyond size limit"
    );
    Ok(Some(data))
}

pub(crate) fn write_cache(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("Invalid cache path")?;
    std::fs::create_dir_all(parent).context("Cannot create OpenLibrary cache")?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(data)?;
    file.persist(path)
        .context("Cannot commit OpenLibrary cache")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hit that is not an edition record, a hit with a hostile key, and a real one.
    #[tokio::test]
    async fn text_search_hits_become_title_and_author_records_and_junk_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let ol = OpenLibrary::new(dir.path().to_owned(), true, false, 4).unwrap();
        let phrase = "a quiet garden where the children gathered";
        let body = serde_json::json!({
            "hits": {"hits": [
                {"edition": {"key": "/books/OL1M", "title": "A Test Book",
                             "authors": [{"key": "/authors/OL9A", "name": "Jane Example"}]}},
                {"_id": "no edition record here"},
                {"edition": {"key": "/books/../etc", "title": "Hostile", "authors": []}},
                {"edition": {"key": "/books/OL2M", "title": "Study Guide",
                             "authors": [{"name": "SparkNotes"}]}},
            ]}
        });
        let name = format!(
            "{:x}",
            Sha256::digest(format!(
                "ol-inside-v1\n\"{}\"",
                crate::matching::query_text(phrase)
            ))
        );
        let path = dir.path().join(&name[..2]).join(format!("{name}.json"));
        write_cache(&path, body.to_string().as_bytes()).unwrap();
        let found = ol.search_page(Query::Passage(phrase), 0).await.unwrap();
        let titles: Vec<_> = found.iter().map(|v| v.info.title.as_str()).collect();
        assert_eq!(titles, ["A Test Book", "Study Guide"]);
        assert_eq!(found[0].info.authors, ["Jane Example"]);
        assert!(found.iter().all(|v| v.id.starts_with("inside")));
    }
}
