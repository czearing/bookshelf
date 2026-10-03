use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;

const ENDPOINT: &str = "https://www.googleapis.com/books/v1/volumes";
/// Only the fields the matcher reads: about a quarter of the default payload.
const MIN_INTERVAL_SECS: f64 = 0.65;
const VOLUME_FIELDS: &str = "id,saleInfo/isEbook,volumeInfo(title,subtitle,authors,language,publisher,publishedDate,pageCount,industryIdentifiers,imageLinks)";
const SEARCH_FIELDS: &str = "kind,totalItems,items(id,saleInfo/isEbook,volumeInfo(title,subtitle,authors,language,publisher,publishedDate,pageCount,industryIdentifiers,imageLinks))";
const MAX_RESPONSE: usize = 2 * 1024 * 1024;
const MAX_ERROR: usize = 64 * 1024;
const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
type QueryResult = Arc<Mutex<Option<Vec<Volume>>>>;
type CoverResult = Arc<Mutex<Option<crate::cover::Signature>>>;
type DetailResult = Arc<Mutex<Option<Volume>>>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Volume {
    pub id: String,
    #[serde(rename = "volumeInfo")]
    pub info: VolumeInfo,
    #[serde(default, rename = "saleInfo")]
    pub sale_info: SaleInfo,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SaleInfo {
    pub is_ebook: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeInfo {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub subtitle: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    /// Alternate spellings the provider itself records for the authors; used only
    /// to recognise the same person under a different transliteration.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub author_aliases: Vec<String>,
    #[serde(default)]
    pub industry_identifiers: Vec<Identifier>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub published_date: Option<String>,
    /// Printed page count the provider records for the work, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_count: Option<u32>,
    #[serde(default)]
    pub publisher: Option<String>,
    #[serde(default)]
    pub image_links: ImageLinks,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageLinks {
    pub extra_large: Option<String>,
    pub large: Option<String>,
    pub medium: Option<String>,
    pub small: Option<String>,
    pub thumbnail: Option<String>,
    pub small_thumbnail: Option<String>,
}

impl ImageLinks {
    pub fn cover_url(&self) -> Option<&str> {
        [
            &self.thumbnail,
            &self.small,
            &self.medium,
            &self.small_thumbnail,
            &self.large,
            &self.extra_large,
        ]
        .into_iter()
        .filter_map(|url| url.as_deref())
        .find(|url| !url.trim().is_empty())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Identifier {
    #[serde(rename = "type")]
    pub kind: String,
    pub identifier: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResponse {
    kind: String,
    total_items: u64,
    #[serde(default)]
    items: Vec<Volume>,
}

#[derive(Clone)]
pub struct Google {
    client: Client,
    endpoint: String,
    key: Option<String>,
    cache: PathBuf,
    offline: bool,
    refresh: bool,
    next_request: Arc<Mutex<tokio::time::Instant>>,
    interval: Duration,
    queries: Arc<Mutex<HashMap<String, QueryResult>>>,
    covers: Arc<Mutex<HashMap<String, CoverResult>>>,
    quota_failure: Arc<Mutex<Option<String>>>,
    details: Arc<Mutex<HashMap<String, DetailResult>>>,
    /// Whether `isbn:` queries are sent. Google's field operators currently return
    /// nothing, so by default they are not (and no quota is spent on them).
    isbn_search: bool,
    /// Fetch a separate record per candidate. A search already returns the same
    /// fields, so by default the search result is reused and no request is spent.
    detail_requests: bool,
    /// Width asked for when downloading a cover. Google returns the original when it
    /// is narrower, never an upscaled copy.
    cover_width: u32,
    seen: Arc<Mutex<HashMap<String, Volume>>>,
}

impl Google {
    pub fn new(cache: PathBuf, offline: bool, refresh: bool, rps: u32) -> Result<Self> {
        ensure!(rps > 0, "Request rate must be positive");
        Ok(Self {
            client: Client::builder()
                .user_agent(concat!("book-manager/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: ENDPOINT.to_owned(),
            key: std::env::var("GOOGLE_BOOKS_API_KEY")
                .ok()
                .filter(|key| !key.trim().is_empty()),
            cache,
            offline,
            refresh,
            next_request: Arc::new(Mutex::new(tokio::time::Instant::now())),
            // Books API quota is 100 queries per minute per user, so never faster
            // than one request every 0.65 s whatever --requests-per-second says.
            interval: Duration::from_secs_f64((1.0 / f64::from(rps)).max(MIN_INTERVAL_SECS)),
            queries: Arc::new(Mutex::new(HashMap::new())),
            covers: Arc::new(Mutex::new(HashMap::new())),
            quota_failure: Arc::new(Mutex::new(None)),
            details: Arc::new(Mutex::new(HashMap::new())),
            isbn_search: false,
            detail_requests: false,
            cover_width: 1200,
            seen: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn with_cover_width(mut self, width: u32) -> Self {
        self.cover_width = width.clamp(200, 2500);
        self
    }

    pub fn with_detail_requests(mut self, enabled: bool) -> Self {
        self.detail_requests = enabled;
        self
    }

    pub fn with_isbn_search(mut self, enabled: bool) -> Self {
        self.isbn_search = enabled;
        self
    }

    pub fn isbn_search(&self) -> bool {
        self.isbn_search
    }

    pub async fn search(&self, query: &str) -> Result<Vec<Volume>> {
        self.search_page(query, 0).await
    }

    pub async fn search_page(&self, query: &str, start: u32) -> Result<Vec<Volume>> {
        self.search_page_lang(query, start, None).await
    }

    /// A search page, optionally restricted to one language (ISO 639-1).
    pub async fn search_page_lang(
        &self,
        query: &str,
        start: u32,
        lang: Option<&str>,
    ) -> Result<Vec<Volume>> {
        ensure!(
            matches!(start, 0 | 40),
            "Search pagination is limited to two pages"
        );
        let name = match lang {
            Some(lang) => page_cache_name_lang(query, lang, start),
            None => page_cache_name(query, start),
        };
        let lock = self
            .queries
            .lock()
            .await
            .entry(name.clone())
            .or_default()
            .clone();
        let mut cached = lock.lock().await;
        if let Some(items) = cached.as_ref() {
            return Ok(items.clone());
        }
        let path = self.cache.join(name);
        if !self.refresh {
            let read_path = path.clone();
            if let Some(data) =
                tokio::task::spawn_blocking(move || read_cache(&read_path)).await??
            {
                let items =
                    parse(&data).context("Invalid cache entry; use --refresh to replace it")?;
                *cached = Some(items.clone());
                self.remember(&items).await;
                return Ok(items);
            }
        }
        ensure!(
            !self.offline,
            "No fresh cached Google Books result (offline mode)"
        );

        let start = start.to_string();
        let mut request = self.client.get(&self.endpoint).query(&[
            ("q", query),
            ("printType", "books"),
            ("fields", SEARCH_FIELDS),
            ("maxResults", "40"),
            ("startIndex", start.as_str()),
        ]);
        if let Some(lang) = lang {
            request = request.query(&[("langRestrict", lang)]);
        }
        if let Some(key) = &self.key {
            request = request.query(&[("key", key)]);
        }
        let data = self.fetch(request, MAX_RESPONSE).await?;
        let items = parse(&data)?;
        tokio::task::spawn_blocking(move || write_cache(&path, &data)).await??;
        *cached = Some(items.clone());
        self.remember(&items).await;
        Ok(items)
    }

    async fn remember(&self, items: &[Volume]) {
        let mut seen = self.seen.lock().await;
        for item in items {
            seen.insert(item.id.clone(), item.clone());
        }
    }

    pub async fn volume(&self, id: &str) -> Result<Volume> {
        if !self.detail_requests
            && let Some(volume) = self.seen.lock().await.get(id)
        {
            return Ok(volume.clone());
        }
        ensure!(
            !id.is_empty()
                && id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c)),
            "Invalid Google volume ID"
        );
        let name = detail_cache_name(id);
        let lock = self
            .details
            .lock()
            .await
            .entry(name.clone())
            .or_default()
            .clone();
        let mut cached = lock.lock().await;
        if let Some(volume) = cached.as_ref() {
            return Ok(volume.clone());
        }
        let path = self.cache.join("details").join(name);
        if !self.refresh {
            let read_path = path.clone();
            if let Some(data) =
                tokio::task::spawn_blocking(move || read_cache(&read_path)).await??
            {
                let volume = parse_volume(&data, id)?;
                *cached = Some(volume.clone());
                return Ok(volume);
            }
        }
        ensure!(
            !self.offline,
            "No fresh cached Google volume details (offline mode)"
        );
        let mut request = self
            .client
            .get(format!("{}/{id}", self.endpoint))
            .query(&[("fields", VOLUME_FIELDS)]);
        if let Some(key) = &self.key {
            request = request.query(&[("key", key)]);
        }
        let data = self.fetch(request, MAX_RESPONSE).await?;
        let volume = parse_volume(&data, id)?;
        tokio::task::spawn_blocking(move || write_cache(&path, &data)).await??;
        *cached = Some(volume.clone());
        Ok(volume)
    }

    pub async fn cover(&self, raw_url: &str) -> Result<crate::cover::Signature> {
        let url = Self::cover_url(raw_url)?;
        let name = Self::cover_cache_name(url.as_str());
        let lock = self
            .covers
            .lock()
            .await
            .entry(name.clone())
            .or_default()
            .clone();
        let mut cached = lock.lock().await;
        if let Some(signature) = cached.as_ref() {
            return Ok(signature.clone());
        }
        let path = self.cache.join("covers").join(name);
        if !self.refresh {
            let read_path = path.clone();
            let bytes = tokio::task::spawn_blocking(move || {
                read_cached_bytes(&read_path, crate::cover::MAX_BYTES)
            })
            .await??;
            if let Some(bytes) = bytes {
                let signature =
                    tokio::task::spawn_blocking(move || crate::cover::signature(&bytes))
                        .await?
                        .context("Invalid cached Google cover; use --refresh to replace it")?;
                *cached = Some(signature.clone());
                return Ok(signature);
            }
        }
        ensure!(!self.offline, "No fresh cached Google cover (offline mode)");
        let bytes = self
            .fetch(self.client.get(url), crate::cover::MAX_BYTES)
            .await?;
        let signature = tokio::task::spawn_blocking(move || {
            let signature = crate::cover::signature(&bytes)?;
            write_cache(&path, &bytes)?;
            Ok::<_, anyhow::Error>(signature)
        })
        .await??;
        *cached = Some(signature.clone());
        Ok(signature)
    }

    /// Raw cover bytes (validated as a decodable image), cached like `cover`.
    pub async fn cover_bytes(&self, raw_url: &str) -> Result<Vec<u8>> {
        // Search results link a 128 pixel thumbnail with a page-curl effect; the
        // same image id is served clean and large (575 wide) at zoom 3.
        let url = high_resolution(&Self::cover_url(raw_url)?, self.cover_width);
        let path = self
            .cache
            .join("covers")
            .join(Self::cover_cache_name(url.as_str()));
        if !self.refresh {
            let read_path = path.clone();
            if let Some(bytes) = tokio::task::spawn_blocking(move || {
                read_cached_bytes(&read_path, crate::cover::MAX_BYTES)
            })
            .await??
            {
                return Ok(bytes);
            }
        }
        ensure!(!self.offline, "No fresh cached Google cover (offline mode)");
        let bytes = self
            .fetch(self.client.get(url), crate::cover::MAX_BYTES)
            .await?;
        ensure!(
            !is_placeholder(&bytes),
            "Google has no cover for this book (it served its image-not-available picture)"
        );
        tokio::task::spawn_blocking(move || {
            crate::cover::signature(&bytes)?;
            write_cache(&path, &bytes)?;
            Ok::<_, anyhow::Error>(bytes)
        })
        .await?
    }

    async fn fetch(&self, request: reqwest::RequestBuilder, max_bytes: usize) -> Result<Vec<u8>> {
        for attempt in 0..5 {
            self.check_quota().await?;
            self.throttle().await;
            self.check_quota().await?;
            crate::provider::REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let sent = request
                .try_clone()
                .context("Cannot clone Google request")?
                .send()
                .await;
            let response = match sent {
                Ok(response) => response,
                Err(error) if (error.is_timeout() || error.is_connect()) && attempt < 4 => {
                    tokio::time::sleep(Duration::from_secs(2 << attempt)).await;
                    continue;
                }
                Err(error) => {
                    return Err(reqwest::Error::without_url(error))
                        .context("Google Books request failed");
                }
            };
            let status = response.status();
            if status.is_success() {
                return read_response(response, max_bytes).await;
            }
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let error_body = read_response(response, MAX_ERROR)
                .await
                .with_context(|| format!("Cannot read Google Books HTTP {status} error"))?;
            let failure = classify_failure(&error_body);
            if matches!(
                status,
                StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
            ) && let ProviderFailure::DailyQuota { limit } = failure
            {
                let access = if self.key.is_some() {
                    "configured-key"
                } else {
                    "keyless"
                };
                let limit = limit
                    .map(|value| format!("; reported daily limit: {value}"))
                    .unwrap_or_default();
                let message = format!(
                    "Google Books HTTP {status}: daily query quota unavailable for this {access} request{limit}. \
                     This is a quota rejection, not a missing-key error. \
                     Further live Google requests are stopped for this run; retry after provider quota is available."
                );
                *self.quota_failure.lock().await = Some(message.clone());
                bail!("{message}");
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                if attempt == 4 {
                    bail!("Google Books HTTP {status} after 5 attempts; retry later");
                }
                let delay = retry_after
                    .as_deref()
                    .map(parse_retry_after)
                    .transpose()?
                    // The per-minute quota resets within a minute: wait it out, and
                    // make every worker wait with us.
                    .unwrap_or(if status == StatusCode::TOO_MANY_REQUESTS {
                        15 * (attempt as u64 + 1)
                    } else {
                        1 << attempt
                    });
                ensure!(
                    delay <= 60,
                    "Google Books requested a long retry delay; retry later"
                );
                let mut next = self.next_request.lock().await;
                *next = (*next).max(tokio::time::Instant::now() + Duration::from_secs(delay));
                continue;
            }
            let reason = match failure {
                ProviderFailure::InvalidKey => "Google explicitly rejected the supplied API key",
                ProviderFailure::Unstructured => {
                    "provider supplied an empty or non-JSON error response"
                }
                _ => {
                    "request rejected by the provider; no specific credential failure was identified"
                }
            };
            bail!("Google Books HTTP {status}: {reason}");
        }
        unreachable!("Every request attempt returns or retries within a bounded loop")
    }

    async fn check_quota(&self) -> Result<()> {
        if let Some(message) = self.quota_failure.lock().await.as_ref() {
            bail!("{message}");
        }
        Ok(())
    }

    fn cover_url(raw: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(raw).context("Invalid Google cover URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && matches!(
                    url.host_str(),
                    Some("books.google.com" | "books.googleusercontent.com")
                )
                && url.username().is_empty()
                && url.password().is_none()
                && url.port().is_none(),
            "Cover URL is not an approved Google Books host"
        );
        url.set_scheme("https")
            .map_err(|()| anyhow::anyhow!("Cannot secure Google cover URL"))?;
        url.set_fragment(None);
        Ok(url)
    }

    pub fn cover_cache_name(url: &str) -> String {
        format!("{:x}.image", Sha256::digest(format!("cover-v1\n{url}")))
    }

    async fn throttle(&self) {
        let mut next = self.next_request.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = tokio::time::Instant::now() + self.interval;
    }
}

/// Google answers a request for a missing cover with HTTP 200 and a grey
/// "image not available" picture, identical for every book. These are the SHA-256
/// digests of that picture at zoom levels 1 to 5.
const PLACEHOLDER_COVERS: [&str; 3] = [
    "e3f8c414b288cbdf4e6d1e00eb6d3826157d10a5b5628b9318f726ea490eca12",
    "3efa8c43e5b4348f303a528c81adf435f0111ea752fe9f0f6241478b60987fa6",
    "3abd7c366bd89e9e5e2fb472424b1245aacc948a34a751abca63381067628585",
];

fn is_placeholder(bytes: &[u8]) -> bool {
    let digest = format!("{:x}", Sha256::digest(bytes));
    PLACEHOLDER_COVERS.contains(&digest.as_str())
}

/// The same cover image without the page-curl effect, at `width` pixels or its
/// original size if that is smaller (`fife` serves the original, never an upscale).
fn high_resolution(url: &reqwest::Url, width: u32) -> reqwest::Url {
    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| key != "zoom" && key != "edge" && key != "fife")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let mut out = url.clone();
    out.query_pairs_mut()
        .clear()
        .extend_pairs(kept)
        .append_pair("fife", &format!("w{width}"));
    out
}

#[derive(Debug, PartialEq, Eq)]
enum ProviderFailure {
    DailyQuota { limit: Option<u64> },
    InvalidKey,
    Other,
    Unstructured,
}

fn classify_failure(data: &[u8]) -> ProviderFailure {
    let Ok(body) = serde_json::from_slice::<serde_json::Value>(data) else {
        return ProviderFailure::Unstructured;
    };
    let error = &body["error"];
    if let Some(details) = error["details"].as_array() {
        for detail in details {
            let metadata = &detail["metadata"];
            let daily = metadata["quota_unit"]
                .as_str()
                .is_some_and(|unit| unit.starts_with("1/d/"))
                || metadata["quota_limit"]
                    .as_str()
                    .is_some_and(|name| name.to_lowercase().contains("perday"));
            if daily && detail["reason"] == "RATE_LIMIT_EXCEEDED" {
                return ProviderFailure::DailyQuota {
                    limit: metadata["quota_limit_value"]
                        .as_str()
                        .and_then(|value| value.parse().ok()),
                };
            }
            if detail["reason"] == "API_KEY_INVALID" {
                return ProviderFailure::InvalidKey;
            }
        }
    }
    if let Some(errors) = error["errors"].as_array() {
        if errors.iter().any(|entry| {
            matches!(
                entry["reason"].as_str(),
                Some("dailyLimitExceeded" | "dailyLimitExceededUnreg")
            )
        }) {
            return ProviderFailure::DailyQuota { limit: None };
        }
        if errors.iter().any(|entry| entry["reason"] == "keyInvalid") {
            return ProviderFailure::InvalidKey;
        }
    }
    ProviderFailure::Other
}

async fn read_response(mut response: reqwest::Response, max_bytes: usize) -> Result<Vec<u8>> {
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= max_bytes as u64),
        "Google Books response exceeds size limit"
    );
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        ensure!(
            data.len() + chunk.len() <= max_bytes,
            "Google Books response exceeds size limit"
        );
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

fn parse_retry_after(value: &str) -> Result<u64> {
    value
        .parse()
        .context("Google Books requested a dated retry; retry later")
}

fn parse(data: &[u8]) -> Result<Vec<Volume>> {
    let response: SearchResponse =
        serde_json::from_slice(data).context("Malformed Google Books response")?;
    ensure!(
        response.kind == "books#volumes",
        "Unexpected Google Books response type"
    );
    ensure!(
        response.total_items == 0 || !response.items.is_empty(),
        "Google Books returned a count without records"
    );
    // An item without an id or a title cannot be matched; drop it rather than
    // losing the whole answer.
    Ok(response
        .items
        .into_iter()
        .filter(|v| !v.id.trim().is_empty() && !v.info.title.trim().is_empty())
        .collect())
}

pub fn cache_name(query: &str) -> String {
    page_cache_name(query, 0)
}

pub fn page_cache_name(query: &str, start: u32) -> String {
    format!("{:x}.json", Sha256::digest(format!("v2\n{query}\n{start}")))
}

pub fn page_cache_name_lang(query: &str, lang: &str, start: u32) -> String {
    format!(
        "{:x}.json",
        Sha256::digest(format!(
            "v2
{query}
lang={lang}
{start}"
        ))
    )
}

pub fn detail_cache_name(id: &str) -> String {
    format!("{:x}.json", Sha256::digest(format!("volume-v1\n{id}")))
}

fn parse_volume(data: &[u8], id: &str) -> Result<Volume> {
    let volume: Volume = serde_json::from_slice(data).context("Malformed Google volume details")?;
    ensure!(
        volume.id == id && !volume.info.title.trim().is_empty(),
        "Google volume details do not match the requested identity"
    );
    Ok(volume)
}

fn read_cache(path: &Path) -> Result<Option<Vec<u8>>> {
    read_cached_bytes(path, MAX_RESPONSE)
}

fn read_cached_bytes(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    use std::io::Read;
    let info = match std::fs::metadata(path) {
        Ok(info) => info,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("Cannot inspect Google Books cache"),
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

fn write_cache(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("Invalid cache path")?;
    std::fs::create_dir_all(parent).context("Cannot create Google Books cache")?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(data)?;
    file.persist(path)
        .context("Cannot commit Google Books cache")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn covers_are_requested_large_and_clean_and_the_missing_cover_picture_is_recognised() {
        let thumb = reqwest::Url::parse("https://books.google.com/books/content?id=AQk_EAAAQBAJ&printsec=frontcover&img=1&zoom=1&edge=curl&source=gbs_api").unwrap();
        let big = high_resolution(&thumb, 1200);
        let query: std::collections::BTreeMap<_, _> = big.query_pairs().into_owned().collect();
        assert_eq!(query["fife"], "w1200");
        assert!(!query.contains_key("zoom"));
        assert_eq!(query["id"], "AQk_EAAAQBAJ");
        assert!(!query.contains_key("edge"), "no page curl");
        assert!(!is_placeholder(b"a real jpeg"));
        assert!(PLACEHOLDER_COVERS.iter().all(|d| d.len() == 64));
    }

    #[tokio::test]
    async fn http_query_then_offline_cache() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/volumes", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let server_hits = hits.clone();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buffer = [0; 8192];
            let count = socket.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..count]);
            assert!(request.contains("q=isbn%3A9780306406157"));
            assert!(request.contains("printType=books"));
            assert!(request.contains("maxResults=40"));
            assert!(request.contains("startIndex=0"));
            assert!(request.contains("key=test-key"));
            server_hits.fetch_add(1, Ordering::Relaxed);
            let body = r#"{"kind":"books#volumes","totalItems":1,"items":[{"id":"test","volumeInfo":{"title":"Test","authors":["Author"]}}]}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 4).unwrap();
        google.endpoint = endpoint;
        google.key = Some("test-key".to_owned());
        let (first, second) = tokio::join!(
            google.search("isbn:9780306406157"),
            google.search("isbn:9780306406157")
        );
        assert_eq!(first.unwrap()[0].id, "test");
        assert_eq!(second.unwrap()[0].id, "test");
        google.offline = true;
        assert_eq!(
            google.search("isbn:9780306406157").await.unwrap()[0].id,
            "test"
        );
        assert!(google.search("missing").await.is_err());
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        server.join().unwrap();
    }

    #[test]
    fn invalid_provider_payloads_are_not_no_matches() {
        for body in [
            r#"{}"#,
            r#"{"error":{}}"#,
            r#"{"kind":"books#volumes","totalItems":1}"#,
        ] {
            assert!(parse(body.as_bytes()).is_err());
        }
        assert!(
            parse(br#"{"kind":"books#volumes","totalItems":0}"#)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn full_details_are_cached_on_disk_and_mismatched_ids_are_rejected() {
        let good = r#"{"id":"expected","volumeInfo":{"title":"A Book","language":"en"},"saleInfo":{"isEbook":true}}"#;
        let wrong = r#"{"id":"other","volumeInfo":{"title":"Wrong Record"}}"#;
        let (endpoint, thread) = server(
            [good, wrong]
                .iter()
                .map(|body| {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                })
                .collect(),
        );
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.endpoint = endpoint;
        google.key = None;
        let found = google.volume("expected").await.unwrap();
        assert_eq!(found.id, "expected");
        assert_eq!(found.sale_info.is_ebook, Some(true));
        assert!(google.volume("requested").await.is_err());
        assert!(
            !directory
                .path()
                .join("details")
                .join(detail_cache_name("requested"))
                .exists()
        );
        thread.join().unwrap();
        let offline = Google::new(directory.path().to_owned(), true, false, 20).unwrap();
        assert_eq!(offline.volume("expected").await.unwrap().id, "expected");
        assert!(offline.volume("requested").await.is_err());
        assert!(offline.volume("../expected").await.is_err());
    }

    fn server(responses: Vec<String>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/volumes", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            for response in responses {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "Expected HTTP request"
                            );
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("{error}"),
                    }
                };
                // On Windows an accepted socket inherits the listener's non-blocking mode.
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0; 1024];
                    let size = socket.read(&mut chunk).unwrap();
                    assert!(size > 0, "Client closed before sending headers");
                    request.extend_from_slice(&chunk[..size]);
                    if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                }
                socket.write_all(response.as_bytes()).unwrap();
            }
        });
        (endpoint, thread)
    }

    #[tokio::test]
    async fn rate_limits_retry_and_refresh_deduplicates() {
        let body = r#"{"kind":"books#volumes","totalItems":0}"#;
        let (endpoint, thread) = server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, true, 20).unwrap();
        google.endpoint = endpoint;
        google.key = None;
        assert!(google.search("test").await.unwrap().is_empty());
        thread.join().unwrap();
        assert!(google.search("test").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn forbidden_is_an_error_and_never_cached() {
        let (endpoint, thread) = server(vec![
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.endpoint = endpoint;
        google.key = Some("must-not-leak".into());
        let message = format!("{:#}", google.search("test").await.unwrap_err());
        assert!(message.contains("403"));
        assert!(!message.contains("must-not-leak"));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        thread.join().unwrap();
    }

    #[tokio::test]
    async fn public_search_sends_no_key_or_authorization_when_unconfigured() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/volumes", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 8192];
            let size = socket.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..size]).to_lowercase();
            assert!(request.contains("q=alice"));
            assert!(!request.contains("key="));
            assert!(!request.contains("authorization:"));
            let body = r#"{"kind":"books#volumes","totalItems":0}"#;
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.key = None;
        google.endpoint = endpoint;
        assert!(google.search("alice").await.unwrap().is_empty());
        thread.join().unwrap();
    }

    #[tokio::test]
    async fn zero_daily_quota_stops_retries_and_other_live_queries() {
        let body = r#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{
            "@type":"type.googleapis.com/google.rpc.ErrorInfo",
            "reason":"RATE_LIMIT_EXCEEDED",
            "metadata":{"quota_unit":"1/d/{project}","quota_limit":"defaultPerDayPerProject","quota_limit_value":"0"}
        }]}}"#;
        let (endpoint, thread) = server(vec![format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )]);
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.key = None;
        google.endpoint = endpoint;
        let first = google.search("first").await.unwrap_err().to_string();
        assert!(first.contains("reported daily limit: 0"), "{first}");
        assert!(first.contains("keyless"));
        assert!(first.contains("not a missing-key error"));
        thread.join().unwrap();
        let second = google
            .search("different query")
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            first, second,
            "A blocked run must not make another network request"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let cached_path = directory.path().join(cache_name("cached"));
        write_cache(&cached_path, br#"{"kind":"books#volumes","totalItems":0}"#).unwrap();
        assert!(google.search("cached").await.unwrap().is_empty());
    }

    #[test]
    fn provider_errors_distinguish_quota_from_credentials() {
        assert_eq!(
            classify_failure(br#"{"error":{"errors":[{"reason":"dailyLimitExceeded"}]}}"#),
            ProviderFailure::DailyQuota { limit: None }
        );
        assert_eq!(
            classify_failure(br#"{"error":{"details":[{"reason":"API_KEY_INVALID"}]}}"#),
            ProviderFailure::InvalidKey
        );
        assert_eq!(
            classify_failure(br#"{"error":{"details":[{"reason":"RATE_LIMIT_EXCEEDED","metadata":{"quota_unit":"1/min/{project}","quota_limit_value":"100"}}]}}"#),
            ProviderFailure::Other
        );
        assert_eq!(
            classify_failure(b"<html>unavailable</html>"),
            ProviderFailure::Unstructured
        );
    }

    #[tokio::test]
    async fn retry_budget_is_bounded() {
        let (endpoint, thread) = server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into();
            5
        ]);
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.endpoint = endpoint;
        google.key = None;
        let message = format!("{:#}", google.search("test").await.unwrap_err());
        assert!(message.contains("after 5 attempts"));
        thread.join().unwrap();
    }

    #[test]
    fn stale_cache_is_not_used() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stale.json");
        write_cache(&path, br#"{"kind":"books#volumes","totalItems":0}"#).unwrap();
        assert!(read_cache(&path).unwrap().is_some());
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(SystemTime::now() - CACHE_TTL - Duration::from_secs(60)),
        )
        .unwrap();
        assert!(read_cache(&path).unwrap().is_none());
    }

    #[test]
    fn cover_urls_are_google_only_and_upgraded_to_https() {
        assert_eq!(
            Google::cover_url("http://books.google.com/books/content?id=test")
                .unwrap()
                .as_str(),
            "https://books.google.com/books/content?id=test"
        );
        for url in [
            "http://127.0.0.1/private",
            "https://books.google.com.attacker.invalid/art",
            "https://books.google.com@attacker.invalid/art",
            "file:///private",
            "https://books.google.com:8080/art",
        ] {
            assert!(Google::cover_url(url).is_err(), "{url}");
        }
    }

    #[tokio::test]
    async fn shared_http_transport_fetches_binary_without_forwarding_api_key() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/cover", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 8192];
            let count = socket.read(&mut request).unwrap();
            assert!(!String::from_utf8_lossy(&request[..count]).contains("private-test-key"));
            let body = [0, 255, 1, 128];
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 4\r\nConnection: close\r\n\r\n").unwrap();
            socket.write_all(&body).unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let mut google = Google::new(directory.path().to_owned(), false, false, 20).unwrap();
        google.key = Some("private-test-key".into());
        let bytes = google
            .fetch(google.client.get(endpoint), crate::cover::MAX_BYTES)
            .await
            .unwrap();
        assert_eq!(bytes, [0, 255, 1, 128]);
        server.join().unwrap();
    }

    #[tokio::test]
    #[ignore = "Contacts live Google Books; optionally set GOOGLE_BOOKS_API_KEY"]
    async fn live_google_lookup() {
        let directory = tempfile::tempdir().unwrap();
        let google = Google::new(directory.path().to_owned(), false, true, 4).unwrap();
        let items = google.search("isbn:9780141439518").await.unwrap();
        assert!(!items.is_empty(), "Known book lookup returned no records");
        assert!(items.iter().any(|item| item.info.title.contains("Pride")));
    }
}
