use crate::{db, epub, fuzzy, grab, import, native_path_str};
use anyhow::Context;
use futures::StreamExt;
use serde::Deserialize;
use std::path::Path;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single event that occurred during a `download_want_list` run.
#[derive(Debug)]
pub enum DownloadEvent {
    Downloaded {
        title: String,
        source_name: String,
    },
    WouldDownload {
        title: String,
        source_name: String,
    },
    NotFound {
        title: String,
    },
}

/// Result of a `download_grab_list` or `download_want_list` call.
#[derive(Debug, Default)]
pub struct DownloadResult {
    pub downloaded: usize,
    pub not_found: usize,
    pub would_download: usize,
    pub events: Vec<DownloadEvent>,
    /// Edition IDs inserted into the DB during this run (real downloads only).
    pub new_edition_ids: Vec<i64>,
    /// (dest_path, want_title, want_authors) for each successfully downloaded book.
    /// Used by the run pipeline to re-apply clean want-list metadata after enrich.
    pub title_overrides: Vec<(String, String, String)>,
}

/// A book entry from a catalog (Standard Ebooks or Gutenberg).
#[derive(Debug, Clone)]
struct CatalogEntry {
    pub title: String,
    pub author: String,
    pub epub_url: String,
}

// ---------------------------------------------------------------------------
// Public function
// ---------------------------------------------------------------------------

/// Download EPUB files for every entry in the grab list.
///
/// For each entry:
/// 1. Try to find it in the Standard Ebooks OPDS catalog (fetched once).
/// 2. If not found there, search Project Gutenberg.
/// 3. Download the EPUB to a temp file, validate it with `parse_epub`, then
///    call `import_path` with `do_move = true`.
/// 4. Counts downloaded and not-found items.
pub async fn download_grab_list(
    pool: &db::DbPool,
    client: &reqwest::Client,
    library_dir: &Path,
    base_url_se: &str,
    base_url_gutenberg: &str,
) -> anyhow::Result<DownloadResult> {
    let grab_list = grab::compute_grab_list(pool, None).await?;

    if grab_list.is_empty() {
        return Ok(DownloadResult::default());
    }

    // Fetch entire SE catalog once.
    let se_catalog = match fetch_standard_ebooks_catalog(client, base_url_se).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("WARNING: could not fetch Standard Ebooks catalog: {e}");
            Vec::new()
        }
    };

    let mut result = DownloadResult::default();

    for grab_entry in &grab_list {
        let title = grab_entry.title.as_deref().unwrap_or("");
        let author = grab_entry.author.as_deref().unwrap_or("");

        // --- Try Standard Ebooks first (AC-56) ---
        let se_match = se_catalog.iter().find(|entry| {
            fuzzy::is_same_work(&entry.title, &entry.author, title, author)
        });

        if let Some(se_entry) = se_match {
            let url = if se_entry.epub_url.starts_with("http") {
                se_entry.epub_url.clone()
            } else {
                format!("{}{}", base_url_se, se_entry.epub_url)
            };

            match try_download_and_import(pool, client, &url, library_dir).await {
                Ok(true) => {
                    result.downloaded += 1;
                    continue;
                }
                Ok(false) => {
                    // parse/import failed — fall through to Gutenberg
                }
                Err(e) => {
                    eprintln!("WARNING: SE download error for '{title}': {e}");
                    // HTTP error — continue to next grab entry (AC-53)
                    result.not_found += 1;
                    continue;
                }
            }
        }

        // --- Try Gutenberg (AC-49) ---
        let gutenberg_results =
            match search_gutenberg(client, base_url_gutenberg, title, author).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("WARNING: Gutenberg search error for '{title}': {e}");
                    result.not_found += 1;
                    continue;
                }
            };

        let gut_match = gutenberg_results
            .iter()
            .find(|entry| fuzzy::is_same_work(&entry.title, &entry.author, title, author));

        if let Some(gut_entry) = gut_match {
            let url = if gut_entry.epub_url.starts_with("http") {
                gut_entry.epub_url.clone()
            } else {
                format!("{}{}", base_url_gutenberg, gut_entry.epub_url)
            };

            match try_download_and_import(pool, client, &url, library_dir).await {
                Ok(true) => {
                    result.downloaded += 1;
                }
                Ok(false) => {
                    // parse/import failed — not counted (AC-54)
                    result.not_found += 1;
                }
                Err(e) => {
                    eprintln!("WARNING: Gutenberg download error for '{title}': {e}");
                    result.not_found += 1;
                }
            }
        } else {
            // Neither SE nor Gutenberg matched (AC-50)
            result.not_found += 1;
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// New pluggable API
// ---------------------------------------------------------------------------

/// Download EPUB files for every entry in the want/grab list using pluggable
/// `DownloadSource` implementations.
///
/// Sources are tried in order; the first `Ok(Some(_))` match wins.  An
/// `Ok(None)` result tries the next source; an `Err` is treated as definitive
/// (the entry is marked not-found and no further sources are tried for it).
///
/// When `dry_run` is `true` the function records what *would* be downloaded but
/// does not actually fetch or import anything.

/// Maximum number of concurrent source-find HTTP requests.
const FIND_CONCURRENCY: usize = 8;

pub async fn download_want_list(
    pool: &db::DbPool,
    library_dir: &std::path::Path,
    sources: &[Box<dyn crate::download_sources::DownloadSource>],
    dry_run: bool,
) -> anyhow::Result<DownloadResult> {
    let grab_list = grab::compute_grab_list(pool, None).await?;

    if grab_list.is_empty() {
        return Ok(DownloadResult::default());
    }

    // ---------------------------------------------------------------------------
    // Phase 1: Find candidates in parallel.
    // Up to FIND_CONCURRENCY source lookups run concurrently.  Each future tries
    // sources in order for one grab entry and returns the first match (or None).
    // Sources are Send+Sync so sharing them across concurrent futures is safe.
    // ---------------------------------------------------------------------------
    let candidates: Vec<Option<crate::download_sources::DownloadCandidate>> =
        futures::stream::iter(grab_list.iter())
            .map(|entry| {
                let title = entry.title.as_deref().unwrap_or("").to_owned();
                let authors = entry.author.as_deref().unwrap_or("").to_owned();
                async move {
                    let mut found = None;
                    for source in sources {
                        match source.find(&title, &authors).await {
                            Ok(Some(c)) => {
                                found = Some(c);
                                break;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                eprintln!(
                                    "WARNING: source '{}' error for '{title}': {e}",
                                    source.name()
                                );
                                break; // error is definitive for this entry
                            }
                        }
                    }
                    found
                }
            })
            .buffered(FIND_CONCURRENCY)
            .collect()
            .await;

    // ---------------------------------------------------------------------------
    // Phase 2: Download and import serially.
    // File I/O and SQLite writes are kept sequential to avoid contention.
    // ---------------------------------------------------------------------------

    // Stage downloads in a subdir of library_dir instead of the system temp dir.
    // This guarantees files are always on the same volume as the library (no
    // cross-drive copies), and avoids Windows Defender scanning system TEMP while
    // a file is being written, which causes spurious "file in use" (os error 32) failures.
    // Only create the directory when we will actually write files (not during dry runs).
    let staging_dir = library_dir.join("_tmp");
    if !dry_run {
        if let Err(e) = std::fs::create_dir_all(&staging_dir) {
            // Non-fatal: errors will surface at tempfile creation time.
            eprintln!("WARNING: could not create staging dir {}: {e}", staging_dir.display());
        }
    }

    let mut result = DownloadResult::default();

    for (entry, candidate) in grab_list.iter().zip(candidates.into_iter()) {
        let title = entry.title.as_deref().unwrap_or("");
        let authors = entry.author.as_deref().unwrap_or("");

        let c = match candidate {
            Some(c) => c,
            None => {
                result.events.push(DownloadEvent::NotFound {
                    title: title.to_string(),
                });
                result.not_found += 1;
                if !dry_run {
                    let _ = db::increment_want_attempts(pool, entry.id).await;
                }
                continue;
            }
        };

        if dry_run {
            result.events.push(DownloadEvent::WouldDownload {
                title: title.to_string(),
                source_name: c.source_name.clone(),
            });
            result.would_download += 1;
            continue;
        }

        // Build a clean filename from the title (no random tempfile suffix).
        let safe_name: String = title
            .chars()
            .map(|ch| if ch.is_alphanumeric() || ch == '-' { ch.to_ascii_lowercase() } else { '-' })
            .collect::<String>()
            .split('-')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("-");
        let ext = if c.format == "pdf" { "pdf" } else { "epub" };

        // Download to a random tempfile first (for atomicity), then rename to
        // a clean name so the library filename never contains random characters.
        let tmp = match tempfile::Builder::new()
            .suffix(&format!(".{ext}"))
            .tempfile_in(&staging_dir)
        {
            Ok(t) => t,
            Err(e) => {
                eprintln!("WARNING: could not create temp file for '{title}': {e}");
                result.events.push(DownloadEvent::NotFound {
                    title: title.to_string(),
                });
                result.not_found += 1;
                continue;
            }
        };

        // Find the source that produced the candidate and fetch.
        let fetch_source = sources.iter().find(|s| s.name() == c.source_name.as_str());
        if let Some(src) = fetch_source {
            if let Err(e) = src.fetch(&c, tmp.path()).await {
                eprintln!("WARNING: fetch error for '{title}': {e}");
                result.events.push(DownloadEvent::NotFound {
                    title: title.to_string(),
                });
                result.not_found += 1;
                continue;
            }
        } else {
            // Source not found in slice (should not happen).
            result.events.push(DownloadEvent::NotFound {
                title: title.to_string(),
            });
            result.not_found += 1;
            continue;
        }

        let tmp_path = tmp.path().to_path_buf();

        if c.format == "pdf" {
            // Validate: check PDF header bytes
            let header = std::fs::read(&tmp_path)
                .ok()
                .and_then(|b| b.get(..5).map(|s| s.to_vec()))
                .unwrap_or_default();
            if !header.starts_with(b"%PDF-") {
                let _ = tmp.close();
                result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                result.not_found += 1;
                continue;
            }

            // Copy PDF to library dir using the clean filename
            let dest = library_dir.join(format!("{safe_name}.pdf"));
            if let Err(e) = std::fs::copy(&tmp_path, &dest) {
                eprintln!("WARNING: could not copy PDF for '{title}': {e}");
                let _ = tmp.close();
                result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                result.not_found += 1;
                continue;
            }

            // Build minimal meta and upsert
            let meta = epub::meta_from_pdf_path(&dest);
            let _ = db::upsert_edition(pool, &meta).await;

            // Provenance (best-effort)
            let dest_str = native_path_str(&dest);
            let _ = db::set_download_provenance(pool, &dest_str, &c.download_url, &c.source_name).await;
            // Use want-list title/authors — they are the user's clean, intended version.
            let _ = db::overwrite_edition_metadata(pool, &dest_str, title, authors).await;
            let _ = db::delete_want(pool, entry.id).await;

            result.title_overrides.push((dest_str.clone(), title.to_string(), authors.to_string()));

            result.events.push(DownloadEvent::Downloaded {
                title: title.to_string(),
                source_name: c.source_name.clone(),
            });
            result.downloaded += 1;
        } else {
            // EPUB path: rename temp to clean name before import so the library
            // filename doesn't contain random tempfile characters.
            if epub::parse_epub(&tmp_path).is_err() {
                let _ = tmp.close();
                result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                result.not_found += 1;
                continue;
            }

            // Rename temp → clean name in temp dir so import_path uses a good filename.
            let clean_tmp = staging_dir.join(format!("{safe_name}.epub"));
            if let Err(e) = std::fs::copy(&tmp_path, &clean_tmp) {
                eprintln!("WARNING: could not rename temp file for '{title}': {e}");
                let _ = tmp.close();
                result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                result.not_found += 1;
                continue;
            }
            let _ = tmp.close();

            let import_result = match import::import_path(pool, &clean_tmp, library_dir, true).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("WARNING: import_path failed for '{title}': {e}");
                    let _ = std::fs::remove_file(&clean_tmp);
                    result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                    result.not_found += 1;
                    continue;
                }
            };

            if import_result.imported == 0 {
                let _ = std::fs::remove_file(&clean_tmp);
                result.events.push(DownloadEvent::NotFound { title: title.to_string() });
                result.not_found += 1;
                continue;
            }

            let dest_path = library_dir.join(clean_tmp.file_name().unwrap());
            let dest_str = native_path_str(&dest_path);
            let _ = db::set_download_provenance(pool, &dest_str, &c.download_url, &c.source_name).await;
            // Always use the want-list title/authors — they are the user's clean, intended
            // version and override whatever messy title the EPUB or catalog embedded.
            let _ = db::overwrite_edition_metadata(pool, &dest_str, title, authors).await;
            let _ = db::delete_want(pool, entry.id).await;

            // Record for post-enrich re-application (enrich may overwrite with bad data).
            result.title_overrides.push((dest_str.clone(), title.to_string(), authors.to_string()));

            result.events.push(DownloadEvent::Downloaded {
                title: title.to_string(),
                source_name: c.source_name.clone(),
            });
            result.downloaded += 1;
            continue; // skip the tmp.close() below — already closed
        }

        let _ = tmp.close();
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Attempt to download from `url`, validate as EPUB, and import.
/// Returns `Ok(true)` on success, `Ok(false)` if parse/import fails,
/// `Err` only on HTTP-level errors.
async fn try_download_and_import(
    pool: &db::DbPool,
    client: &reqwest::Client,
    url: &str,
    library_dir: &Path,
) -> anyhow::Result<bool> {
    let temp = download_to_tempfile(client, url).await?;

    // Validate EPUB.
    let epub_path = temp.path().to_path_buf();
    if epub::parse_epub(&epub_path).is_err() {
        // Remove temp file; do not count as downloaded (AC-54)
        let _ = temp.close();
        return Ok(false);
    }

    // Import the file. do_move=true so the temp file is consumed.
    // We persist the temp file path before calling import, because
    // import_path does its own copy into library_dir.
    let temp_path = temp.path().to_path_buf();
    // Keep the file alive during import (don't drop temp yet).
    match import::import_path(pool, &temp_path, library_dir, true).await {
        Ok(import_result) => {
            let _ = temp.close();
            Ok(import_result.imported > 0)
        }
        Err(e) => {
            eprintln!("WARNING: import_path failed: {e}");
            let _ = temp.close();
            Ok(false)
        }
    }
}

/// Download `url` to a `NamedTempFile` and return it.
/// Returns `Err` if the HTTP request fails or returns a non-2xx status.
async fn download_to_tempfile(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<tempfile::NamedTempFile> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("HTTP request failed for {url}"))?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP {} for {url}", resp.status());
    }

    let bytes = resp
        .bytes()
        .await
        .with_context(|| format!("reading response body for {url}"))?;

    let mut tmp = tempfile::NamedTempFile::with_suffix(".epub")
        .context("creating temp file for EPUB download")?;

    use std::io::Write;
    tmp.write_all(&bytes)
        .context("writing EPUB bytes to temp file")?;

    Ok(tmp)
}

/// Fetch the full Standard Ebooks OPDS catalog by paginating through all
/// `<link rel="next">` pages.
async fn fetch_standard_ebooks_catalog(
    client: &reqwest::Client,
    base_url: &str,
) -> anyhow::Result<Vec<CatalogEntry>> {
    let mut entries = Vec::new();
    let mut url = format!("{base_url}/feeds/opds/all");

    loop {
        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("SE OPDS request failed for {url}"))?;

        if !resp.status().is_success() {
            anyhow::bail!("SE OPDS returned HTTP {} for {url}", resp.status());
        }

        let body = resp
            .text()
            .await
            .with_context(|| format!("reading SE OPDS body for {url}"))?;

        let (page_entries, next_url) = parse_opds_feed(&body, base_url)?;
        entries.extend(page_entries);

        match next_url {
            Some(next) => url = next,
            None => break,
        }
    }

    Ok(entries)
}

/// Parse one page of an Atom OPDS feed.
/// Returns the list of `CatalogEntry` items and the optional next-page URL.
fn parse_opds_feed(
    xml: &str,
    base_url: &str,
) -> anyhow::Result<(Vec<CatalogEntry>, Option<String>)> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut entries: Vec<CatalogEntry> = Vec::new();
    let mut next_url: Option<String> = None;

    // State for current entry being parsed.
    let mut in_entry = false;
    let mut current_title = String::new();
    let mut current_author = String::new();
    let mut in_author_element = false;
    let mut current_epub_url = String::new();
    // Track whether we are collecting text for a specific element.
    let mut collecting: Option<&'static str> = None;

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = e.name().into_inner().to_vec();
                let local = local_name(&name);

                match local {
                    "entry" => {
                        in_entry = true;
                        current_title.clear();
                        current_author.clear();
                        current_epub_url.clear();
                        in_author_element = false;
                        collecting = None;
                    }
                    "title" if in_entry => {
                        collecting = Some("title");
                    }
                    "author" if in_entry => {
                        in_author_element = true;
                    }
                    "name" if in_entry && in_author_element => {
                        collecting = Some("name");
                    }
                    "link" => {
                        // Check rel and href attributes.
                        let mut rel = String::new();
                        let mut href = String::new();
                        for attr in e.attributes().flatten() {
                            let key = attr.key.into_inner().to_vec();
                            let key_str = std::str::from_utf8(&key).unwrap_or("").to_string();
                            let val = attr
                                .decode_and_unescape_value(&reader)
                                .unwrap_or_default()
                                .to_string();
                            if key_str == "rel" {
                                rel = val;
                            } else if key_str == "href" {
                                href = val;
                            }
                        }

                        if rel == "http://opds-spec.org/acquisition" && !href.is_empty() {
                            if in_entry {
                                current_epub_url = href;
                            }
                        } else if rel == "next" && !href.is_empty() {
                            // Build absolute URL if needed.
                            if href.starts_with("http") {
                                next_url = Some(href);
                            } else {
                                next_url = Some(format!("{base_url}{href}"));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = e.name().into_inner().to_vec();
                let local = local_name(&name);

                if local == "link" {
                    let mut rel = String::new();
                    let mut href = String::new();
                    for attr in e.attributes().flatten() {
                        let key = attr.key.into_inner().to_vec();
                        let key_str = std::str::from_utf8(&key).unwrap_or("").to_string();
                        let val = attr
                            .decode_and_unescape_value(&reader)
                            .unwrap_or_default()
                            .to_string();
                        if key_str == "rel" {
                            rel = val;
                        } else if key_str == "href" {
                            href = val;
                        }
                    }

                    if rel == "http://opds-spec.org/acquisition" && !href.is_empty() {
                        if in_entry {
                            current_epub_url = href;
                        }
                    } else if rel == "next" && !href.is_empty() {
                        if href.starts_with("http") {
                            next_url = Some(href);
                        } else {
                            next_url = Some(format!("{base_url}{href}"));
                        }
                    }
                }
            }
            Ok(Event::Text(ref e)) => {
                if let Some(field) = collecting {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match field {
                        "title" => current_title = text,
                        "name" => current_author = text,
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name().into_inner().to_vec();
                let local = local_name(&name);

                match local {
                    "entry" if in_entry => {
                        if !current_title.is_empty() && !current_epub_url.is_empty() {
                            entries.push(CatalogEntry {
                                title: current_title.clone(),
                                author: current_author.clone(),
                                epub_url: current_epub_url.clone(),
                            });
                        }
                        in_entry = false;
                        collecting = None;
                    }
                    "title" => collecting = None,
                    "name" => collecting = None,
                    "author" if in_entry => {
                        in_author_element = false;
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(anyhow::anyhow!("XML parse error: {e}"));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok((entries, next_url))
}

/// Extract the local part of a potentially namespace-prefixed XML element name.
/// e.g. `b"dc:title"` → `"title"`, `b"title"` → `"title"`.
fn local_name(name: &[u8]) -> &str {
    let s = std::str::from_utf8(name).unwrap_or("");
    if let Some(pos) = s.find(':') {
        &s[pos + 1..]
    } else {
        s
    }
}

// ---------------------------------------------------------------------------
// Gutenberg search
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GutenbergResponse {
    results: Vec<GutenbergBook>,
}

#[derive(Debug, Deserialize)]
struct GutenbergBook {
    title: String,
    #[serde(default)]
    authors: Vec<GutenbergAuthor>,
    #[serde(default)]
    formats: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct GutenbergAuthor {
    name: String,
}

/// Search Project Gutenberg (gutendex.com) for books matching `title` and `author`.
async fn search_gutenberg(
    client: &reqwest::Client,
    base_url: &str,
    title: &str,
    author: &str,
) -> anyhow::Result<Vec<CatalogEntry>> {
    let query = format!("{title} {author}");
    let encoded = urlencoding::encode(&query);
    let url = format!("{base_url}/books/?search={encoded}");

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Gutenberg search request failed for '{title}'"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "Gutenberg search returned HTTP {} for '{title}'",
            resp.status()
        );
    }

    let data: GutenbergResponse = resp
        .json()
        .await
        .context("deserializing Gutenberg search response")?;

    let entries = data
        .results
        .into_iter()
        .filter_map(|book| {
            let epub_url = book
                .formats
                .get("application/epub+zip")
                .cloned()?;
            let author_str = book
                .authors
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            Some(CatalogEntry {
                title: book.title,
                author: author_str,
                epub_url,
            })
        })
        .collect();

    Ok(entries)
}
