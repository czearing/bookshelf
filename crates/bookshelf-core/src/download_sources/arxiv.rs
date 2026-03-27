use super::{DownloadCandidate, DownloadSource};
use anyhow::Context;
use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::path::Path;

pub struct ArxivSource {
    client: reqwest::Client,
    base_url: String,
}

impl ArxivSource {
    pub fn new(client: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

#[async_trait]
impl DownloadSource for ArxivSource {
    fn name(&self) -> &str {
        "arxiv"
    }

    /// Search arXiv using the Atom API.
    ///
    /// Strategy:
    ///   1. Query `ti:{title} AND au:{last_name}` (last name = first token of authors string).
    ///   2. If no results, fall back to `ti:{title}` only (author field can be noisy).
    ///   3. Fuzzy-match each result against the original title+authors.
    ///   4. Return the PDF link from the first matching entry.
    async fn find(&self, title: &str, authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        // Build primary query: title + first author last name.
        let last_name = authors
            .split_whitespace()
            .last()
            .unwrap_or("")
            .trim_matches(',');

        let queries: &[String] = &[
            format!("ti:{} AND au:{}", title, last_name),
            format!("ti:{}", title),
        ];

        for q in queries {
            let encoded = urlencoding::encode(q);
            let url = format!(
                "{}/api/query?search_query={}&max_results=10&sortBy=relevance",
                self.base_url, encoded
            );

            let resp = self
                .client
                .get(&url)
                .send()
                .await
                .with_context(|| format!("arXiv search request failed for '{title}'"))?;

            if !resp.status().is_success() {
                anyhow::bail!(
                    "arXiv search returned HTTP {} for '{title}'",
                    resp.status()
                );
            }

            let body = resp
                .text()
                .await
                .with_context(|| format!("reading arXiv response body for '{title}'"))?;

            let entries = parse_atom_feed(&body)?;

            if entries.is_empty() {
                continue; // try fallback query
            }

            for entry in entries {
                let pdf_url = match entry.pdf_url {
                    Some(ref u) => u.clone(),
                    None => continue,
                };

                // arXiv author names arrive as "Lastname, Firstname" — normalize.
                let normalized_authors = entry
                    .authors
                    .iter()
                    .map(|a| crate::author::normalize_author(a))
                    .collect::<Vec<_>>()
                    .join(", ");

                if crate::fuzzy::is_same_work(
                    &entry.title,
                    &normalized_authors,
                    title,
                    authors,
                ) {
                    return Ok(Some(DownloadCandidate {
                        source_name: "arxiv".to_string(),
                        title: entry.title,
                        authors: normalized_authors,
                        download_url: pdf_url,
                        format: "pdf".to_string(),
                    }));
                }
            }

            // Results existed but none fuzzy-matched — don't try the broader fallback
            // query, as it will only return less relevant results.
            break;
        }

        Ok(None)
    }

    async fn fetch(&self, candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        let resp = self
            .client
            .get(&candidate.download_url)
            .send()
            .await
            .with_context(|| format!("HTTP request failed for {}", candidate.download_url))?;

        if !resp.status().is_success() {
            anyhow::bail!("HTTP {} for {}", resp.status(), candidate.download_url);
        }

        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("reading response body for {}", candidate.download_url))?;

        std::fs::write(dest, &bytes)
            .with_context(|| format!("writing bytes to {}", dest.display()))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Atom feed parser
// ---------------------------------------------------------------------------

struct AtomEntry {
    title: String,
    authors: Vec<String>,
    pdf_url: Option<String>,
}

/// Parse arXiv's Atom feed and return one `AtomEntry` per `<entry>` element.
/// Namespace prefixes are stripped so `arxiv:primary_category` etc. are handled.
fn parse_atom_feed(xml: &str) -> anyhow::Result<Vec<AtomEntry>> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut entries: Vec<AtomEntry> = Vec::new();

    let mut in_entry = false;
    let mut in_author = false;
    let mut current_title = String::new();
    let mut current_authors: Vec<String> = Vec::new();
    let mut current_pdf_url: Option<String> = None;
    let mut collecting: Option<&'static str> = None;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let local = local_name(e.name().into_inner());
                match local {
                    "entry" => {
                        in_entry = true;
                        current_title.clear();
                        current_authors.clear();
                        current_pdf_url = None;
                        in_author = false;
                        collecting = None;
                    }
                    "title" if in_entry => collecting = Some("title"),
                    "author" if in_entry => in_author = true,
                    "name" if in_entry && in_author => collecting = Some("name"),
                    "link" if in_entry => {
                        // <link> as a start tag (rare but possible) — check attributes.
                        if let Some(href) = pdf_link_href(e.attributes()) {
                            current_pdf_url = Some(href);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                // Self-closing tags: <link ... /> is the common case in arXiv feeds.
                let local = local_name(e.name().into_inner());
                if local == "link" && in_entry {
                    if let Some(href) = pdf_link_href(e.attributes()) {
                        current_pdf_url = Some(href);
                    }
                }
            }
            Ok(Event::Text(ref e)) => {
                if let Some(field) = collecting {
                    let text = e.unescape().unwrap_or_default();
                    // Collapse whitespace — arXiv titles can contain line breaks.
                    let clean: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    match field {
                        "title" => current_title = clean,
                        "name" => {
                            if !clean.is_empty() {
                                current_authors.push(clean);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let local = local_name(e.name().into_inner());
                match local {
                    "entry" if in_entry => {
                        if !current_title.is_empty() {
                            entries.push(AtomEntry {
                                title: current_title.clone(),
                                authors: current_authors.clone(),
                                pdf_url: current_pdf_url.clone(),
                            });
                        }
                        in_entry = false;
                        collecting = None;
                    }
                    "title" | "name" => collecting = None,
                    "author" if in_entry => in_author = false,
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => anyhow::bail!("arXiv XML parse error: {e}"),
            _ => {}
        }
        buf.clear();
    }

    Ok(entries)
}

/// Extract `href` from a `<link>` element if it has `title="pdf"` or
/// `type="application/pdf"`.
fn pdf_link_href(
    attrs: quick_xml::events::attributes::Attributes,
) -> Option<String> {
    let mut href = String::new();
    let mut is_pdf = false;

    for attr in attrs.flatten() {
        let key = std::str::from_utf8(attr.key.into_inner()).unwrap_or("").to_string();
        let val = attr.decode_and_unescape_value(&Reader::from_str(""))
            .unwrap_or_default()
            .to_string();
        match key.as_str() {
            "href" => href = val,
            "title" if val == "pdf" => is_pdf = true,
            "type" if val == "application/pdf" => is_pdf = true,
            _ => {}
        }
    }

    if is_pdf && !href.is_empty() {
        Some(href)
    } else {
        None
    }
}

fn local_name(name: &[u8]) -> &str {
    let s = std::str::from_utf8(name).unwrap_or("");
    s.find(':').map(|i| &s[i + 1..]).unwrap_or(s)
}
