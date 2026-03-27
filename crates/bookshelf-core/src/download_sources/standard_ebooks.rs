use super::{DownloadCandidate, DownloadSource};
use anyhow::Context;
use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::path::Path;

pub struct StandardEbooksSource {
    client: reqwest::Client,
    base_url: String,
    credentials: Option<(String, String)>,
}

impl StandardEbooksSource {
    pub fn new(client: reqwest::Client, base_url: impl Into<String>, credentials: Option<(String, String)>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
            credentials,
        }
    }
}

#[async_trait]
impl DownloadSource for StandardEbooksSource {
    fn name(&self) -> &str {
        "standard-ebooks"
    }

    async fn find(&self, title: &str, authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        let catalog = fetch_catalog(&self.client, &self.base_url).await?;

        let matched = catalog.into_iter().find(|entry| {
            crate::fuzzy::is_same_work(&entry.title, &entry.author, title, authors)
        });

        Ok(matched.map(|entry| {
            let download_url = if entry.epub_url.starts_with("http") {
                entry.epub_url.clone()
            } else {
                format!("{}{}", self.base_url, entry.epub_url)
            };
            DownloadCandidate {
                source_name: "standard-ebooks".to_string(),
                title: entry.title,
                authors: entry.author,
                download_url,
                format: "epub".to_string(),
            }
        }))
    }

    async fn fetch(&self, candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()> {
        let mut req = self.client.get(&candidate.download_url);
        if let Some((ref email, ref password)) = self.credentials {
            req = req.basic_auth(email, Some(password));
        }
        let resp = req
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
// Private catalog types
// ---------------------------------------------------------------------------

struct CatalogEntry {
    title: String,
    author: String,
    epub_url: String,
}

async fn fetch_catalog(
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

fn parse_opds_feed(
    xml: &str,
    base_url: &str,
) -> anyhow::Result<(Vec<CatalogEntry>, Option<String>)> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut entries: Vec<CatalogEntry> = Vec::new();
    let mut next_url: Option<String> = None;

    let mut in_entry = false;
    let mut current_title = String::new();
    let mut current_author = String::new();
    let mut in_author_element = false;
    let mut current_epub_url = String::new();
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

fn local_name(name: &[u8]) -> &str {
    let s = std::str::from_utf8(name).unwrap_or("");
    if let Some(pos) = s.find(':') {
        &s[pos + 1..]
    } else {
        s
    }
}
