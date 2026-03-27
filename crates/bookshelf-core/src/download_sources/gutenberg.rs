use super::{DownloadCandidate, DownloadSource};
use anyhow::Context;
use async_trait::async_trait;
use serde::Deserialize;
use std::path::Path;

pub struct GutenbergSource {
    client: reqwest::Client,
    base_url: String,
}

impl GutenbergSource {
    pub fn new(client: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

#[async_trait]
impl DownloadSource for GutenbergSource {
    fn name(&self) -> &str {
        "gutenberg"
    }

    async fn find(&self, title: &str, authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        // Search by title only — author matching is done via fuzzy after results arrive.
        // Gutenberg stores authors as "Lastname, Firstname"; the fuzzy check handles
        // normalized forms on both sides.
        let encoded = urlencoding::encode(title);
        let url = format!(
            "{}/books/?search={}&mime_type=application/epub%2Bzip",
            self.base_url, encoded
        );

        let resp = self
            .client
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

        for book in data.results {
            let epub_url = match book.formats.get("application/epub+zip") {
                Some(u) => u.clone(),
                None => continue,
            };

            // Authors arrive as "Lastname, Firstname"; normalize to "Firstname Lastname".
            let raw_author = book
                .authors
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let normalized_author = crate::author::normalize_author(&raw_author);

            if crate::fuzzy::is_same_work(&book.title, &normalized_author, title, authors) {
                return Ok(Some(DownloadCandidate {
                    source_name: "gutenberg".to_string(),
                    title: book.title,
                    authors: normalized_author,
                    download_url: epub_url,
                    format: "epub".to_string(),
                }));
            }
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
// Private serde structs
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GutenbergResponse {
    results: Vec<GutenbergBook>,
}

#[derive(Deserialize)]
struct GutenbergBook {
    title: String,
    #[serde(default)]
    authors: Vec<GutenbergAuthor>,
    #[serde(default)]
    formats: std::collections::HashMap<String, String>,
}

#[derive(Deserialize)]
struct GutenbergAuthor {
    name: String,
}
