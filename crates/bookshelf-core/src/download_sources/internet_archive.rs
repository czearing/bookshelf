use super::{DownloadCandidate, DownloadSource};
use anyhow::Context;
use async_trait::async_trait;
use serde::Deserialize;
use std::path::Path;

pub struct InternetArchiveSource {
    client: reqwest::Client,
    base_url: String,
    allowed_formats: Vec<String>,
}

impl InternetArchiveSource {
    pub fn new(client: reqwest::Client, base_url: impl Into<String>, allowed_formats: Vec<String>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
            allowed_formats,
        }
    }
}

#[async_trait]
impl DownloadSource for InternetArchiveSource {
    fn name(&self) -> &str {
        "internet-archive"
    }

    async fn find(&self, title: &str, authors: &str) -> anyhow::Result<Option<DownloadCandidate>> {
        // Try epub first (preferred)
        if self.allowed_formats.iter().any(|f| f == "epub") {
            match self.search_pass(title, authors, "epub").await {
                Ok(Some(c)) => return Ok(Some(c)),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }

        // Fallback to PDF
        if self.allowed_formats.iter().any(|f| f == "pdf") {
            match self.search_pass(title, authors, "pdf").await {
                Ok(Some(c)) => return Ok(Some(c)),
                Ok(None) => {}
                Err(e) => return Err(e),
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

impl InternetArchiveSource {
    async fn search_pass(
        &self,
        title: &str,
        authors: &str,
        format_ext: &str,  // "epub" or "pdf"
    ) -> anyhow::Result<Option<DownloadCandidate>> {
        let ia_format = if format_ext == "pdf" { "PDF" } else { "epub" };

        // Try title+creator first; fall back to title-only if no results.
        // IA stores creators in various formats (e.g. "Lastname, Firstname"),
        // so the title-only fallback catches cases where the creator field doesn't
        // match our normalized author string.
        let queries = [
            format!("title:({title}) AND creator:({authors}) AND mediatype:texts AND format:{ia_format}"),
            format!("title:({title}) AND mediatype:texts AND format:{ia_format}"),
        ];

        let mut all_docs: Vec<IaDoc> = Vec::new();
        for q in &queries {
            let encoded_q = urlencoding::encode(q);
            let search_url = format!(
                "{}/advancedsearch.php?q={}&fl[]=identifier,title,creator&output=json&rows=20",
                self.base_url, encoded_q
            );

            let resp = self
                .client
                .get(&search_url)
                .send()
                .await
                .with_context(|| format!("IA search request failed for '{title}'"))?;

            if !resp.status().is_success() {
                anyhow::bail!(
                    "IA search returned HTTP {} for '{title}'",
                    resp.status()
                );
            }

            let search_data: IaSearchResponse = resp
                .json()
                .await
                .context("deserializing IA search response")?;

            if !search_data.response.docs.is_empty() {
                all_docs = search_data.response.docs;
                break; // got results from this query, no need for fallback
            }
        }

        for doc in all_docs {
            let identifier = doc.identifier.clone();

            let meta_url = format!("{}/metadata/{}", self.base_url, identifier);
            let meta_resp = self
                .client
                .get(&meta_url)
                .send()
                .await
                .with_context(|| format!("IA metadata request failed for '{identifier}'"))?;

            if !meta_resp.status().is_success() {
                continue;
            }

            let meta_data: IaMetadataResponse = match meta_resp.json().await {
                Ok(m) => m,
                Err(_) => continue,
            };

            // Skip access-restricted items
            if meta_data.metadata.access_restricted_item.as_deref() == Some("true") {
                continue;
            }

            // Find a file with the right extension that has plausible size
            let found_file = meta_data
                .files
                .iter()
                .find(|f| {
                    f.name.to_lowercase().ends_with(&format!(".{format_ext}"))
                        && f.is_plausible_size()
                });

            let file_name = match found_file {
                Some(f) => f.name.clone(),
                None => continue,
            };

            // Fuzzy match
            let result_title_full = doc.title.as_deref().unwrap_or("");
            let result_title = result_title_full
                .split(&[':', ';'][..])
                .next()
                .unwrap_or(result_title_full)
                .trim();
            let result_creator_raw = doc.creator.as_ref().map(|c| c.first()).unwrap_or("");
            let result_creator = crate::author::normalize_author(result_creator_raw);
            if !crate::fuzzy::is_same_work(result_title, &result_creator, title, authors) {
                continue;
            }

            let encoded_filename = urlencoding::encode(&file_name);
            return Ok(Some(DownloadCandidate {
                source_name: "internet-archive".to_string(),
                title: doc.title.unwrap_or_default(),
                authors: result_creator,
                download_url: format!(
                    "{}/download/{}/{}",
                    self.base_url, identifier, encoded_filename
                ),
                format: format_ext.to_string(),
            }));
        }

        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Private serde structs
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct IaSearchResponse {
    response: IaSearchBody,
}

#[derive(Deserialize)]
struct IaSearchBody {
    docs: Vec<IaDoc>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum IaCreator {
    One(String),
    Many(Vec<String>),
}

impl IaCreator {
    fn first(&self) -> &str {
        match self {
            IaCreator::One(s) => s.as_str(),
            IaCreator::Many(v) => v.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }
}

#[derive(Deserialize)]
struct IaDoc {
    identifier: String,
    title: Option<String>,
    creator: Option<IaCreator>,
}

#[derive(Deserialize)]
struct IaMetadataResponse {
    metadata: IaMetadataMeta,
    #[serde(default)]
    files: Vec<IaFile>,
}

#[derive(Deserialize)]
struct IaMetadataMeta {
    /// "true" when IA has restricted access (lending-only or in-copyright).
    /// None / absent means the item is freely downloadable.
    #[serde(rename = "access-restricted-item")]
    access_restricted_item: Option<String>,
}


#[derive(Deserialize)]
struct IaFile {
    name: String,
    /// File size in bytes as a string (IA returns sizes as strings).
    #[serde(default)]
    size: Option<String>,
}

impl IaFile {
    /// Returns true if the file size is plausible (> 1 KB or unknown).
    fn is_plausible_size(&self) -> bool {
        match &self.size {
            None => true, // unknown size — don't reject
            Some(s) => s.parse::<u64>().map(|n| n > 1024).unwrap_or(true),
        }
    }
}
