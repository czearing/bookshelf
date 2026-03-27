use async_trait::async_trait;
use std::path::Path;

pub mod arxiv;
pub mod gutenberg;
pub mod internet_archive;
pub mod standard_ebooks;

pub use arxiv::ArxivSource;
pub use gutenberg::GutenbergSource;
pub use internet_archive::InternetArchiveSource;
pub use standard_ebooks::StandardEbooksSource;

/// Metadata for a downloadable EPUB found by a source.
#[derive(Debug, Clone)]
pub struct DownloadCandidate {
    pub source_name: String,
    pub title: String,
    pub authors: String,
    pub download_url: String,
    pub format: String,
}

/// A pluggable download source. Implement this trait to add a new source.
#[async_trait]
pub trait DownloadSource: Send + Sync {
    fn name(&self) -> &str;
    async fn find(&self, title: &str, authors: &str) -> anyhow::Result<Option<DownloadCandidate>>;
    async fn fetch(&self, candidate: &DownloadCandidate, dest: &Path) -> anyhow::Result<()>;
}
