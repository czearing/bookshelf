use crate::{db, follow, series, want};
use std::path::Path;

pub struct SyncResult {
    pub authors: Option<anyhow::Result<follow::AuthorsSyncOutcome>>,
    pub series: Option<anyhow::Result<usize>>,
    pub goodreads: Option<anyhow::Result<want::ImportSummary>>,
}

pub async fn sync_all(
    pool: &db::DbPool,
    client: &reqwest::Client,
    base_url: &str,
    goodreads_path: Option<&Path>,
    all_shelves: bool,
    language: Option<&str>,
) -> SyncResult {
    let goodreads = if let Some(path) = goodreads_path {
        Some(want::import_goodreads_csv(pool, path, all_shelves).await)
    } else {
        None
    };
    let series = Some(series::series_fill(pool, client, base_url, language).await);
    let authors = Some(follow::sync_authors_detailed(pool, client, base_url, language).await);
    SyncResult {
        authors,
        series,
        goodreads,
    }
}
