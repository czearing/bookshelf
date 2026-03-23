use crate::{db, want};
use anyhow::Context;
use chrono::Utc;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// OL deserialization structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlAuthorSearchResponse {
    docs: Vec<OlAuthorDoc>,
}

#[derive(Debug, Deserialize)]
struct OlAuthorDoc {
    key: String, // e.g. "/authors/OL23919A"
}

#[derive(Debug, Deserialize)]
struct OlAuthorWorksResponse {
    #[serde(default)]
    entries: Vec<OlAuthorWork>,
}

#[derive(Debug, Deserialize)]
struct OlAuthorWork {
    title: Option<String>,
    key: String, // e.g. "/works/OL45804W"
}

// ---------------------------------------------------------------------------
// Public result type
// ---------------------------------------------------------------------------

/// Result of a `follow_add` call.
#[derive(Debug, PartialEq)]
pub enum FollowAddResult {
    Added { works_queued: usize },
    AlreadyFollowed,
    AuthorNotFound,
}

// ---------------------------------------------------------------------------
// Public functions
// ---------------------------------------------------------------------------

/// Add an author to the follow list, query OL for their catalog, and queue
/// unowned/unwanted works in `want_list` with `source = "author_follow"`.
pub async fn follow_add(
    pool: &db::DbPool,
    client: &reqwest::Client,
    author_name: &str,
    base_url: &str,
) -> anyhow::Result<FollowAddResult> {
    // AC-11: already followed?
    if db::find_followed_author_by_name(pool, author_name).await?.is_some() {
        return Ok(FollowAddResult::AlreadyFollowed);
    }

    // AC-10/12: search OL for the author.
    let encoded = urlencoding::encode(author_name);
    let url = format!("{base_url}/search/authors.json?q={encoded}");
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("OL author search request failed for '{author_name}'"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "OL author search returned HTTP {} for '{author_name}'",
            resp.status()
        );
    }

    let search: OlAuthorSearchResponse = resp
        .json()
        .await
        .context("deserializing OL author search response")?;

    if search.docs.is_empty() {
        return Ok(FollowAddResult::AuthorNotFound);
    }

    let ol_key = search.docs[0].key.clone();

    // AC-5: store author with ol_key (also sets last_synced = now).
    db::insert_followed_author(pool, author_name, Some(&ol_key)).await?;

    // Fetch works and queue them.
    let queued = fetch_and_queue_author_works(pool, client, author_name, &ol_key, base_url).await?;

    if queued == 0 {
        eprintln!("Author found but no works in catalog yet.");
    }

    Ok(FollowAddResult::Added { works_queued: queued })
}

/// Remove an author from the follow list. Returns `true` if deleted, `false`
/// if not found. Does NOT touch `want_list` (AC-15).
pub async fn follow_remove(pool: &db::DbPool, author_name: &str) -> anyhow::Result<bool> {
    db::delete_followed_author(pool, author_name).await
}

/// List all followed authors.
pub async fn follow_list(pool: &db::DbPool) -> anyhow::Result<Vec<db::FollowedAuthorRow>> {
    db::list_followed_authors(pool).await
}

/// Re-sync all followed authors: query OL and queue new works.
/// Returns the count of authors successfully synced.
/// Returns `Err` if any author failed (AC-22), but processes all of them.
pub async fn follow_sync(
    pool: &db::DbPool,
    client: &reqwest::Client,
    base_url: &str,
) -> anyhow::Result<usize> {
    let authors = db::list_followed_authors(pool).await?;

    if authors.is_empty() {
        println!("No followed authors to sync.");
        return Ok(0);
    }

    let mut synced = 0usize;
    let mut had_error = false;

    for row in &authors {
        // Resolve ol_key: use stored value, or re-query if missing.
        let ol_key = match row.ol_key.clone() {
            Some(k) => k,
            None => {
                // Re-query OL to get the key.
                match search_author_key(client, &row.name, base_url).await {
                    Ok(Some(k)) => k,
                    Ok(None) => {
                        eprintln!(
                            "WARNING: sync failed for '{}': author not found on OL",
                            row.name
                        );
                        had_error = true;
                        continue;
                    }
                    Err(e) => {
                        eprintln!("WARNING: sync failed for '{}': {e}", row.name);
                        had_error = true;
                        continue;
                    }
                }
            }
        };

        match fetch_and_queue_author_works(pool, client, &row.name, &ol_key, base_url).await {
            Ok(_) => {
                let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                if let Err(e) =
                    db::update_followed_author_synced(pool, &row.name, Some(&ol_key), &now).await
                {
                    eprintln!("WARNING: could not update last_synced for '{}': {e}", row.name);
                }
                synced += 1;
            }
            Err(e) => {
                eprintln!("WARNING: sync failed for '{}': {e}", row.name);
                had_error = true;
            }
        }
    }

    if had_error {
        Err(anyhow::anyhow!("one or more authors failed to sync"))
    } else {
        Ok(synced)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Search OL for an author by name and return the first matching key.
async fn search_author_key(
    client: &reqwest::Client,
    author_name: &str,
    base_url: &str,
) -> anyhow::Result<Option<String>> {
    let encoded = urlencoding::encode(author_name);
    let url = format!("{base_url}/search/authors.json?q={encoded}");
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("OL author search failed for '{author_name}'"))?;

    if !resp.status().is_success() {
        anyhow::bail!(
            "OL author search returned HTTP {} for '{author_name}'",
            resp.status()
        );
    }

    let search: OlAuthorSearchResponse = resp
        .json()
        .await
        .context("deserializing OL author search response")?;

    Ok(search.docs.into_iter().next().map(|d| d.key))
}

/// Paginate through OL author works and insert unowned/unwanted entries into
/// `want_list`. Returns the count of entries queued.
async fn fetch_and_queue_author_works(
    pool: &db::DbPool,
    client: &reqwest::Client,
    author_name: &str,
    ol_key: &str,
    base_url: &str,
) -> anyhow::Result<usize> {
    // Strip leading "/authors/" prefix if present.
    let bare_key = ol_key
        .strip_prefix("/authors/")
        .unwrap_or(ol_key);

    let mut offset = 0usize;
    let limit = 50usize;
    let mut queued = 0usize;

    loop {
        let url = format!(
            "{base_url}/authors/{bare_key}/works.json?limit={limit}&offset={offset}"
        );
        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("OL author works request failed for '{author_name}'"))?;

        if !resp.status().is_success() {
            anyhow::bail!(
                "OL author works returned HTTP {} for '{author_name}'",
                resp.status()
            );
        }

        let works: OlAuthorWorksResponse = resp
            .json()
            .await
            .context("deserializing OL author works response")?;

        if works.entries.is_empty() {
            break;
        }

        for entry in &works.entries {
            let title = match entry.title.as_deref().filter(|t| !t.is_empty()) {
                Some(t) => t,
                None => continue,
            };

            if want::is_already_owned(pool, title, Some(author_name), None).await? {
                continue;
            }
            if want::find_existing_want(pool, title, Some(author_name), None)
                .await?
                .is_some()
            {
                continue;
            }

            db::insert_want(
                pool,
                title,
                Some(author_name),
                None,
                "author_follow",
                Some(entry.key.as_str()),
                5,
                None,
            )
            .await?;
            queued += 1;
        }

        offset += limit;
    }

    Ok(queued)
}
