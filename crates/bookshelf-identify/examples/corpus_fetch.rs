//! Builds a stratified, reproducible real-book corpus from Project Gutenberg.
//!
//! Gold identity comes from the Gutenberg catalogue (via Gutendex), which is
//! independent of the lookup providers being evaluated. The result is a
//! `manifest.json` (committed) plus EPUBs under the output directory (not committed).
//!
//!   cargo run --release --example corpus_fetch -- corpus/manifest.json test-data/corpus
//!
//! If the manifest already exists it is reused as-is, so the evaluated set never
//! drifts; delete it to rebuild.

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::PathBuf, time::Duration};

const API: &str = "https://gutendex.com/books/";

struct Stratum {
    name: &'static str,
    query: &'static [(&'static str, &'static str)],
    take: usize,
    filter: fn(&Value) -> bool,
}

fn any(_: &Value) -> bool {
    true
}
fn translated(book: &Value) -> bool {
    book["translators"]
        .as_array()
        .is_some_and(|t| !t.is_empty())
}
fn multi_author(book: &Value) -> bool {
    book["authors"].as_array().is_some_and(|a| a.len() >= 2)
}
fn single_language(book: &Value) -> bool {
    book["languages"].as_array().is_some_and(|l| l.len() == 1)
}

const STRATA: &[Stratum] = &[
    Stratum {
        name: "en_popular",
        query: &[("languages", "en")],
        take: 30,
        filter: single_language,
    },
    Stratum {
        name: "fr",
        query: &[("languages", "fr")],
        take: 10,
        filter: single_language,
    },
    Stratum {
        name: "de",
        query: &[("languages", "de")],
        take: 10,
        filter: single_language,
    },
    Stratum {
        name: "es",
        query: &[("languages", "es")],
        take: 8,
        filter: single_language,
    },
    Stratum {
        name: "it",
        query: &[("languages", "it")],
        take: 6,
        filter: single_language,
    },
    Stratum {
        name: "pt",
        query: &[("languages", "pt")],
        take: 4,
        filter: single_language,
    },
    Stratum {
        name: "nl",
        query: &[("languages", "nl")],
        take: 4,
        filter: single_language,
    },
    Stratum {
        name: "fi",
        query: &[("languages", "fi")],
        take: 4,
        filter: single_language,
    },
    Stratum {
        name: "translated",
        query: &[("languages", "en")],
        take: 10,
        filter: translated,
    },
    Stratum {
        name: "multi_author",
        query: &[("languages", "en")],
        take: 6,
        filter: multi_author,
    },
    Stratum {
        name: "collections",
        query: &[("languages", "en"), ("search", "complete works")],
        take: 6,
        filter: any,
    },
    Stratum {
        name: "poetry",
        query: &[("languages", "en"), ("topic", "poetry")],
        take: 6,
        filter: single_language,
    },
    Stratum {
        name: "drama",
        query: &[("languages", "en"), ("topic", "drama")],
        take: 6,
        filter: single_language,
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let manifest_path = PathBuf::from(
        args.next()
            .context("usage: corpus_fetch MANIFEST OUT_DIR")?,
    );
    let out = PathBuf::from(
        args.next()
            .context("usage: corpus_fetch MANIFEST OUT_DIR")?,
    );
    let client = reqwest::Client::builder()
        .user_agent("book-manager corpus builder (local research)")
        .timeout(Duration::from_secs(90))
        .build()?;
    fs::create_dir_all(&out)?;

    let mut manifest: Value = if manifest_path.exists() {
        serde_json::from_slice(&fs::read(&manifest_path)?)?
    } else {
        json!({ "books": [] })
    };
    if manifest["books"].as_array().is_none_or(Vec::is_empty) {
        manifest["books"] = Value::Array(select(&client).await?);
        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
        println!("wrote {}", manifest_path.display());
    }

    let mut failures = 0;
    for book in manifest["books"].as_array_mut().context("books")? {
        let id = book["id"].as_u64().context("id")?;
        let target = out.join(format!("{id}.epub"));
        if !target.exists() {
            let url = book["epub_url"].as_str().context("epub_url")?.to_owned();
            match download(&client, &url).await {
                Ok(bytes) => fs::write(&target, bytes)?,
                Err(error) => {
                    eprintln!("FAILED {id}: {error:#}");
                    failures += 1;
                    continue;
                }
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        let digest = format!("{:x}", Sha256::digest(fs::read(&target)?));
        book["sha256"] = json!(digest);
    }
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    println!(
        "done: {} books, {failures} download failures",
        manifest["books"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}

async fn download(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    for attempt in 0..3 {
        match client.get(url).send().await {
            Ok(response) if response.status().is_success() => {
                let bytes = response.bytes().await?;
                ensure!(bytes.starts_with(b"PK"), "not a ZIP/EPUB");
                return Ok(bytes.to_vec());
            }
            Ok(response) if attempt == 2 => bail!("HTTP {}", response.status()),
            Err(error) if attempt == 2 => return Err(error.into()),
            _ => tokio::time::sleep(Duration::from_secs(2 << attempt)).await,
        }
    }
    unreachable!()
}

async fn get_json(client: &reqwest::Client, url: &str) -> Result<Value> {
    let mut last = None;
    for attempt in 0..4 {
        match client.get(url).send().await {
            Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                return Ok(Value::Null); // past the last page
            }
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json().await {
                    Ok(value) => return Ok(value),
                    Err(error) => last = Some(error),
                },
                Err(error) => last = Some(error),
            },
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_secs(3 << attempt)).await;
    }
    Err(last.expect("an error was recorded").into())
}

async fn select(client: &reqwest::Client) -> Result<Vec<Value>> {
    let mut chosen: Vec<Value> = Vec::new();
    let mut seen = BTreeSet::new();
    // Holdout sets start deeper in each listing and skip books already in another manifest.
    for path in std::env::var("CORPUS_EXCLUDE")
        .unwrap_or_default()
        .split(',')
        .filter(|p| !p.is_empty())
    {
        let other: Value = serde_json::from_slice(&fs::read(path)?)?;
        seen.extend(
            other["books"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|b| b["id"].as_u64()),
        );
    }
    let start_page = std::env::var("CORPUS_START_PAGE").unwrap_or_else(|_| "1".into());
    for stratum in STRATA {
        let mut taken = 0;
        let take = stratum.take
            * std::env::var("CORPUS_MULT")
                .ok()
                .and_then(|m| m.parse().ok())
                .unwrap_or(1);
        let mut url = reqwest::Url::parse_with_params(API, stratum.query)?;
        url.query_pairs_mut().append_pair("page", &start_page);
        let mut url = url.to_string();
        for _ in 0..80 {
            if taken >= take {
                break;
            }
            let page = get_json(client, &url).await?;
            let Some(results) = page["results"].as_array() else {
                break; // past the last page
            };
            for book in results {
                let id = book["id"].as_u64().context("id")?;
                let epub = book["formats"]["application/epub+zip"].as_str();
                let Some(epub) = epub else { continue };
                if taken >= take || seen.contains(&id) || !(stratum.filter)(book) {
                    continue;
                }
                if book["authors"].as_array().is_none_or(Vec::is_empty) {
                    continue;
                }
                seen.insert(id);
                taken += 1;
                chosen.push(json!({
                    "id": id,
                    "stratum": stratum.name,
                    "title": book["title"],
                    "authors": book["authors"].as_array().map(|a| a.iter().map(|x| x["name"].clone()).collect::<Vec<_>>()),
                    "translators": book["translators"].as_array().map(|a| a.iter().map(|x| x["name"].clone()).collect::<Vec<_>>()),
                    "languages": book["languages"],
                    "epub_url": epub,
                }));
            }
            match page["next"].as_str() {
                Some(next) => url = next.to_owned(),
                None => break,
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        println!("{}: {taken}/{take}", stratum.name);
    }
    Ok(chosen)
}
