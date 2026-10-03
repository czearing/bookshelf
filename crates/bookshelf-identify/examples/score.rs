//! Measures identification accuracy against the gold corpus.
//!
//!   cargo run --release --example score -- corpus/manifest.json test-data/corpus \
//!       --provider openlibrary --cache-dir test-data/ol-cache --out test-data/score.json
//!
//! Every book is scored twice: as shipped, and after its embedded title/author
//! are corrupted in four realistic ways. The one number that must be zero is
//! `wrong_auto`: a book the tool would rename to an identity that is not the
//! gold identity. Recall (`correct_auto`) is tracked separately and may improve
//! over time; precision may not regress.

use anyhow::{Context, Result};
use bookshelf_identify::{
    epub, google,
    matching::{self, Identification},
    provider::{OpenLibrary, Provider},
    tagwrite::{self, Tags},
    text::{credit_names_author, same_author, title_similarity},
};
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug)]
struct Gold {
    id: String,
    stratum: String,
    title: String,
    authors: Vec<String>,
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Verdict {
    /// Renamed to the gold identity.
    CorrectAuto,
    /// Renamed, author agrees, title is related but not clearly the same; a human decides.
    Adjudicate,
    /// Renamed to something that is not the gold identity. Must never happen.
    WrongAuto,
    Review,
    Error,
}

fn judge(gold: &Gold, result: &Identification) -> (Verdict, String) {
    let Some(work) = &result.work else {
        return (Verdict::Review, result.reason.clone());
    };
    // Catalogue names carry parenthetical expansions: "Forster, E. M. (Edward Morgan)".
    let gold_author = gold
        .authors
        .first()
        .map(|g| g.split('(').next().unwrap_or(g).trim().to_owned())
        .unwrap_or_default();
    let strict = work.authors.iter().any(|w| same_author(&gold_author, w));
    // A fuller or shorter spelling of the same person ("Bernard Shaw" vs "George
    // Bernard Shaw") is not a wrong book, but a human should confirm it.
    let loose = work
        .authors
        .iter()
        .any(|w| credit_names_author(&gold_author, w));
    let author_ok = strict || loose;
    // Catalogue titles keep MARC subfield markers: "Liebe ist ewig : $b Roman".
    let gold_title = gold
        .title
        .split('$')
        .next()
        .unwrap_or(&gold.title)
        .trim_end_matches([' ', ':', '/', ';']);
    let gold_title = bookshelf_identify::text::split_title(gold_title).title;
    let sim = title_similarity(&gold_title, &work.title);
    let verdict = if strict && sim >= 0.85 {
        Verdict::CorrectAuto
    } else if author_ok && sim >= 0.5 {
        Verdict::Adjudicate
    } else {
        Verdict::WrongAuto
    };
    (
        verdict,
        format!(
            "{} / {:?} (title similarity {sim:.2})",
            work.title, work.authors
        ),
    )
}

#[derive(Default)]
struct Tally(BTreeMap<String, [u32; 5]>);

impl Tally {
    fn add(&mut self, key: &str, verdict: Verdict) {
        let row = self.0.entry(key.to_owned()).or_default();
        row[match verdict {
            Verdict::CorrectAuto => 0,
            Verdict::Adjudicate => 1,
            Verdict::WrongAuto => 2,
            Verdict::Review => 3,
            Verdict::Error => 4,
        }] += 1;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let manifest: PathBuf = args
        .next()
        .context("usage: score MANIFEST CORPUS_DIR [options]")?
        .into();
    let corpus: PathBuf = args
        .next()
        .context("usage: score MANIFEST CORPUS_DIR [options]")?
        .into();
    let (mut provider_name, mut cache, mut out, mut offline, mut limit) = (
        "openlibrary".to_owned(),
        None::<PathBuf>,
        None::<PathBuf>,
        false,
        usize::MAX,
    );
    let mut rps = 4u32;
    let mut only_shipped = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--provider" => provider_name = args.next().context("--provider value")?,
            "--cache-dir" => cache = Some(args.next().context("--cache-dir value")?.into()),
            "--out" => out = Some(args.next().context("--out value")?.into()),
            "--offline" => offline = true,
            "--only-shipped" => only_shipped = true,
            "--rps" => rps = args.next().context("--rps value")?.parse()?,
            "--limit" => limit = args.next().context("--limit value")?.parse()?,
            other => anyhow::bail!("unknown option {other}"),
        }
    }
    let cache = cache.unwrap_or_else(|| PathBuf::from(format!("test-data/{provider_name}-cache")));
    let provider = match provider_name.as_str() {
        "google" => Provider::Google(google::Google::new(cache, offline, false, rps)?),
        "google-first" => Provider::Google(google::Google::new(cache, offline, false, rps)?),
        "openlibrary" => Provider::OpenLibrary(OpenLibrary::new(cache, offline, false, rps)?),
        other => anyhow::bail!("unknown provider {other}"),
    };

    let manifest: Value = serde_json::from_slice(&std::fs::read(&manifest)?)?;
    let golds: Vec<Gold> = manifest["books"]
        .as_array()
        .context("books")?
        .iter()
        .filter_map(|b| {
            let id = match &b["id"] {
                Value::Number(n) => n.to_string(),
                Value::String(text) => text.clone(),
                _ => return None,
            };
            let path = corpus.join(format!("{id}.epub"));
            path.exists().then(|| Gold {
                id,
                stratum: b["stratum"].as_str().unwrap_or("").to_owned(),
                title: b["title"].as_str().unwrap_or("").to_owned(),
                authors: b["authors"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|n| n.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                path,
            })
        })
        .take(limit)
        .collect();
    println!("{} books, provider {provider_name}", golds.len());

    let scratch = tempfile::tempdir()?;
    let scratch_path = scratch.path().to_owned();
    let donors: Vec<_> = golds.clone();
    let cases: Vec<_> = golds
        .iter()
        .enumerate()
        .flat_map(|(i, gold)| {
            let donor = donors[(i * 7 + 3) % donors.len()].clone();
            let donor = if donor.id == gold.id {
                donors[(i + 1) % donors.len()].clone()
            } else {
                donor
            };
            [
                ("as_shipped", None),
                (
                    "wrong_author",
                    Some((gold.title.clone(), donor.authors.clone())),
                ),
                (
                    "wrong_title",
                    Some((donor.title.clone(), gold.authors.clone())),
                ),
                (
                    "swapped_identity",
                    Some((donor.title.clone(), donor.authors.clone())),
                ),
                (
                    "author_unknown",
                    Some((gold.title.clone(), vec!["Unknown".to_owned()])),
                ),
            ]
            .into_iter()
            .map(move |(kind, mutation)| (gold.clone(), kind, mutation))
            .collect::<Vec<_>>()
        })
        .collect();

    let cases: Vec<_> = cases
        .into_iter()
        .filter(|(_, kind, _)| !only_shipped || *kind == "as_shipped")
        .collect();
    let results = stream::iter(cases.into_iter().map(|(gold, kind, mutation)| {
        let provider = provider.clone();
        let scratch = scratch_path.clone();
        async move {
            let path = match &mutation {
                None => gold.path.clone(),
                Some((title, authors)) => {
                    let dst = scratch.join(format!("{}-{kind}.epub", gold.id));
                    let tags = Tags {
                        title: title.clone(),
                        authors: authors.clone(),
                        ..Tags::default()
                    };
                    let (src, dst2) = (gold.path.clone(), dst.clone());
                    let made = tokio::task::spawn_blocking(move || {
                        tagwrite::write(&src, &dst2, &tags, None)
                    })
                    .await;
                    match made {
                        Ok(Ok(_)) => dst,
                        Ok(Err(error)) => {
                            return (
                                gold,
                                kind,
                                Verdict::Error,
                                format!("mutation failed: {error:#}"),
                            );
                        }
                        Err(error) => return (gold, kind, Verdict::Error, error.to_string()),
                    }
                }
            };
            let read_path = path.clone();
            let metadata = match tokio::task::spawn_blocking(move || epub::read(&read_path)).await {
                Ok(Ok((metadata, _))) => metadata,
                Ok(Err(error)) => return (gold, kind, Verdict::Error, format!("{error:#}")),
                Err(error) => return (gold, kind, Verdict::Error, error.to_string()),
            };
            match matching::identify(&provider, &metadata).await {
                Ok(mut result) => {
                    // The same last step the CLI takes when every provider answered and
                    // none has the book.
                    if result.work.is_none()
                        && let Some(named) =
                            matching::self_consistent(&metadata, &result.candidates)
                    {
                        result = named;
                    }
                    let (verdict, detail) = judge(&gold, &result);
                    (gold, kind, verdict, detail)
                }
                Err(error) => (gold, kind, Verdict::Error, format!("{error:#}")),
            }
        }
    }))
    .buffer_unordered(6)
    .collect::<Vec<_>>()
    .await;

    let mut by_kind = Tally::default();
    let mut by_stratum = Tally::default();
    let mut wrong = Vec::new();
    let mut adjudicate = Vec::new();
    let mut errors: BTreeMap<String, u32> = BTreeMap::new();
    let mut reviews: BTreeMap<String, u32> = BTreeMap::new();
    for (gold, kind, verdict, detail) in &results {
        by_kind.add(kind, *verdict);
        if *kind == "as_shipped" {
            by_stratum.add(&gold.stratum, *verdict);
        }
        let row = json!({ "id": gold.id, "kind": kind, "stratum": gold.stratum, "gold": gold.title, "result": detail });
        match verdict {
            Verdict::WrongAuto => wrong.push(row),
            Verdict::Adjudicate => adjudicate.push(row),
            Verdict::Error => {
                *errors
                    .entry(detail.chars().take(110).collect())
                    .or_default() += 1
            }
            Verdict::Review if *kind == "as_shipped" => {
                *reviews
                    .entry(detail.chars().take(150).collect())
                    .or_default() += 1
            }
            _ => {}
        }
    }

    let header = "correct_auto  adjudicate  WRONG_AUTO  review  error";
    for (title, tally) in [
        ("By case kind", &by_kind),
        ("By stratum (as shipped)", &by_stratum),
    ] {
        println!("\n{title}\n{:<22}{header}", "");
        for (key, r) in &tally.0 {
            println!(
                "{key:<22}{:>12}{:>12}{:>12}{:>8}{:>7}",
                r[0], r[1], r[2], r[3], r[4]
            );
        }
    }
    if !reviews.is_empty() {
        println!("\nWhy unmodified books went to review:");
        for (cause, n) in &reviews {
            println!("  {n:>3}  {cause}");
        }
    }
    if !errors.is_empty() {
        println!("\nError causes:");
        for (cause, n) in &errors {
            println!("  {n:>3}  {cause}");
        }
    }
    println!("\nWRONG auto-renames: {}", wrong.len());
    for w in &wrong {
        println!("  {w}");
    }
    println!("Needs human adjudication: {}", adjudicate.len());
    if let Some(out) = out {
        let report = json!({
            "provider": provider_name, "books": golds.len(),
            "wrong_auto": wrong, "adjudicate": adjudicate, "errors": errors, "review_reasons": reviews,
            "by_kind": by_kind.0, "by_stratum": by_stratum.0,
        });
        std::fs::write(out, serde_json::to_vec_pretty(&report)?)?;
    }
    if !wrong.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
