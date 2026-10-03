use crate::{
    cover::{self, Comparison},
    epub::{Metadata, normalize_isbn},
    google::Volume,
    language::{self, Assessment},
    provider::{Provider, Query},
    text::{credit_names_author, normalized, same_author, split_title, title_similarity},
};
use anyhow::Result;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug, Serialize)]
pub struct WorkIdentity {
    pub title: String,
    pub authors: Vec<String>,
    pub language: Option<String>,
    pub confidence: &'static str,
    pub evidence: Vec<String>,
    pub supporting_google_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Edition {
    pub status: String,
    pub google_id: Option<String>,
    pub identifiers: Vec<String>,
    pub publisher: Option<String>,
    pub published_date: Option<String>,
    pub language: Option<String>,
    pub is_ebook: Option<bool>,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Candidate {
    pub google_id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub authors: Vec<String>,
    pub language: Option<String>,
    pub publisher: Option<String>,
    pub published_date: Option<String>,
    pub is_ebook: Option<bool>,
    pub isbn_agrees: bool,
    pub found_by_title_author: bool,
    pub title_similarity: f64,
    pub content_agrees: bool,
    pub cover: Option<Comparison>,
    pub details_loaded: bool,
    pub eligible: bool,
    pub edition_supported: bool,
    pub edition_conflicts: Vec<String>,
    pub evidence: String,
    pub accepted: bool,
    #[serde(skip)]
    pub cover_url: Option<String>,
}

impl Candidate {
    /// A candidate rebuilt from a stored result: only what reporting and cover
    /// download need.
    pub fn stored(id: String, evidence: String, cover_url: Option<String>, accepted: bool) -> Self {
        Self {
            google_id: id,
            title: String::new(),
            subtitle: None,
            authors: Vec::new(),
            language: None,
            publisher: None,
            published_date: None,
            is_ebook: None,
            isbn_agrees: false,
            found_by_title_author: false,
            title_similarity: 0.0,
            content_agrees: false,
            cover: None,
            details_loaded: false,
            eligible: accepted,
            edition_supported: false,
            edition_conflicts: Vec::new(),
            evidence,
            accepted,
            cover_url,
        }
    }
}

#[derive(Debug)]
pub struct Identification {
    pub work: Option<WorkIdentity>,
    pub edition: Edition,
    pub language: Assessment,
    pub candidates: Vec<Candidate>,
    pub warnings: Vec<String>,
    pub reason: String,
}

/// For a book that no provider has a record of: the file's own package tags and its
/// own printed title page name the same work and author, its language is confirmed by
/// the text, and nothing marks it as a bundle or as contradicting itself. That is the
/// book's own consistent statement of what it is; it is labelled as such and no
/// edition fields are ever written. Callers use it only when every provider answered.
pub fn self_consistent(metadata: &Metadata, candidates: &[Candidate]) -> Option<Identification> {
    let content = &metadata.content;
    // A provider record with this very title under a different author is a competing
    // identity (a misspelled author, a different book): the file may not name itself
    // against it.
    let competing = candidates
        .iter()
        .any(|c| title_similarity(&metadata.title, &c.title) >= 0.92);
    if competing
        || content.title_author_documents.is_empty()
        || !content.identity_conflicts.is_empty()
        || !metadata.collection_signals.is_empty()
        || content.language.status != "confirmed"
        || normalized(&metadata.title).is_empty()
        || metadata.authors.is_empty()
    {
        return None;
    }
    let authors: Vec<String> = metadata
        .authors
        .iter()
        .map(|author| tidy(&display_author(author, metadata)))
        .collect();
    Some(Identification {
        work: Some(WorkIdentity {
            title: tidy(&split_title(metadata.title.trim()).title),
            authors,
            language: content.language.language.clone(),
            confidence: "self_consistent",
            evidence: vec![
                "Package tags and the book's own title page name the same title and author".into(),
                "No provider has a matching record, so no edition information is written".into(),
            ],
            supporting_google_ids: Vec::new(),
        }),
        edition: Edition {
            status: "unknown".into(),
            ..Default::default()
        },
        language: content.language.clone(),
        candidates: Vec::new(),
        warnings: metadata.warnings.clone(),
        reason: "Work named from the file's own tags and title page; no provider has a record"
            .into(),
    })
}

/// A file whose own title says it is a collection is named by that title: there is no
/// single work to look up, and calling it by what it declares is not a guess.
fn declared_collection_identification(metadata: &Metadata) -> Identification {
    // Only the first creator: bundles often carry a publisher or a catalogue code
    // as an extra "author", and none of that belongs in a name.
    let authors: Vec<String> = metadata
        .authors
        .iter()
        .take(1)
        .map(|author| tidy(&display_author(author, metadata)))
        .collect();
    let signals = metadata.collection_signals.join("; ");
    Identification {
        work: Some(WorkIdentity {
            title: tidy(metadata.title.trim()),
            authors,
            language: metadata.content.language.language.clone(),
            confidence: "declared_collection",
            evidence: vec![format!("The title declares a collection ({signals})")],
            supporting_google_ids: Vec::new(),
        }),
        edition: Edition {
            status: "unknown".into(),
            ..Default::default()
        },
        language: metadata.content.language.clone(),
        candidates: Vec::new(),
        warnings: metadata.warnings.clone(),
        reason: format!("Collection named by its own declared title ({signals})"),
    }
}

fn review(metadata: &Metadata, reason: &str) -> Identification {
    Identification {
        work: None,
        edition: Edition {
            status: "unknown".into(),
            ..Default::default()
        },
        language: metadata.content.language.clone(),
        candidates: Vec::new(),
        warnings: metadata.warnings.clone(),
        reason: reason.to_owned(),
    }
}

pub async fn identify(google: &Provider, metadata: &Metadata) -> Result<Identification> {
    identify_with(google, &[], metadata).await
}

/// Like `identify`, with further providers consulted only as full-text witnesses
/// (their own records are never used to name the book).
pub async fn identify_with(
    google: &Provider,
    witnesses: &[Provider],
    metadata: &Metadata,
) -> Result<Identification> {
    if normalized(&metadata.title).is_empty()
        || metadata.authors.is_empty()
        || metadata
            .authors
            .iter()
            .any(|name| normalized(name).is_empty())
    {
        return Ok(review(
            metadata,
            "A usable title and author are both required; an ISBN alone cannot establish the work",
        ));
    }
    // A title page that disagrees with the tags is not the end: it may be a spelling
    // or edition variant. Neither the page text nor the cover may then be used to
    // accept the book, but the book's own text, found in a full-text index under the
    // tagged title and author, still can.
    let conflicted = !metadata.content.identity_conflicts.is_empty();
    // A title that says it is a collection ("Complete Works", a list of several
    // works) is named as that collection below. Structural signs alone (chapter
    // numbering restarting, repeated bylines) do not decide anything by themselves,
    // since long single works show them too, but they forbid accepting the file as
    // one work on weaker evidence: only the book's own text, all of it, may do that.
    let declared_collection = metadata
        .collection_signals
        .iter()
        .any(|s| s.starts_with("title says") || s.starts_with("title lists"));
    let structural_only = !metadata.collection_signals.is_empty() && !declared_collection;
    // A claimed collection that was not corroborated above is treated like a bundle:
    // only the book's own text may accept it.
    let structural_only = structural_only || declared_collection;
    if metadata.content.language.conflicted() {
        return Ok(review(
            metadata,
            "Detected text language conflicts with declared language, or multiple languages need review",
        ));
    }
    if metadata.content.language.language.is_none() {
        return Ok(review(
            metadata,
            "Book language is unknown; neither reliable text detection nor a valid declaration is available",
        ));
    }
    // A file may not simply claim to be a collection: nothing in it may contradict the
    // claim, and either its own title page agrees or its contents really are a bundle
    // (numbering restarting, a byline per work). Otherwise it is looked up like any
    // other book, where tags that contradict the text are refused.
    if declared_collection {
        let page_agrees = !metadata.content.title_author_documents.is_empty();
        let looks_bundled = metadata
            .collection_signals
            .iter()
            .any(|s| s.starts_with("chapter numbering") || s.starts_with("contents repeat"));
        // For bundled contents the title and the author must each be shown somewhere
        // in the book's own front text; the title page agreeing already covers both.
        let author_named = metadata.authors.first().is_some_and(|author| {
            metadata.content.documents.iter().any(|doc| {
                doc.lines
                    .iter()
                    .chain(&doc.bylines)
                    .chain(&doc.headings)
                    .any(|line| credit_names_author(author, line))
            })
        });
        let title_named = metadata
            .content
            .documents
            .iter()
            .any(|doc| crate::text::shows_title(&metadata.title, doc));
        if !conflicted && (page_agrees || (looks_bundled && author_named && title_named)) {
            return Ok(declared_collection_identification(metadata));
        }
    }

    let query = Query::TitleAuthor {
        title: &metadata.title,
        author: &metadata.authors[0],
    };
    let mut volumes = google.search(query).await?;
    let first_page_full = volumes.len() == 40;
    let mut text_ids: HashSet<_> = volumes.iter().map(|volume| volume.id.clone()).collect();
    for isbn in metadata.isbns.iter().take(4) {
        volumes.extend(google.search(Query::Isbn(isbn)).await?);
    }
    if !volumes
        .iter()
        .any(|volume| eligible(metadata, volume).is_ok())
        && first_page_full
    {
        let page = google.search_page(query, 40).await?;
        text_ids.extend(page.iter().map(|volume| volume.id.clone()));
        volumes.extend(page);
    }
    if !volumes
        .iter()
        .any(|volume| eligible(metadata, volume).is_ok())
    {
        let alternate = Query::LooseTitle {
            title: &metadata.title,
            author: &metadata.authors[0],
            lang: metadata.content.language.language.as_deref(),
        };
        let alternatives = google.search(alternate).await?;
        text_ids.extend(alternatives.iter().map(|volume| volume.id.clone()));
        volumes.extend(alternatives);
    }
    volumes.sort_by(|a, b| a.id.cmp(&b.id));
    volumes.dedup_by(|a, b| a.id == b.id);

    // Page text and covers may not accept a book that contradicts its own tags or
    // looks like a bundle; only the book's own text may.
    let strict = conflicted || structural_only;
    let body_agrees = !strict && !metadata.content.title_author_documents.is_empty();
    let mut shortlist: Vec<_> = volumes
        .iter()
        .enumerate()
        .filter(|(_, volume)| eligible(metadata, volume).is_ok())
        .map(|(index, volume)| {
            let priority = u32::from(isbn_agrees(metadata, volume)) * 100
                + u32::from(publisher_agrees(metadata, volume)) * 10
                + u32::from(date_agrees(metadata, volume)) * 5
                + u32::from(volume.info.image_links.cover_url().is_some());
            (index, priority)
        })
        .collect();
    shortlist.sort_by_key(|candidate| std::cmp::Reverse(candidate.1));
    let mut detailed = HashSet::new();
    let mut comparisons = HashMap::new();
    let mut warnings = metadata.warnings.clone();
    if metadata.content.language.status == "corrected" {
        warnings.push(format!(
            "Declared language {:?} disagrees with strongly detected {}; the detected language is used",
            metadata.content.language.declared,
            metadata.content.language.language.as_deref().unwrap_or("?")
        ));
    }
    for &(index, _) in shortlist.iter().take(4) {
        let id = volumes[index].id.clone();
        match google.volume(&id).await {
            Ok(mut volume) => {
                if volume.info.authors.is_empty() {
                    volume.info.authors = volumes[index].info.authors.clone();
                }
                if volume.info.language.is_none() {
                    volume.info.language = volumes[index].info.language.clone();
                }
                if volume.info.image_links.cover_url().is_none() {
                    volume.info.image_links = volumes[index].info.image_links.clone();
                }
                volumes[index] = volume;
                detailed.insert(id.clone());
            }
            Err(error) => warnings.push(format!("Volume details unavailable for {id}: {error:#}")),
        }
        let volume = &volumes[index];
        if eligible(metadata, volume).is_err() {
            continue;
        }
        let need_cover = !body_agrees || isbn_agrees(metadata, volume);
        if need_cover
            && let (Some(local), Some(url)) =
                (metadata.cover.as_ref(), volume.info.image_links.cover_url())
        {
            match google.cover(url).await {
                Ok(remote) => {
                    comparisons.insert(id, cover::compare(local, &remote));
                }
                Err(error) => warnings.push(format!("Cover unavailable for {id}: {error:#}")),
            }
        }
    }
    // Nothing in the file itself corroborates the work (no title page text, no cover
    // match), yet a provider record agrees with the tags. Ask a full-text index which
    // book the file's own text belongs to: a passage that is found under this title
    // and author is evidence that does not depend on the tags at all.
    let mut witness = false;
    let any_eligible = volumes
        .iter()
        .any(|volume| eligible(metadata, volume).is_ok());
    if !body_agrees
        && any_eligible
        && (strict || !comparisons.values().any(|comparison| comparison.strong))
    {
        let passages: Vec<&String> = metadata.content.passages.iter().take(3).collect();
        // For a possible bundle every passage must be found under this one title: a
        // passage from the first work alone would also be found, and prove nothing
        // about the rest. At least two passages are needed.
        let mut found_count = 0;
        'passages: for passage in &passages {
            // Any witness may confirm a passage: Google's text search first, then the
            // others (OpenLibrary's covers different scans).
            let others = witnesses.iter().filter(|w| w.name() != google.name());
            for source in std::iter::once(google).chain(others) {
                match source.search(Query::Passage(passage)).await {
                    Ok(found) => {
                        if found
                            .iter()
                            .any(|volume| eligible(metadata, volume).is_ok())
                        {
                            found_count += 1;
                            if !structural_only {
                                break 'passages;
                            }
                            continue 'passages;
                        }
                    }
                    Err(error) => warnings.push(format!(
                        "{} text search unavailable: {error:#}",
                        source.name()
                    )),
                }
            }
        }
        witness = if structural_only {
            passages.len() >= 2 && found_count == passages.len()
        } else {
            found_count > 0
        };
    }
    let mut result = select(
        metadata,
        volumes,
        &text_ids,
        &detailed,
        comparisons,
        warnings,
        Corroboration {
            witness,
            conflicted: strict,
        },
    );
    if result.work.is_none() {
        if conflicted {
            result.reason =
                "Book text conflicts with package title/author; manual review required".into();
        } else if structural_only {
            result.reason = format!(
                "Looks like an omnibus or collection ({}); a single work cannot be named",
                metadata.collection_signals.join("; ")
            );
        }
    }
    Ok(result)
}

pub fn query_text(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c == '"' || c.is_control() {
                ' '
            } else if matches!(c, '\u{2019}' | '\u{2018}') {
                '\''
            } else {
                c
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// "The Iliad of Homer" is "The Iliad": a title may end with " of" or " by" and the
/// author's own name. Only the file's own author counts, so "Life of Pi" stays itself.
fn without_author_suffix(title: &str, authors: &[String]) -> Option<String> {
    let normal = normalized(title);
    for author in authors {
        let key = normalized(author);
        let surname = key.split_whitespace().last().unwrap_or("").to_owned();
        for name in [key.as_str(), surname.as_str()] {
            if name.is_empty() {
                continue;
            }
            for joiner in [" of ", " by "] {
                if let Some(stripped) = normal.strip_suffix(&format!("{joiner}{name}"))
                    && !stripped.trim().is_empty()
                {
                    return Some(stripped.to_owned());
                }
            }
        }
    }
    None
}

fn title_score(metadata: &Metadata, volume: &Volume) -> f64 {
    let info = &volume.info;
    let mut score = title_similarity(&metadata.title, &info.title);
    if let Some(plain) = without_author_suffix(&info.title, &metadata.authors) {
        score = score.max(title_similarity(&metadata.title, &plain));
    }
    if let Some(subtitle) = &info.subtitle {
        score = score.max(title_similarity(
            &metadata.title,
            &format!("{}: {subtitle}", info.title),
        ));
        let local_words: HashSet<_> = normalized(&metadata.title)
            .split_whitespace()
            .filter(|word| !["a", "an", "the", "in", "of", "and"].contains(word))
            .map(str::to_owned)
            .collect();
        let remote_words: HashSet<_> = normalized(&info.title)
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if local_words.intersection(&remote_words).count() >= 2 {
            score = score
                .max(title_similarity(&metadata.title, subtitle.trim_matches(['(', ')'])) * 0.97);
        }
    }
    score
}

fn eligible(metadata: &Metadata, volume: &Volume) -> std::result::Result<f64, String> {
    let info = &volume.info;
    let expected_language = metadata.content.language.language.as_deref();
    if let Some(remote) = info.language.as_deref().and_then(language::normalize)
        && Some(remote.as_str()) != expected_language
    {
        return Err("Provider language differs from the book's language".into());
    }
    if !metadata.authors.iter().all(|author| {
        info.authors
            .iter()
            .chain(&info.author_aliases)
            .any(|other| same_author(author, other))
    }) {
        return Err("Author differs; an ISBN cannot override the conflict".into());
    }
    let local = format!(" {} ", normalized(&metadata.title));
    let remote = format!(
        " {} ",
        normalized(&format!(
            "{} {}",
            info.title,
            info.subtitle.as_deref().unwrap_or("")
        ))
    );
    for marker in [
        "omnibus",
        "collection",
        "box set",
        "boxed set",
        "complete works",
        "study guide",
        "summary",
        "workbook",
        "abridged",
        "adapted",
        "adaptation",
        "retold",
        "retelling",
        "simplified",
    ] {
        let marker = format!(" {marker} ");
        if remote.contains(&marker) && !local.contains(&marker) {
            return Err("Collection, adaptation, or companion-book wording is not supported by local evidence".into());
        }
    }
    if info.subtitle.as_deref().is_some_and(|subtitle| {
        let subtitle = subtitle.trim().to_lowercase();
        (subtitle.starts_with('&')
            || subtitle.starts_with("and ")
            || subtitle.starts_with("including "))
            && !normalized(&metadata.title).contains(&normalized(&subtitle))
    }) {
        return Err(
            "Provider record combines additional works not present in the local title".into(),
        );
    }
    let score = title_score(metadata, volume);
    if score < 0.92 {
        return Err("Title differs; no supported title variant matches".into());
    }
    length_conflict(metadata, volume)?;
    Ok(score)
}

/// Compares the book's own length (words / 275 is roughly a page) with the page
/// count the provider records for the work. A veto only, with wide margins because
/// editions differ a lot: far longer than the work suggests a bundled collection,
/// far shorter suggests a sample or excerpt.
fn length_conflict(metadata: &Metadata, volume: &Volume) -> std::result::Result<(), String> {
    let Some(pages) = volume.info.page_count.filter(|pages| *pages >= 40) else {
        return Ok(());
    };
    if metadata.words == 0 {
        return Ok(());
    }
    let pages = u64::from(pages);
    let estimated = metadata.words / 275;
    // Page counts mix abridged and children's editions, so a mismatch alone proves
    // little; the file must also be longer than almost any single novel (~500k words).
    if estimated > pages * 3 + 100 && estimated > 1_800 {
        return Err(format!(
            "The file is about {estimated} pages but the work has about {pages}: it looks like a collection"
        ));
    }
    if pages >= 150 && estimated * 7 < pages {
        return Err(format!(
            "The file is about {estimated} pages but the work has about {pages}: it looks like a sample or excerpt"
        ));
    }
    Ok(())
}

fn isbns(volume: &Volume) -> Vec<String> {
    let mut values: Vec<_> = volume
        .info
        .industry_identifiers
        .iter()
        .filter(|id| matches!(id.kind.as_str(), "ISBN_10" | "ISBN_13"))
        .filter_map(|id| normalize_isbn(&id.identifier))
        .collect();
    values.sort();
    values.dedup();
    values
}

fn isbn_agrees(metadata: &Metadata, volume: &Volume) -> bool {
    isbns(volume)
        .iter()
        .any(|isbn| metadata.isbns.contains(isbn))
}

fn publisher_agrees(metadata: &Metadata, volume: &Volume) -> bool {
    metadata
        .publisher
        .as_deref()
        .zip(volume.info.publisher.as_deref())
        .is_some_and(|(a, b)| !normalized(a).is_empty() && normalized(a) == normalized(b))
}

fn year(value: &str) -> Option<&str> {
    let year = value.get(..4)?;
    year.chars().all(|c| c.is_ascii_digit()).then_some(year)
}

fn date_agrees(metadata: &Metadata, volume: &Volume) -> bool {
    metadata
        .published_date
        .as_deref()
        .and_then(year)
        .zip(volume.info.published_date.as_deref().and_then(year))
        .is_some_and(|(a, b)| a == b)
}

fn edition_conflicts(
    metadata: &Metadata,
    volume: &Volume,
    comparison: Option<&Comparison>,
) -> Vec<String> {
    let mut conflicts = Vec::new();
    if metadata.publisher.is_some()
        && volume.info.publisher.is_some()
        && !publisher_agrees(metadata, volume)
    {
        conflicts.push("publisher differs".into());
    }
    if metadata.published_date.as_deref().and_then(year).is_some()
        && volume
            .info
            .published_date
            .as_deref()
            .and_then(year)
            .is_some()
        && !date_agrees(metadata, volume)
    {
        conflicts.push("publication year differs".into());
    }
    if volume.sale_info.is_ebook == Some(false) {
        conflicts.push("Google does not identify this record as an ebook".into());
    }
    if comparison.is_some_and(|cover| cover.informative && !cover.strong) {
        conflicts.push("cover artwork differs; edition remains uncertain".into());
    }
    conflicts
}

/// Evidence beyond the file's own pages: whether the book's text was found under this
/// title and author in a full-text index, and whether its title page disagreed with
/// the tags (in which case page text and cover may not be used to accept it).
struct Corroboration {
    witness: bool,
    conflicted: bool,
}

fn select(
    metadata: &Metadata,
    volumes: Vec<Volume>,
    text_ids: &HashSet<String>,
    detailed: &HashSet<String>,
    mut comparisons: HashMap<String, Comparison>,
    warnings: Vec<String>,
    extra: Corroboration,
) -> Identification {
    let Corroboration {
        witness,
        conflicted,
    } = extra;
    let content_agrees = !conflicted && !metadata.content.title_author_documents.is_empty();
    let mut candidates = Vec::new();
    for volume in &volumes {
        let eligibility = eligible(metadata, volume);
        let cover = comparisons.remove(&volume.id);
        let cover_agrees = !conflicted && cover.as_ref().is_some_and(|cover| cover.strong);
        let accepted = eligibility.is_ok() && (content_agrees || cover_agrees || witness);
        let isbn_agrees = isbn_agrees(metadata, volume);
        let conflicts = edition_conflicts(metadata, volume, cover.as_ref());
        let edition_supported = accepted
            && isbn_agrees
            && detailed.contains(&volume.id)
            && volume.sale_info.is_ebook == Some(true)
            && conflicts.is_empty()
            && ((publisher_agrees(metadata, volume) && date_agrees(metadata, volume))
                || (cover_agrees
                    && (publisher_agrees(metadata, volume) || date_agrees(metadata, volume))));
        let evidence = match &eligibility {
            Err(reason) => reason.clone(),
            Ok(_) if content_agrees => "Title/author agree with visible book text and Google; cover is not required for work identity".into(),
            Ok(_) if cover_agrees => "Title/author and actual cover corroborate the work; edition is assessed separately".into(),
            Ok(_) if witness => "Title/author agree with the provider, and the book's own text was found in its full-text index under this title and author".into(),
            _ => "Metadata alone is insufficient: no corroborating title-page/author text or strong cover match".into(),
        };
        candidates.push(Candidate {
            google_id: volume.id.clone(),
            title: volume.info.title.clone(),
            subtitle: volume.info.subtitle.clone(),
            authors: volume.info.authors.clone(),
            language: volume.info.language.clone(),
            publisher: volume.info.publisher.clone(),
            published_date: volume.info.published_date.clone(),
            is_ebook: volume.sale_info.is_ebook,
            isbn_agrees,
            found_by_title_author: text_ids.contains(&volume.id),
            title_similarity: eligibility.as_ref().copied().unwrap_or(0.0),
            content_agrees,
            cover,
            details_loaded: detailed.contains(&volume.id),
            eligible: eligibility.is_ok(),
            edition_supported,
            edition_conflicts: conflicts,
            evidence,
            accepted,
            cover_url: volume.info.image_links.cover_url().map(str::to_owned),
        });
    }
    let accepted: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.accepted)
        .collect();
    let edition = assess_edition(metadata, &volumes, &candidates);
    let work = if accepted.is_empty() {
        None
    } else {
        let mut evidence = if content_agrees {
            metadata
                .content
                .title_author_documents
                .iter()
                .map(|path| format!("Visible title and author: {path}"))
                .collect::<Vec<_>>()
        } else {
            if witness {
                vec!["The book's own text was found in the provider's full-text index under this title and author".into()]
            } else {
                vec!["Local cover and bibliographic metadata agree with a Google candidate".into()]
            }
        };
        evidence.push(format!(
            "Language: {} ({})",
            metadata
                .content
                .language
                .language
                .as_deref()
                .unwrap_or("unknown"),
            metadata.content.language.status
        ));
        if !metadata.isbns.is_empty() && !accepted.iter().any(|candidate| candidate.isbn_agrees) {
            evidence.push("Local ISBN not corroborated; ignored for work identity and not used to assign an edition".into());
        }
        // The local text stays the identity. Only when the local form is all
        // capitals or all lower case, and an accepted provider record spells the
        // very same words in normal case, is the provider's spelling adopted.
        let title = {
            let local = metadata.title.trim();
            let title = accepted
                .iter()
                .find(|c| {
                    mono_case(local)
                        && !mono_case(&c.title)
                        && normalized(&c.title) == normalized(local)
                })
                .map_or_else(|| local.to_owned(), |c| c.title.trim().to_owned());
            tidy(&title)
        };
        let authors = metadata
            .authors
            .iter()
            .map(|author| {
                let shown = tidy(&display_author(author, metadata));
                if !mono_case(&shown) {
                    return shown;
                }
                // Same words in normal case, or the provider's fuller or shorter
                // spelling of the same person (compared as local evidence only).
                let names = accepted.iter().flat_map(|c| &c.authors);
                names
                    .clone()
                    .find(|name| !mono_case(name) && same_author(&shown, name))
                    .or_else(|| {
                        names
                            .clone()
                            .find(|name| !mono_case(name) && credit_names_author(&shown, name))
                    })
                    .cloned()
                    .unwrap_or(shown)
            })
            .collect();
        Some(WorkIdentity {
            title,
            authors,
            language: metadata.content.language.language.clone(),
            confidence: "corroborated_work",
            evidence,
            supporting_google_ids: accepted
                .iter()
                .map(|candidate| candidate.google_id.clone())
                .collect(),
        })
    };
    let reason = if let Some(work) = &work {
        let isbn_note = if !metadata.isbns.is_empty()
            && !accepted.iter().any(|candidate| candidate.isbn_agrees)
        {
            "; uncorroborated ISBN ignored"
        } else {
            ""
        };
        format!(
            "Work identified: {}{}; edition {} (not inferred from title/author alone)",
            work.title, isbn_note, edition.status
        )
    } else {
        "No corroborated work match; conflicting or insufficient evidence requires review".into()
    };
    Identification {
        work,
        edition,
        language: metadata.content.language.clone(),
        candidates,
        warnings,
        reason,
    }
}

/// Drops stray trailing punctuation and spaces ("Oscar Wilde ." -> "Oscar Wilde")
/// while keeping the full stop that ends an initial or an abbreviation ("Jr.").
fn tidy(value: &str) -> String {
    let trimmed = value.trim().trim_end_matches([' ', ',', ';', ':']);
    let Some(body) = trimmed.strip_suffix('.') else {
        return trimmed.to_owned();
    };
    let body = body.trim_end();
    let last = body
        .rsplit(|c: char| c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_lowercase();
    let abbreviation = last.chars().count() <= 1
        || [
            "jr", "sr", "st", "dr", "mr", "mrs", "ms", "etc", "vs", "no", "vol",
        ]
        .contains(&last.as_str());
    if abbreviation && body.len() == trimmed.len() - 1 {
        trimmed.to_owned()
    } else if abbreviation {
        format!("{body}.")
    } else {
        body.to_owned()
    }
}

/// True for text written entirely in capitals or entirely in lower case.
fn mono_case(value: &str) -> bool {
    let letters: Vec<char> = value.chars().filter(|c| c.is_alphabetic()).collect();
    letters.len() >= 2
        && (letters.iter().all(|c| c.is_uppercase()) || letters.iter().all(|c| c.is_lowercase()))
}

fn display_author(author: &str, metadata: &Metadata) -> String {
    let comma_form = author.contains(',');
    // A properly cased "First Last" local spelling is kept exactly as written, so
    // accents and punctuation are never replaced by a title page's plainer text.
    if !comma_form && !mono_case(author) {
        return author.to_owned();
    }
    for document in &metadata.content.documents {
        for line in document.bylines.iter().chain(&document.lines) {
            if same_author(author, line) && !line.contains(',') && !mono_case(line) {
                return line.clone();
            }
        }
    }
    let parts: Vec<_> = author.split(',').map(str::trim).collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        format!("{} {}", parts[1], parts[0])
    } else {
        author.to_owned()
    }
}

fn assess_edition(metadata: &Metadata, volumes: &[Volume], candidates: &[Candidate]) -> Edition {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, candidate) in candidates
        .iter()
        .enumerate()
        .filter(|(_, c)| c.edition_supported)
    {
        let mut matched: Vec<_> = isbns(&volumes[index])
            .into_iter()
            .filter(|isbn| metadata.isbns.contains(isbn))
            .collect();
        matched.sort();
        let key = format!(
            "{}|{}|{}|{}",
            matched.join(","),
            normalized(candidate.publisher.as_deref().unwrap_or("")),
            candidate
                .published_date
                .as_deref()
                .and_then(year)
                .unwrap_or(""),
            candidate
                .language
                .as_deref()
                .and_then(language::normalize)
                .unwrap_or_default()
        );
        groups.entry(key).or_default().push(index);
    }
    if groups.len() == 1 {
        let index = groups.values().next().unwrap()[0];
        let candidate = &candidates[index];
        return Edition {
            status: "supported".into(), google_id: Some(candidate.google_id.clone()),
            identifiers: isbns(&volumes[index]).into_iter().filter(|isbn| metadata.isbns.contains(isbn)).collect(),
            publisher: candidate.publisher.clone(), published_date: candidate.published_date.clone(),
            language: candidate.language.clone(), is_ebook: candidate.is_ebook,
            evidence: vec!["ISBN, full Google record and additional edition-specific evidence agree; this is not a guarantee of exact printing".into()],
        };
    }
    let conflicts: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.isbn_agrees)
        .filter(|candidate| !candidate.eligible || !candidate.edition_conflicts.is_empty())
        .map(|candidate| {
            format!(
                "{}: {} {}",
                candidate.google_id,
                candidate.evidence,
                candidate.edition_conflicts.join("; ")
            )
        })
        .collect();
    Edition {
        status: if groups.len() > 1 {
            "ambiguous"
        } else if !conflicts.is_empty() {
            "conflict"
        } else {
            "unknown"
        }
        .into(),
        evidence: if conflicts.is_empty() {
            vec!["No unique ebook edition has sufficient independent evidence; edition fields are left unset".into()]
        } else {
            conflicts
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_preserves_apostrophes_and_removes_quoted_field_injection() {
        assert_eq!(query_text("Alice’s Adventures"), "Alice's Adventures");
        assert_eq!(query_text("A \"Title\"\n"), "A Title");
    }

    #[test]
    fn unknown_edition_has_no_borrowed_identifiers_or_publication_fields() {
        let edition = assess_edition(&Metadata::default(), &[], &[]);
        assert_eq!(edition.status, "unknown");
        assert!(edition.google_id.is_none());
        assert!(edition.identifiers.is_empty());
        assert!(edition.publisher.is_none());
        assert!(edition.published_date.is_none());
    }
}

#[cfg(test)]
mod display_tests {
    use super::*;

    #[test]
    fn length_only_vetoes_extreme_mismatches() {
        let book = |words: u64| Metadata {
            words,
            ..Metadata::default()
        };
        let work = |pages: Option<u32>| Volume {
            id: "x".into(),
            info: crate::google::VolumeInfo {
                title: "T".into(),
                page_count: pages,
                ..Default::default()
            },
            sale_info: Default::default(),
        };
        // A normal novel (~300 pages, 82k words) against its own page count.
        assert!(length_conflict(&book(82_000), &work(Some(300))).is_ok());
        // Editions vary: a 250 page work as a 450 page large-print file is fine.
        assert!(length_conflict(&book(124_000), &work(Some(250))).is_ok());
        // A six-novel bundle (1.2M words) under one novel's title is vetoed.
        assert!(length_conflict(&book(1_200_000), &work(Some(500))).is_err());
        // Noisy provider data must not veto real long books: unabridged Monte Cristo
        // (~1,700 pages) against a median of 479, Crime and Punishment against 64.
        assert!(length_conflict(&book(464_000), &work(Some(479))).is_ok());
        assert!(length_conflict(&book(207_000), &work(Some(64))).is_ok());
        assert!(length_conflict(&book(587_000), &work(Some(1_225))).is_ok());
        // A 12k word sample of a 400 page work is vetoed.
        assert!(length_conflict(&book(12_000), &work(Some(400))).is_err());
        // No page count, no words, or a short work: no opinion.
        assert!(length_conflict(&book(1_200_000), &work(None)).is_ok());
        assert!(length_conflict(&book(0), &work(Some(500))).is_ok());
        assert!(length_conflict(&book(3_000), &work(Some(60))).is_ok());
    }

    #[test]
    fn tidy_removes_stray_punctuation_but_keeps_abbreviations() {
        assert_eq!(tidy("Oscar Wilde ."), "Oscar Wilde");
        assert_eq!(
            tidy("The picture of Dorian Gray."),
            "The picture of Dorian Gray"
        );
        assert_eq!(tidy("Martin Luther King Jr."), "Martin Luther King Jr.");
        assert_eq!(tidy("Ernest Thompson Seton, "), "Ernest Thompson Seton");
        assert_eq!(tidy("J."), "J.");
        assert_eq!(tidy("Plain Title"), "Plain Title");
    }
}
