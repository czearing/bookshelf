use crate::{db, db::EditionRow, want};
use anyhow::Context;
use sqlx::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Public data types
// ---------------------------------------------------------------------------

/// One entry in a series (owned or a detected gap).
#[derive(Debug, Clone, Serialize)]
pub struct SeriesEntry {
    pub series_name: String,
    pub position: String,
    pub owned: bool,
}

/// Aggregated view of one series.
#[derive(Debug, Clone, Serialize)]
pub struct SeriesView {
    pub series_name: String,
    pub entries: Vec<SeriesEntry>,
    pub non_numeric: bool,
}

// ---------------------------------------------------------------------------
// OL series search deserialization structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OlSeriesSearchResponse {
    docs: Vec<OlSeriesDoc>,
}

#[derive(Debug, Deserialize)]
struct OlSeriesDoc {
    title: Option<String>,
    author_name: Option<Vec<String>>,
    isbn: Option<Vec<String>>,
    series: Option<Vec<String>>,
    #[allow(dead_code)]
    series_number: Option<String>,
    key: Option<String>,
    language: Option<Vec<String>>,
    subject: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Pure computation functions
// ---------------------------------------------------------------------------

/// Group owned editions by `series_name`, detect numeric gaps, and return
/// a sorted list of `SeriesView`s. Pure function — no I/O.
pub fn compute_series_views(editions: &[EditionRow]) -> Vec<SeriesView> {
    // Group by series_name (skip editions with NULL series_name).
    let mut groups: BTreeMap<String, Vec<&EditionRow>> = BTreeMap::new();
    for ed in editions {
        if let Some(ref name) = ed.series_name {
            groups.entry(name.clone()).or_default().push(ed);
        }
    }

    let mut views: Vec<SeriesView> = Vec::new();

    for (series_name, eds) in &groups {
        // Collect positions (skip None).
        let positions: Vec<String> = eds
            .iter()
            .filter_map(|e| e.series_position.clone())
            .collect();

        // If all series_position values are NULL, treat as non-numeric (no gap detection).
        if positions.is_empty() {
            views.push(SeriesView {
                series_name: series_name.clone(),
                entries: vec![],
                non_numeric: true,
            });
            continue;
        }

        // Try to parse all positions as f64.
        let parsed: Option<Vec<f64>> = positions
            .iter()
            .map(|p| p.parse::<f64>().ok())
            .collect();

        let (entries, non_numeric) = match parsed {
            None => {
                // At least one position is non-numeric — lexicographic sort, no gaps.
                let mut sorted = positions.clone();
                sorted.sort();
                let entries = sorted
                    .into_iter()
                    .map(|pos| SeriesEntry {
                        series_name: series_name.clone(),
                        position: pos,
                        owned: true,
                    })
                    .collect();
                (entries, true)
            }
            Some(mut numeric_positions) => {
                numeric_positions.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let mut entries: Vec<SeriesEntry> = Vec::new();

                for (i, &p) in numeric_positions.iter().enumerate() {
                    entries.push(SeriesEntry {
                        series_name: series_name.clone(),
                        position: format_position(p),
                        owned: true,
                    });

                    // Insert gap entries between this and the next position.
                    if let Some(&next_p) = numeric_positions.get(i + 1) {
                        if next_p - p > 1.5 {
                            let gap_start = p.floor() as i64 + 1;
                            let gap_end = next_p.floor() as i64 - 1;
                            for gap in gap_start..=gap_end {
                                entries.push(SeriesEntry {
                                    series_name: series_name.clone(),
                                    position: gap.to_string(),
                                    owned: false,
                                });
                            }
                        }
                    }
                }

                (entries, false)
            }
        };

        views.push(SeriesView {
            series_name: series_name.clone(),
            entries,
            non_numeric,
        });
    }

    // BTreeMap already sorts by key (series_name), so views are in order.
    views
}

/// Format a numeric position: if it is a whole number, display as integer;
/// otherwise display with the minimal decimal digits needed.
fn format_position(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{}", p as i64)
    } else {
        format!("{p}")
    }
}

// ---------------------------------------------------------------------------
// Output formatters
// ---------------------------------------------------------------------------

/// Format series views as human-readable text.
pub fn format_series_text(views: &[SeriesView]) -> String {
    if views.is_empty() {
        return "No series found in library.\n".to_string();
    }

    let mut out = String::new();
    for view in views {
        if view.non_numeric {
            out.push_str(&format!(
                "Series: {} (non-numeric positions — gaps not detected)\n",
                view.series_name
            ));
        } else {
            out.push_str(&format!("Series: {}\n", view.series_name));
        }
        for entry in &view.entries {
            if entry.owned {
                out.push_str(&format!("  [owned]   {}\n", entry.position));
            } else {
                out.push_str(&format!("  [MISSING] {}\n", entry.position));
            }
        }
    }
    out
}

/// Serialize series views to pretty-printed JSON.
pub fn format_series_json(views: &[SeriesView]) -> anyhow::Result<String> {
    serde_json::to_string_pretty(views).context("serializing series views to JSON")
}

/// Serialize series views to RFC 4180 CSV.
/// Header: `series_name,position,owned`. One row per `SeriesEntry`.
pub fn format_series_csv(views: &[SeriesView]) -> anyhow::Result<String> {
    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record(["series_name", "position", "owned"])
        .context("writing CSV header")?;

    for view in views {
        for entry in &view.entries {
            wtr.write_record([
                entry.series_name.as_str(),
                entry.position.as_str(),
                if entry.owned { "1" } else { "0" },
            ])
            .context("writing CSV row")?;
        }
    }

    let bytes = wtr.into_inner().context("finalizing CSV writer")?;
    String::from_utf8(bytes).context("CSV output is not valid UTF-8")
}

// ---------------------------------------------------------------------------
// series_gap: pure gap detection for SeriesGaps command
// ---------------------------------------------------------------------------

/// A series where the user owns some books but not consecutive positions.
#[derive(Debug, Clone)]
pub struct SeriesGap {
    pub series_name: String,
    /// Authors taken from any owned book in the series.
    pub authors: String,
    /// Integer positions (1-based) that are missing within the owned range.
    pub missing_positions: Vec<u32>,
}

/// Given a slice of owned editions (with series_name and series_position set),
/// compute which series have gaps in the consecutive integer positions.
///
/// Rules:
/// - Group by series_name.
/// - Parse series_position as f32; skip non-integer positions (0.5, etc.).
/// - Find min and max integer positions among owned books.
/// - Any integer in min..=max not present in the owned set is a gap.
/// - Only report a gap if the series has >= 2 owned books (single-book series
///   cannot be gapped).
pub fn compute_series_gaps(editions: &[EditionRow]) -> Vec<SeriesGap> {
    use std::collections::BTreeMap;

    // Group editions by series_name.
    let mut groups: BTreeMap<String, Vec<&EditionRow>> = BTreeMap::new();
    for ed in editions {
        if let Some(ref name) = ed.series_name {
            groups.entry(name.clone()).or_default().push(ed);
        }
    }

    let mut result: Vec<SeriesGap> = Vec::new();

    for (series_name, eds) in &groups {
        // Collect integer positions only.
        let int_positions: Vec<u32> = eds
            .iter()
            .filter_map(|e| {
                let pos_str = e.series_position.as_deref()?;
                let f: f32 = pos_str.parse().ok()?;
                // Keep only whole-number positions.
                if f.fract() == 0.0 && f >= 1.0 {
                    Some(f as u32)
                } else {
                    None
                }
            })
            .collect();

        // Need at least 2 owned books to detect a gap.
        if int_positions.len() < 2 {
            continue;
        }

        let min_pos = *int_positions.iter().min().unwrap();
        let max_pos = *int_positions.iter().max().unwrap();
        let owned_set: std::collections::HashSet<u32> = int_positions.iter().copied().collect();

        let missing: Vec<u32> = (min_pos..=max_pos)
            .filter(|p| !owned_set.contains(p))
            .collect();

        if missing.is_empty() {
            continue;
        }

        // Take authors from the first edition in the series that has one.
        let authors = eds
            .iter()
            .find_map(|e| e.authors.as_deref().filter(|a| !a.is_empty()))
            .unwrap_or("")
            .to_string();

        result.push(SeriesGap {
            series_name: series_name.clone(),
            authors,
            missing_positions: missing,
        });
    }

    result
}

// ---------------------------------------------------------------------------
// series_fill: OL query and want_list insertion
// ---------------------------------------------------------------------------

/// Query OL for each series found in `editions`, and insert missing entries
/// into `want_list` with `source = "series_fill"` and `priority = 7`.
/// Returns the total count of rows inserted.
/// Returns `Err` if any series OL request failed (but processes all series).
pub async fn series_fill(
    pool: &db::DbPool,
    client: &reqwest::Client,
    base_url: &str,
    config_language: Option<&str>,
) -> anyhow::Result<usize> {
    let editions = db::editions_with_series(pool).await?;

    if editions.is_empty() {
        println!("No series found — nothing to fill.");
        return Ok(0);
    }

    let views = compute_series_views(&editions);

    let mut inserted = 0usize;
    let mut had_error = false;

    for view in &views {
        let series_name = &view.series_name;

        // Search OL using the series name as a keyword plus the author field.
        // OL's `series:` field is sparsely populated — most records lack it — so
        // `q=series:"Mistborn"` returns only 4 results.  Using the series name as
        // a general keyword + `author:"Brandon Sanderson"` returns all 21 Mistborn
        // books because OL indexes series names in descriptions and subjects.
        let encoded_name = urlencoding::encode(series_name);
        // Derive the author from the first owned edition in this series view.
        let first_edition_author = editions
            .iter()
            .find(|e| e.series_name.as_deref() == Some(series_name.as_str()))
            .and_then(|e| e.authors.clone());
        let q = match &first_edition_author {
            Some(author) if !author.is_empty() => {
                let encoded_author = urlencoding::encode(author);
                format!("{encoded_name}+author%3A%22{encoded_author}%22")
            }
            _ => encoded_name.to_string(),
        };
        // Derive the target language code from the first owned edition.
        // EPUB language tags are BCP-47 ("en", "en-US"); OL uses ISO 639-2 ("eng").
        // We only filter when the owned edition is English — for other languages
        // we accept all results rather than risk over-filtering niche series.
        let target_lang = editions
            .iter()
            .find(|e| e.series_name.as_deref() == Some(series_name.as_str()))
            .and_then(|e| e.language.clone())
            .and_then(|lang| bcp47_to_iso639_2(&lang));

        // AC-15: use config language as fallback when no owned edition has a language tag.
        let target_lang = target_lang.or_else(|| {
            config_language.map(|l| l.to_string())
        });

        let url = format!(
            "{base_url}/search.json?q={q}&fields=title,author_name,isbn,series,series_number,key,language,subject&limit=50"
        );

        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("WARNING: OL error for series '{series_name}': {e}");
                had_error = true;
                continue;
            }
        };

        if !resp.status().is_success() {
            eprintln!(
                "WARNING: OL error for series '{series_name}': HTTP {}",
                resp.status()
            );
            had_error = true;
            continue;
        }

        let search: OlSeriesSearchResponse = match resp.json().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("WARNING: could not parse OL response for series '{series_name}': {e}");
                had_error = true;
                continue;
            }
        };

        for doc in &search.docs {
            // Filter: keep docs whose `series` field contains the queried series name
            // (case-insensitive substring match), OR docs where `series` is absent/empty
            // (trusted because we used a field-specific series:"..." query — OL already
            // filtered to this series, so missing `series` field just means OL omitted it).
            let series_matches = match &doc.series {
                Some(series_vec) if !series_vec.is_empty() => series_vec
                    .iter()
                    .any(|s| s.to_lowercase().contains(&series_name.to_lowercase())),
                // `series` absent or empty — trust the targeted query.
                // OL's search API rarely populates `series` in search results
                // even for genuine series members; noise is handled by is_noise_record.
                _ => true,
            };
            if !series_matches {
                continue;
            }

            let title = match doc.title.as_deref().filter(|t| !t.is_empty()) {
                Some(t) => t,
                None => continue,
            };

            // Language filter: when we can determine the target language, skip
            // results that have language data but don't include it.  Results with
            // no language data pass through (OL often omits the field on valid records).
            if let Some(ref tl) = target_lang {
                if let Some(ref doc_langs) = doc.language {
                    if !doc_langs.is_empty() && !doc_langs.iter().any(|l| l == tl) {
                        continue;
                    }
                }
            }

            // Noise filter: skip records that are not individual downloadable books.
            if is_noise_record(title, doc.subject.as_deref().unwrap_or(&[])) {
                continue;
            }

            let author = doc.author_name.as_ref().and_then(|v| v.first()).cloned();
            let raw_isbn = doc.isbn.as_ref().and_then(|v| v.first()).cloned();
            let isbn = raw_isbn
                .as_deref()
                .map(crate::fuzzy::normalize_isbn);

            // Owned check: Strategy A (ISBN exact), Strategy B (work_id), and
            // Strategy C (fuzzy title+author within same series_name) as fallback.
            // Fuzzy title matching is scoped to editions that share the exact series_name,
            // which prevents the false-positive "Dune Messiah" vs owned "Dune" problem
            // (different series positions but same series), while still catching books
            // that were stored without an ISBN.
            let already_owned = series_is_already_owned(
                pool,
                isbn.as_deref(),
                title,
                author.as_deref().unwrap_or(""),
                series_name,
            )
            .await?;
            if already_owned {
                continue;
            }
            if want::find_existing_want(pool, title, author.as_deref(), isbn.as_deref())
                .await?
                .is_some()
            {
                continue;
            }

            db::insert_want(
                pool,
                title,
                author.as_deref(),
                isbn.as_deref(),
                "series_fill",
                doc.key.as_deref(),
                7,
                None,
            )
            .await?;
            inserted += 1;
        }
    }

    if had_error {
        Err(anyhow::anyhow!("one or more series failed during OL query"))
    } else {
        Ok(inserted)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Check whether a book is already owned using:
/// - Strategy A: ISBN exact match on an owned edition.
/// - Strategy B: cross-edition work_id linkage.
/// - Strategy C: fuzzy title+author match against owned editions with the same
///   `series_name` (handles books stored without an ISBN).
///
/// Fuzzy matching in Strategy C is scoped to the same `series_name`, which
/// prevents false positives between sequels in the same series (e.g.
/// "Dune Messiah" vs owned "Dune") because each book occupies a unique
/// position — they will not fuzzy-match each other across different positions
/// unless they are genuinely the same work.
async fn series_is_already_owned(
    pool: &db::DbPool,
    isbn: Option<&str>,
    title: &str,
    authors: &str,
    series_name: &str,
) -> anyhow::Result<bool> {
    // Strategies A and B require an ISBN.
    if let Some(isbn_str) = isbn {
        // Strategy A: direct ISBN match on an owned edition.
        let row = sqlx::query("SELECT 1 FROM editions WHERE isbn = ? AND owned = 1 LIMIT 1")
            .bind(isbn_str)
            .fetch_optional(pool)
            .await
            .context("series_is_already_owned isbn check")?;
        if row.is_some() {
            return Ok(true);
        }

        // Strategy B: cross-edition work_id match (different edition, same work, owned).
        let work_row = sqlx::query(
            "SELECT work_id FROM editions WHERE isbn = ? AND work_id IS NOT NULL LIMIT 1",
        )
        .bind(isbn_str)
        .fetch_optional(pool)
        .await
        .context("series_is_already_owned work_id lookup")?;

        if let Some(wr) = work_row {
            let work_id: Option<i64> = wr.try_get("work_id").unwrap_or(None);
            if let Some(wid) = work_id {
                let owned = sqlx::query(
                    "SELECT 1 FROM editions WHERE work_id = ? AND owned = 1 LIMIT 1",
                )
                .bind(wid)
                .fetch_optional(pool)
                .await
                .context("series_is_already_owned work owned check")?;
                if owned.is_some() {
                    return Ok(true);
                }
            }
        }
    }

    // Fetch owned editions in the same series for Strategies C and D.
    let candidates = sqlx::query(
        "SELECT title, authors FROM editions WHERE owned = 1 AND series_name = ?",
    )
    .bind(series_name)
    .fetch_all(pool)
    .await
    .context("series_is_already_owned series candidates query")?;

    if isbn.is_some() {
        // Strategy D: title-suffix check for OL docs that have an ISBN not in our DB.
        // Handles the case where the EPUB was imported without an ISBN (so enrich
        // never ran), but OL knows the ISBN.  E.g. we own "Mistborn: The Final Empire"
        // (no ISBN in DB) and OL returns "The Final Empire" with ISBN 9780765377135.
        // "mistborn: the final empire".ends_with("the final empire") → true → same work.
        //
        // Uses suffix rather than fuzzy to avoid the "Dune Messiah" false positive:
        // "dune messiah".ends_with("dune") → false, and
        // "dune".ends_with("dune messiah") → false → correctly treated as different books.
        //
        // Minimum suffix length of 5 chars avoids trivially short matches (e.g. "A").
        let ol_lower = title.trim().to_lowercase();
        for row in &candidates {
            let db_title: Option<String> = row.try_get("title").unwrap_or(None);
            let db_lower = db_title.as_deref().unwrap_or("").trim().to_lowercase();
            let min_len = ol_lower.len().min(db_lower.len());
            if min_len >= 5
                && (db_lower.ends_with(ol_lower.as_str())
                    || ol_lower.ends_with(db_lower.as_str())
                    // Subtitle pattern: owned "Mistborn: The Final Empire" vs
                    // OL "Mistborn" — db starts with ol_title + colon/dash.
                    || db_lower.starts_with(&format!("{}:", ol_lower))
                    || db_lower.starts_with(&format!("{} -", ol_lower)))
            {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    // Strategy C: fuzzy title+author fallback, only when OL doc has NO ISBN.
    // Fuzzy matching is safe here because there is no ISBN to distinguish sequels —
    // the scope is limited to owned editions in the same series.
    for row in &candidates {
        let db_title: Option<String> = row.try_get("title").unwrap_or(None);
        let db_authors: Option<String> = row.try_get("authors").unwrap_or(None);
        let db_title = db_title.as_deref().unwrap_or("");
        let db_authors = db_authors.as_deref().unwrap_or("");
        if crate::fuzzy::is_same_work(title, authors, db_title, db_authors) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Convert a BCP-47 language tag (as stored in EPUB metadata) to an ISO 639-2
/// three-letter code as used by OpenLibrary's `language` field.
/// Returns `None` for unknown/unsupported codes so callers can skip filtering.
fn bcp47_to_iso639_2(tag: &str) -> Option<String> {
    // Take only the primary subtag ("en-US" → "en").
    let primary = tag.split('-').next()?.to_lowercase();
    let iso = match primary.as_str() {
        "en" => "eng",
        "fr" => "fre",
        "de" => "ger",
        "es" => "spa",
        "it" => "ita",
        "pt" => "por",
        "nl" => "dut",
        "pl" => "pol",
        "ru" => "rus",
        "zh" => "chi",
        "ja" => "jpn",
        "ko" => "kor",
        _ => return None,
    };
    Some(iso.to_string())
}

/// Return `true` for OL records that are not individual downloadable books.
///
/// Uses OL metadata signals rather than title string matching to avoid
/// false positives on legitimate titles (e.g. "The Omnibus", "The Adventure Game",
/// "Complete Works of a Single Author").
///
/// Signals:
/// - Non-ASCII title: reliable foreign-language indicator when OL omits the language field.
/// - "Untitled …" prefix: publishing placeholder, never a finished book title.
/// - Retail/packaging terms: "box set", "boxed set", "collection set", "sampler" —
///   never appear as standalone novel titles.
/// - " / " in title: bilingual publication pattern (e.g. "Nacidos de la Bruma / Mistborn").
/// - Ends with " series": series-reference catalogue entry, not an individual book.
/// - Subject contains "games": tabletop RPG sourcebooks and game supplements.
/// - Adjacent standalone digit words: e.g. "Volumes 1 2 3" — multi-volume box
///   set enumeration, never appears in a real novel title.
/// - Parenthetical with ≥ 3 commas: e.g. "(Title A, Title B, Title C, Title D)"
///   — bundle/anthology listing, never a real subtitle.
pub(crate) fn is_noise_record(title: &str, subjects: &[String]) -> bool {
    if !title.is_ascii() {
        return true;
    }
    let lower = title.to_lowercase();
    // "Untitled" and broken short forms like "Untitle".
    if lower.starts_with("untitle") {
        return true;
    }
    if lower.contains("boxed set")
        || lower.contains("box set")
        || lower.contains("book set")    // "3-Book Set"
        || lower.contains("books set")  // "3 Books Set", "Skyward Series 3 Books Set"
        || lower.contains("bundle")     // "Bundles Set", "Book Bundle"
        || lower.contains("collection set")
        || lower.ends_with(" sampler")
        || lower.contains("omnibus")
        || lower.contains("calendar")
        || lower.contains("sparknotes")
        || lower.contains("literature guide")
        || lower.contains("to be announced")
        || lower.ends_with(" anthology")      // "Dragon Writers: An Anthology"
        || lower.contains("writers of the future") // anthology series
    {
        return true;
    }
    // Bundled works: " / " or " & " separating two titles.
    if lower.contains(" / ") || lower.contains(" & ") {
        return true;
    }
    if lower.ends_with(" series") {
        return true;
    }
    // OL subject tag indicates a game supplement, not a novel.
    if subjects.iter().any(|s| s.to_lowercase().contains("games")) {
        return true;
    }
    // Two adjacent standalone digit words → multi-volume enumeration ("1 2 3").
    let words: Vec<&str> = lower.split_whitespace().collect();
    if words
        .windows(2)
        .any(|w| w[0].chars().all(|c| c.is_ascii_digit()) && w[1].chars().all(|c| c.is_ascii_digit()))
    {
        return true;
    }
    // Parenthetical with 3+ commas → bundled title listing ("(A, B, C, D)").
    if let Some(open) = title.find('(') {
        if let Some(rel_close) = title[open..].find(')') {
            let inside = &title[open + 1..open + rel_close];
            if inside.matches(',').count() >= 3 {
                return true;
            }
        }
    }
    false
}
