use crate::{db, db::EditionRow, want};
use anyhow::Context;
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
        let encoded = urlencoding::encode(series_name);
        let url = format!(
            "{base_url}/search.json?q={encoded}&fields=title,author_name,isbn,series,series_number,key&limit=50"
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
            // Filter: keep only docs whose `series` field contains the queried
            // series name (case-insensitive substring match — resolved Q2).
            let series_matches = doc.series.as_ref().is_some_and(|series_vec| {
                series_vec
                    .iter()
                    .any(|s| s.to_lowercase().contains(&series_name.to_lowercase()))
            });
            if !series_matches {
                continue;
            }

            let title = match doc.title.as_deref().filter(|t| !t.is_empty()) {
                Some(t) => t,
                None => continue,
            };

            let author = doc.author_name.as_ref().and_then(|v| v.first()).cloned();
            let raw_isbn = doc.isbn.as_ref().and_then(|v| v.first()).cloned();
            let isbn = raw_isbn
                .as_deref()
                .map(crate::fuzzy::normalize_isbn);

            if want::is_already_owned(pool, title, author.as_deref(), isbn.as_deref()).await? {
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
