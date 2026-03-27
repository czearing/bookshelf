/// Unit tests for `series::compute_series_gaps`.
use bookshelf_core::{db::EditionRow, series};

fn make_edition(series_name: &str, series_position: &str, authors: &str, id: i64) -> EditionRow {
    EditionRow {
        id,
        title: Some(format!("{series_name} #{series_position}")),
        authors: Some(authors.to_string()),
        isbn: None,
        series_name: Some(series_name.to_string()),
        series_position: Some(series_position.to_string()),
        publisher: None,
        publish_date: None,
        language: None,
        description: None,
        cover_image_path: None,
        source_path: format!("/tmp/{series_name}_{series_position}.epub"),
        work_id: None,
        owned: 1,
        enriched_at: None,
        enrichment_attempted: 0,
        tagged_at: None,
        download_url: None,
        download_source: None,
        reading_status: None,
        reading_started_at: None,
        reading_finished_at: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_no_gaps_returns_empty() {
    // Positions 1, 2, 3 — no gaps.
    let editions = vec![
        make_edition("The Expanse", "1", "James S.A. Corey", 1),
        make_edition("The Expanse", "2", "James S.A. Corey", 2),
        make_edition("The Expanse", "3", "James S.A. Corey", 3),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert!(gaps.is_empty(), "consecutive positions 1-3 should produce no gaps");
}

#[test]
fn test_gap_in_middle_detected() {
    // Own 1, 3 — missing 2.
    let editions = vec![
        make_edition("The Expanse", "1", "James S.A. Corey", 1),
        make_edition("The Expanse", "3", "James S.A. Corey", 3),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert_eq!(gaps.len(), 1, "should detect 1 series with a gap");
    assert_eq!(gaps[0].series_name, "The Expanse");
    assert_eq!(gaps[0].missing_positions, vec![2u32]);
}

#[test]
fn test_multiple_gaps() {
    // Own 1, 3, 6 — missing 2, 4, 5.
    let editions = vec![
        make_edition("Mistborn", "1", "Brandon Sanderson", 1),
        make_edition("Mistborn", "3", "Brandon Sanderson", 3),
        make_edition("Mistborn", "6", "Brandon Sanderson", 6),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].missing_positions, vec![2, 4, 5]);
}

#[test]
fn test_single_book_not_reported() {
    // Only own position 1 — cannot determine gap.
    let editions = vec![make_edition("Dune", "1", "Frank Herbert", 1)];
    let gaps = series::compute_series_gaps(&editions);
    assert!(gaps.is_empty(), "single-book series should not be reported as a gap");
}

#[test]
fn test_non_integer_positions_skipped() {
    // Own 1, 1.5, 3 — 1.5 is skipped; only integers 1 and 3 are used — gap at 2.
    let editions = vec![
        make_edition("Series A", "1", "Author One", 1),
        make_edition("Series A", "1.5", "Author One", 2),
        make_edition("Series A", "3", "Author One", 3),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert_eq!(gaps.len(), 1, "should still detect gap at position 2");
    assert_eq!(gaps[0].missing_positions, vec![2u32]);
}

#[test]
fn test_all_non_integer_positions_no_report() {
    // Only fractional positions — after filtering, fewer than 2 integer positions remain.
    let editions = vec![
        make_edition("Series B", "0.5", "Author Two", 1),
        make_edition("Series B", "1.5", "Author Two", 2),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert!(gaps.is_empty(), "all non-integer positions should yield no gaps");
}

#[test]
fn test_multiple_series_independent() {
    let editions = vec![
        // Series A: 1, 3 — gap at 2.
        make_edition("Series A", "1", "Author A", 1),
        make_edition("Series A", "3", "Author A", 3),
        // Series B: 1, 2, 3 — no gap.
        make_edition("Series B", "1", "Author B", 4),
        make_edition("Series B", "2", "Author B", 5),
        make_edition("Series B", "3", "Author B", 6),
    ];
    let gaps = series::compute_series_gaps(&editions);
    assert_eq!(gaps.len(), 1, "only Series A should have a gap");
    assert_eq!(gaps[0].series_name, "Series A");
}
