/// Comprehensive real-world E2E test for the grab-list deduplication pipeline.
///
/// Creates a realistic EPUB library (5 owned books), imports a Goodreads CSV with
/// 12 rows covering exact-ISBN matches, different-edition fuzzy matches, genuinely
/// unowned books, and within-want-list duplicate rows, then asserts the final grab
/// list contains exactly the correct 6 entries with zero duplicates.
use bookshelf_core::{db, grab, scan, want};
use std::io::Write as _;
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};

// ---------------------------------------------------------------------------
// EPUB fixture builder (same pattern as integration_scan.rs)
// ---------------------------------------------------------------------------

fn make_epub(
    title: &str,
    authors: &[&str],
    isbn: Option<&str>,
    series_name: Option<&str>,
    series_position: Option<&str>,
    dest: &Path,
) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)
        .unwrap();
    let mut zip = zip::ZipWriter::new(file);
    let opts =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    zip.start_file("mimetype", opts).unwrap();
    zip.write_all(b"application/epub+zip").unwrap();

    zip.start_file("META-INF/container.xml", opts).unwrap();
    zip.write_all(
        br#"<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>"#,
    )
    .unwrap();

    let mut opf = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:opf="http://www.idpf.org/2007/opf">
"#,
    );

    opf.push_str(&format!("    <dc:title>{title}</dc:title>\n"));
    for author in authors {
        opf.push_str(&format!("    <dc:creator>{author}</dc:creator>\n"));
    }
    if let Some(v) = isbn {
        opf.push_str(&format!(
            "    <dc:identifier opf:scheme=\"ISBN\">{v}</dc:identifier>\n"
        ));
    }
    if let Some(sname) = series_name {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series\" content=\"{sname}\"/>\n"
        ));
    }
    if let Some(spos) = series_position {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series_index\" content=\"{spos}\"/>\n"
        ));
    }

    opf.push_str("  </metadata>\n  <manifest/>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn temp_pool() -> (db::DbPool, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".db").unwrap();
    let pool = db::open(tmp.path()).await.unwrap();
    (pool, tmp)
}

/// Write a temp CSV file and return (path, guard).
fn write_temp_csv(content: &str) -> (std::path::PathBuf, NamedTempFile) {
    let tmp = NamedTempFile::with_suffix(".csv").unwrap();
    std::fs::write(tmp.path(), content).unwrap();
    (tmp.path().to_path_buf(), tmp)
}

// ---------------------------------------------------------------------------
// The Goodreads CSV fixture
// ---------------------------------------------------------------------------
//
// Columns as exported by Goodreads (ISBNs wrapped in ="..." Excel formula format):
//   Book Id, Title, Author, Author l-f, Additional Authors, ISBN, ISBN13,
//   My Rating, Average Rating, Publisher, Binding, Number of Pages,
//   Year Published, Original Publication Year, Date Read, Date Added,
//   Bookshelves, Bookshelves with positions, Exclusive Shelf, My Review,
//   Spoiler, Private Notes, Read Count, Owned Copies
//
// Expected outcomes after pipeline:
//   Row  1 (Dune, ISBN 9780441013593):            exact ISBN match → owned → SKIP
//   Row  2 (Dune, ISBN 9780441013570 diff ed):    fuzzy title+author match → owned → SKIP
//   Row  3 (Name of Wind, ISBN 9780756404079):    exact ISBN match → owned → SKIP
//   Row  4 (Words of Radiance):                   not owned → GRAB LIST
//   Row  5 (The Final Empire, ISBN 9780765350381):not owned → GRAB LIST
//   Row  6 (Project Hail Mary, 9780593135204):    exact ISBN match → owned → SKIP
//   Row  7 (The Martian):                         not owned → GRAB LIST
//   Row  8 (Red Rising):                          not owned → GRAB LIST
//   Row  9 (Lies of Locke Lamora, diff ISBN):     fuzzy title+author match → owned → SKIP
//   Row 10 (Mistborn:The Final Empire, 9780765350381): same ISBN as row 5 →
//          within-want-list dedup (ISBN match) → updates row 5 → no new row → GRAB LIST ONCE
//   Row 11 (The Blade Itself):                    not owned → GRAB LIST
//   Row 12 (Rhythm of War):                       not owned → GRAB LIST
//
// Final grab list: 6 entries.

const GOODREADS_CSV: &str = concat!(
    "Book Id,Title,Author,Author l-f,Additional Authors,ISBN,ISBN13,My Rating,Average Rating,",
    "Publisher,Binding,Number of Pages,Year Published,Original Publication Year,",
    "Date Read,Date Added,Bookshelves,Bookshelves with positions,Exclusive Shelf,",
    "My Review,Spoiler,Private Notes,Read Count,Owned Copies\n",
    // Row 1 — Dune exact edition (same ISBN as owned)
    "1,\"Dune\",\"Frank Herbert\",\"Herbert, Frank\",\"\",=\"0441013597\",=\"9780441013593\",0,4.25,",
    "\"Ace\",\"Paperback\",896,2019,1965,\"\",\"2024-01-15\",\"to-read\",\"to-read (#1)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 2 — Dune different edition (ISBN 9780441013570, owned has 9780441013593)
    "2,\"Dune\",\"Frank Herbert\",\"Herbert, Frank\",\"\",=\"0441013570\",=\"9780441013570\",0,4.25,",
    "\"Ace\",\"Mass Market Paperback\",896,1990,1965,\"\",\"2024-01-15\",\"to-read\",\"to-read (#2)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 3 — The Name of the Wind exact edition
    "3,\"The Name of the Wind\",\"Patrick Rothfuss\",\"Rothfuss, Patrick\",\"\",=\"0756404079\",=\"9780756404079\",",
    "0,4.54,\"DAW\",\"Hardcover\",662,2007,2007,\"\",\"2024-01-16\",\"to-read\",\"to-read (#3)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 4 — Words of Radiance (not owned)
    "4,\"Words of Radiance\",\"Brandon Sanderson\",\"Sanderson, Brandon\",\"\",=\"0765326361\",=\"9780765326362\",",
    "0,4.75,\"Tor Books\",\"Hardcover\",1087,2014,2014,\"\",\"2024-01-17\",\"to-read\",\"to-read (#4)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 5 — The Final Empire (not owned)
    "5,\"The Final Empire\",\"Brandon Sanderson\",\"Sanderson, Brandon\",\"\",=\"0765350386\",=\"9780765350381\",",
    "0,4.45,\"Tor Books\",\"Paperback\",541,2010,2006,\"\",\"2024-01-18\",\"to-read\",\"to-read (#5)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 6 — Project Hail Mary exact edition
    "6,\"Project Hail Mary\",\"Andy Weir\",\"Weir, Andy\",\"\",=\"0593135202\",=\"9780593135204\",",
    "0,4.52,\"Ballantine Books\",\"Hardcover\",476,2021,2021,\"\",\"2024-01-19\",\"to-read\",\"to-read (#6)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 7 — The Martian (not owned)
    "7,\"The Martian\",\"Andy Weir\",\"Weir, Andy\",\"\",=\"0804139021\",=\"9780804139021\",",
    "0,4.40,\"Crown\",\"Paperback\",369,2014,2011,\"\",\"2024-01-20\",\"to-read\",\"to-read (#7)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 8 — Red Rising (not owned)
    "8,\"Red Rising\",\"Pierce Brown\",\"Brown, Pierce\",\"\",=\"0345539788\",=\"9780345539786\",",
    "0,4.27,\"Del Rey\",\"Paperback\",382,2014,2014,\"\",\"2024-01-21\",\"to-read\",\"to-read (#8)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 9 — Lies of Locke Lamora different edition (owned has 9780553588941; CSV has 9780553588958)
    "9,\"The Lies of Locke Lamora\",\"Scott Lynch\",\"Lynch, Scott\",\"\",=\"0553588958\",=\"9780553588958\",",
    "0,4.31,\"Bantam\",\"Paperback\",752,2007,2006,\"\",\"2024-01-22\",\"to-read\",\"to-read (#9)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 10 — Mistborn: The Final Empire (same ISBN 9780765350381 as row 5 → deduped in want list)
    "10,\"Mistborn: The Final Empire\",\"Brandon Sanderson\",\"Sanderson, Brandon\",\"\",=\"0765350386\",=\"9780765350381\",",
    "0,4.45,\"Tor Books\",\"Paperback\",541,2010,2006,\"\",\"2024-01-23\",\"to-read\",\"to-read (#10)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 11 — The Blade Itself (not owned)
    "11,\"The Blade Itself\",\"Joe Abercrombie\",\"Abercrombie, Joe\",\"\",=\"0316387312\",=\"9780316387316\",",
    "0,4.17,\"Orbit\",\"Paperback\",515,2007,2006,\"\",\"2024-01-24\",\"to-read\",\"to-read (#11)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n",
    // Row 12 — Rhythm of War (not owned)
    "12,\"Rhythm of War\",\"Brandon Sanderson\",\"Sanderson, Brandon\",\"\",=\"0765326388\",=\"9780765326386\",",
    "0,4.64,\"Tor Books\",\"Hardcover\",1232,2020,2020,\"\",\"2024-01-25\",\"to-read\",\"to-read (#12)\",",
    "\"to-read\",\"\",\"\",\"\",0,0\n"
);

// ---------------------------------------------------------------------------
// Core E2E library test (no CLI)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_grab_dedup_full_pipeline() {
    let (pool, _db_tmp) = temp_pool().await;
    let epub_dir = TempDir::new().unwrap();

    // ------------------------------------------------------------------
    // Step 1: Create 5 owned EPUB fixtures
    // ------------------------------------------------------------------

    make_epub(
        "Dune",
        &["Frank Herbert"],
        Some("9780441013593"),
        Some("Dune Chronicles"),
        Some("1"),
        &epub_dir.path().join("dune.epub"),
    );

    make_epub(
        "The Name of the Wind",
        &["Patrick Rothfuss"],
        Some("9780756404079"),
        Some("Kingkiller Chronicle"),
        Some("1"),
        &epub_dir.path().join("name_of_the_wind.epub"),
    );

    make_epub(
        "The Way of Kings",
        &["Brandon Sanderson"],
        Some("9780765326355"),
        Some("The Stormlight Archive"),
        Some("1"),
        &epub_dir.path().join("way_of_kings.epub"),
    );

    make_epub(
        "Project Hail Mary",
        &["Andy Weir"],
        Some("9780593135204"),
        None,
        None,
        &epub_dir.path().join("project_hail_mary.epub"),
    );

    make_epub(
        "The Lies of Locke Lamora",
        &["Scott Lynch"],
        Some("9780553588941"),
        Some("Gentleman Bastard"),
        Some("1"),
        &epub_dir.path().join("lies_of_locke_lamora.epub"),
    );

    // ------------------------------------------------------------------
    // Step 2: Scan EPUBs into DB
    // ------------------------------------------------------------------

    let scan_result = scan::scan_directory(&pool, epub_dir.path()).await.unwrap();
    assert_eq!(scan_result.inserted, 5, "should have inserted 5 owned books");
    assert_eq!(scan_result.errors.len(), 0, "no scan errors expected");

    // Verify all 5 editions are in DB and marked owned
    let editions = db::list_editions(&pool).await.unwrap();
    assert_eq!(editions.len(), 5, "5 editions in DB");
    assert!(
        editions.iter().all(|e| e.owned == 1),
        "all scanned editions must be owned"
    );

    // ------------------------------------------------------------------
    // Step 3: Import Goodreads CSV
    // ------------------------------------------------------------------

    let (csv_path, _csv_file) = write_temp_csv(GOODREADS_CSV);
    let import_summary = want::import_goodreads_csv(&pool, &csv_path)
        .await
        .unwrap();

    // 5 rows skipped as already owned:
    //   row 1 (Dune exact ISBN), row 2 (Dune fuzzy diff edition),
    //   row 3 (Name of Wind exact ISBN), row 6 (Project Hail Mary exact ISBN),
    //   row 9 (Lies of Locke Lamora fuzzy diff edition)
    assert_eq!(
        import_summary.skipped_owned, 5,
        "expected 5 rows skipped as already owned, got {}",
        import_summary.skipped_owned
    );

    // 7 distinct want entries (rows 4,5,7,8,10,11,12) but row 10 dedupes into row 5
    // via ISBN match, so 6 unique want rows inserted.
    // imported counts updates (row 10 update) + new inserts.
    // The want list should have 6 entries.
    let want_rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(
        want_rows.len(),
        6,
        "want list should have 6 rows after dedup; got {}. Titles: {:?}",
        want_rows.len(),
        want_rows.iter().map(|r| &r.title).collect::<Vec<_>>()
    );

    // ------------------------------------------------------------------
    // Step 4: Compute grab list
    // ------------------------------------------------------------------

    let grab_list = grab::compute_grab_list(&pool).await.unwrap();

    let grab_titles: Vec<&str> = grab_list
        .iter()
        .map(|e| e.title.as_deref().unwrap_or(""))
        .collect();

    // Assert exact count
    assert_eq!(
        grab_list.len(),
        6,
        "grab list must contain exactly 6 entries, got {}. Titles: {:?}",
        grab_list.len(),
        grab_titles
    );

    // ------------------------------------------------------------------
    // Step 5: Assert owned books are NOT in grab list
    // ------------------------------------------------------------------

    // Row 1: Dune — exact ISBN match (9780441013593)
    assert!(
        !grab_titles.contains(&"Dune"),
        "Dune (exact ISBN) must NOT be in grab list"
    );

    // Row 3: The Name of the Wind — exact ISBN match
    assert!(
        !grab_titles.iter().any(|t| *t == "The Name of the Wind"),
        "The Name of the Wind must NOT be in grab list"
    );

    // Row 6: Project Hail Mary — exact ISBN match
    assert!(
        !grab_titles.iter().any(|t| *t == "Project Hail Mary"),
        "Project Hail Mary must NOT be in grab list"
    );

    // Row 9: The Lies of Locke Lamora — different edition but fuzzy title+author match
    // (score = 1.0, well above 0.85 threshold)
    assert!(
        !grab_titles.iter().any(|t| *t == "The Lies of Locke Lamora"),
        "The Lies of Locke Lamora (different edition, fuzzy match) must NOT be in grab list"
    );

    // ------------------------------------------------------------------
    // Step 6: Assert unowned books ARE in grab list
    // ------------------------------------------------------------------

    assert!(
        grab_titles.iter().any(|t| *t == "Words of Radiance"),
        "Words of Radiance must be in grab list; got {:?}",
        grab_titles
    );

    assert!(
        grab_titles.iter().any(|t| *t == "The Martian"),
        "The Martian must be in grab list; got {:?}",
        grab_titles
    );

    assert!(
        grab_titles.iter().any(|t| *t == "Red Rising"),
        "Red Rising must be in grab list; got {:?}",
        grab_titles
    );

    assert!(
        grab_titles.iter().any(|t| *t == "The Blade Itself"),
        "The Blade Itself must be in grab list; got {:?}",
        grab_titles
    );

    assert!(
        grab_titles.iter().any(|t| *t == "Rhythm of War"),
        "Rhythm of War must be in grab list; got {:?}",
        grab_titles
    );

    // ------------------------------------------------------------------
    // Step 7: Assert "The Final Empire" / "Mistborn" appears exactly once
    // ------------------------------------------------------------------

    // Row 5 (The Final Empire, ISBN 9780765350381) and row 10 (Mistborn: The Final Empire,
    // ISBN 9780765350381) share the same ISBN-13 so find_existing_want deduplicates them.
    // Row 10 updates row 5's title to "Mistborn: The Final Empire"; only one want row exists.
    // Exactly one of these titles should appear in the grab list.
    let final_empire_count = grab_list
        .iter()
        .filter(|e| {
            e.title.as_deref() == Some("The Final Empire")
                || e.title.as_deref() == Some("Mistborn: The Final Empire")
        })
        .count();
    assert_eq!(
        final_empire_count, 1,
        "The Final Empire / Mistborn must appear exactly once in grab list; titles: {:?}",
        grab_titles
    );

    // ------------------------------------------------------------------
    // Step 8: No duplicates — all grab list titles are unique
    // ------------------------------------------------------------------

    let mut seen = std::collections::HashSet::new();
    for title in &grab_titles {
        assert!(
            seen.insert(*title),
            "duplicate title in grab list: {title:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// ISBN strip verification test (the ="..." format from Goodreads)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_e2e_goodreads_isbn_excel_format_stripped() {
    // Verify that the ="9780441013593" Excel formula wrapper is properly stripped
    // during CSV import so the ISBN is stored as plain digits.
    let (pool, _tmp) = temp_pool().await;

    let csv = concat!(
        "Book Id,Title,Author,Author l-f,Additional Authors,ISBN,ISBN13,My Rating,Average Rating,",
        "Publisher,Binding,Number of Pages,Year Published,Original Publication Year,",
        "Date Read,Date Added,Bookshelves,Bookshelves with positions,Exclusive Shelf,",
        "My Review,Spoiler,Private Notes,Read Count,Owned Copies\n",
        "99,\"Dune\",\"Frank Herbert\",\"Herbert, Frank\",\"\",=\"0441013597\",=\"9780441013593\",",
        "0,4.25,\"Ace\",\"Paperback\",896,2019,1965,\"\",\"2024-01-15\",\"to-read\",",
        "\"to-read (#1)\",\"to-read\",\"\",\"\",\"\",0,0\n"
    );

    let (path, _file) = write_temp_csv(csv);
    want::import_goodreads_csv(&pool, &path).await.unwrap();

    let rows = db::list_want(&pool, None).await.unwrap();
    assert_eq!(rows.len(), 1, "should have inserted 1 row");
    assert_eq!(
        rows[0].isbn13.as_deref(),
        Some("9780441013593"),
        "ISBN-13 must be stripped of =\"...\" wrapper; got {:?}",
        rows[0].isbn13
    );
}

// ---------------------------------------------------------------------------
// Fuzzy score contract tests — document actual scores for key pairs
// ---------------------------------------------------------------------------

#[test]
fn test_fuzzy_dune_different_edition_matches() {
    // Row 2 scenario: Dune with a different ISBN but same title+author.
    // The fuzzy match fires on title+author alone (score = 1.0 when both strings are identical).
    let score = bookshelf_core::fuzzy::book_similarity(
        "Dune",
        "Frank Herbert",
        "Dune",
        "Frank Herbert",
    );
    assert!(
        score >= bookshelf_core::fuzzy::DEDUP_THRESHOLD,
        "Dune/Frank Herbert vs Dune/Frank Herbert must match: score={score:.4}"
    );
}

#[test]
fn test_fuzzy_lies_of_locke_lamora_different_edition_matches() {
    // Row 9 scenario: same title and author, different ISBN.
    let score = bookshelf_core::fuzzy::book_similarity(
        "The Lies of Locke Lamora",
        "Scott Lynch",
        "The Lies of Locke Lamora",
        "Scott Lynch",
    );
    assert!(
        score >= bookshelf_core::fuzzy::DEDUP_THRESHOLD,
        "Lies of Locke Lamora must match across editions: score={score:.4}"
    );
}

#[test]
fn test_fuzzy_mistborn_final_empire_below_threshold() {
    // Row 10 scenario: "Mistborn: The Final Empire" vs "The Final Empire".
    // Score is 0.7674, below the 0.85 threshold — these do NOT fuzzy-match.
    // Dedup happens via shared ISBN-13 (9780765350381) in find_existing_want instead.
    let score = bookshelf_core::fuzzy::book_similarity(
        "Mistborn: The Final Empire",
        "Brandon Sanderson",
        "The Final Empire",
        "Brandon Sanderson",
    );
    assert!(
        score < bookshelf_core::fuzzy::DEDUP_THRESHOLD,
        "Mistborn:The Final Empire vs The Final Empire must NOT fuzzy-match (score={score:.4} \
         must be below {})",
        bookshelf_core::fuzzy::DEDUP_THRESHOLD
    );
    // Document the actual score is approximately 0.7674
    assert!(
        (score - 0.767).abs() < 0.01,
        "expected score ~0.767, got {score:.4}"
    );
}

#[test]
fn test_fuzzy_words_of_radiance_does_not_match_way_of_kings() {
    // Words of Radiance (wanted, not owned) vs The Way of Kings (owned) — same series,
    // different books. Must NOT match.
    let score = bookshelf_core::fuzzy::book_similarity(
        "Words of Radiance",
        "Brandon Sanderson",
        "The Way of Kings",
        "Brandon Sanderson",
    );
    assert!(
        score < bookshelf_core::fuzzy::DEDUP_THRESHOLD,
        "Words of Radiance vs The Way of Kings must NOT match: score={score:.4}"
    );
}

// ---------------------------------------------------------------------------
// CLI binary E2E test
// ---------------------------------------------------------------------------

/// Run the CLI binary with the given args and temp DB, returning (stdout, stderr, exit_code).
fn run_cli(bin: &Path, db_path: &Path, args: &[&str]) -> (String, String, std::process::ExitStatus) {
    let mut cmd = std::process::Command::new(bin);
    cmd.arg("--db").arg(db_path);
    for a in args {
        cmd.arg(a);
    }
    let output = cmd.output().expect("failed to run CLI binary");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status)
}

#[test]
fn test_e2e_cli_full_pipeline() {
    // Build paths
    let bin = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/bookshelf.exe");

    // Skip if binary not built
    if !bin.exists() {
        eprintln!("SKIP: binary not found at {}; run `cargo build --release` first", bin.display());
        return;
    }

    let db_tmp = NamedTempFile::with_suffix(".db").unwrap();
    let db_path = db_tmp.path();
    let epub_dir = TempDir::new().unwrap();

    // Create EPUB fixtures
    make_epub(
        "Dune",
        &["Frank Herbert"],
        Some("9780441013593"),
        Some("Dune Chronicles"),
        Some("1"),
        &epub_dir.path().join("dune.epub"),
    );
    make_epub(
        "The Name of the Wind",
        &["Patrick Rothfuss"],
        Some("9780756404079"),
        Some("Kingkiller Chronicle"),
        Some("1"),
        &epub_dir.path().join("name_of_the_wind.epub"),
    );
    make_epub(
        "The Way of Kings",
        &["Brandon Sanderson"],
        Some("9780765326355"),
        Some("The Stormlight Archive"),
        Some("1"),
        &epub_dir.path().join("way_of_kings.epub"),
    );
    make_epub(
        "Project Hail Mary",
        &["Andy Weir"],
        Some("9780593135204"),
        None,
        None,
        &epub_dir.path().join("project_hail_mary.epub"),
    );
    make_epub(
        "The Lies of Locke Lamora",
        &["Scott Lynch"],
        Some("9780553588941"),
        Some("Gentleman Bastard"),
        Some("1"),
        &epub_dir.path().join("lies_of_locke_lamora.epub"),
    );

    // Write CSV
    let csv_tmp = NamedTempFile::with_suffix(".csv").unwrap();
    std::fs::write(csv_tmp.path(), GOODREADS_CSV).unwrap();

    // --- bookshelf scan ---
    let (stdout, stderr, status) = run_cli(&bin, db_path, &["scan", epub_dir.path().to_str().unwrap()]);
    assert!(
        status.success(),
        "scan must exit 0; stderr: {stderr}; stdout: {stdout}"
    );
    assert!(
        stdout.contains("inserted 5"),
        "scan output must mention 5 inserted books; got: {stdout}"
    );

    // --- bookshelf want import goodreads ---
    let (stdout, stderr, status) = run_cli(
        &bin,
        db_path,
        &["want", "import", "goodreads", csv_tmp.path().to_str().unwrap()],
    );
    assert!(
        status.success(),
        "want import must exit 0; stderr: {stderr}; stdout: {stdout}"
    );
    // Should print "Already owned:" for the 5 owned books
    let owned_count = stdout.lines().filter(|l| l.starts_with("Already owned:")).count();
    assert_eq!(
        owned_count, 5,
        "want import must print 'Already owned:' 5 times; got {owned_count}; output:\n{stdout}"
    );

    // --- bookshelf grab --output text ---
    let (stdout, stderr, status) = run_cli(&bin, db_path, &["grab", "--output", "text"]);
    assert!(
        status.success(),
        "grab text must exit 0; stderr: {stderr}"
    );

    // Assert owned books are absent
    assert!(!stdout.contains("Project Hail Mary"), "Project Hail Mary must not be in grab text output");
    assert!(!stdout.contains("Name of the Wind"), "Name of the Wind must not be in grab text output");

    // Assert unowned books are present
    assert!(stdout.contains("Words of Radiance"), "Words of Radiance must be in grab text; output:\n{stdout}");
    assert!(stdout.contains("The Martian"), "The Martian must be in grab text; output:\n{stdout}");
    assert!(stdout.contains("Red Rising"), "Red Rising must be in grab text; output:\n{stdout}");
    assert!(stdout.contains("The Blade Itself"), "The Blade Itself must be in grab text; output:\n{stdout}");
    assert!(stdout.contains("Rhythm of War"), "Rhythm of War must be in grab text; output:\n{stdout}");

    // Assert The Final Empire / Mistborn appears exactly once
    let fe_count = stdout.lines().filter(|l| {
        l.contains("The Final Empire") || l.contains("Mistborn")
    }).count();
    assert_eq!(
        fe_count, 1,
        "The Final Empire / Mistborn must appear exactly once in text output; output:\n{stdout}"
    );

    // --- bookshelf grab --output json ---
    let (stdout, stderr, status) = run_cli(&bin, db_path, &["grab", "--output", "json"]);
    assert!(
        status.success(),
        "grab json must exit 0; stderr: {stderr}"
    );

    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("grab --output json must produce valid JSON");
    assert!(parsed.is_array(), "grab json output must be a JSON array");
    let arr = parsed.as_array().unwrap();
    assert_eq!(
        arr.len(),
        6,
        "grab JSON array must contain exactly 6 entries; got {}",
        arr.len()
    );

    // Verify no owned book appears in the JSON array
    let json_titles: Vec<&str> = arr
        .iter()
        .filter_map(|v| v["title"].as_str())
        .collect();
    assert!(!json_titles.contains(&"Dune"), "Dune must not be in JSON grab list");
    assert!(!json_titles.contains(&"Project Hail Mary"), "Project Hail Mary must not be in JSON grab list");

    // Verify all 5 unowned books appear
    let expected_in_grab = ["Words of Radiance", "The Martian", "Red Rising", "The Blade Itself", "Rhythm of War"];
    for expected in &expected_in_grab {
        assert!(
            json_titles.contains(expected),
            "{expected} must be in JSON grab list; got {:?}",
            json_titles
        );
    }

    // Verify no duplicates in JSON
    let mut seen = std::collections::HashSet::new();
    for title in &json_titles {
        assert!(seen.insert(*title), "duplicate in JSON grab list: {title:?}");
    }
}
