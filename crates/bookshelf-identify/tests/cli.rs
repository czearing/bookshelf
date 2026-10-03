use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    process::{Command, Output},
};
use tempfile::TempDir;
use zip::{ZipWriter, write::SimpleFileOptions};
mod support;

const ISBN: &str = "9780306406157";
const TEXT_QUERY: &str = "A Test Book Jane Example";
const COVER_URL: &str = "https://books.google.com/books/content?id=test-cover";
const OTHER_COVER_URL: &str = "https://books.google.com/books/content?id=other-cover";

fn epub(path: &Path, metadata: &str) {
    epub_with_cover(path, metadata, Some(&cover_bytes(3)));
}

fn cover_bytes(seed: u32) -> Vec<u8> {
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(160, 240, |x, y| {
        let cell = (x / 16) + 10 * (y / 16);
        let value = cell.wrapping_mul(0x9e3779b9).rotate_left(seed) ^ seed.wrapping_mul(0x85ebca6b);
        image::Rgb([(value >> 16) as u8, (value >> 8) as u8, value as u8])
    }));
    let mut bytes = std::io::Cursor::new(Vec::new());
    image.write_to(&mut bytes, image::ImageFormat::Png).unwrap();
    bytes.into_inner()
}

fn epub_with_cover(path: &Path, metadata: &str, cover: Option<&[u8]>) {
    let manifest = if cover.is_some() {
        r#"<item id="cover-image" href="images/cover%20art.png" media-type="image/png" properties="cover-image"/>"#
    } else {
        ""
    };
    epub_with_layout(path, metadata, cover, manifest, "");
}

fn epub_with_layout(
    path: &Path,
    metadata: &str,
    cover: Option<&[u8]>,
    manifest: &str,
    guide: &str,
) {
    epub_with_documents(
        path,
        metadata,
        cover,
        manifest,
        guide,
        &[(
            "chapter.xhtml",
            r#"<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Test</title></head><body><p>Original book content.</p></body></html>"#,
        )],
    );
}

fn epub_with_documents(
    path: &Path,
    metadata: &str,
    cover: Option<&[u8]>,
    manifest: &str,
    guide: &str,
    documents: &[(&str, &str)],
) {
    let mut zip = ZipWriter::new(File::create(path).unwrap());
    zip.start_file("mimetype", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"application/epub+zip").unwrap();
    zip.start_file("META-INF/container.xml", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(br#"<?xml version="1.0"?>
        <container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
          <rootfiles><rootfile full-path="EPUB/package.opf" media-type="application/oebps-package+xml"/></rootfiles>
        </container>"#).unwrap();
    zip.start_file("EPUB/package.opf", SimpleFileOptions::default())
        .unwrap();
    let items = documents
        .iter()
        .enumerate()
        .map(|(index, (name, _))| {
            format!(r#"<item id="text-{index}" href="{name}" media-type="application/xhtml+xml"/>"#)
        })
        .collect::<String>();
    let spine = (0..documents.len())
        .map(|index| format!(r#"<itemref idref="text-{index}"/>"#))
        .collect::<String>();
    write!(zip, r#"<?xml version="1.0"?>
        <package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" version="3.0" unique-identifier="book">
          <metadata>{metadata}</metadata>
          <manifest>{manifest}{items}<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/></manifest>
          <spine>{spine}</spine>
          {guide}
        </package>"#).unwrap();
    for (name, xml) in documents {
        zip.start_file(format!("EPUB/{name}"), SimpleFileOptions::default())
            .unwrap();
        zip.write_all(xml.as_bytes()).unwrap();
    }
    zip.start_file("EPUB/nav.xhtml", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(br#"<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops"><head><title>Contents</title></head><body><nav epub:type="toc"><ol><li><a href="chapter.xhtml">Chapter</a></li></ol></nav></body></html>"#).unwrap();
    if let Some(cover) = cover {
        zip.start_file("EPUB/images/cover art.png", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(cover).unwrap();
        zip.start_file("EPUB/text/cover.xhtml", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(br#"<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Cover</title></head><body><img src="../images/cover%20art.png" alt="cover"/></body></html>"#).unwrap();
    }
    zip.finish().unwrap();
}

fn metadata() -> String {
    format!(
        r##"<dc:identifier id="book">urn:isbn:{ISBN}</dc:identifier>
        <dc:title>A Test Book</dc:title>
        <dc:creator id="author">Jane Example</dc:creator>
        <meta refines="#author" property="role" scheme="marc:relators">aut</meta>
        <dc:creator id="translator">A Translator</dc:creator>
        <meta refines="#translator" property="role" scheme="marc:relators">trl</meta>
        <dc:language>en</dc:language><meta property="dcterms:modified">2026-01-01T00:00:00Z</meta>"##
    )
}

fn volume(id: &str) -> Value {
    json!({
        "id": id,
        "volumeInfo": {
            "title": "A Test Book",
            "authors": ["Jane Example"],
            "language": "en",
            "imageLinks": {"thumbnail": COVER_URL},
            "industryIdentifiers": [{"type": "ISBN_13", "identifier": ISBN}]
        }
    })
}

fn cache(dir: &Path, query: &str, volumes: Vec<Value>) {
    cache_response(dir, r#""A Test Book" Jane Example"#, &[]);
    if query.starts_with("isbn:") {
        cache_response(dir, TEXT_QUERY, &volumes);
    }
    cache_response(dir, query, &volumes);
    cache_cover(dir, COVER_URL, &cover_bytes(3));
    cache_cover(dir, OTHER_COVER_URL, &cover_bytes(11));
}

fn cache_cover(dir: &Path, url: &str, bytes: &[u8]) {
    let dir = dir.join("covers");
    fs::create_dir_all(&dir).unwrap();
    let name = format!("{:x}.image", Sha256::digest(format!("cover-v1\n{url}")));
    fs::write(dir.join(name), bytes).unwrap();
}

fn cache_response(dir: &Path, query: &str, volumes: &[Value]) {
    fs::create_dir_all(dir).unwrap();
    let body = serde_json::to_vec(&json!({
        "kind": "books#volumes",
        "totalItems": volumes.len(),
        "items": volumes
    }))
    .unwrap();
    fs::write(
        dir.join(bookshelf_identify::google::cache_name(query)),
        &body,
    )
    .unwrap();
    // The second-attempt (quoted phrase) query is restricted to the book's language.
    if query.starts_with('"') {
        for lang in ["en", "fr", "de", "es", "ru"] {
            fs::write(
                dir.join(bookshelf_identify::google::page_cache_name_lang(
                    query, lang, 0,
                )),
                &body,
            )
            .unwrap();
        }
    }
    fs::create_dir_all(dir.join("details")).unwrap();
    for volume in volumes {
        let id = volume["id"].as_str().unwrap();
        fs::write(
            dir.join("details")
                .join(bookshelf_identify::google::detail_cache_name(id)),
            serde_json::to_vec(volume).unwrap(),
        )
        .unwrap();
    }
}

fn run(path: &Path, cache: &Path, apply: bool) -> (Output, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bs-identify"));
    command.arg(path).arg("--cache-dir").arg(cache).args([
        "--offline",
        "--json",
        "--google-isbn-search",
        "--google-details",
    ]);
    if apply {
        command.arg("--apply");
    }
    let output = command.output().unwrap();
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "Invalid report: {error}; stderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, report)
}
fn xhtml(body: &str) -> String {
    format!(
        r#"<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops"><body>{body}</body></html>"#
    )
}

fn text_book(path: &Path, metadata: &str, title: &str, author: &str, prose: &str) {
    let front = xhtml(&format!("<h1>{title}</h1><p>by {author}</p>"));
    let chapter = xhtml(&format!("<h1>Chapter One</h1><p>{prose}</p>"));
    epub_with_documents(
        path,
        metadata,
        None,
        "",
        "",
        &[("title.xhtml", &front), ("chapter.xhtml", &chapter)],
    );
}

fn without_isbn() -> String {
    metadata().replace(&format!("urn:isbn:{ISBN}"), "local-book")
}

#[test]
fn title_page_without_cover_identifies_work_and_detects_five_languages() {
    for &(language, prose) in support::LANGUAGES {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        let metadata = without_isbn().replace("<dc:language>en</dc:language>", "");
        text_book(&path, &metadata, "A Test Book", "Jane Example", prose);
        let before = fs::read(&path).unwrap();
        let mut candidate = volume("text-backed");
        candidate["volumeInfo"]["language"] = json!(language);
        candidate["volumeInfo"]["imageLinks"] = json!({});
        let cache_dir = dir.path().join("cache");
        cache(&cache_dir, TEXT_QUERY, vec![candidate]);
        let (output, report) = run(&path, &cache_dir, true);
        assert!(output.status.success(), "{language}: {report}");
        assert_eq!(report["renamed"], 1);
        let record = &report["records"][0];
        assert_eq!(record["language"]["status"], "detected");
        assert_eq!(record["work"]["language"], language);
        assert_eq!(record["edition"]["status"], "unknown");
        assert!(record["google_id"].is_null());
        assert!(record["candidates"][0]["cover"].is_null());
        assert_eq!(
            before,
            fs::read(dir.path().join("Jane Example - A Test Book.epub")).unwrap()
        );
    }
}

#[test]
fn copied_metadata_and_cover_cannot_override_contradictory_title_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let title = xhtml("<h1>An Entirely Different Story</h1><p>by Another Writer</p>");
    epub_with_documents(
        &path,
        &metadata(),
        Some(&cover_bytes(3)),
        r#"<item id="cover-image" href="images/cover%20art.png" media-type="image/png" properties="cover-image"/>"#,
        "",
        &[("title.xhtml", &title)],
    );
    let before = fs::read(&path).unwrap();
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("copied")]);
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Book text conflicts")
    );
    assert_eq!(before, fs::read(&path).unwrap());
}

#[test]
fn incidental_references_and_hidden_title_pages_are_not_corroboration() {
    for body in [
        "<h1>References</h1><p>A Test Book</p><p>Jane Example</p>",
        r#"<div hidden=""><h1>A Test Book</h1><p>by Jane Example</p></div>"#,
        r#"<nav><h1>A Test Book</h1><p>by Jane Example</p></nav>"#,
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        let doc = xhtml(body);
        epub_with_documents(
            &path,
            &without_isbn(),
            None,
            "",
            "",
            &[("chapter.xhtml", &doc)],
        );
        let cache_dir = dir.path().join("cache");
        cache(&cache_dir, TEXT_QUERY, vec![volume("unproven")]);
        let (output, report) = run(&path, &cache_dir, true);
        assert_eq!(output.status.code(), Some(1), "{report}");
        assert_eq!(report["review"], 1);
        assert!(path.exists());
    }
}

#[test]
fn wrong_declared_or_provider_language_blocks_rename() {
    for declared_wrong in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        let metadata = if declared_wrong {
            without_isbn()
        } else {
            without_isbn().replace(
                "<dc:language>en</dc:language>",
                "<dc:language>fr</dc:language>",
            )
        };
        text_book(
            &path,
            &metadata,
            "A Test Book",
            "Jane Example",
            support::FRENCH,
        );
        let cache_dir = dir.path().join("cache");
        cache(&cache_dir, TEXT_QUERY, vec![volume("english-provider")]);
        let (output, report) = run(&path, &cache_dir, true);
        assert_eq!(output.status.code(), Some(1), "{report}");
        assert_eq!(report["review"], 1);
        assert_eq!(report["records"][0]["language"]["language"], "fr");
        assert_eq!(
            report["records"][0]["language"]["status"],
            if declared_wrong {
                "conflict"
            } else {
                "confirmed"
            }
        );
        assert!(path.exists());
    }
}

#[test]
fn supported_ebook_edition_requires_additional_agreement_and_no_conflicts() {
    for (case, publisher, date, ebook, expected) in [
        (
            "supported",
            "Test Press",
            "2020-06-01",
            Some(true),
            "supported",
        ),
        (
            "publisher-conflict",
            "Other Press",
            "2020-06-01",
            Some(true),
            "conflict",
        ),
        (
            "date-conflict",
            "Test Press",
            "2019",
            Some(true),
            "conflict",
        ),
        ("print-only", "Test Press", "2020", Some(false), "conflict"),
        ("format-unknown", "Test Press", "2020", None, "unknown"),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        let metadata = format!(
            "{}<dc:publisher>Test Press</dc:publisher><dc:date>2020</dc:date>",
            metadata()
        );
        text_book(
            &path,
            &metadata,
            "A Test Book",
            "Jane Example",
            support::ENGLISH,
        );
        let mut candidate = volume(case);
        candidate["volumeInfo"]["publisher"] = json!(publisher);
        candidate["volumeInfo"]["publishedDate"] = json!(date);
        candidate["saleInfo"] = json!({"isEbook": ebook});
        let cache_dir = dir.path().join("cache");
        cache(&cache_dir, &format!("isbn:{ISBN}"), vec![candidate]);
        let (output, report) = run(&path, &cache_dir, true);
        assert!(output.status.success(), "{case}: {report}");
        assert_eq!(report["renamed"], 1);
        let record = &report["records"][0];
        assert_eq!(record["edition"]["status"], expected, "{case}: {report}");
        if expected == "supported" {
            assert_eq!(record["google_id"], case);
            assert_eq!(record["edition"]["publisher"], "Test Press");
            assert_eq!(record["edition"]["identifiers"], json!([ISBN]));
            assert_eq!(record["edition"]["is_ebook"], true);
        } else {
            assert!(record["google_id"].is_null());
            assert!(record["edition"]["publisher"].is_null());
            assert!(record["edition"]["published_date"].is_null());
            assert_eq!(record["edition"]["identifiers"], json!([]));
        }
    }
}

#[test]
fn full_volume_details_can_reject_a_plausible_search_hit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    text_book(
        &path,
        &without_isbn(),
        "A Test Book",
        "Jane Example",
        support::ENGLISH,
    );
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, TEXT_QUERY, vec![volume("changed")]);
    let mut details = volume("changed");
    details["volumeInfo"]["authors"] = json!(["Someone Else"]);
    fs::write(
        cache_dir
            .join("details")
            .join(bookshelf_identify::google::detail_cache_name("changed")),
        serde_json::to_vec(&details).unwrap(),
    )
    .unwrap();
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    assert!(path.exists());
}

#[test]
fn pagination_and_broader_query_recover_candidates_outside_initial_results() {
    for paginated in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        text_book(
            &path,
            &without_isbn(),
            "A Test Book",
            "Jane Example",
            support::ENGLISH,
        );
        let cache_dir = dir.path().join("cache");
        let wrong = (0..if paginated { 40 } else { 1 })
            .map(|index| {
                let mut candidate = volume(&format!("wrong-{index}"));
                candidate["volumeInfo"]["title"] = json!("An Unrelated Book");
                candidate
            })
            .collect();
        cache(&cache_dir, TEXT_QUERY, wrong);
        if paginated {
            let next =
                json!({"kind":"books#volumes", "totalItems":41, "items":[volume("correct")]});
            fs::write(
                cache_dir.join(bookshelf_identify::google::page_cache_name(TEXT_QUERY, 40)),
                serde_json::to_vec(&next).unwrap(),
            )
            .unwrap();
            fs::write(
                cache_dir
                    .join("details")
                    .join(bookshelf_identify::google::detail_cache_name("correct")),
                serde_json::to_vec(&volume("correct")).unwrap(),
            )
            .unwrap();
        } else {
            cache_response(
                &cache_dir,
                r#""A Test Book" Jane Example"#,
                &[volume("correct")],
            );
        }
        let (output, report) = run(&path, &cache_dir, true);
        assert!(output.status.success(), "{report}");
        assert_eq!(
            report["records"][0]["work"]["supporting_google_ids"],
            json!(["correct"])
        );
        assert_eq!(report["records"][0]["edition"]["status"], "unknown");
    }
}
#[test]
fn differing_cover_does_not_veto_text_supported_work() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let front = xhtml("<h1>A Test Book</h1><p>by Jane Example</p>");
    let chapter = xhtml(&format!("<p>{}</p>", support::ENGLISH));
    epub_with_documents(
        &path,
        &metadata(),
        Some(&cover_bytes(11)),
        r#"<item id="cover-image" href="images/cover%20art.png" media-type="image/png" properties="cover-image"/>"#,
        "",
        &[("title.xhtml", &front), ("chapter.xhtml", &chapter)],
    );
    let cache_dir = dir.path().join("cache");
    cache(
        &cache_dir,
        &format!("isbn:{ISBN}"),
        vec![volume("different-art")],
    );
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    assert_eq!(
        report["records"][0]["candidates"][0]["cover"]["strong"],
        false
    );
    assert_eq!(report["records"][0]["edition"]["status"], "conflict");
    assert!(report["records"][0]["google_id"].is_null());
}

#[test]
fn bilingual_text_within_one_spine_document_requires_review() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    text_book(
        &path,
        &without_isbn(),
        "A Test Book",
        "Jane Example",
        &format!("{}</p><p>{}", support::ENGLISH, support::FRENCH),
    );
    let (output, report) = run(&path, &dir.path().join("unused-cache"), true);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    assert_eq!(report["records"][0]["language"]["status"], "multilingual");
    assert!(report["records"][0]["work"].is_null());
    assert!(path.exists());
}

#[test]
fn conflicting_supported_editions_remain_ambiguous_but_duplicate_records_do_not() {
    const OTHER_ISBN: &str = "9780141439518";
    for distinct_editions in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("input.epub");
        let metadata = format!(
            "{}<dc:identifier>{OTHER_ISBN}</dc:identifier><dc:publisher>Test Press</dc:publisher><dc:date>2020</dc:date>",
            metadata()
        );
        text_book(
            &path,
            &metadata,
            "A Test Book",
            "Jane Example",
            support::ENGLISH,
        );
        let mut first = volume("first");
        first["volumeInfo"]["publisher"] = json!("Test Press");
        first["volumeInfo"]["publishedDate"] = json!("2020");
        first["saleInfo"] = json!({"isEbook":true});
        let mut second = first.clone();
        second["id"] = json!("second");
        if distinct_editions {
            second["volumeInfo"]["industryIdentifiers"][0]["identifier"] = json!(OTHER_ISBN);
        }
        let cache_dir = dir.path().join("cache");
        cache(
            &cache_dir,
            &format!("isbn:{ISBN}"),
            vec![first.clone(), second.clone()],
        );
        cache_response(&cache_dir, &format!("isbn:{OTHER_ISBN}"), &[first, second]);
        let (output, report) = run(&path, &cache_dir, true);
        assert!(output.status.success(), "{report}");
        assert_eq!(report["renamed"], 1);
        assert_eq!(
            report["records"][0]["edition"]["status"],
            if distinct_editions {
                "ambiguous"
            } else {
                "supported"
            }
        );
        if distinct_editions {
            assert!(report["records"][0]["google_id"].is_null());
            assert_eq!(report["records"][0]["edition"]["identifiers"], json!([]));
        }
    }
}

#[test]
fn preview_apply_and_repeat_are_safe_and_byte_identical() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    let path = books.join("untagged.EPUB");
    let cache_dir = dir.path().join("cache");
    epub(&path, &metadata());
    let original = fs::read(&path).unwrap();
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);

    let (output, report) = run(&books, &cache_dir, false);
    assert!(output.status.success());
    assert_eq!(report["planned"], 1);
    assert_eq!(
        report["records"][0]["metadata"]["authors"],
        json!(["Jane Example"])
    );
    assert!(path.exists());
    let destination = books.join("Jane Example - A Test Book.epub");
    assert!(!destination.exists());

    let (output, report) = run(&books, &cache_dir, true);
    assert!(output.status.success());
    assert_eq!(report["renamed"], 1);
    assert!(!path.exists());
    assert_eq!(fs::read(&destination).unwrap(), original);

    let (output, report) = run(&books, &cache_dir, true);
    assert!(output.status.success());
    assert_eq!(report["unchanged"], 1);
    assert_eq!(report["renamed"], 0);
    assert_eq!(fs::read(&destination).unwrap(), original);
}

#[test]
fn identical_copies_of_one_book_never_collide_and_never_need_a_human() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    epub(&books.join("one.epub"), &metadata());
    epub(&books.join("two.epub"), &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let (output, report) = run(&books, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["errors"], 0);
    assert_eq!(report["renamed"], 1, "{report}");
    assert_eq!(report["duplicates"], 1, "{report}");
    assert!(books.join("Jane Example - A Test Book.epub").exists());
    // The identical copy is set aside exactly where it was: nothing is deleted.
    assert!(books.join("two.epub").exists());
}

#[test]
fn two_different_files_of_one_book_both_keep_their_content_under_distinct_names() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    epub(&books.join("one.epub"), &metadata());
    // Different bytes (an extra subject), same book and same artwork.
    epub(
        &books.join("two.epub"),
        &metadata().replace(
            "<dc:language>en</dc:language>",
            "<dc:language>en</dc:language><dc:subject>extra</dc:subject>",
        ),
    );
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let (output, report) = run(&books, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 2, "{report}");
    assert_eq!(report["duplicates"], 0);
    assert!(books.join("Jane Example - A Test Book.epub").exists());
    assert!(books.join("Jane Example - A Test Book (2).epub").exists());
}

#[test]
fn multiple_records_for_the_same_work_do_not_invent_an_edition() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(
        &cache_dir,
        &format!("isbn:{ISBN}"),
        vec![volume("one"), volume("two")],
    );
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    assert_eq!(report["records"][0]["edition"]["status"], "unknown");
    assert!(report["records"][0]["google_id"].is_null());
    assert_eq!(
        report["records"][0]["candidates"].as_array().unwrap().len(),
        2
    );
    assert!(dir.path().join("Jane Example - A Test Book.epub").exists());
}

#[test]
fn isbn_does_not_override_conflicting_cover_language_author_or_collection() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    let mut wrong_isbn = volume("wrong-isbn");
    wrong_isbn["volumeInfo"]["industryIdentifiers"][0]["identifier"] = json!("9780804429573");
    wrong_isbn["volumeInfo"]["imageLinks"]["thumbnail"] = json!(OTHER_COVER_URL);
    let mut wrong_language = volume("wrong-language");
    wrong_language["volumeInfo"]["language"] = json!("de");
    let mut wrong_author = volume("wrong-author");
    wrong_author["volumeInfo"]["authors"] = json!(["Another Author"]);
    let mut collection = volume("collection");
    collection["volumeInfo"]["title"] = json!("A Test Book Collection");
    cache(
        &cache_dir,
        &format!("isbn:{ISBN}"),
        vec![wrong_isbn, wrong_language, wrong_author, collection],
    );
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report["review"], 1);
    assert!(
        report["records"][0]["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["accepted"] == false)
    );
    assert!(path.exists());
}

#[test]
fn title_author_fallback_is_explicit_about_edition() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(
        &path,
        r#"<dc:title>A Test Book</dc:title><dc:creator>Jane Example</dc:creator><dc:language>en</dc:language>"#,
    );
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, TEXT_QUERY, vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success());
    assert_eq!(report["planned"], 1);
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("edition unknown")
    );
}

#[test]
fn invalid_epub_and_missing_cache_are_explicit_errors() {
    let dir = TempDir::new().unwrap();
    let bad = dir.path().join("bad.epub");
    fs::write(&bad, b"not a ZIP").unwrap();
    let cache_dir = dir.path().join("cache");
    let (output, report) = run(&bad, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report["errors"], 1);
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Invalid EPUB ZIP")
    );
    let good = dir.path().join("good.epub");
    epub(&good, &metadata());
    let (output, report) = run(&good, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("offline mode")
    );
    assert!(good.exists());
}

#[test]
fn malformed_cache_is_not_reported_as_no_match() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let entry = cache_dir.join(bookshelf_identify::google::cache_name(TEXT_QUERY));
    fs::write(entry, b"broken json").unwrap();
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report["errors"], 1);
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("--refresh")
    );
}

#[test]
fn existing_destination_is_never_overwritten() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let destination = dir.path().join("Jane Example - A Test Book.epub");
    fs::write(&destination, b"unrelated existing file").unwrap();
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    // Whatever was there is untouched; the book takes the next free numbered name.
    assert_eq!(fs::read(&destination).unwrap(), b"unrelated existing file");
    assert!(
        dir.path()
            .join("Jane Example - A Test Book (2).epub")
            .exists()
    );
    assert!(!path.exists());
}

#[test]
fn overlapping_inputs_do_not_process_files_twice() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let output = Command::new(env!("CARGO_BIN_EXE_bs-identify"))
        .arg(&path)
        .arg(dir.path())
        .arg("--cache-dir")
        .arg(&cache_dir)
        .args(["--offline", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["total"], 1);
    assert_eq!(report["planned"], 1);
}

#[test]
fn main_title_refinement_overrides_subtitle_order() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    let metadata = metadata().replace(
        "<dc:title>A Test Book</dc:title>",
        r##"<dc:title id="sub">A Subtitle</dc:title>
        <meta refines="#sub" property="title-type">subtitle</meta>
        <dc:title id="main">A Test Book</dc:title>
        <meta refines="#main" property="title-type">main</meta>"##,
    );
    epub(&path, &metadata);
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success());
    assert_eq!(report["records"][0]["metadata"]["title"], "A Test Book");
    assert_eq!(report["planned"], 1);
}

#[test]
fn oversized_metadata_and_missing_identity_are_errors() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("oversized.epub");
    epub(
        &path,
        &format!(
            "<dc:description>{}</dc:description>",
            "a".repeat(4 * 1024 * 1024)
        ),
    );
    let cache_dir = dir.path().join("cache");
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("too large")
    );
    let path = dir.path().join("unknown.epub");
    epub(&path, "<dc:title>Unknown</dc:title>");
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Insufficient embedded metadata")
    );
    assert!(path.exists());
}

#[test]
fn cached_batch_performance() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    for index in 0..256 {
        let book_dir = books.join(index.to_string());
        fs::create_dir(&book_dir).unwrap();
        epub(&book_dir.join("original.epub"), &metadata());
    }
    let start = std::time::Instant::now();
    let (output, report) = run(&books, &cache_dir, false);
    let elapsed = start.elapsed();
    assert!(output.status.success());
    assert_eq!(report["total"], 256);
    assert_eq!(report["planned"], 256);
    eprintln!(
        "Cached 256-book preview: {:.3}s ({:.0} books/s); synthetic EPUBs with real cover decoding",
        elapsed.as_secs_f64(),
        256.0 / elapsed.as_secs_f64()
    );
}

#[test]
fn valid_but_wrong_isbn_recovers_through_independent_text_and_cover() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let original = fs::read(&path).unwrap();
    let cache_dir = dir.path().join("cache");
    let mut wrong = volume("wrong-isbn-book");
    wrong["volumeInfo"]["title"] = json!("Entirely Different Book");
    wrong["volumeInfo"]["authors"] = json!(["Another Person"]);
    wrong["volumeInfo"]["imageLinks"]["thumbnail"] = json!(OTHER_COVER_URL);
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![wrong]);
    let mut correct = volume("correct-book");
    correct["volumeInfo"]["industryIdentifiers"][0]["identifier"] = json!("9780804429573");
    cache_response(&cache_dir, TEXT_QUERY, &[correct]);

    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    assert_eq!(
        report["records"][0]["work"]["supporting_google_ids"],
        json!(["correct-book"])
    );
    assert!(report["records"][0]["google_id"].is_null());
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("ISBN ignored")
    );
    let candidates = report["records"][0]["candidates"].as_array().unwrap();
    let correct = candidates
        .iter()
        .find(|c| c["google_id"] == "correct-book")
        .unwrap();
    assert_eq!(correct["isbn_agrees"], false);
    assert_eq!(correct["found_by_title_author"], true);
    assert_eq!(correct["cover"]["strong"], true);
    let wrong = candidates
        .iter()
        .find(|c| c["google_id"] == "wrong-isbn-book")
        .unwrap();
    assert_eq!(wrong["isbn_agrees"], true);
    assert_eq!(wrong["accepted"], false);
    assert_eq!(
        fs::read(dir.path().join("Jane Example - A Test Book.epub")).unwrap(),
        original
    );
}

#[test]
fn empty_isbn_results_do_not_veto_independent_cover_match() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![]);
    let mut correct = volume("correct");
    correct["volumeInfo"]["industryIdentifiers"] = json!([]);
    cache_response(&cache_dir, TEXT_QUERY, &[correct]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["planned"], 1);
    assert_eq!(report["records"][0]["candidates"][0]["isbn_agrees"], false);
}

#[test]
fn isbn_alone_or_with_only_one_text_field_never_renames() {
    let dir = TempDir::new().unwrap();
    let cache_dir = dir.path().join("empty-cache");
    for (index, fields) in [
        "",
        "<dc:title>A Test Book</dc:title>",
        "<dc:creator>Jane Example</dc:creator>",
        "<dc:title>!!!</dc:title><dc:creator>Jane Example</dc:creator>",
    ]
    .iter()
    .enumerate()
    {
        let path = dir.path().join(format!("{index}.epub"));
        epub(
            &path,
            &format!(r#"<dc:identifier>{ISBN}</dc:identifier>{fields}"#),
        );
        let original = fs::read(&path).unwrap();
        let (output, report) = run(&path, &cache_dir, true);
        assert_eq!(output.status.code(), Some(1), "{report}");
        assert_eq!(report["review"], 1);
        assert!(
            report["records"][0]["reason"]
                .as_str()
                .unwrap()
                .contains("both required")
        );
        assert_eq!(fs::read(&path).unwrap(), original);
    }
    assert!(
        !cache_dir.exists(),
        "Missing corroboration must not make API/cache requests"
    );
}

#[test]
fn copied_isbn_title_and_author_do_not_override_the_actual_cover() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub_with_cover(&path, &metadata(), Some(&cover_bytes(11)));
    let original = fs::read(&path).unwrap();
    let cache_dir = dir.path().join("cache");
    cache(
        &cache_dir,
        &format!("isbn:{ISBN}"),
        vec![volume("wrong-book")],
    );
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    let candidate = &report["records"][0]["candidates"][0];
    assert_eq!(candidate["isbn_agrees"], true);
    assert_eq!(candidate["found_by_title_author"], true);
    assert_eq!(candidate["cover"]["strong"], false);
    assert_eq!(candidate["accepted"], false);
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn cover_disambiguates_even_when_the_wrong_art_has_the_matching_isbn() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    let mut wrong = volume("wrong-art");
    wrong["volumeInfo"]["imageLinks"]["thumbnail"] = json!(OTHER_COVER_URL);
    let mut correct = volume("correct-art");
    correct["volumeInfo"]["industryIdentifiers"] = json!([]);
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![wrong.clone()]);
    cache_response(&cache_dir, TEXT_QUERY, &[correct, wrong]);
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(
        report["records"][0]["work"]["supporting_google_ids"],
        json!(["correct-art"])
    );
    assert_eq!(report["renamed"], 1);
}

#[test]
fn isbn_candidate_can_be_supported_by_actual_cover_not_search_membership() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    cache_response(&cache_dir, TEXT_QUERY, &[]);
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    assert_eq!(
        report["records"][0]["candidates"][0]["found_by_title_author"],
        false
    );
    assert!(dir.path().join("Jane Example - A Test Book.epub").exists());
}

#[test]
fn missing_local_cover_cannot_fall_back_to_metadata_only() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub_with_cover(&path, &metadata(), None);
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, true);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("No corroborated work match")
    );
    assert!(path.exists());
}

#[test]
fn incomplete_other_edition_does_not_veto_corroborated_work() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    let mut unknown = volume("unknown-art");
    unknown["volumeInfo"]["imageLinks"] = json!({});
    cache(
        &cache_dir,
        &format!("isbn:{ISBN}"),
        vec![volume("known-art"), unknown],
    );
    let (output, report) = run(&path, &cache_dir, true);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    assert_eq!(report["records"][0]["edition"]["status"], "unknown");
    assert!(dir.path().join("Jane Example - A Test Book.epub").exists());
}

#[test]
fn corrupt_and_missing_cached_covers_never_become_matches() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("original.epub");
    epub(&path, &metadata());
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let cover_path = cache_dir.join("covers").join(format!(
        "{:x}.image",
        Sha256::digest(format!("cover-v1\n{COVER_URL}"))
    ));
    fs::write(&cover_path, b"not an image").unwrap();
    for missing in [false, true] {
        if missing {
            fs::remove_file(&cover_path).unwrap();
        }
        let (output, report) = run(&path, &cache_dir, true);
        assert_eq!(output.status.code(), Some(1), "{report}");
        assert_eq!(report["review"], 1);
        assert!(
            report["records"][0]["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|warning| warning.as_str().unwrap().contains("Cover unavailable"))
        );
        assert!(path.exists());
    }
}

#[test]
fn epub2_metadata_cover_and_guide_cover_are_supported() {
    let dir = TempDir::new().unwrap();
    let cache_dir = dir.path().join("cache");
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let image_manifest = r#"<item id="art" href="images/cover%20art.png" media-type="image/png"/>"#;
    let cover = cover_bytes(3);
    for guided in [false, true] {
        let path = dir.path().join(format!("legacy-{guided}.epub"));
        let (meta, manifest, guide) = if guided {
            (
                metadata(),
                format!(
                    r#"{image_manifest}<item id="coverpage" href="text/cover.xhtml" media-type="application/xhtml+xml"/>"#
                ),
                r#"<guide><reference type="cover" href="text/cover.xhtml#top"/></guide>"#,
            )
        } else {
            (
                format!(r#"{}<meta name="cover" content="art"/>"#, metadata()),
                image_manifest.to_owned(),
                "",
            )
        };
        epub_with_layout(&path, &meta, Some(&cover), &manifest, guide);
        let (output, report) = run(&path, &cache_dir, false);
        assert!(output.status.success(), "{report}");
        assert_eq!(report["planned"], 1);
        assert_eq!(
            report["records"][0]["candidates"][0]["cover"]["strong"],
            true
        );
    }
}

fn run_with(path: &Path, cache: &Path, extra: &[&str]) -> (Output, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_bs-identify"))
        .arg(path)
        .arg("--cache-dir")
        .arg(cache)
        .args([
            "--offline",
            "--json",
            "--google-isbn-search",
            "--google-details",
        ])
        .args(extra)
        .output()
        .unwrap();
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "Invalid report: {error}; stderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, report)
}

#[test]
fn tagging_normalizes_case_is_idempotent_ignores_backups_and_undoes_exactly() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    let path = books.join("shouty.epub");
    let cache_dir = dir.path().join("cache");
    epub(
        &path,
        &metadata()
            .replace("A Test Book", "A TEST BOOK")
            .replace("Jane Example", "JANE EXAMPLE"),
    );
    let original = fs::read(&path).unwrap();
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    cache_response(&cache_dir, "A TEST BOOK JANE EXAMPLE", &[volume("one")]);

    let (output, report) = run_with(&books, &cache_dir, &["--apply", "--tag"]);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1);
    let destination = books.join("Jane Example - A Test Book.epub");
    let (metadata, _) = bookshelf_identify::epub::read(&destination).unwrap();
    assert_eq!(metadata.title, "A Test Book");
    assert_eq!(metadata.authors, ["Jane Example"]);
    assert_eq!(
        metadata.translators,
        ["A Translator"],
        "other credits survive"
    );

    // The backup folder must never be scanned as part of the library.
    let (output, report) = run_with(&books, &cache_dir, &["--apply", "--tag"]);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["total"], 1);
    assert_eq!(report["unchanged"], 1, "{report}");

    let undo = Command::new(env!("CARGO_BIN_EXE_bs-identify"))
        .arg("--undo")
        .arg(books.join(".book-manager-backup"))
        .output()
        .unwrap();
    assert!(
        undo.status.success(),
        "{}",
        String::from_utf8_lossy(&undo.stderr)
    );
    assert!(!destination.exists());
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn state_database_resumes_skips_unchanged_files_and_tracks_the_review_queue() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    let path = books.join("untagged.epub");
    let cache_dir = dir.path().join("cache");
    let state = dir.path().join("state.db");
    epub(&path, &metadata());
    cache(&cache_dir, &format!("isbn:{ISBN}"), vec![volume("one")]);
    let state_arg = state.to_str().unwrap();

    // First run does the lookups and records the outcome.
    let (output, report) = run_with(&books, &cache_dir, &["--state", state_arg]);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["planned"], 1);

    // With the cache gone and no network, the stored result still answers.
    let empty = dir.path().join("empty-cache");
    let (output, report) = run_with(&books, &empty, &["--state", state_arg]);
    assert!(output.status.success(), "{report}");
    assert_eq!(
        report["planned"], 1,
        "an unchanged file must come from the state file"
    );

    // A changed file is examined again (and fails offline, proving it was not reused).
    fs::write(&path, b"not an epub any more").unwrap();
    let (_, report) = run_with(&books, &empty, &["--state", state_arg]);
    assert_eq!(report["errors"], 1, "{report}");

    // Status and the review queue are readable without touching the library.
    let status = Command::new(env!("CARGO_BIN_EXE_bs-identify"))
        .args(["--state", state_arg, "--status"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&status.stdout).contains("1 books recorded"));
}

#[test]
fn approved_and_skipped_review_items_are_remembered_and_applied_from_the_state_file() {
    let dir = TempDir::new().unwrap();
    let books = dir.path().join("books");
    fs::create_dir(&books).unwrap();
    let path = books.join("mystery.epub");
    let other = books.join("other.epub");
    let cache_dir = dir.path().join("cache");
    let state = dir.path().join("state.db");
    let state_arg = state.to_str().unwrap();
    // No provider answers for these, so both go to review.
    epub(&path, &metadata());
    epub(&other, &metadata().replace("A Test Book", "Another Book"));
    cache_response(&cache_dir, TEXT_QUERY, &[]);
    cache_response(&cache_dir, r#""A Test Book" Jane Example"#, &[]);
    cache_response(&cache_dir, &format!("isbn:{ISBN}"), &[]);
    cache_response(&cache_dir, "Another Book Jane Example", &[]);
    cache_response(&cache_dir, r#""Another Book" Jane Example"#, &[]);

    let (_, report) = run_with(&books, &cache_dir, &["--state", state_arg]);
    assert_eq!(report["review"], 2, "{report}");

    let bin = env!("CARGO_BIN_EXE_bs-identify");
    let approve = Command::new(bin)
        .args(["--state", state_arg, "--approve"])
        .arg(&path)
        .args(["--title", "A Test Book", "--author", "Jane Example"])
        .output()
        .unwrap();
    assert!(
        approve.status.success(),
        "{}",
        String::from_utf8_lossy(&approve.stderr)
    );
    let skip = Command::new(bin)
        .args(["--state", state_arg, "--skip"])
        .arg(&other)
        .output()
        .unwrap();
    assert!(skip.status.success());

    // Even with no cache at all, the decisions drive the apply.
    let empty = dir.path().join("empty");
    let (output, report) = run_with(&books, &empty, &["--state", state_arg, "--apply"]);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["renamed"], 1, "{report}");
    assert!(books.join("Jane Example - A Test Book.epub").exists());
    assert!(other.exists(), "a skipped book is never touched");
    let queue = Command::new(bin)
        .args(["--state", state_arg, "--queue", "10"])
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&queue.stdout).trim().is_empty(),
        "queue should be empty"
    );
}

/// A book with running prose only: no title page and no cover, so nothing in the file
/// corroborates the tags. Returns the passages the tool will search for.
fn prose_only_book(path: &Path, front: Option<(&str, &str)>) -> Vec<String> {
    // Three distinct English paragraphs, so the book is single-language and offers
    // several different passages.
    let second = support::ENGLISH
        .replace("library", "museum")
        .replace("children", "visitors");
    let third = support::ENGLISH
        .replace("garden", "harbour")
        .replace("stories", "voyages");
    let prose = format!("<p>{}</p><p>{second}</p><p>{third}</p>", support::ENGLISH);
    let body = xhtml(&format!("<h1>Chapter One</h1>{prose}"));
    let title_page =
        front.map(|(title, author)| xhtml(&format!("<h1>{title}</h1><p>by {author}</p>")));
    let mut docs: Vec<(&str, &str)> = Vec::new();
    if let Some(page) = &title_page {
        docs.push(("title.xhtml", page));
    }
    docs.push(("chapter.xhtml", &body));
    epub_with_documents(path, &without_isbn(), None, "", "", &docs);
    let (metadata, _) = bookshelf_identify::epub::read(path).unwrap();
    metadata.content.passages
}

fn passage_query(passage: &str) -> String {
    format!("\"{}\"", bookshelf_identify::matching::query_text(passage))
}

fn witness_fixture(cache_dir: &Path, passages: &[String], found: Vec<Value>) {
    cache(cache_dir, TEXT_QUERY, vec![volume("one")]);
    for passage in passages {
        cache_response(cache_dir, &passage_query(passage), &found);
    }
}

#[test]
fn text_found_in_the_full_text_index_under_the_tagged_title_and_author_corroborates_the_work() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    let passages = prose_only_book(&path, None);
    assert!(
        !passages.is_empty(),
        "the book must offer passages to search"
    );

    // The tags agree with a provider record but nothing in the file confirms them,
    // and the text search returns nothing: the book stays in review.
    witness_fixture(&cache_dir, &passages, vec![]);
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);

    // The text is found in a book with this title and author: accepted.
    witness_fixture(&cache_dir, &passages, vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["planned"], 1, "{report}");
    let evidence = report["records"][0]["work"]["evidence"].to_string();
    assert!(evidence.contains("full-text index"), "{evidence}");
}

#[test]
fn text_that_belongs_to_a_different_book_never_corroborates_the_claimed_one() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    let passages = prose_only_book(&path, None);
    let mut other = volume("other");
    other["volumeInfo"]["title"] = json!("A Completely Different Novel");
    other["volumeInfo"]["authors"] = json!(["Someone Else"]);
    witness_fixture(&cache_dir, &passages, vec![other]);
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["review"], 1);
    assert!(report["records"][0]["work"].is_null());
}

#[test]
fn a_title_page_that_contradicts_the_tags_is_settled_only_by_the_text() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    let passages = prose_only_book(
        &path,
        Some(("Another Title Entirely", "Somebody Different")),
    );

    // Contradiction and no text evidence: review, with the conflict as the reason.
    witness_fixture(&cache_dir, &passages, vec![]);
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert!(
        report["records"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("conflicts"),
        "{report}"
    );

    // The text is found under the tagged title and author: the page was a variant.
    witness_fixture(&cache_dir, &passages, vec![volume("one")]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["planned"], 1, "{report}");
}

fn self_consistent_book(path: &Path) {
    // Package tags and a printed title page agree; the prose is plainly English.
    text_book(
        path,
        &without_isbn(),
        "A Test Book",
        "Jane Example",
        support::ENGLISH,
    );
}

#[test]
fn a_book_no_provider_has_is_named_from_its_own_consistent_tags_and_title_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    self_consistent_book(&path);
    cache(&cache_dir, TEXT_QUERY, vec![]);
    let (output, report) = run(&path, &cache_dir, false);
    assert!(output.status.success(), "{report}");
    assert_eq!(report["planned"], 1, "{report}");
    let work = &report["records"][0]["work"];
    assert_eq!(work["confidence"], "self_consistent");
    assert_eq!(report["records"][0]["edition"]["status"], "unknown");
}

#[test]
fn a_provider_failure_never_lets_a_book_name_itself() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    self_consistent_book(&path);
    // Offline with an empty cache: the provider cannot answer at all.
    let (output, report) = run(&path, &dir.path().join("empty"), false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["planned"], 0, "{report}");
}

#[test]
fn a_competing_record_with_the_same_title_blocks_self_naming_and_the_flag_disables_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    self_consistent_book(&path);
    // The provider knows this title, but under a different author.
    let mut other = volume("other");
    other["volumeInfo"]["authors"] = json!(["Someone Else"]);
    cache(&cache_dir, TEXT_QUERY, vec![other]);
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["planned"], 0, "{report}");

    // With no record at all, the opt-out flag keeps even a consistent book in review.
    cache(&cache_dir, TEXT_QUERY, vec![]);
    let (output, report) = run_with(&path, &cache_dir, &["--require-provider-match"]);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["planned"], 0, "{report}");
}

#[test]
fn a_file_cannot_simply_claim_to_be_a_collection() {
    // Tags say "Complete Works of Jane Example" but the book is one ordinary story whose
    // own title page names a different title: nothing may rename it as a collection.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("input.epub");
    let cache_dir = dir.path().join("cache");
    let metadata = without_isbn().replace("A Test Book", "Complete Works of Jane Example");
    text_book(
        &path,
        &metadata,
        "One Short Story",
        "Somebody Else",
        support::ENGLISH,
    );
    cache_response(
        &cache_dir,
        "Complete Works of Jane Example Jane Example",
        &[],
    );
    let (output, report) = run(&path, &cache_dir, false);
    assert_eq!(output.status.code(), Some(1), "{report}");
    assert_eq!(report["planned"], 0, "{report}");

    // Where the title page agrees with the claim, it is named by that title.
    let honest = dir.path().join("honest.epub");
    text_book(
        &honest,
        &metadata,
        "Complete Works of Jane Example",
        "Jane Example",
        support::ENGLISH,
    );
    let (_, report) = run(&honest, &cache_dir, false);
    let confidence = report["records"][0]["work"]["confidence"]
        .as_str()
        .unwrap_or("");
    assert!(
        confidence == "declared_collection" || confidence == "self_consistent",
        "{report}"
    );
}
