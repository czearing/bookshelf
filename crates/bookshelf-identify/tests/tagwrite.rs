use bookshelf_identify::{
    epub,
    tagwrite::{self, CoverAction, Tags},
};
use std::{
    fs::File,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};
mod support;

fn image(seed: u32, format: image::ImageFormat) -> Vec<u8> {
    let img = image::DynamicImage::ImageRgb8(image::RgbImage::from_fn(160, 240, |x, y| {
        let cell = (x / 16) + 10 * (y / 16);
        let value = cell.wrapping_mul(0x9e3779b9).rotate_left(seed) ^ seed.wrapping_mul(0x85ebca6b);
        image::Rgb([(value >> 16) as u8, (value >> 8) as u8, value as u8])
    }));
    let mut bytes = Cursor::new(Vec::new());
    img.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}

struct Book<'a> {
    opf: &'a str,
    members: Vec<(&'a str, Vec<u8>)>,
}

fn build(dir: &Path, name: &str, book: &Book) -> PathBuf {
    let path = dir.join(name);
    let mut zip = ZipWriter::new(File::create(&path).unwrap());
    let stored = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("mimetype", stored).unwrap();
    zip.write_all(b"application/epub+zip").unwrap();
    zip.start_file("META-INF/container.xml", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(br#"<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#).unwrap();
    zip.start_file("OEBPS/content.opf", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(book.opf.as_bytes()).unwrap();
    zip.start_file("OEBPS/chapter.xhtml", SimpleFileOptions::default())
        .unwrap();
    zip.write_all(
        format!(
            r#"<html xmlns="http://www.w3.org/1999/xhtml"><head><title>T</title></head><body><p>{}</p></body></html>"#,
            support::ENGLISH
        )
        .as_bytes(),
    )
    .unwrap();
    for (member, data) in &book.members {
        zip.start_file(*member, SimpleFileOptions::default())
            .unwrap();
        zip.write_all(data).unwrap();
    }
    zip.finish().unwrap();
    path
}

fn member(path: &Path, name: &str) -> Vec<u8> {
    let mut data = Vec::new();
    ZipArchive::new(File::open(path).unwrap())
        .unwrap()
        .by_name(name)
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();
    data
}

fn names(path: &Path) -> Vec<String> {
    let zip = ZipArchive::new(File::open(path).unwrap()).unwrap();
    zip.file_names().map(str::to_owned).collect()
}

fn tags() -> Tags {
    Tags {
        title: "The Real Title".into(),
        authors: vec!["Jane Example".into(), "John Second".into()],
        isbn13: Some("978-0-306-40615-7".into()),
        publisher: Some("Real House".into()),
        published_date: Some("2001".into()),
        language: Some("en".into()),
        replace_language: false,
        series: None,
    }
}

const EPUB2: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
  <metadata>
    <dc:title>wrong title</dc:title>
    <dc:creator opf:role="aut">Wrong Author</dc:creator>
    <dc:creator opf:role="trl">Tina Translator</dc:creator>
    <dc:identifier id="id">urn:uuid:1234</dc:identifier>
    <dc:language>en</dc:language>
    <dc:publisher>Old House</dc:publisher>
  </metadata>
  <manifest>
    <item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine><itemref idref="c"/></spine>
</package>"#;

const EPUB3: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="id">urn:uuid:1234</dc:identifier>
    <dc:title id="t1">Wrong Title</dc:title>
    <meta refines="#t1" property="title-type">main</meta>
    <dc:title id="t2">Wrong Subtitle</dc:title>
    <meta refines="#t2" property="title-type">subtitle</meta>
    <dc:creator id="a1">Wrong Author</dc:creator>
    <meta refines="#a1" property="role" scheme="marc:relators">aut</meta>
    <meta refines="#a1" property="file-as">Author, Wrong</meta>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
  </metadata>
  <manifest>
    <item id="cov" href="images/cover.jpg" media-type="image/jpeg" properties="cover-image"/>
    <item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine><itemref idref="c"/></spine>
</package>"##;

#[test]
fn epub2_without_cover_gets_tags_added_cover_and_untouched_members() {
    let dir = TempDir::new().unwrap();
    let extra = vec![("OEBPS/extra.css", b"body{}".to_vec())];
    let src = build(
        dir.path(),
        "a.epub",
        &Book {
            opf: EPUB2,
            members: extra,
        },
    );
    let dst = dir.path().join("out.epub");
    let png = image(3, image::ImageFormat::Png);
    let outcome = tagwrite::write(&src, &dst, &tags(), Some(&png)).unwrap();
    assert_eq!(
        outcome.cover,
        CoverAction::Added("OEBPS/book-manager-cover.png".into())
    );
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.title, "The Real Title");
    assert_eq!(meta.authors, ["Jane Example", "John Second"]);
    assert_eq!(meta.translators, ["Tina Translator"]);
    assert!(meta.isbns.contains(&"9780306406157".to_owned()));
    assert_eq!(meta.publisher.as_deref(), Some("Real House"));
    assert_eq!(meta.published_date.as_deref(), Some("2001"));
    assert!(meta.cover.is_some());
    assert_eq!(member(&dst, "OEBPS/book-manager-cover.png"), png);
    assert_eq!(member(&dst, "OEBPS/extra.css"), b"body{}");
    assert_eq!(
        member(&dst, "OEBPS/chapter.xhtml"),
        member(&src, "OEBPS/chapter.xhtml")
    );
    let opf = String::from_utf8(member(&dst, "OEBPS/content.opf")).unwrap();
    assert!(!opf.contains("wrong title") && !opf.contains("Wrong Author"));
    assert!(!opf.contains("Old House"));
    assert!(opf.contains("urn:uuid:1234"));
    assert!(opf.contains(r#"<meta name="cover" content="book-manager-cover"/>"#));
    assert!(
        !opf.contains("properties=\"cover-image\""),
        "EPUB2 must not use EPUB3 properties"
    );
    assert!(names(&dst).iter().any(|n| n == "mimetype"));
    assert_eq!(names(&dst)[0], "mimetype");
}

#[test]
fn epub3_refinements_are_dropped_and_declared_cover_is_replaced_in_place() {
    let dir = TempDir::new().unwrap();
    let old = image(9, image::ImageFormat::Jpeg);
    let book = Book {
        opf: EPUB3,
        members: vec![("OEBPS/images/cover.jpg", old)],
    };
    let src = build(dir.path(), "b.epub", &book);
    let dst = dir.path().join("out.epub");
    let webp = image(5, image::ImageFormat::WebP);
    let outcome = tagwrite::write(&src, &dst, &tags(), Some(&webp)).unwrap();
    assert_eq!(
        outcome.cover,
        CoverAction::Replaced("OEBPS/images/cover.jpg".into())
    );
    let opf = String::from_utf8(member(&dst, "OEBPS/content.opf")).unwrap();
    assert!(
        !opf.contains("refines"),
        "refinements of removed elements must go"
    );
    assert!(
        !opf.contains("file-as"),
        "refinements of removed creators must go"
    );
    assert!(opf.contains(r#"media-type="image/webp""#));
    assert!(
        opf.contains("dcterms:modified"),
        "unrelated metadata survives"
    );
    assert_eq!(member(&dst, "OEBPS/images/cover.jpg"), webp);
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.title, "The Real Title");
    assert_eq!(meta.authors.len(), 2);
}

#[test]
fn prefixed_opf_namespace_and_xml_special_characters_round_trip() {
    let dir = TempDir::new().unwrap();
    let opf = r#"<?xml version="1.0"?>
<opf:package xmlns:opf="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0" unique-identifier="i">
<opf:metadata>
<dc:title>x</dc:title><dc:creator>y</dc:creator><dc:identifier id="i">z</dc:identifier><dc:language>en</dc:language>
</opf:metadata>
<opf:manifest><opf:item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/></opf:manifest>
<opf:spine><opf:itemref idref="c"/></opf:spine>
</opf:package>"#;
    let src = build(
        dir.path(),
        "c.epub",
        &Book {
            opf,
            members: vec![],
        },
    );
    let dst = dir.path().join("out.epub");
    let mut t = tags();
    t.title = r#"Tom & Jerry's <"Big"> Adventure: Ünïcödé 日本語"#.into();
    t.authors = vec!["Ægir O'Brien".into()];
    let png = image(1, image::ImageFormat::Png);
    tagwrite::write(&src, &dst, &t, Some(&png)).unwrap();
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.title, t.title);
    assert_eq!(meta.authors, t.authors);
    assert!(meta.cover.is_some());
}

#[test]
fn unprefixed_package_without_dc_declaration_in_scope_still_works() {
    let dir = TempDir::new().unwrap();
    let opf = r#"<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="i">
<metadata><dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">x</dc:title><dc:creator xmlns:dc="http://purl.org/dc/elements/1.1/">y</dc:creator><dc:identifier xmlns:dc="http://purl.org/dc/elements/1.1/" id="i">z</dc:identifier><dc:language xmlns:dc="http://purl.org/dc/elements/1.1/">en</dc:language></metadata>
<manifest><item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest>
<spine><itemref idref="c"/></spine></package>"#;
    let src = build(
        dir.path(),
        "d.epub",
        &Book {
            opf,
            members: vec![],
        },
    );
    let dst = dir.path().join("out.epub");
    tagwrite::write(&src, &dst, &tags(), None).unwrap();
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.title, "The Real Title");
    assert_eq!(meta.authors.len(), 2);
}

#[test]
fn rewriting_reaches_a_fixed_point() {
    let dir = TempDir::new().unwrap();
    let src = build(
        dir.path(),
        "e.epub",
        &Book {
            opf: EPUB2,
            members: vec![],
        },
    );
    let png = image(2, image::ImageFormat::Png);
    let mut previous = src;
    let mut opfs = Vec::new();
    for round in 0..3 {
        let next = dir.path().join(format!("round{round}.epub"));
        let outcome = tagwrite::write(&previous, &next, &tags(), Some(&png)).unwrap();
        if round > 0 {
            assert_eq!(
                outcome.cover,
                CoverAction::Replaced("OEBPS/book-manager-cover.png".into())
            );
        }
        opfs.push(member(&next, "OEBPS/content.opf"));
        previous = next;
    }
    assert_eq!(
        opfs[1], opfs[2],
        "a second rewrite must not change the package"
    );
    let text = String::from_utf8(opfs[2].clone()).unwrap();
    assert!(
        !text.contains(
            "
    
"
        ),
        "no blank-line accumulation"
    );
}

#[test]
fn invalid_requests_fail_without_creating_output_or_touching_source() {
    let dir = TempDir::new().unwrap();
    let src = build(
        dir.path(),
        "f.epub",
        &Book {
            opf: EPUB2,
            members: vec![],
        },
    );
    let before = std::fs::read(&src).unwrap();
    let dst = dir.path().join("out.epub");
    let png = image(4, image::ImageFormat::Png);
    let mut cases: Vec<(&str, Tags, Option<Vec<u8>>)> = Vec::new();
    let mut t = tags();
    t.isbn13 = Some("9780306406158".into());
    cases.push(("bad isbn", t, None));
    let mut t = tags();
    t.title = "  ".into();
    cases.push(("blank title", t, None));
    let mut t = tags();
    t.authors.clear();
    cases.push(("no authors", t, None));
    let mut t = tags();
    t.title = "bad\u{0}control".into();
    cases.push(("control char", t, None));
    cases.push(("garbage cover", tags(), Some(b"not an image".to_vec())));
    cases.push(("gif cover", tags(), Some(b"GIF89a       ;".to_vec())));
    cases.push((
        "tiny cover",
        tags(),
        Some(image(1, image::ImageFormat::Png)[..20].to_vec()),
    ));
    for (label, t, cover) in cases {
        assert!(
            tagwrite::write(&src, &dst, &t, cover.as_deref()).is_err(),
            "{label} should fail"
        );
        assert!(!dst.exists(), "{label} left output behind");
    }
    assert!(tagwrite::write(&src, &src, &tags(), Some(&png)).is_err());
    std::fs::write(&dst, b"existing").unwrap();
    assert!(tagwrite::write(&src, &dst, &tags(), None).is_err());
    assert_eq!(std::fs::read(&dst).unwrap(), b"existing");
    assert_eq!(std::fs::read(&src).unwrap(), before);
}

#[test]
fn ambiguous_cover_declarations_are_refused() {
    let dir = TempDir::new().unwrap();
    let opf = EPUB3.replace(
        r#"<item id="c" href="chapter.xhtml""#,
        r#"<item id="cov2" href="images/other.jpg" media-type="image/jpeg" properties="cover-image"/><item id="c" href="chapter.xhtml""#,
    );
    let book = Book {
        opf: &opf,
        members: vec![
            ("OEBPS/images/cover.jpg", image(9, image::ImageFormat::Jpeg)),
            ("OEBPS/images/other.jpg", image(8, image::ImageFormat::Jpeg)),
        ],
    };
    let src = build(dir.path(), "g.epub", &book);
    let dst = dir.path().join("out.epub");
    let png = image(4, image::ImageFormat::Png);
    assert!(tagwrite::write(&src, &dst, &tags(), Some(&png)).is_err());
    assert!(!dst.exists());
}

#[test]
fn real_gutenberg_epub_round_trips_when_present() {
    let src = Path::new("test-data/alice/gutenberg-11-original.epub");
    if !src.exists() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let dst = dir.path().join("alice.epub");
    let png = image(7, image::ImageFormat::Png);
    let t = Tags {
        title: "Alice's Adventures in Wonderland".into(),
        authors: vec!["Lewis Carroll".into()],
        language: Some("en".into()),
        ..Tags::default()
    };
    tagwrite::write(src, &dst, &t, Some(&png)).unwrap();
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.title, t.title);
    assert!(meta.cover.is_some());
}

#[test]
fn creators_with_several_marc_roles_are_still_authors_and_are_replaced() {
    let dir = TempDir::new().unwrap();
    let opf = EPUB3.replace(
        r##"<meta refines="#a1" property="role" scheme="marc:relators">aut</meta>"##,
        r##"<meta refines="#a1" property="role" scheme="marc:relators">ann</meta>
    <meta refines="#a1" property="role" scheme="marc:relators">aut</meta>
    <meta refines="#a1" property="role" scheme="marc:relators">dto</meta>"##,
    );
    let book = Book {
        opf: &opf,
        members: vec![("OEBPS/images/cover.jpg", image(9, image::ImageFormat::Jpeg))],
    };
    let src = build(dir.path(), "roles.epub", &book);
    let (before, _) = epub::read(&src).unwrap();
    assert_eq!(before.authors, ["Wrong Author"], "ann+aut+dto is an author");
    let dst = dir.path().join("out.epub");
    tagwrite::write(&src, &dst, &tags(), None).unwrap();
    let text = String::from_utf8(member(&dst, "OEBPS/content.opf")).unwrap();
    assert!(!text.contains("Wrong Author") && !text.contains("refines=\"#a1\""));
    assert_eq!(epub::read(&dst).unwrap().0.authors.len(), 2);
}

const LEGACY: &str = r#"<?xml version="1.0"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">
 <metadata>
  <dc-metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <dc:title>ALICE'S ADVENTURES</dc:title>
   <dc:creator>CARROLL, LEWIS</dc:creator>
   <dc:language xsi:type="dcterms:RFC3066">en-GB</dc:language>
   <dc:identifier id="bookid">urn:uuid:1</dc:identifier>
  </dc-metadata>
 </metadata>
 <manifest><item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest>
 <spine><itemref idref="c"/></spine>
</package>"#;

#[test]
fn legacy_dc_metadata_wrapper_is_read_and_rewritten_in_place() {
    let dir = TempDir::new().unwrap();
    let src = build(
        dir.path(),
        "legacy.epub",
        &Book {
            opf: LEGACY,
            members: vec![],
        },
    );
    let (before, _) = epub::read(&src).unwrap();
    assert_eq!(before.title, "ALICE'S ADVENTURES");
    assert_eq!(before.authors, ["CARROLL, LEWIS"]);
    let dst = dir.path().join("out.epub");
    let png = image(6, image::ImageFormat::Png);
    tagwrite::write(&src, &dst, &tags(), Some(&png)).unwrap();
    let (after, _) = epub::read(&dst).unwrap();
    assert_eq!(after.title, "The Real Title");
    assert_eq!(after.authors, ["Jane Example", "John Second"]);
    assert!(after.cover.is_some());
    let opf = String::from_utf8(member(&dst, "OEBPS/content.opf")).unwrap();
    assert!(!opf.contains("CARROLL"), "old author removed");
    assert_eq!(opf.matches("<dc:title").count(), 1);
}

#[test]
fn missing_mimetype_member_is_tolerated_but_a_wrong_one_is_not() {
    let dir = TempDir::new().unwrap();
    let make = |name: &str, mimetype: Option<&[u8]>| {
        let path = dir.path().join(name);
        let mut zip = ZipWriter::new(File::create(&path).unwrap());
        let opts = SimpleFileOptions::default();
        if let Some(value) = mimetype {
            zip.start_file("mimetype", opts).unwrap();
            zip.write_all(value).unwrap();
        }
        zip.start_file("META-INF/container.xml", opts).unwrap();
        zip.write_all(br#"<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#).unwrap();
        zip.start_file("content.opf", opts).unwrap();
        zip.write_all(EPUB2.as_bytes()).unwrap();
        zip.start_file("chapter.xhtml", opts).unwrap();
        zip.write_all(
            format!(
                r#"<html xmlns="http://www.w3.org/1999/xhtml"><body><p>{}</p></body></html>"#,
                support::ENGLISH
            )
            .as_bytes(),
        )
        .unwrap();
        zip.finish().unwrap();
        path
    };
    assert!(epub::read(&make("none.epub", None)).is_ok());
    assert!(epub::read(&make("newline.epub", Some(b"application/epub+zip\n"))).is_ok());
    assert!(epub::read(&make("wrong.epub", Some(b"application/zip"))).is_err());
}

#[test]
fn a_verified_language_correction_replaces_the_wrong_declaration_only_when_asked() {
    let dir = TempDir::new().unwrap();
    let src = build(
        dir.path(),
        "lang.epub",
        &Book {
            opf: EPUB2,
            members: vec![],
        },
    );
    let mut t = tags();
    t.language = Some("de".into());
    // Without the flag an existing declaration is kept.
    let kept = dir.path().join("kept.epub");
    tagwrite::write(&src, &kept, &t, None).unwrap();
    assert_eq!(epub::read(&kept).unwrap().0.declared_languages, ["en"]);
    // With it, the wrong declaration is replaced and the change is verified.
    t.replace_language = true;
    let fixed = dir.path().join("fixed.epub");
    let outcome = tagwrite::write(&src, &fixed, &t, None).unwrap();
    assert_eq!(epub::read(&fixed).unwrap().0.declared_languages, ["de"]);
    assert!(
        outcome
            .changes
            .iter()
            .any(|c| c.contains("language corrected"))
    );
}

#[test]
fn series_is_written_as_calibre_and_epub3_collection_metadata_and_round_trips() {
    let dir = TempDir::new().unwrap();
    let src = build(
        dir.path(),
        "s3.epub",
        &Book {
            opf: EPUB3,
            members: vec![("OEBPS/images/cover.jpg", image(9, image::ImageFormat::Jpeg))],
        },
    );
    let dst = dir.path().join("out.epub");
    let mut t = tags();
    t.series = Some(("The Hunger Games".into(), Some("2".into())));
    tagwrite::write(&src, &dst, &t, None).unwrap();
    let opf = String::from_utf8(member(&dst, "OEBPS/content.opf")).unwrap();
    assert!(opf.contains(r#"name="calibre:series" content="The Hunger Games""#));
    assert!(opf.contains(r#"name="calibre:series_index" content="2""#));
    assert!(opf.contains("belongs-to-collection") && opf.contains("group-position"));
    let (meta, _) = epub::read(&dst).unwrap();
    assert_eq!(meta.series.as_deref(), Some("The Hunger Games"));
    assert_eq!(meta.series_index.as_deref(), Some("2"));
    // A second pass leaves existing series metadata alone (no duplicates).
    let again = dir.path().join("again.epub");
    tagwrite::write(&dst, &again, &t, None).unwrap();
    let opf = String::from_utf8(member(&again, "OEBPS/content.opf")).unwrap();
    assert_eq!(opf.matches("calibre:series\"").count(), 1);
}
