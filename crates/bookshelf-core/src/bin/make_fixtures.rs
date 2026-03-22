/// One-off binary to generate committed test fixture EPUBs.
/// Run: cargo run --bin make_fixtures
/// Output: tests/fixtures/the_hobbit.epub, tests/fixtures/the_hobbit_alt.epub
use bookshelf_core::epub::xml_escape;
use std::io::Write as _;
use std::path::Path;

#[allow(clippy::too_many_arguments)]
fn make_epub(
    title: &str,
    authors: &[&str],
    isbn: Option<&str>,
    series_name: Option<&str>,
    series_position: Option<&str>,
    publisher: Option<&str>,
    publish_date: Option<&str>,
    language: Option<&str>,
    description: Option<&str>,
    cover_item_id: Option<&str>,
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

    opf.push_str(&format!("    <dc:title>{}</dc:title>\n", xml_escape(title)));
    for author in authors {
        opf.push_str(&format!("    <dc:creator>{}</dc:creator>\n", xml_escape(author)));
    }
    if let Some(v) = isbn {
        opf.push_str(&format!(
            "    <dc:identifier opf:scheme=\"ISBN\">{v}</dc:identifier>\n"
        ));
    }
    if let Some(v) = publisher {
        opf.push_str(&format!("    <dc:publisher>{}</dc:publisher>\n", xml_escape(v)));
    }
    if let Some(v) = publish_date {
        opf.push_str(&format!("    <dc:date>{}</dc:date>\n", xml_escape(v)));
    }
    if let Some(v) = language {
        opf.push_str(&format!("    <dc:language>{v}</dc:language>\n"));
    }
    if let Some(v) = description {
        opf.push_str(&format!("    <dc:description>{}</dc:description>\n", xml_escape(v)));
    }
    if let Some(sname) = series_name {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series\" content=\"{}\"/>\n",
            xml_escape(sname)
        ));
    }
    if let Some(spos) = series_position {
        opf.push_str(&format!(
            "    <meta name=\"calibre:series_index\" content=\"{spos}\"/>\n"
        ));
    }
    if let Some(cover_id) = cover_item_id {
        opf.push_str(&format!(
            "    <meta name=\"cover\" content=\"{cover_id}\"/>\n"
        ));
    }

    opf.push_str("  </metadata>\n  <manifest>\n");

    if let Some(cover_id) = cover_item_id {
        opf.push_str(&format!(
            "    <item id=\"{cover_id}\" href=\"cover.jpg\" media-type=\"image/jpeg\"/>\n"
        ));
    }

    opf.push_str("  </manifest>\n  <spine/>\n</package>\n");

    zip.start_file("content.opf", opts).unwrap();
    zip.write_all(opf.as_bytes()).unwrap();
    zip.finish().unwrap();
}

fn main() {
    let fixtures_dir = Path::new("tests/fixtures");
    std::fs::create_dir_all(fixtures_dir).unwrap();

    make_epub(
        "The Hobbit",
        &["J.R.R. Tolkien"],
        Some("9780261102217"),
        Some("The Lord of the Rings"),
        Some("0"),
        Some("George Allen & Unwin"),
        Some("1937"),
        Some("en"),
        Some("A young hobbit is swept into an epic quest."),
        Some("cover-img"),
        &fixtures_dir.join("the_hobbit.epub"),
    );

    make_epub(
        "The Hobbit, or There and Back Again",
        &["J.R.R. Tolkien"],
        None,
        Some("The Lord of the Rings"),
        Some("0"),
        None,
        None,
        Some("en"),
        None,
        None,
        &fixtures_dir.join("the_hobbit_alt.epub"),
    );

    println!("Fixtures written to {}", fixtures_dir.display());
}
