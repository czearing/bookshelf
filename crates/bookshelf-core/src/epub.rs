use anyhow::{anyhow, Context};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::io::{BufReader, Read};
use std::path::Path;
use zip::ZipArchive;

/// Metadata extracted from a single EPUB file's OPF document.
#[derive(Debug, Clone, Default)]
pub struct EpubMeta {
    pub title: Option<String>,
    /// Comma-separated author names in document order.
    pub authors: Option<String>,
    pub isbn: Option<String>,
    pub series_name: Option<String>,
    pub series_position: Option<String>,
    pub publisher: Option<String>,
    pub publish_date: Option<String>,
    pub language: Option<String>,
    pub description: Option<String>,
    /// Relative href to the cover image as written in the OPF manifest.
    pub cover_image_path: Option<String>,
    /// Absolute path to the source `.epub` file.
    pub source_path: String,
}

/// Parse an EPUB file and return its OPF metadata.
///
/// Returns `Err` if the file is not a valid ZIP archive or if
/// `META-INF/container.xml` is absent. Individual missing metadata
/// fields are represented as `None` (AC-19).
pub fn parse_epub(path: &Path) -> anyhow::Result<EpubMeta> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    let mut archive = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("not a valid ZIP archive: {}", path.display()))?;

    // Step 1: read META-INF/container.xml to find the OPF path.
    let opf_path = {
        let mut container = archive
            .by_name("META-INF/container.xml")
            .map_err(|_| anyhow!("META-INF/container.xml missing in {}", path.display()))?;
        let mut buf = String::new();
        container.read_to_string(&mut buf)?;
        extract_opf_path(&buf)
            .ok_or_else(|| anyhow!("no rootfile element in META-INF/container.xml"))?
    };

    // Step 2: read the OPF file.
    let opf_content = {
        let mut opf_file = archive
            .by_name(&opf_path)
            .map_err(|_| anyhow!("OPF file '{}' missing in {}", opf_path, path.display()))?;
        let mut buf = String::new();
        opf_file.read_to_string(&mut buf)?;
        buf
    };

    // Step 3: parse OPF XML.
    let mut meta = parse_opf(&opf_content)?;
    meta.source_path = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string();
    Ok(meta)
}

/// Extract the OPF file path from `META-INF/container.xml` content.
fn extract_opf_path(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Empty(ref e)) | Ok(Event::Start(ref e)) => {
                let name_bytes = e.name().into_inner().to_vec();
                let local = local_name(&name_bytes);
                if local == b"rootfile" {
                    if let Some(attr) = find_attr(e, b"full-path") {
                        return Some(attr);
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    None
}

/// Parse the OPF XML and extract all metadata fields.
fn parse_opf(xml: &str) -> anyhow::Result<EpubMeta> {
    let mut meta = EpubMeta::default();
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    // State machine
    let mut buf = Vec::new();
    let mut current_element: Option<Vec<u8>> = None;
    // Track whether current dc:identifier is an ISBN identifier
    let mut current_id_is_isbn = false;
    // OPF 2 cover: <meta name="cover" content="{item_id}">
    let mut cover_item_id: Option<String> = None;
    // OPF 3 series state
    let mut in_belongs_to_collection = false;
    let mut in_group_position = false;
    // Track authors collected
    let mut authors: Vec<String> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let local = local_name(e.name().as_ref()).to_vec();
                match local.as_slice() {
                    b"dc:title"
                    | b"dc:creator"
                    | b"dc:publisher"
                    | b"dc:date"
                    | b"dc:language"
                    | b"dc:description" => {
                        current_element = Some(local);
                    }
                    b"dc:identifier" => {
                        current_element = Some(local);
                        // Check for opf:scheme or scheme attribute
                        current_id_is_isbn = find_attr(e, b"opf:scheme")
                            .or_else(|| find_attr(e, b"scheme"))
                            .map(|s| s.eq_ignore_ascii_case("ISBN"))
                            .unwrap_or(false);
                    }
                    b"meta" => {
                        // Handle OPF 2 <meta name="..." content="...">
                        let name_attr = find_attr(e, b"name");
                        let content_attr = find_attr(e, b"content");
                        let prop_attr = find_attr(e, b"property");

                        if let (Some(name), Some(content)) = (name_attr.as_deref(), content_attr.as_deref()) {
                            match name {
                                "calibre:series" => {
                                    meta.series_name = Some(content.to_string());
                                }
                                "calibre:series_index" => {
                                    meta.series_position = Some(content.to_string());
                                }
                                "cover" => {
                                    cover_item_id = Some(content.to_string());
                                }
                                _ => {}
                            }
                        }

                        // Handle OPF 3 <meta property="...">
                        if let Some(prop) = prop_attr.as_deref() {
                            match prop {
                                "belongs-to-collection" => {
                                    in_belongs_to_collection = true;
                                    current_element = Some(b"__belongs_to_collection".to_vec());
                                }
                                "group-position" => {
                                    in_group_position = true;
                                    current_element = Some(b"__group_position".to_vec());
                                }
                                _ => {}
                            }
                        }
                    }
                    b"item" => {
                        // OPF manifest item — check for cover image
                        let props = find_attr(e, b"properties").unwrap_or_default();
                        let href = find_attr(e, b"href");
                        let id_attr = find_attr(e, b"id");

                        if props.split_whitespace().any(|p| p == "cover-image") {
                            if let Some(h) = href.as_deref() {
                                meta.cover_image_path = Some(h.to_string());
                            }
                        } else if let (Some(item_id), Some(cover_id)) =
                            (id_attr.as_deref(), cover_item_id.as_deref())
                        {
                            if item_id == cover_id {
                                if let Some(h) = href.as_deref() {
                                    meta.cover_image_path = Some(h.to_string());
                                }
                            }
                        }
                        current_element = None;
                    }
                    _ => {
                        current_element = None;
                    }
                }
            }
            Ok(Event::Empty(ref e)) => {
                let local = local_name(e.name().as_ref()).to_vec();
                if local.as_slice() == b"meta" {
                    let name_attr = find_attr(e, b"name");
                    let content_attr = find_attr(e, b"content");

                    if let (Some(name), Some(content)) = (name_attr.as_deref(), content_attr.as_deref()) {
                        match name {
                            "calibre:series" => {
                                meta.series_name = Some(content.to_string());
                            }
                            "calibre:series_index" => {
                                meta.series_position = Some(content.to_string());
                            }
                            "cover" => {
                                cover_item_id = Some(content.to_string());
                            }
                            _ => {}
                        }
                    }
                } else if local.as_slice() == b"item" {
                    let props = find_attr(e, b"properties").unwrap_or_default();
                    let href = find_attr(e, b"href");
                    let id_attr = find_attr(e, b"id");

                    if props.split_whitespace().any(|p| p == "cover-image") {
                        if let Some(h) = href.as_deref() {
                            meta.cover_image_path = Some(h.to_string());
                        }
                    } else if let (Some(item_id), Some(cover_id)) =
                        (id_attr.as_deref(), cover_item_id.as_deref())
                    {
                        if item_id == cover_id {
                            if let Some(h) = href.as_deref() {
                                meta.cover_image_path = Some(h.to_string());
                            }
                        }
                    }
                }
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().trim().to_string();
                if text.is_empty() {
                    continue;
                }
                match current_element.as_deref() {
                    Some(b"dc:title") => {
                        meta.title = Some(text);
                    }
                    Some(b"dc:creator") => {
                        authors.push(text);
                    }
                    Some(b"dc:publisher") => {
                        meta.publisher = Some(text);
                    }
                    Some(b"dc:date") => {
                        meta.publish_date = Some(text);
                    }
                    Some(b"dc:language") => {
                        meta.language = Some(text);
                    }
                    Some(b"dc:description") => {
                        meta.description = Some(text);
                    }
                    Some(b"dc:identifier") => {
                        if (current_id_is_isbn || looks_like_isbn(&text))
                            && meta.isbn.is_none()
                        {
                            meta.isbn = Some(text);
                        }
                    }
                    Some(b"__belongs_to_collection") => {
                        meta.series_name = Some(text);
                        in_belongs_to_collection = false;
                    }
                    Some(b"__group_position") => {
                        meta.series_position = Some(text);
                        in_group_position = false;
                    }
                    _ => {}
                }
                current_element = None;
            }
            Ok(Event::End(_)) => {
                // Reset flags on any end element if they somehow linger
                if in_belongs_to_collection || in_group_position {
                    in_belongs_to_collection = false;
                    in_group_position = false;
                }
                current_element = None;
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow!("XML parse error: {e}")),
            _ => {}
        }
        buf.clear();
    }

    if !authors.is_empty() {
        meta.authors = Some(authors.join(", "));
    }

    Ok(meta)
}

/// Returns the local name (after colon) from a qualified XML name byte slice.
/// E.g. b"dc:title" -> b"dc:title" (returned as-is, caller matches on full prefixed name).
/// For unprefixed names, returns the name itself.
fn local_name(name: &[u8]) -> &[u8] {
    // We keep the full prefixed name so match arms can use b"dc:title" etc.
    name
}

/// Find an attribute value by name (case-sensitive byte comparison).
fn find_attr(e: &quick_xml::events::BytesStart<'_>, name: &[u8]) -> Option<String> {
    for attr in e.attributes().flatten() {
        if attr.key.as_ref() == name {
            return String::from_utf8(attr.value.to_vec()).ok();
        }
    }
    None
}

/// Escape XML special characters in text content.
/// Exported for use in integration test fixture builders.
pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Heuristic: does a string look like an ISBN (10 or 13 digits, possibly hyphenated)?
fn looks_like_isbn(s: &str) -> bool {
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.len() == 10 || digits.len() == 13
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Build a minimal valid EPUB ZIP in memory and write to a temp file.
    pub fn make_test_epub(
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
    ) -> NamedTempFile {
        let tmp = NamedTempFile::with_suffix(".epub").unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        // mimetype (must be first, uncompressed)
        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();

        // META-INF/container.xml
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

        // Build OPF content
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

        if let Some(isbn_val) = isbn {
            opf.push_str(&format!(
                "    <dc:identifier opf:scheme=\"ISBN\">{isbn_val}</dc:identifier>\n"
            ));
        }

        if let Some(pub_val) = publisher {
            opf.push_str(&format!("    <dc:publisher>{}</dc:publisher>\n", xml_escape(pub_val)));
        }

        if let Some(date_val) = publish_date {
            opf.push_str(&format!("    <dc:date>{date_val}</dc:date>\n"));
        }

        if let Some(lang_val) = language {
            opf.push_str(&format!("    <dc:language>{lang_val}</dc:language>\n"));
        }

        if let Some(desc_val) = description {
            opf.push_str(&format!("    <dc:description>{desc_val}</dc:description>\n"));
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
        tmp
    }

    #[test]
    fn test_parse_epub_title_and_authors() {
        let tmp = make_test_epub(
            "The Hobbit",
            &["J.R.R. Tolkien"],
            Some("9780261102217"),
            Some("The Lord of the Rings"),
            Some("0"),
            Some("George Allen & Unwin"),
            Some("1937"),
            Some("en"),
            Some("A fantasy novel"),
            Some("cover-img"),
        );
        let meta = parse_epub(tmp.path()).unwrap();
        assert_eq!(meta.title.as_deref(), Some("The Hobbit"));
        assert_eq!(meta.authors.as_deref(), Some("J.R.R. Tolkien"));
        assert_eq!(meta.isbn.as_deref(), Some("9780261102217"));
        assert_eq!(meta.series_name.as_deref(), Some("The Lord of the Rings"));
        assert_eq!(meta.series_position.as_deref(), Some("0"));
        assert_eq!(meta.publisher.as_deref(), Some("George Allen & Unwin"));
        assert_eq!(meta.publish_date.as_deref(), Some("1937"));
        assert_eq!(meta.language.as_deref(), Some("en"));
        assert_eq!(meta.description.as_deref(), Some("A fantasy novel"));
        assert_eq!(meta.cover_image_path.as_deref(), Some("cover.jpg"));
    }

    #[test]
    fn test_parse_epub_multiple_authors() {
        let tmp = make_test_epub(
            "Good Omens",
            &["Terry Pratchett", "Neil Gaiman"],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let meta = parse_epub(tmp.path()).unwrap();
        assert_eq!(meta.authors.as_deref(), Some("Terry Pratchett, Neil Gaiman"));
    }

    #[test]
    fn test_parse_epub_missing_fields_are_none() {
        let tmp = make_test_epub("Untitled", &[], None, None, None, None, None, None, None, None);
        let meta = parse_epub(tmp.path()).unwrap();
        assert_eq!(meta.isbn, None);
        assert_eq!(meta.series_name, None);
        assert_eq!(meta.publisher, None);
        assert_eq!(meta.description, None);
    }

    #[test]
    fn test_parse_epub_invalid_zip_returns_err() {
        let tmp = NamedTempFile::with_suffix(".epub").unwrap();
        std::fs::write(tmp.path(), b"not a zip").unwrap();
        assert!(parse_epub(tmp.path()).is_err());
    }

    #[test]
    fn test_parse_epub_missing_container_xml_returns_err() {
        let tmp = NamedTempFile::with_suffix(".epub").unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("mimetype", opts).unwrap();
        zip.write_all(b"application/epub+zip").unwrap();
        zip.finish().unwrap();
        assert!(parse_epub(tmp.path()).is_err());
    }

    #[test]
    fn test_looks_like_isbn() {
        assert!(looks_like_isbn("9780261102217"));
        assert!(looks_like_isbn("978-0-261-10221-7"));
        assert!(looks_like_isbn("0261102214"));
        assert!(!looks_like_isbn("123"));
        assert!(!looks_like_isbn("not-an-isbn"));
    }
}
