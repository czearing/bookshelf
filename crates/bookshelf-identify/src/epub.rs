use anyhow::{Context, Result, bail, ensure};
use roxmltree::Document;
use serde::Serialize;
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    time::SystemTime,
};
use zip::ZipArchive;

const DC: &str = "http://purl.org/dc/elements/1.1/";
const OPF: &str = "http://www.idpf.org/2007/opf";
const CONTAINER: &str = "urn:oasis:names:tc:opendocument:xmlns:container";
const MAX_XML: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Metadata {
    /// The title with library decoration ("(Series, #N)", ": A Novel") removed.
    pub title: String,
    /// The title exactly as the package declares it.
    pub declared_title: String,
    pub series: Option<String>,
    pub series_index: Option<String>,
    pub authors: Vec<String>,
    pub isbns: Vec<String>,
    pub language: Option<String>,
    pub declared_languages: Vec<String>,
    pub alternate_titles: Vec<String>,
    pub publisher: Option<String>,
    pub published_date: Option<String>,
    pub translators: Vec<String>,
    /// Structural signs that the file bundles several works (empty for a single work).
    pub collection_signals: Vec<String>,
    /// Words of reading text in the whole book (a length estimate, not a page count).
    pub words: u64,
    /// Whether the package declares a readable cover (kept when results are stored).
    pub has_cover: bool,
    pub content: crate::text::ContentEvidence,
    pub warnings: Vec<String>,
    #[serde(skip)]
    pub cover: Option<crate::cover::Signature>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    pub len: u64,
    pub modified: SystemTime,
}

pub fn fingerprint(path: &Path) -> Result<Fingerprint> {
    let info = fs::symlink_metadata(path).context("Cannot inspect source file")?;
    ensure!(info.file_type().is_file(), "Source is not a regular file");
    Ok(Fingerprint {
        len: info.len(),
        modified: info.modified().context("Cannot read modification time")?,
    })
}

pub fn read(path: &Path) -> Result<(Metadata, Fingerprint)> {
    let before = fingerprint(path)?;
    let file = File::open(path).context("Cannot open EPUB")?;
    let mut zip = ZipArchive::new(file).context("Invalid EPUB ZIP archive")?;
    check_mimetype(&mut zip)?;
    let opf_path = package_path(&mut zip)?;
    let opf_path = opf_path.as_str();
    let xml = decode_xml(read_member(&mut zip, opf_path, MAX_XML)?)?;
    let doc = Document::parse(&xml).context("Invalid EPUB package XML")?;
    ensure!(
        doc.root_element().has_tag_name((OPF, "package")),
        "Invalid EPUB package root"
    );
    let metadata = doc
        .root_element()
        .children()
        .find(|node| node.has_tag_name((OPF, "metadata")))
        .context("Missing EPUB metadata")?;
    let text = |node: roxmltree::Node<'_, '_>| {
        node.descendants()
            .filter(|n| n.is_text())
            .filter_map(|n| n.text())
            .collect::<String>()
            .trim()
            .to_owned()
    };
    let refinement = |node: roxmltree::Node<'_, '_>, property: &str| {
        let target = format!("#{}", node.attribute("id")?);
        members(metadata).into_iter().find_map(|meta| {
            (meta.has_tag_name((OPF, "meta"))
                && meta.attribute("property") == Some(property)
                && meta.attribute("refines") == Some(target.as_str()))
            .then(|| text(meta))
        })
    };
    let mut result = Metadata::default();
    let mut main_title_found = false;
    let mut meta_series: Option<String> = None;
    let mut meta_index: Option<String> = None;
    for node in members(metadata) {
        if node.tag_name().name() == "meta" {
            let content = node.attribute("content").map(|c| c.trim().to_owned());
            match node.attribute("name") {
                Some("calibre:series") => meta_series = content.filter(|c| !c.is_empty()),
                Some("calibre:series_index") => meta_index = content.filter(|c| !c.is_empty()),
                _ => {}
            }
        }
        let value = text(node);
        if value.is_empty() {
            continue;
        }
        match (node.tag_name().namespace(), node.tag_name().name()) {
            (Some(DC), "title") => {
                let is_main = refinement(node, "title-type").as_deref() == Some("main");
                ensure!(
                    !is_main || !main_title_found,
                    "Multiple main titles require review"
                );
                if is_main || result.title.is_empty() {
                    result.title = value;
                } else {
                    result.alternate_titles.push(value);
                }
                main_title_found |= is_main;
            }
            (Some(DC), "creator") => {
                let roles = roles_of(node, metadata);
                if (roles.is_empty() || roles.iter().any(|role| role == "aut"))
                    && !result.authors.contains(&value)
                {
                    result.authors.push(value);
                } else if roles.iter().any(|role| role == "trl") {
                    result.translators.push(value);
                }
            }
            (Some(DC), "contributor") => {
                if roles_of(node, metadata).iter().any(|role| role == "trl") {
                    result.translators.push(value);
                }
            }
            (Some(DC), "identifier") => {
                if let Some(isbn) = normalize_isbn(&value)
                    && !result.isbns.contains(&isbn)
                {
                    result.isbns.push(isbn);
                }
            }
            (Some(DC), "language") => {
                if result.language.is_none() {
                    result.language = Some(value.to_lowercase());
                }
                result.declared_languages.push(value.to_lowercase());
            }
            (Some(DC), "publisher") if result.publisher.is_none() => result.publisher = Some(value),
            (Some(DC), "date") if result.published_date.is_none() => {
                result.published_date = Some(value)
            }
            _ => {}
        }
    }
    result.declared_title = result.title.clone();
    let parts = crate::text::split_title(&result.title);
    if !parts.title.is_empty() {
        result.title = parts.title;
        result.series = parts.series;
        result.series_index = parts.series_index;
    }
    if result.series.is_none() {
        result.series = meta_series;
        result.series_index = meta_index;
    }
    result.cover = match read_cover(&mut zip, &doc, opf_path) {
        Ok(cover) => cover,
        Err(error) => {
            result
                .warnings
                .push(format!("Cover unavailable: {error:#}"));
            None
        }
    };
    result.has_cover = result.cover.is_some();
    result.content = read_content(&mut zip, &doc, opf_path, &result.declared_languages)?;
    result.words = count_words(&mut zip, &doc, opf_path);
    let labels = toc_labels(&mut zip, &doc, opf_path);
    result.collection_signals = crate::text::collection_signals(&result.title, &labels);
    crate::text::corroborate(
        &mut result.content,
        &result.title,
        &result.authors,
        &result.translators,
    );
    ensure!(
        before == fingerprint(path)?,
        "File changed while reading metadata"
    );
    ensure!(
        !result.isbns.is_empty() || (!result.title.is_empty() && !result.authors.is_empty()),
        "Insufficient embedded metadata: need an ISBN or both title and author"
    );
    Ok((result, before))
}

/// Every MARC role declared for a contributor: the EPUB 2 `opf:role` attribute and
/// all EPUB 3 `role` refinements (a creator may carry several, e.g. ann, aut, dto).
/// The metadata entries, including those nested in the OPF 2.0 `dc-metadata` and
/// `x-metadata` wrapper elements that older files still use.
pub(crate) fn members<'a, 'i>(metadata: roxmltree::Node<'a, 'i>) -> Vec<roxmltree::Node<'a, 'i>> {
    let mut out = Vec::new();
    for child in metadata.children().filter(|n| n.is_element()) {
        if matches!(child.tag_name().name(), "dc-metadata" | "x-metadata") {
            out.extend(child.children().filter(|n| n.is_element()));
        } else {
            out.push(child);
        }
    }
    out
}

/// The element that directly holds the Dublin Core entries: the `dc-metadata`
/// wrapper when there is one, otherwise `metadata` itself.
pub(crate) fn dc_container<'a, 'i>(metadata: roxmltree::Node<'a, 'i>) -> roxmltree::Node<'a, 'i> {
    metadata
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "dc-metadata")
        .unwrap_or(metadata)
}

pub(crate) fn roles_of(
    node: roxmltree::Node<'_, '_>,
    metadata: roxmltree::Node<'_, '_>,
) -> Vec<String> {
    let mut roles: Vec<String> = node
        .attribute((OPF, "role"))
        .map(|role| vec![role.trim().to_owned()])
        .unwrap_or_default();
    if let Some(id) = node.attribute("id") {
        let target = format!("#{id}");
        roles.extend(
            members(metadata)
                .into_iter()
                .filter(|meta| {
                    meta.has_tag_name((OPF, "meta"))
                        && meta.attribute("property") == Some("role")
                        && meta.attribute("refines") == Some(target.as_str())
                })
                .map(|meta| meta.text().unwrap_or("").trim().to_owned()),
        );
    }
    roles
}

/// A `mimetype` member that is present must say EPUB. A missing one is tolerated:
/// many real files omit it, and `META-INF/container.xml` is still required.
pub(crate) fn check_mimetype(zip: &mut ZipArchive<File>) -> Result<()> {
    if zip.by_name("mimetype").is_err() {
        return Ok(());
    }
    let value = read_member(zip, "mimetype", 64)?;
    ensure!(
        value.trim_ascii() == b"application/epub+zip",
        "Not an EPUB: invalid mimetype"
    );
    Ok(())
}

pub(crate) fn package_path(zip: &mut ZipArchive<File>) -> Result<String> {
    let container = decode_xml(read_member(zip, "META-INF/container.xml", MAX_XML)?)?;
    let doc = Document::parse(&container).context("Invalid EPUB container XML")?;
    let rootfiles: Vec<_> = doc
        .descendants()
        .filter(|node| {
            node.has_tag_name((CONTAINER, "rootfile"))
                && node.attribute("media-type") == Some("application/oebps-package+xml")
        })
        .collect();
    ensure!(
        rootfiles.len() == 1,
        "EPUB must have exactly one package document; multiple renditions need review"
    );
    let opf_path = rootfiles[0]
        .attribute("full-path")
        .context("Missing EPUB package path")?;
    ensure!(
        !opf_path.starts_with('/')
            && !opf_path.contains('\\')
            && !opf_path.split('/').any(|p| p == ".." || p == "."),
        "Invalid EPUB package path"
    );
    Ok(opf_path.to_owned())
}

fn read_content(
    zip: &mut ZipArchive<File>,
    doc: &Document<'_>,
    opf_path: &str,
    languages: &[String],
) -> Result<crate::text::ContentEvidence> {
    use std::collections::{BTreeSet, HashMap};
    let root = doc.root_element();
    let manifest: HashMap<_, _> = root
        .children()
        .filter(|node| node.has_tag_name((OPF, "manifest")))
        .flat_map(|node| node.children())
        .filter(|node| node.has_tag_name((OPF, "item")))
        .filter_map(|node| node.attribute("id").map(|id| (id, node)))
        .collect();
    let spine: Vec<_> = root
        .children()
        .filter(|node| node.has_tag_name((OPF, "spine")))
        .flat_map(|node| node.children())
        .filter(|node| {
            node.has_tag_name((OPF, "itemref")) && node.attribute("linear") != Some("no")
        })
        .filter_map(|node| node.attribute("idref"))
        .collect();
    let mut selected: BTreeSet<usize> = (0..spine.len().min(8)).collect();
    if !spine.is_empty() {
        selected.insert(spine.len() / 2);
        selected.insert(spine.len() - 1);
    }
    let mut result = crate::text::ContentEvidence::default();
    let mut prose = Vec::new();
    for index in selected {
        let item = manifest
            .get(spine[index])
            .context("Spine points to a missing manifest item")?;
        if item.attribute("media-type") != Some("application/xhtml+xml") {
            continue;
        }
        if item
            .attribute("properties")
            .is_some_and(|value| value.split_whitespace().any(|p| p == "nav"))
        {
            continue;
        }
        let path = resolve_member(
            opf_path,
            item.attribute("href").context("Spine item has no href")?,
        )?;
        let xml = decode_xml(read_member(zip, &path, MAX_XML)?)?;
        let (evidence, text) = crate::text::read_document(&path, &xml)
            .with_context(|| format!("Cannot inspect EPUB text document {path}"))?;
        if !text.is_empty() {
            prose.push((path, text));
        }
        result.documents.push(evidence);
    }
    result.language = crate::language::assess(languages, &prose);
    // Passages from the later documents first: they are body text, not front matter.
    for document in result.documents.iter().rev() {
        for passage in &document.passages {
            if result.passages.len() < 3 && !result.passages.contains(passage) {
                result.passages.push(passage.clone());
            }
        }
    }
    Ok(result)
}

/// Labels of the table of contents, from an EPUB 3 navigation document and/or an
/// EPUB 2 NCX, in reading order. Bounded, and never fatal: a book whose contents
/// cannot be read simply yields no collection evidence.
pub fn toc_labels(zip: &mut ZipArchive<File>, doc: &Document<'_>, opf_path: &str) -> Vec<String> {
    const LIMIT: usize = 4000;
    // The EPUB 3 navigation document wins over the EPUB 2 NCX when both exist.
    let root = doc.root_element();
    let items: Vec<_> = root
        .children()
        .filter(|n| n.has_tag_name((OPF, "manifest")))
        .flat_map(|n| n.children())
        .filter(|n| n.has_tag_name((OPF, "item")))
        .collect();
    let mut labels = Vec::new();
    let mut ordered = items.clone();
    ordered.sort_by_key(|item| {
        !item
            .attribute("properties")
            .is_some_and(|p| p.split_whitespace().any(|p| p == "nav"))
    });
    for item in &ordered {
        let is_nav = item
            .attribute("properties")
            .is_some_and(|p| p.split_whitespace().any(|p| p == "nav"));
        let is_ncx = item.attribute("media-type") == Some("application/x-dtbncx+xml");
        if !(is_nav || is_ncx) {
            continue;
        }
        let Some(href) = item.attribute("href") else {
            continue;
        };
        let Ok(member) = resolve_member(opf_path, href) else {
            continue;
        };
        let Ok(bytes) = read_member(zip, &member, MAX_XML) else {
            continue;
        };
        let Ok(text) = decode_xml(bytes) else {
            continue;
        };
        let Ok(text) = crate::text::normalize_entities(&text) else {
            continue;
        };
        let Ok(toc) = Document::parse(&text) else {
            continue;
        };
        if is_ncx {
            for label in toc
                .descendants()
                .filter(|n| n.is_element() && n.tag_name().name() == "navLabel")
            {
                labels.push(label_text(label));
            }
        } else {
            let toc_nav = toc.descendants().find(|n| {
                n.is_element()
                    && n.tag_name().name() == "nav"
                    && n.attributes().any(|a| {
                        a.name() == "type" && a.value().split_whitespace().any(|v| v == "toc")
                    })
            });
            if let Some(nav) = toc_nav {
                for link in nav
                    .descendants()
                    .filter(|n| n.is_element() && matches!(n.tag_name().name(), "a" | "span"))
                {
                    labels.push(label_text(link));
                }
            }
        }
        // One table of contents is enough; reading both would count every entry twice.
        if !labels.is_empty() {
            break;
        }
    }
    labels.retain(|label| !label.is_empty());
    labels.truncate(LIMIT);
    labels
}

fn label_text(node: roxmltree::Node<'_, '_>) -> String {
    node.descendants()
        .filter(|n| n.is_text())
        .filter_map(|n| n.text())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Total words of reading text across the spine. Used only as a rough length
/// (words / ~275 is a page) to compare with a provider's page count. Bounded and
/// never fatal: an unreadable document simply contributes nothing.
pub fn count_words(zip: &mut ZipArchive<File>, doc: &Document<'_>, opf_path: &str) -> u64 {
    const MAX_DOCUMENTS: usize = 4000;
    const MAX_TOTAL_BYTES: u64 = 96 * 1024 * 1024;
    let root = doc.root_element();
    let manifest: std::collections::HashMap<_, _> = root
        .children()
        .filter(|n| n.has_tag_name((OPF, "manifest")))
        .flat_map(|n| n.children())
        .filter(|n| n.has_tag_name((OPF, "item")))
        .filter_map(|n| n.attribute("id").map(|id| (id, n)))
        .collect();
    let mut words = 0u64;
    let mut read = 0u64;
    let idrefs = root
        .children()
        .filter(|n| n.has_tag_name((OPF, "spine")))
        .flat_map(|n| n.children())
        .filter(|n| n.has_tag_name((OPF, "itemref")))
        .filter_map(|n| n.attribute("idref"))
        .take(MAX_DOCUMENTS);
    for idref in idrefs {
        let Some(item) = manifest.get(idref) else {
            continue;
        };
        if item.attribute("media-type") != Some("application/xhtml+xml") {
            continue;
        }
        let is_nav = item
            .attribute("properties")
            .is_some_and(|p| p.split_whitespace().any(|p| p == "nav"));
        let Some(href) = item.attribute("href") else {
            continue;
        };
        let (true, Ok(member)) = (!is_nav, resolve_member(opf_path, href)) else {
            continue;
        };
        let Ok(bytes) = read_member(zip, &member, 4 * 1024 * 1024) else {
            continue;
        };
        read += bytes.len() as u64;
        if read > MAX_TOTAL_BYTES {
            break;
        }
        let Ok(text) = decode_xml(bytes) else {
            continue;
        };
        let Ok(text) = crate::text::normalize_entities(&text) else {
            continue;
        };
        let Ok(page) = Document::parse_with_options(
            &text,
            roxmltree::ParsingOptions {
                allow_dtd: true,
                nodes_limit: 400_000,
            },
        ) else {
            continue;
        };
        for node in page.descendants().filter(|n| n.is_text()) {
            let hidden = node.ancestors().any(|a| {
                a.is_element() && matches!(a.tag_name().name(), "script" | "style" | "head" | "nav")
            });
            if !hidden {
                words += node.text().unwrap_or("").split_whitespace().count() as u64;
            }
        }
    }
    words
}

fn read_cover(
    zip: &mut ZipArchive<File>,
    doc: &Document<'_>,
    opf_path: &str,
) -> Result<Option<crate::cover::Signature>> {
    let root = doc.root_element();
    let manifest: Vec<_> = root
        .children()
        .filter(|n| n.has_tag_name((OPF, "manifest")))
        .flat_map(|n| n.children())
        .filter(|n| n.has_tag_name((OPF, "item")))
        .collect();
    let legacy_id = root
        .children()
        .filter(|n| n.has_tag_name((OPF, "metadata")))
        .flat_map(members)
        .find(|n| n.has_tag_name((OPF, "meta")) && n.attribute("name") == Some("cover"))
        .and_then(|n| n.attribute("content"));
    let mut covers = Vec::new();
    for item in &manifest {
        let declared = item
            .attribute("properties")
            .is_some_and(|p| p.split_whitespace().any(|p| p == "cover-image"))
            || legacy_id.is_some_and(|id| item.attribute("id") == Some(id));
        if declared {
            covers.push(resolve_member(
                opf_path,
                item.attribute("href").context("Cover item has no href")?,
            )?);
        }
    }
    if covers.is_empty()
        && let Some(reference) = root
            .children()
            .filter(|n| n.has_tag_name((OPF, "guide")))
            .flat_map(|n| n.children())
            .find(|n| n.has_tag_name((OPF, "reference")) && n.attribute("type") == Some("cover"))
    {
        let page = resolve_member(
            opf_path,
            reference
                .attribute("href")
                .context("Cover guide has no href")?,
        )?;
        let is_image = manifest.iter().any(|item| {
            item.attribute("media-type")
                .is_some_and(|m| m.starts_with("image/"))
                && item
                    .attribute("href")
                    .is_some_and(|href| resolve_member(opf_path, href).is_ok_and(|p| p == page))
        });
        if is_image {
            covers.push(page);
        } else {
            let xml = decode_xml(read_member(zip, &page, MAX_XML)?)?;
            let page_doc = Document::parse(&xml).context("Invalid declared cover page XML")?;
            for image in page_doc.descendants().filter(|n| {
                n.has_tag_name(("http://www.w3.org/1999/xhtml", "img"))
                    || n.has_tag_name(("http://www.w3.org/2000/svg", "image"))
            }) {
                let href = image
                    .attribute("src")
                    .or_else(|| image.attribute("href"))
                    .or_else(|| image.attribute(("http://www.w3.org/1999/xlink", "href")))
                    .context("Declared cover page image has no source")?;
                covers.push(resolve_member(&page, href)?);
            }
        }
    }
    covers.sort();
    covers.dedup();
    ensure!(
        covers.len() <= 1,
        "Multiple declared cover images require review"
    );
    covers
        .first()
        .map(|path| {
            let bytes = read_member(zip, path, crate::cover::MAX_BYTES as u64)?;
            crate::cover::signature(&bytes).context("Invalid EPUB cover")
        })
        .transpose()
}

pub(crate) fn resolve_member(base: &str, href: &str) -> Result<String> {
    ensure!(
        !href.contains('\\') && !href.contains('\0'),
        "Invalid EPUB cover reference"
    );
    let base = reqwest::Url::parse("https://epub.invalid/")?.join(base)?;
    let url = base.join(href)?;
    ensure!(
        url.origin() == base.origin()
            && url.query().is_none()
            && url.username().is_empty()
            && url.password().is_none(),
        "External or invalid EPUB cover reference"
    );
    let path = percent_encoding::percent_decode_str(url.path()).decode_utf8()?;
    ensure!(
        !path.contains('\\')
            && !path.contains('\0')
            && !path.split('/').any(|part| part == ".." || part == "."),
        "Invalid encoded EPUB cover path"
    );
    Ok(path.trim_start_matches('/').to_owned())
}

pub(crate) fn read_member(zip: &mut ZipArchive<File>, name: &str, limit: u64) -> Result<Vec<u8>> {
    let member = zip
        .by_name(name)
        .with_context(|| format!("Missing or unreadable EPUB member: {name}"))?;
    ensure!(member.size() <= limit, "EPUB metadata member is too large");
    let mut data = Vec::new();
    member.take(limit + 1).read_to_end(&mut data)?;
    ensure!(
        data.len() as u64 <= limit,
        "EPUB metadata exceeds size limit"
    );
    Ok(data)
}

pub(crate) fn decode_xml(data: Vec<u8>) -> Result<String> {
    if data.starts_with(&[0xff, 0xfe]) || data.starts_with(&[0xfe, 0xff]) {
        ensure!(data.len().is_multiple_of(2), "Malformed UTF-16 XML");
        let little = data[0] == 0xff;
        let units: Vec<_> = data[2..]
            .chunks_exact(2)
            .map(|pair| {
                if little {
                    u16::from_le_bytes([pair[0], pair[1]])
                } else {
                    u16::from_be_bytes([pair[0], pair[1]])
                }
            })
            .collect();
        return String::from_utf16(&units).context("Invalid UTF-16 XML");
    }
    let xml = String::from_utf8(data).context("EPUB XML must be UTF-8 or BOM-marked UTF-16")?;
    if xml.contains('\0') {
        bail!("Unsupported XML encoding");
    }
    Ok(xml.trim_start_matches('\u{feff}').to_owned())
}

pub fn normalize_isbn(raw: &str) -> Option<String> {
    let raw = raw.trim().to_ascii_uppercase();
    let raw = raw
        .strip_prefix("URN:ISBN:")
        .or_else(|| raw.strip_prefix("ISBN:"))
        .unwrap_or(&raw);
    let compact: String = raw
        .chars()
        .filter(|c| *c != '-' && !c.is_ascii_whitespace())
        .collect();
    if compact.len() == 10 {
        let mut sum = 0;
        for (index, c) in compact.chars().enumerate() {
            let digit = if index == 9 && c == 'X' {
                10
            } else {
                c.to_digit(10)?
            };
            sum += digit * (10 - index as u32);
        }
        if sum % 11 != 0 {
            return None;
        }
        let prefix = format!("978{}", &compact[..9]);
        let check = check_digit(&prefix)?;
        Some(format!("{prefix}{check}"))
    } else if compact.len() == 13
        && (compact.starts_with("978") || compact.starts_with("979"))
        && compact.chars().last()?.to_digit(10)? == check_digit(&compact[..12])?
    {
        Some(compact)
    } else {
        None
    }
}

fn check_digit(prefix: &str) -> Option<u32> {
    let sum = prefix.chars().enumerate().try_fold(0, |sum, (i, c)| {
        Some(sum + c.to_digit(10)? * if i % 2 == 0 { 1 } else { 3 })
    })?;
    Some((10 - sum % 10) % 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isbn_checksums_and_conversion() {
        assert_eq!(
            normalize_isbn("0-306-40615-2").as_deref(),
            Some("9780306406157")
        );
        assert_eq!(
            normalize_isbn("urn:isbn:9780306406157").as_deref(),
            Some("9780306406157")
        );
        assert_eq!(
            normalize_isbn("080442957X").as_deref(),
            Some("9780804429573")
        );
        for value in [
            "9780306406158",
            "1234567890",
            "uuid:9780306406157",
            "",
            "97800000000é",
        ] {
            assert_eq!(normalize_isbn(value), None, "{value}");
        }
    }
}
