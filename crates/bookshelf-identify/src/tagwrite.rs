//! Writes corrected bibliographic metadata and a cover into a copy of an EPUB.
//!
//! The package document is edited by splicing byte ranges reported by the XML
//! parser, so everything not being changed stays byte-for-byte identical. All
//! other archive members are copied raw. The output is a new file, re-read and
//! compared with the source before it is accepted; on any mismatch it is deleted.

use crate::epub::{self, decode_xml, normalize_isbn, package_path, read_member, resolve_member};
use anyhow::{Context, Result, bail, ensure};
use image::ImageFormat;
use roxmltree::{Document, Node};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

const DC: &str = "http://purl.org/dc/elements/1.1/";
const OPF: &str = "http://www.idpf.org/2007/opf";
const MAX_XML: u64 = 4 * 1024 * 1024;
const MAX_COVER: usize = 8 * 1024 * 1024;
const COVER_ID: &str = "book-manager-cover";

#[derive(Clone, Debug, Default)]
pub struct Tags {
    pub title: String,
    pub authors: Vec<String>,
    /// Added only when the package declares no language; an existing one is kept
    /// unless `replace_language` is set.
    pub language: Option<String>,
    /// Replace every declared language with `language` (a verified correction).
    pub replace_language: bool,
    pub isbn13: Option<String>,
    /// Series name and position, written as Calibre and EPUB 3 collection metadata.
    pub series: Option<(String, Option<String>)>,
    pub publisher: Option<String>,
    pub published_date: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CoverAction {
    Unchanged,
    Replaced(String),
    Added(String),
}

#[derive(Debug)]
pub struct Outcome {
    pub cover: CoverAction,
    pub changes: Vec<String>,
}

struct Edit {
    start: usize,
    end: usize,
    text: String,
}

fn valid_xml_text(value: &str) -> bool {
    value.chars().all(|c| {
        matches!(c, '\t' | '\n' | '\r')
            || ('\u{20}'..='\u{d7ff}').contains(&c)
            || ('\u{e000}'..='\u{fffd}').contains(&c)
            || c >= '\u{10000}'
    })
}

fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Opening/closing name and an optional inline namespace declaration for `local`.
fn name(scope: Node<'_, '_>, uri: &str, fallback_prefix: &str, local: &str) -> (String, String) {
    match scope.lookup_prefix(uri) {
        Some("") => (local.to_owned(), String::new()),
        Some(prefix) => (format!("{prefix}:{local}"), String::new()),
        None => match scope.default_namespace() {
            Some(ns) if ns == uri => (local.to_owned(), String::new()),
            _ => (
                format!("{fallback_prefix}:{local}"),
                format!(" xmlns:{fallback_prefix}=\"{uri}\""),
            ),
        },
    }
}

fn dc_element(scope: Node<'_, '_>, local: &str, value: &str) -> String {
    let (tag, ns) = name(scope, DC, "dc", local);
    format!("<{tag}{ns}>{}</{tag}>", escape(value))
}

fn indent_before(xml: &str, pos: usize) -> &str {
    let head = &xml[..pos];
    let line_start = head.rfind('\n').map_or(0, |i| i + 1);
    let line = &head[line_start..];
    let trimmed = line.trim_start_matches([' ', '\t']);
    if trimmed.is_empty() && head.contains('\n') {
        line
    } else {
        ""
    }
}

/// Insertion point just after the last element child, plus matching indentation.
fn append_point<'a>(xml: &'a str, parent: Node<'_, '_>) -> Result<(usize, &'a str)> {
    let last = parent
        .children()
        .rfind(Node::is_element)
        .context("Package section has no elements to anchor an insertion")?;
    let range = last.range();
    Ok((range.end, indent_before(xml, range.start)))
}

fn apply(xml: &str, mut edits: Vec<Edit>) -> Result<String> {
    let mut order: Vec<usize> = (0..edits.len()).collect();
    order.sort_by_key(|&i| (edits[i].start, i));
    let mut previous_end = 0;
    for &i in &order {
        let e = &edits[i];
        ensure!(e.start <= e.end && e.end <= xml.len(), "Invalid edit range");
        ensure!(e.start >= previous_end, "Overlapping package edits");
        previous_end = e.end.max(e.start);
    }
    let mut out = xml.to_owned();
    for &i in order.iter().rev() {
        let e = std::mem::replace(
            &mut edits[i],
            Edit {
                start: 0,
                end: 0,
                text: String::new(),
            },
        );
        out.replace_range(e.start..e.end, &e.text);
    }
    Ok(out)
}

struct CoverKind {
    media: &'static str,
    ext: &'static str,
}

fn validate(tags: &Tags, cover: Option<&[u8]>) -> Result<(Tags, Option<CoverKind>)> {
    let mut clean = Tags {
        title: tags.title.trim().to_owned(),
        language: tags.language.as_ref().map(|v| v.trim().to_owned()),
        replace_language: tags.replace_language,
        isbn13: None,
        series: tags.series.clone(),
        publisher: tags.publisher.as_ref().map(|v| v.trim().to_owned()),
        published_date: tags.published_date.as_ref().map(|v| v.trim().to_owned()),
        authors: Vec::new(),
    };
    ensure!(!clean.title.is_empty(), "A title is required");
    for author in &tags.authors {
        let author = author.trim();
        ensure!(!author.is_empty(), "Empty author name");
        if !clean.authors.iter().any(|a| a == author) {
            clean.authors.push(author.to_owned());
        }
    }
    ensure!(!clean.authors.is_empty(), "At least one author is required");
    if let Some(raw) = &tags.isbn13 {
        clean.isbn13 =
            Some(normalize_isbn(raw).with_context(|| format!("Invalid ISBN checksum: {raw}"))?);
    }
    let texts = std::iter::once(&clean.title)
        .chain(&clean.authors)
        .chain(clean.language.iter())
        .chain(clean.publisher.iter())
        .chain(clean.published_date.iter());
    for text in texts {
        ensure!(
            valid_xml_text(text),
            "Metadata contains characters invalid in XML"
        );
        ensure!(!text.is_empty(), "Empty metadata value");
    }
    let format = match cover {
        None => None,
        Some(bytes) => {
            ensure!(bytes.len() <= MAX_COVER, "Cover image exceeds 8 MiB");
            let format = image::guess_format(bytes).context("Unrecognized cover image format")?;
            let (media, ext) = match format {
                ImageFormat::Jpeg => ("image/jpeg", "jpg"),
                ImageFormat::Png => ("image/png", "png"),
                ImageFormat::WebP => ("image/webp", "webp"),
                other => bail!("Unsupported cover format {other:?}"),
            };
            crate::cover::signature(bytes).context("Cover image cannot be decoded")?;
            Some(CoverKind { media, ext })
        }
    };
    Ok((clean, format))
}

pub fn write(src: &Path, dst: &Path, tags: &Tags, cover: Option<&[u8]>) -> Result<Outcome> {
    ensure!(
        !dst.exists(),
        "Destination already exists: {}",
        dst.display()
    );
    ensure!(src != dst, "Refusing to overwrite the source EPUB");
    let (tags, cover_kind) = validate(tags, cover)?;
    let before = epub::fingerprint(src)?;
    let mut zip = ZipArchive::new(File::open(src).context("Cannot open EPUB")?)
        .context("Invalid EPUB ZIP archive")?;
    epub::check_mimetype(&mut zip)?;
    let opf_path = package_path(&mut zip)?;
    let xml = decode_xml(read_member(&mut zip, &opf_path, MAX_XML)?)?;
    let doc = Document::parse(&xml).context("Invalid EPUB package XML")?;
    let package = doc.root_element();
    ensure!(
        package.has_tag_name((OPF, "package")),
        "Invalid EPUB package root"
    );
    let metadata = package
        .children()
        .find(|n| n.has_tag_name((OPF, "metadata")))
        .context("Missing EPUB metadata")?;
    let manifest = package
        .children()
        .find(|n| n.has_tag_name((OPF, "manifest")))
        .context("Missing EPUB manifest")?;
    let epub3 = package
        .attribute("version")
        .is_some_and(|v| v.trim_start().starts_with('3'));

    let mut edits: Vec<Edit> = Vec::new();
    let mut changes = Vec::new();
    let mut removed_ids: Vec<&str> = Vec::new();
    let remove = |node: Node<'_, '_>, edits: &mut Vec<Edit>| {
        let range = node.range();
        // Also take the node's own line break and indentation so repeated
        // rewrites do not accumulate blank lines.
        let head = &xml[..range.start];
        let trimmed = head.trim_end_matches([' ', '\u{9}']);
        let start = match trimmed.strip_suffix('\u{a}') {
            Some(before) => before.strip_suffix('\u{d}').map_or(before.len(), str::len),
            None => range.start,
        };
        edits.push(Edit {
            start,
            end: range.end,
            text: String::new(),
        });
    };
    let dc_children = |local: &str| -> Vec<Node<'_, '_>> {
        epub::members(metadata)
            .into_iter()
            .filter(|n| n.has_tag_name((DC, local)))
            .collect()
    };

    let container = epub::dc_container(metadata);
    let (insert_at, indent) = append_point(&xml, container)?;
    let mut inserts: Vec<String> = Vec::new();

    for node in dc_children("title") {
        removed_ids.extend(node.attribute("id"));
        remove(node, &mut edits);
    }
    inserts.push(dc_element(container, "title", &tags.title));
    changes.push(format!("title = {}", tags.title));

    for node in dc_children("creator") {
        let roles = epub::roles_of(node, metadata);
        if roles.is_empty() || roles.iter().any(|r| r == "aut") {
            removed_ids.extend(node.attribute("id"));
            remove(node, &mut edits);
        }
    }
    for author in &tags.authors {
        inserts.push(dc_element(container, "creator", author));
    }
    changes.push(format!("authors = {}", tags.authors.join("; ")));

    for (local, value) in [
        ("publisher", &tags.publisher),
        ("date", &tags.published_date),
    ] {
        if let Some(value) = value {
            for node in dc_children(local) {
                removed_ids.extend(node.attribute("id"));
                remove(node, &mut edits);
            }
            inserts.push(dc_element(container, local, value));
            changes.push(format!("{local} = {value}"));
        }
    }
    if let Some(language) = &tags.language {
        let existing = dc_children("language");
        let blank = existing
            .iter()
            .all(|n| n.text().is_none_or(|t| t.trim().is_empty()));
        if tags.replace_language {
            let same = existing.len() == 1
                && existing[0]
                    .text()
                    .is_some_and(|t| t.trim().eq_ignore_ascii_case(language));
            if !same {
                for node in &existing {
                    remove(*node, &mut edits);
                }
                inserts.push(dc_element(container, "language", language));
                changes.push(format!("language corrected to {language}"));
            }
        } else if blank {
            inserts.push(dc_element(container, "language", language));
            changes.push(format!("language = {language}"));
        }
    }
    if let Some((series, index)) = &tags.series {
        let has_calibre = epub::members(metadata)
            .iter()
            .any(|n| n.attribute("name") == Some("calibre:series"));
        if !has_calibre {
            let (meta_tag, meta_ns) = name(container, OPF, "opf", "meta");
            inserts.push(format!(
                "<{meta_tag}{meta_ns} name=\"calibre:series\" content=\"{}\"/>",
                escape(series)
            ));
            if let Some(index) = index {
                inserts.push(format!(
                    "<{meta_tag} name=\"calibre:series_index\" content=\"{}\"/>",
                    escape(index)
                ));
            }
            if epub3 {
                let used: Vec<&str> = doc
                    .descendants()
                    .filter_map(|n| n.attribute("id"))
                    .collect();
                if !used.contains(&"book-manager-series") {
                    inserts.push(format!(
                        "<{meta_tag} property=\"belongs-to-collection\" id=\"book-manager-series\">{}</{meta_tag}>",
                        escape(series)
                    ));
                    inserts.push(format!(
                        "<{meta_tag} refines=\"#book-manager-series\" property=\"collection-type\">series</{meta_tag}>"
                    ));
                    if let Some(index) = index {
                        inserts.push(format!(
                            "<{meta_tag} refines=\"#book-manager-series\" property=\"group-position\">{}</{meta_tag}>",
                            escape(index)
                        ));
                    }
                }
            }
            changes.push(format!(
                "series = {series} #{}",
                index.as_deref().unwrap_or("?")
            ));
        }
    }
    if let Some(isbn) = &tags.isbn13
        && !dc_children("identifier")
            .iter()
            .any(|n| n.text().and_then(normalize_isbn).as_deref() == Some(isbn))
    {
        inserts.push(dc_element(
            metadata,
            "identifier",
            &format!("urn:isbn:{isbn}"),
        ));
        changes.push(format!("isbn = {isbn}"));
    }
    for meta in epub::members(metadata)
        .into_iter()
        .filter(|n| n.has_tag_name((OPF, "meta")))
    {
        if meta
            .attribute("refines")
            .and_then(|r| r.strip_prefix('#'))
            .is_some_and(|id| removed_ids.contains(&id))
        {
            remove(meta, &mut edits);
        }
    }

    // Cover: overwrite the declared image in place so every reference stays valid;
    // otherwise declare a new one.
    let legacy_id = epub::members(metadata)
        .into_iter()
        .find(|n| n.has_tag_name((OPF, "meta")) && n.attribute("name") == Some("cover"))
        .and_then(|n| n.attribute("content"));
    let items: Vec<_> = manifest
        .children()
        .filter(|n| n.has_tag_name((OPF, "item")))
        .collect();
    let declared: Vec<_> = items
        .iter()
        .filter(|item| {
            item.attribute("properties")
                .is_some_and(|p| p.split_whitespace().any(|p| p == "cover-image"))
                || legacy_id.is_some_and(|id| item.attribute("id") == Some(id))
        })
        .collect();
    let mut cover_entry: Option<(String, Vec<u8>)> = None;
    let mut cover_action = CoverAction::Unchanged;
    if let (Some(bytes), Some(CoverKind { media, ext })) = (cover, cover_kind) {
        let hrefs: Vec<_> = declared
            .iter()
            .filter_map(|i| i.attribute("href"))
            .collect();
        match declared.first() {
            Some(item) => {
                let paths: std::collections::BTreeSet<_> = hrefs
                    .iter()
                    .map(|h| resolve_member(&opf_path, h))
                    .collect::<Result<_>>()?;
                ensure!(
                    paths.len() == 1,
                    "Ambiguous cover declaration; manual review required"
                );
                let path = paths.into_iter().next().expect("one path");
                if item.attribute("media-type") != Some(media)
                    && let Some(attr) = item.attributes().find(|a| a.name() == "media-type")
                {
                    let r = attr.range_value();
                    edits.push(Edit {
                        start: r.start,
                        end: r.end,
                        text: media.to_owned(),
                    });
                }
                cover_action = CoverAction::Replaced(path.clone());
                cover_entry = Some((path, bytes.to_vec()));
            }
            None => {
                let dir = opf_path.rsplit_once('/').map_or("", |(d, _)| d);
                let href = format!("book-manager-cover.{ext}");
                let path = if dir.is_empty() {
                    href.clone()
                } else {
                    format!("{dir}/{href}")
                };
                ensure!(
                    zip.by_name(&path).is_err(),
                    "Cover path already in use: {path}"
                );
                ensure!(
                    !items.iter().any(|i| i.attribute("id") == Some(COVER_ID)),
                    "Cover id already in use"
                );
                let (manifest_at, manifest_indent) = append_point(&xml, manifest)?;
                let (item_tag, item_ns) = name(manifest, OPF, "opf", "item");
                let props = if epub3 {
                    " properties=\"cover-image\""
                } else {
                    ""
                };
                edits.push(Edit {
                    start: manifest_at,
                    end: manifest_at,
                    text: format!(
                        "\n{manifest_indent}<{item_tag}{item_ns} id=\"{COVER_ID}\" href=\"{href}\" media-type=\"{media}\"{props}/>"
                    ),
                });
                let (meta_tag, meta_ns) = name(container, OPF, "opf", "meta");
                inserts.push(format!(
                    "<{meta_tag}{meta_ns} name=\"cover\" content=\"{COVER_ID}\"/>"
                ));
                cover_action = CoverAction::Added(path.clone());
                cover_entry = Some((path, bytes.to_vec()));
            }
        }
        changes.push(format!("cover = {} bytes {media}", bytes.len()));
    }

    let joined: String = inserts.iter().map(|i| format!("\n{indent}{i}")).collect();
    edits.push(Edit {
        start: insert_at,
        end: insert_at,
        text: joined,
    });
    let new_opf = apply(&xml, edits)?;
    Document::parse(&new_opf).context("Edited package is not well-formed XML")?;

    let sources: HashMap<String, (u32, u64)> = (0..zip.len())
        .map(|i| {
            let f = zip.by_index_raw(i)?;
            Ok((f.name().to_owned(), (f.crc32(), f.size())))
        })
        .collect::<Result<_>>()?;

    let result = (|| -> Result<()> {
        let out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dst)
            .context("Cannot create destination EPUB")?;
        let mut writer = ZipWriter::new(out);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for i in 0..zip.len() {
            let file = zip.by_index_raw(i)?;
            let entry = file.name().to_owned();
            if entry == opf_path {
                writer.start_file(entry, options)?;
                writer.write_all(new_opf.as_bytes())?;
            } else if cover_entry.as_ref().is_some_and(|(p, _)| *p == entry) {
                let (_, bytes) = cover_entry.as_ref().expect("cover");
                writer.start_file(
                    entry,
                    options.compression_method(zip::CompressionMethod::Stored),
                )?;
                writer.write_all(bytes)?;
            } else {
                writer.raw_copy_file(file)?;
            }
        }
        if let (CoverAction::Added(path), Some((_, bytes))) = (&cover_action, &cover_entry) {
            writer.start_file(
                path.clone(),
                options.compression_method(zip::CompressionMethod::Stored),
            )?;
            writer.write_all(bytes)?;
        }
        writer.finish()?.sync_all()?;
        verify(dst, &tags, cover, &opf_path, &cover_entry, &sources)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(dst);
        return Err(error.context("Tagged copy failed verification and was removed"));
    }
    ensure!(
        before == epub::fingerprint(src)?,
        "Source changed while tagging"
    );
    Ok(Outcome {
        cover: cover_action,
        changes,
    })
}

fn verify(
    dst: &Path,
    tags: &Tags,
    cover: Option<&[u8]>,
    opf_path: &str,
    cover_entry: &Option<(String, Vec<u8>)>,
    sources: &HashMap<String, (u32, u64)>,
) -> Result<()> {
    let (read, _) = epub::read(dst)?;
    // The reader strips library decoration, so compare against the cleaned title.
    ensure!(
        read.title == crate::text::split_title(&tags.title).title,
        "Title did not round-trip"
    );
    ensure!(read.authors == tags.authors, "Authors did not round-trip");
    if tags.replace_language
        && let Some(language) = &tags.language
    {
        ensure!(
            read.declared_languages == [language.to_lowercase()],
            "Language did not round-trip"
        );
    }
    if let Some(isbn) = &tags.isbn13 {
        ensure!(read.isbns.contains(isbn), "ISBN did not round-trip");
    }
    if let Some(bytes) = cover {
        ensure!(read.cover.is_some(), "Cover missing after write");
        let (path, _) = cover_entry.as_ref().context("Cover entry not recorded")?;
        let mut stored = Vec::new();
        ZipArchive::new(File::open(dst)?)?
            .by_name(path)
            .context("Cover member missing after write")?
            .read_to_end(&mut stored)?;
        ensure!(stored == bytes, "Cover bytes did not round-trip");
    }
    let mut zip = ZipArchive::new(File::open(dst)?)?;
    let mut seen = 0;
    for i in 0..zip.len() {
        let file = zip.by_index_raw(i)?;
        let entry = file.name().to_owned();
        let touched = entry == opf_path || cover_entry.as_ref().is_some_and(|(p, _)| *p == entry);
        match sources.get(&entry) {
            Some(original) if !touched => {
                ensure!(
                    *original == (file.crc32(), file.size()),
                    "Untouched member changed: {entry}"
                );
                seen += 1;
            }
            None => ensure!(touched, "Unexpected new member: {entry}"),
            _ => {}
        }
    }
    let untouched = sources
        .keys()
        .filter(|k| k.as_str() != opf_path && cover_entry.as_ref().is_none_or(|(p, _)| p != *k))
        .count();
    ensure!(seen == untouched, "Archive members were lost");
    Ok(())
}
