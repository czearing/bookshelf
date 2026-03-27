use crate::db::{self, DbPool, EditionRow};
use crate::epub;
use anyhow::{anyhow, Context};
use std::io::{Read, Write};
use std::path::Path;
use tempfile::NamedTempFile;
use zip::ZipArchive;

/// Summary of a completed tag-all operation.
#[derive(Debug, Default)]
pub struct TagResult {
    pub tagged: usize,
    pub skipped: usize,
}

/// Describes what fields would be updated for a given edition (dry-run output).
#[derive(Debug)]
pub struct TagDiff {
    pub edition_id: i64,
    /// Each entry is `(field_name, new_value)`.
    pub fields: Vec<(String, String)>,
}

/// Tag a single edition by ID.
///
/// Steps:
/// 1. Load the edition row from DB — Err if not found (AC-15).
/// 2. Verify `source_path` exists — Err if not (AC-33).
/// 3. If `dry_run`: print diff, return Ok without any file or DB changes (AC-31).
/// 4. Verify no `.epub.bak` file exists alongside the source — Err if one does (AC-17).
/// 5. Copy source to `.epub.bak` (AC-16).
/// 6. Rewrite the EPUB OPF with DB metadata (AC-18 through AC-25, AC-28).
/// 7. Set `tagged_at` in DB (AC-27).
pub async fn tag_edition(pool: &DbPool, edition_id: i64, dry_run: bool) -> anyhow::Result<()> {
    // Step 1: load edition.
    let row = db::get_edition(pool, edition_id)
        .await
        .context("get_edition")?
        .ok_or_else(|| anyhow!("edition {} not found", edition_id))?;

    // Step 2: verify source file exists.
    let path = Path::new(&row.source_path);
    if !path.exists() {
        return Err(anyhow!(
            "source file does not exist: {}",
            path.display()
        ));
    }

    // Step 3: dry-run — print diff and return without changes.
    if dry_run {
        let diff = compute_tag_diff(&row);
        for (field, value) in &diff.fields {
            println!("{}: {}", field, value);
        }
        return Ok(());
    }

    // Step 4: check that no backup already exists.
    let bak = path.with_extension("epub.bak");
    if bak.exists() {
        return Err(anyhow!(
            "backup file already exists: {}; refusing to overwrite",
            bak.display()
        ));
    }

    // Step 5: byte-for-byte backup (AC-16).
    std::fs::copy(path, &bak)
        .with_context(|| format!("failed to create backup {}", bak.display()))?;

    // Step 6: rewrite OPF in EPUB.
    if let Err(e) = rewrite_epub_opf(path, &row) {
        // If rewrite fails, try to restore from backup.
        let _ = std::fs::copy(&bak, path);
        let _ = std::fs::remove_file(&bak);
        return Err(e.context("rewrite_epub_opf failed"));
    }

    // Step 7: record tagged_at in DB (AC-27).
    db::set_tagged_at(pool, edition_id)
        .await
        .context("set_tagged_at")?;

    Ok(())
}

/// Tag all owned, enriched, not-yet-tagged editions.
///
/// Returns a summary of how many were tagged and how many were skipped (AC-30).
pub async fn tag_all(pool: &DbPool, dry_run: bool) -> anyhow::Result<TagResult> {
    let editions = db::list_owned_enriched_editions(pool)
        .await
        .context("list_owned_enriched_editions")?;

    let mut result = TagResult::default();

    for row in &editions {
        // Skip already-tagged editions (AC-30).
        if row.tagged_at.is_some() {
            result.skipped += 1;
            continue;
        }

        match tag_edition(pool, row.id, dry_run).await {
            Ok(()) => {
                result.tagged += 1;
            }
            Err(e) => {
                eprintln!("WARNING: could not tag edition {}: {}", row.id, e);
                result.skipped += 1;
            }
        }
    }

    println!("{} tagged, {} skipped", result.tagged, result.skipped);
    Ok(result)
}

/// Compute a list of field changes that would be written to the OPF for `row`.
/// Fields that are `None` in the DB row are excluded.
pub fn compute_tag_diff(row: &EditionRow) -> TagDiff {
    let mut fields = Vec::new();

    if let Some(v) = &row.title {
        fields.push(("title".to_string(), v.clone()));
    }
    if let Some(v) = &row.authors {
        fields.push(("authors".to_string(), v.clone()));
    }
    if let Some(v) = &row.publisher {
        fields.push(("publisher".to_string(), v.clone()));
    }
    if let Some(v) = &row.publish_date {
        fields.push(("publish_date".to_string(), v.clone()));
    }
    if let Some(v) = &row.description {
        fields.push(("description".to_string(), v.clone()));
    }
    if let Some(v) = &row.isbn {
        fields.push(("isbn".to_string(), v.clone()));
    }
    if let Some(v) = &row.series_name {
        fields.push(("series_name".to_string(), v.clone()));
    }
    if let Some(v) = &row.series_position {
        fields.push(("series_position".to_string(), v.clone()));
    }

    TagDiff {
        edition_id: row.id,
        fields,
    }
}

// ---------------------------------------------------------------------------
// EPUB rewrite internals
// ---------------------------------------------------------------------------

/// Open the EPUB at `path`, update the OPF with the non-null fields from `row`,
/// and atomically replace `path` with the updated EPUB.
fn rewrite_epub_opf(path: &Path, row: &EditionRow) -> anyhow::Result<()> {
    // Step 1: read all ZIP entries into memory.
    let file = std::fs::File::open(path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    let mut archive = ZipArchive::new(std::io::BufReader::new(file))
        .with_context(|| format!("cannot open ZIP {}", path.display()))?;

    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        entries.push((name, data));
    }

    // Step 2: find OPF path from META-INF/container.xml.
    let container_bytes = entries
        .iter()
        .find(|(name, _)| name == "META-INF/container.xml")
        .map(|(_, data)| data.clone())
        .ok_or_else(|| anyhow!("META-INF/container.xml not found in {}", path.display()))?;

    let container_str = String::from_utf8_lossy(&container_bytes);
    let opf_path = extract_opf_path_from_container(&container_str)
        .ok_or_else(|| anyhow!("rootfile full-path not found in container.xml"))?;

    // Step 3: produce updated OPF.
    let opf_bytes = entries
        .iter()
        .find(|(name, _)| name == &opf_path)
        .map(|(_, data)| data.clone())
        .ok_or_else(|| anyhow!("OPF file '{}' not found in archive", opf_path))?;

    let opf_str = String::from_utf8_lossy(&opf_bytes);
    let updated_opf = update_opf_xml(&opf_str, row);

    // Step 4: write new ZIP to a temp file alongside the original.
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = NamedTempFile::with_suffix_in(".epub", parent)
        .context("cannot create temp file for EPUB rewrite")?;

    {
        let tmp_file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .context("cannot open temp file for writing")?;

        let mut writer = zip::ZipWriter::new(tmp_file);
        let stored_opts = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        for (name, data) in &entries {
            writer
                .start_file(name, stored_opts)
                .with_context(|| format!("zip start_file '{name}'"))?;
            if name == &opf_path {
                writer
                    .write_all(updated_opf.as_bytes())
                    .context("write updated OPF")?;
            } else {
                writer.write_all(data).with_context(|| format!("write entry '{name}'"))?;
            }
        }
        writer.finish().context("zip finish")?;
    }

    // Step 5: atomic replace — persist temp file to path.
    let tmp_path = tmp.into_temp_path();
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename temp to {}", path.display()))?;

    Ok(())
}

/// Extract the OPF `full-path` from container.xml content using string search.
fn extract_opf_path_from_container(xml: &str) -> Option<String> {
    let needle = "full-path=\"";
    let start = xml.find(needle)?;
    let after_quote = start + needle.len();
    let end = xml[after_quote..].find('"')?;
    Some(xml[after_quote..after_quote + end].to_string())
}

/// Update OPF XML string using the non-null fields in `row`.
/// Only fields where the DB row has `Some(value)` are touched (AC-26).
/// Uses string-level find/replace — no XML DOM.
fn update_opf_xml(xml: &str, row: &EditionRow) -> String {
    let mut result = xml.to_string();

    // dc:title
    if let Some(v) = &row.title {
        result = replace_element_content(&result, "dc:title", epub::xml_escape(v).as_str());
    }

    // dc:creator (authors) — special: remove all, re-insert after dc:title
    if let Some(v) = &row.authors {
        result = replace_creators(&result, v);
    }

    // dc:publisher
    if let Some(v) = &row.publisher {
        result = replace_element_content(&result, "dc:publisher", epub::xml_escape(v).as_str());
    }

    // dc:date
    if let Some(v) = &row.publish_date {
        result = replace_element_content(&result, "dc:date", epub::xml_escape(v).as_str());
    }

    // dc:description
    if let Some(v) = &row.description {
        result = replace_element_content(&result, "dc:description", epub::xml_escape(v).as_str());
    }

    // dc:identifier with opf:scheme="ISBN"
    if let Some(v) = &row.isbn {
        result = replace_isbn(&result, epub::xml_escape(v).as_str());
    }

    // <meta name="calibre:series" content="...">
    if let Some(v) = &row.series_name {
        result = replace_calibre_meta(&result, "calibre:series", epub::xml_escape(v).as_str());
    }

    // <meta name="calibre:series_index" content="...">
    if let Some(v) = &row.series_position {
        result =
            replace_calibre_meta(&result, "calibre:series_index", epub::xml_escape(v).as_str());
    }

    result
}

/// Replace the text content between `<tag...>` and `</tag>`.
/// Preserves any attributes on the opening tag.
/// If the element is not present, this is a no-op.
fn replace_element_content(xml: &str, tag: &str, new_content: &str) -> String {
    let open_tag = format!("<{tag}");
    let close_tag = format!("</{tag}>");

    // Find the opening tag.
    let open_start = match xml.find(&open_tag) {
        Some(pos) => pos,
        None => return xml.to_string(),
    };

    // Verify the character after the tag name is '>' or a space/attribute, not a longer name.
    // e.g. avoid matching <dc:title> on <dc:titlepage>
    let after_name = &xml[open_start + open_tag.len()..];
    let next_char = after_name.chars().next();
    if !matches!(next_char, Some('>') | Some(' ') | Some('\t') | Some('\n') | Some('\r') | Some('/')) {
        // Try to find a second occurrence.
        let search_from = open_start + open_tag.len();
        return match xml[search_from..].find(&open_tag) {
            None => xml.to_string(),
            Some(rel) => {
                let second_start = search_from + rel;
                replace_element_content_from(xml, &open_tag, &close_tag, second_start, new_content)
            }
        };
    }

    replace_element_content_from(xml, &open_tag, &close_tag, open_start, new_content)
}

fn replace_element_content_from(
    xml: &str,
    _open_tag: &str,
    close_tag: &str,
    open_start: usize,
    new_content: &str,
) -> String {
    // Find the '>' that ends the opening tag.
    let gt_pos = match xml[open_start..].find('>') {
        Some(pos) => open_start + pos,
        None => return xml.to_string(),
    };
    let content_start = gt_pos + 1;

    // Find the closing tag after content_start.
    let close_start = match xml[content_start..].find(close_tag) {
        Some(pos) => content_start + pos,
        None => return xml.to_string(),
    };
    let close_end = close_start + close_tag.len();

    // Reconstruct: prefix + original open tag (with attributes) + new content + close tag + suffix
    let open_with_attrs = &xml[open_start..=gt_pos];
    format!(
        "{}{}{}{}{}",
        &xml[..open_start],
        open_with_attrs,
        new_content,
        close_tag,
        &xml[close_end..]
    )
}

/// Remove all `<dc:creator>...</dc:creator>` elements, then insert new ones
/// after `<dc:title>...</dc:title>`.
fn replace_creators(xml: &str, authors_str: &str) -> String {
    // Step 1: remove all existing dc:creator elements.
    let mut result = xml.to_string();
    loop {
        let start = result.find("<dc:creator");
        let close_tag = "</dc:creator>";
        match start {
            None => break,
            Some(s) => match result[s..].find(close_tag) {
                None => break,
                Some(rel_end) => {
                    let end = s + rel_end + close_tag.len();
                    result.replace_range(s..end, "");
                }
            },
        }
    }

    // Step 2: build new dc:creator elements.
    let new_creators: String = authors_str
        .split(", ")
        .map(|a| format!("<dc:creator>{}</dc:creator>", epub::xml_escape(a)))
        .collect::<Vec<_>>()
        .join("\n    ");

    // Step 3: insert after </dc:title>.
    let close_title = "</dc:title>";
    match result.find(close_title) {
        None => {
            // No dc:title — return as-is (best effort).
            result
        }
        Some(pos) => {
            let insert_pos = pos + close_title.len();
            format!(
                "{}\n    {}{}",
                &result[..insert_pos],
                new_creators,
                &result[insert_pos..]
            )
        }
    }
}

/// Replace `<dc:identifier opf:scheme="ISBN">...</dc:identifier>`.
/// If none exists, insert before `</metadata>`.
fn replace_isbn(xml: &str, new_isbn: &str) -> String {
    let needle = "opf:scheme=\"ISBN\"";
    if let Some(attr_pos) = xml.find(needle) {
        // Walk back to find the '<' of this element.
        let open_start = match xml[..attr_pos].rfind('<') {
            Some(p) => p,
            None => return xml.to_string(),
        };
        // Find the '>' closing the open tag.
        let gt_pos = match xml[open_start..].find('>') {
            Some(p) => open_start + p,
            None => return xml.to_string(),
        };
        let content_start = gt_pos + 1;
        let close_tag = "</dc:identifier>";
        let close_start = match xml[content_start..].find(close_tag) {
            Some(p) => content_start + p,
            None => return xml.to_string(),
        };
        let close_end = close_start + close_tag.len();

        format!(
            "{}{}{}{}",
            &xml[..content_start],
            new_isbn,
            close_tag,
            &xml[close_end..]
        )
    } else {
        // Insert before </metadata>.
        let insert_marker = "</metadata>";
        match xml.find(insert_marker) {
            None => xml.to_string(),
            Some(pos) => format!(
                "{}    <dc:identifier opf:scheme=\"ISBN\">{}</dc:identifier>\n  {}",
                &xml[..pos],
                new_isbn,
                &xml[pos..]
            ),
        }
    }
}

/// Replace `content="..."` on `<meta name="{name}" ...>` elements.
/// If none exists, insert a new self-closing element before `</metadata>`.
fn replace_calibre_meta(xml: &str, meta_name: &str, new_content: &str) -> String {
    let needle_dq = format!("name=\"{}\"", meta_name);
    let needle_sq = format!("name='{}'", meta_name);

    let attr_pos = xml.find(&needle_dq).or_else(|| xml.find(&needle_sq));

    if let Some(attr_start) = attr_pos {
        // Walk back to find '<' of this element.
        let open_start = match xml[..attr_start].rfind('<') {
            Some(p) => p,
            None => return xml.to_string(),
        };
        // Find the '>' closing the element (could be '/>' or '>').
        let close_pos = match xml[open_start..].find('>') {
            Some(p) => open_start + p,
            None => return xml.to_string(),
        };
        let tag_text = &xml[open_start..=close_pos];

        // Replace content="..." within the tag text.
        let new_tag = replace_attr_value(tag_text, "content", new_content);
        format!("{}{}{}", &xml[..open_start], new_tag, &xml[close_pos + 1..])
    } else {
        // Insert before </metadata>.
        let insert_marker = "</metadata>";
        match xml.find(insert_marker) {
            None => xml.to_string(),
            Some(pos) => format!(
                "{}    <meta name=\"{}\" content=\"{}\"/>\n  {}",
                &xml[..pos],
                meta_name,
                new_content,
                &xml[pos..]
            ),
        }
    }
}

/// Replace the value of `attr_name="..."` or `attr_name='...'` within a tag string.
/// If the attribute is absent, append it before the closing '>' or '/>'.
fn replace_attr_value(tag: &str, attr_name: &str, new_value: &str) -> String {
    let needle_dq = format!("{}=\"", attr_name);
    let needle_sq = format!("{}='", attr_name);

    if let Some(start) = tag.find(&needle_dq) {
        let value_start = start + needle_dq.len();
        let value_end = match tag[value_start..].find('"') {
            Some(p) => value_start + p,
            None => return tag.to_string(),
        };
        format!(
            "{}{}=\"{}\"{}",
            &tag[..start],
            attr_name,
            new_value,
            &tag[value_end + 1..]
        )
    } else if let Some(start) = tag.find(&needle_sq) {
        let value_start = start + needle_sq.len();
        let value_end = match tag[value_start..].find('\'') {
            Some(p) => value_start + p,
            None => return tag.to_string(),
        };
        format!(
            "{}{}'{}'{}",
            &tag[..start],
            attr_name,
            new_value,
            &tag[value_end + 1..]
        )
    } else {
        // Attribute not present — append it before '/>' or '>'.
        if let Some(close) = tag.rfind("/>") {
            format!("{} {}=\"{}\"/>", &tag[..close], attr_name, new_value)
        } else if let Some(close) = tag.rfind('>') {
            format!("{} {}=\"{}\">", &tag[..close], attr_name, new_value)
        } else {
            tag.to_string()
        }
    }
}
