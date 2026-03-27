use std::cmp::Reverse;
use std::path::{Path, PathBuf};
use crate::{author, db, native_path_str};

/// Windows reserved device names that cannot be used as filenames (with or without extension).
const WINDOWS_RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

pub fn sanitize_path_component(s: &str) -> String {
    // Replace chars invalid in filenames on Windows/Mac/Linux with '-'
    // Invalid: / \ : * ? " < > | and control chars
    let result: String = s.chars().map(|c| {
        match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0'..='\x1f' => '-',
            _ => c,
        }
    }).collect();
    // Collapse multiple dashes
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in result.chars() {
        if ch == '-' {
            if !prev_dash { out.push(ch); }
            prev_dash = true;
        } else {
            out.push(ch);
            prev_dash = false;
        }
    }
    // Trim leading/trailing dashes, dots (Windows rejects trailing dots), and whitespace.
    let out = out.trim_matches(|c: char| c == '-' || c == '.' || c.is_whitespace()).to_string();
    // Truncate to 100 Unicode scalar values to keep total paths well under MAX_PATH (260).
    let out: String = out.chars().take(100).collect();
    // After truncation, re-trim trailing dashes/dots that may have been exposed.
    let out = out.trim_end_matches(|c: char| c == '-' || c == '.').to_string();
    // Append '_' to Windows reserved device names (e.g. "NUL" → "NUL_") so the OS
    // does not silently redirect writes to the null device or a COM/LPT port.
    if WINDOWS_RESERVED_NAMES.contains(&out.to_uppercase().as_str()) {
        format!("{out}_")
    } else {
        out
    }
}

pub struct OrganizePlan {
    pub edition_id: i64,
    pub old_path: PathBuf,
    pub new_path: PathBuf,
    pub title: String,
}

pub struct OrganizeResult {
    pub moved: usize,
    pub skipped: usize,
    pub errors: Vec<String>,
}

/// Score an edition's metadata completeness. Higher is better.
/// Used by `dedup_editions` to choose which copy to keep.
fn edition_quality_score(row: &db::EditionRow) -> u32 {
    let mut score = 0u32;
    if row.enriched_at.is_some()  { score += 10; }
    if row.isbn.is_some()         { score += 4; }
    if row.description.is_some()  { score += 3; }
    if row.publisher.is_some()    { score += 1; }
    if row.series_name.is_some()  { score += 1; }
    if row.language.is_some()     { score += 1; }
    let ext = PathBuf::from(&row.source_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext == "epub"              { score += 2; }
    score
}

/// Remove duplicate editions that share the same (title, authors) pair.
///
/// For each group of 2+ duplicates the edition with the highest quality score
/// (most metadata; EPUB preferred over PDF; lowest ID as tie-breaker) is kept.
/// All others are deleted from disk and from the DB.
///
/// When `dry_run` is `true` duplicates are only printed, nothing is removed.
/// Returns the number of editions removed.
pub async fn dedup_editions(pool: &db::DbPool, dry_run: bool) -> anyhow::Result<usize> {
    let groups = db::find_duplicate_editions(pool).await?;
    if groups.is_empty() {
        return Ok(0);
    }

    let mut removed = 0usize;
    for group in &groups {
        let best = group
            .iter()
            .max_by_key(|e| (edition_quality_score(e), Reverse(e.id)))
            .expect("group is non-empty");

        for edition in group {
            if edition.id == best.id {
                continue;
            }
            let title = edition.title.as_deref().unwrap_or("(no title)");
            let path = PathBuf::from(&edition.source_path);

            if dry_run {
                println!(
                    "[DRY RUN] Would remove duplicate: '{}' (#{}) — would keep #{}",
                    title, edition.id, best.id
                );
                removed += 1;
                continue;
            }

            // Only delete the file when it is at a DIFFERENT physical location
            // from the keeper. On Windows, Path normalises both `/` and `\`
            // internally, so two paths that differ only in separator style
            // compare equal here — preventing us from deleting the shared file.
            let keeper_path = PathBuf::from(&best.source_path);
            if path.exists() && path != keeper_path {
                if let Err(e) = std::fs::remove_file(&path) {
                    eprintln!("  WARNING: could not delete {}: {e}", path.display());
                }
            }

            match db::delete_edition(pool, edition.id).await {
                Ok(()) => {
                    println!(
                        "  Removed duplicate: '{}' (#{}) — kept #{}",
                        title, edition.id, best.id
                    );
                    removed += 1;
                }
                Err(e) => {
                    eprintln!("  ERROR removing duplicate #{}: {e}", edition.id);
                }
            }
        }
    }

    Ok(removed)
}

pub fn compute_organize_plan(editions: &[db::EditionRow], library_root: &Path) -> Vec<OrganizePlan> {
    let mut plans = Vec::new();
    for edition in editions {
        // Skip if missing required metadata
        let title = match &edition.title {
            Some(t) if !t.is_empty() => t.clone(),
            _ => continue,
        };
        let authors = match &edition.authors {
            Some(a) if !a.is_empty() => a.clone(),
            _ => continue,
        };
        // Source path must exist
        let old_path = PathBuf::from(&edition.source_path);

        // Normalize the full authors string, then take the first author for the folder name
        let normalized_all = author::normalize_authors(&authors);
        let first_author = normalized_all.split(", ").next().unwrap_or(&normalized_all).to_string();

        let author_dir = sanitize_path_component(&first_author);
        let title_file = sanitize_path_component(&title);

        if author_dir.is_empty() || title_file.is_empty() {
            continue;
        }

        let ext = old_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("epub")
            .to_lowercase();
        let new_path = library_root.join(&author_dir).join(format!("{}.{}", title_file, ext));

        // Skip if already in the right place
        if old_path == new_path {
            continue;
        }

        plans.push(OrganizePlan {
            edition_id: edition.id,
            old_path,
            new_path,
            title: title.clone(),
        });
    }
    plans
}

fn resolve_dest(base: &Path, force: bool) -> PathBuf {
    if !base.exists() || force {
        return base.to_path_buf();
    }
    let parent = base.parent().unwrap_or(Path::new("."));
    let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("book");
    let mut n = 2usize;
    loop {
        let candidate = parent.join(format!("{} ({}).epub", stem, n));
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
    }
}

pub async fn organize_editions(
    pool: &db::DbPool,
    library_root: &Path,
    dry_run: bool,
    force: bool,
) -> anyhow::Result<OrganizeResult> {
    // Remove duplicate editions before computing paths.
    let deduped = dedup_editions(pool, dry_run).await?;
    if deduped > 0 && !dry_run {
        println!("Removed {} duplicate edition{}.", deduped, if deduped == 1 { "" } else { "s" });
    }

    let editions = db::list_editions(pool).await?;
    let plans = compute_organize_plan(&editions, library_root);

    if plans.is_empty() {
        println!("Library is already organized.");
        return Ok(OrganizeResult { moved: 0, skipped: 0, errors: vec![] });
    }

    let mut result = OrganizeResult { moved: 0, skipped: 0, errors: vec![] };

    for plan in &plans {
        let display_old = plan.old_path.display();
        let final_dest = resolve_dest(&plan.new_path, force);
        let display_new = final_dest.display();

        if dry_run {
            println!("[DRY RUN] {} → {}", display_old, display_new);
            continue;
        }

        // Skip if source doesn't exist
        if !plan.old_path.exists() {
            println!("  SKIP (file not found): {}", display_old);
            result.skipped += 1;
            continue;
        }

        // Create destination directory
        if let Some(parent) = final_dest.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                result.errors.push(format!("create_dir {}: {}", parent.display(), e));
                continue;
            }
        }

        // Move file (use copy+delete to handle cross-device moves)
        match std::fs::rename(&plan.old_path, &final_dest) {
            Ok(()) => {}
            Err(_) => {
                // Cross-device: try copy + remove
                if let Err(e) = std::fs::copy(&plan.old_path, &final_dest) {
                    result.errors.push(format!("copy {}: {}", display_old, e));
                    continue;
                }
                if let Err(e) = std::fs::remove_file(&plan.old_path) {
                    result.errors.push(format!("remove original {}: {}", display_old, e));
                    continue;
                }
            }
        }

        // Update DB — if this fails, attempt to roll the file move back.
        let dest_str = native_path_str(&final_dest);
        if let Err(e) = db::update_source_path(pool, plan.edition_id, &dest_str).await {
            if let Err(re) = std::fs::rename(&final_dest, &plan.old_path) {
                result.errors.push(format!(
                    "db update #{} failed AND rollback failed — file is now at {}: db={}, rollback={}",
                    plan.edition_id, display_new, e, re
                ));
            } else {
                result.errors.push(format!(
                    "db update #{}: {} (file rolled back to original location)",
                    plan.edition_id, e
                ));
            }
        } else {
            println!("Moved: '{}' → {}", plan.title, display_new);
            result.moved += 1;
        }
    }

    // Remove any directories under library_root that are now empty.
    remove_empty_dirs(library_root);

    Ok(result)
}

/// Recursively remove empty subdirectories under `root` (does not remove `root` itself).
fn remove_empty_dirs(root: &Path) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_empty_dirs(&path);
            // After recursing, try to remove if now empty.
            let _ = std::fs::remove_dir(&path); // no-op if not empty
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::EditionRow;

    fn make_edition(id: i64, title: Option<&str>, authors: Option<&str>, source_path: &str) -> EditionRow {
        EditionRow {
            id,
            title: title.map(String::from),
            authors: authors.map(String::from),
            isbn: None,
            series_name: None,
            series_position: None,
            publisher: None,
            publish_date: None,
            language: None,
            description: None,
            cover_image_path: None,
            source_path: source_path.to_string(),
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

    #[test]
    fn test_sanitize_basic() {
        assert_eq!(sanitize_path_component("Hello World"), "Hello World");
    }

    #[test]
    fn test_sanitize_invalid_chars() {
        assert_eq!(sanitize_path_component("A/B\\C:D"), "A-B-C-D");
    }

    #[test]
    fn test_sanitize_collapses_dashes() {
        assert_eq!(sanitize_path_component("A//B"), "A-B");
    }

    #[test]
    fn test_sanitize_trims_dashes() {
        assert_eq!(sanitize_path_component("/Hello/"), "Hello");
    }

    #[test]
    fn test_sanitize_empty() {
        assert_eq!(sanitize_path_component(""), "");
    }

    #[test]
    fn test_sanitize_star_question() {
        assert_eq!(sanitize_path_component("What?*"), "What");
    }

    #[test]
    fn test_sanitize_windows_reserved_nul() {
        assert_eq!(sanitize_path_component("NUL"), "NUL_");
        assert_eq!(sanitize_path_component("nul"), "nul_");
    }

    #[test]
    fn test_sanitize_windows_reserved_com_lpt() {
        assert_eq!(sanitize_path_component("COM1"), "COM1_");
        assert_eq!(sanitize_path_component("LPT3"), "LPT3_");
    }

    #[test]
    fn test_sanitize_non_reserved_not_modified() {
        // "NULL" is not a reserved name, "COM10" is not reserved, etc.
        assert_eq!(sanitize_path_component("NULL"), "NULL");
        assert_eq!(sanitize_path_component("COM10"), "COM10");
    }

    #[test]
    fn test_sanitize_trailing_dot_trimmed() {
        assert_eq!(sanitize_path_component("Mr. Smith."), "Mr. Smith");
        assert_eq!(sanitize_path_component("Book..."), "Book");
    }

    #[test]
    fn test_sanitize_truncates_at_100_chars() {
        let long = "A".repeat(150);
        let result = sanitize_path_component(&long);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_compute_organize_plan_basic() {
        let editions = vec![
            make_edition(1, Some("The Hobbit"), Some("J.R.R. Tolkien"), "/old/hobbit.epub"),
        ];
        let root = Path::new("/library");
        let plans = compute_organize_plan(&editions, root);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].edition_id, 1);
        assert_eq!(plans[0].title, "The Hobbit");
        // new path should be /library/J.R.R. Tolkien/The Hobbit.epub
        assert!(plans[0].new_path.to_string_lossy().contains("The Hobbit"));
        assert!(plans[0].new_path.to_string_lossy().contains("Tolkien"));
    }

    #[test]
    fn test_compute_organize_plan_skips_no_title() {
        let editions = vec![
            make_edition(1, None, Some("Author"), "/old/book.epub"),
        ];
        let root = Path::new("/library");
        let plans = compute_organize_plan(&editions, root);
        assert_eq!(plans.len(), 0);
    }

    #[test]
    fn test_compute_organize_plan_skips_no_authors() {
        let editions = vec![
            make_edition(1, Some("Title"), None, "/old/book.epub"),
        ];
        let root = Path::new("/library");
        let plans = compute_organize_plan(&editions, root);
        assert_eq!(plans.len(), 0);
    }

    #[test]
    fn test_compute_organize_plan_skips_already_correct() {
        // The path already matches what we would compute
        let root = Path::new("/library");
        // Author "J.R.R. Tolkien" normalized stays "J.R.R. Tolkien"
        // sanitize("J.R.R. Tolkien") = "J.R.R. Tolkien"
        // sanitize("The Hobbit") = "The Hobbit"
        let expected = root.join("J.R.R. Tolkien").join("The Hobbit.epub");
        let editions = vec![
            make_edition(1, Some("The Hobbit"), Some("J.R.R. Tolkien"), &expected.to_string_lossy()),
        ];
        let plans = compute_organize_plan(&editions, root);
        assert_eq!(plans.len(), 0, "already-correct path should produce no plan");
    }

    #[test]
    fn test_compute_organize_plan_inverted_author() {
        // "Tolkien, J.R.R." should normalize to "J.R.R. Tolkien"
        let editions = vec![
            make_edition(1, Some("The Hobbit"), Some("Tolkien, J.R.R."), "/old/hobbit.epub"),
        ];
        let root = Path::new("/library");
        let plans = compute_organize_plan(&editions, root);
        assert_eq!(plans.len(), 1);
        let new_str = plans[0].new_path.to_string_lossy();
        assert!(new_str.contains("J.R.R. Tolkien"), "inverted author should be normalized: {}", new_str);
    }
}
