use crate::db::{self, DbPool, EditionRow};
use std::path::{Path, PathBuf};

/// Tokens that the rename pattern may contain.
const SUPPORTED_TOKENS: &[&str] = &[
    "{title}",
    "{author}",
    "{series}",
    "{position}",
    "{year}",
    "{isbn}",
];

/// Summary of a rename operation.
#[derive(Debug)]
pub struct RenameResult {
    pub renamed: usize,
    pub skipped: usize,
    /// Populated during dry-run: pairs of (old_path, new_path) that would have
    /// been renamed. Empty for actual (non-dry-run) runs.
    pub planned: Vec<(PathBuf, PathBuf)>,
}

/// One planned rename for a single edition.
pub struct RenamePlan {
    pub edition_id: i64,
    pub old_path: PathBuf,
    pub new_path: PathBuf,
}

/// Validate that every `{...}` token in `pattern` is in `SUPPORTED_TOKENS`.
/// Returns `Err` with the first unrecognized token if any are found.
pub fn validate_pattern(pattern: &str) -> anyhow::Result<()> {
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            // Find matching '}'
            if let Some(close) = bytes[i..].iter().position(|&b| b == b'}') {
                let token = &pattern[i..i + close + 1];
                if !SUPPORTED_TOKENS.contains(&token) {
                    anyhow::bail!("unrecognized pattern token: {}", token);
                }
                i += close + 1;
            } else {
                // No closing brace — treat rest as literal, stop scanning.
                break;
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// Extract the first run of exactly 4 consecutive ASCII digits from `s`.
fn extract_year(s: &str) -> &str {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i + 3 < len {
        // Check if bytes[i..i+4] are all ASCII digits.
        if bytes[i].is_ascii_digit()
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
        {
            // Make sure the run is exactly 4 digits (not part of a longer run).
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_digit();
            let after_ok = i + 4 >= len || !bytes[i + 4].is_ascii_digit();
            if before_ok && after_ok {
                return &s[i..i + 4];
            }
        }
        i += 1;
    }
    ""
}

/// Windows reserved device names that cannot be used as filenames.
const WINDOWS_RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Replace filesystem-invalid characters with `_`, trim trailing dots/spaces
/// (Windows rejects them), truncate to 100 chars, and escape Windows reserved
/// device names (e.g. `NUL` → `NUL_`).
fn sanitize(s: String) -> String {
    let replaced: String = s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            other => other,
        })
        .collect();
    // Windows rejects filenames ending in dots or spaces (before the extension).
    let trimmed = replaced.trim_end_matches(|c: char| c == '.' || c == ' ').to_string();
    // Truncate to 100 chars so total paths stay well under MAX_PATH (260).
    let truncated: String = trimmed.chars().take(100).collect();
    // Re-trim after truncation in case we cut mid-word ending in a dot/space.
    let truncated = truncated.trim_end_matches(|c: char| c == '.' || c == ' ' || c == '_').to_string();
    // Append '_' to Windows reserved device names to prevent OS redirection.
    if WINDOWS_RESERVED_NAMES.contains(&truncated.to_uppercase().as_str()) {
        format!("{truncated}_")
    } else {
        truncated
    }
}

/// Expand `pattern` using the fields from `edition`, WITHOUT sanitizing.
/// The `/` separator in patterns like `"{author}/{title}"` is preserved so
/// that `build_target_path` can split on it later.
fn expand_pattern_raw(pattern: &str, title: &str, author: &str, series: &str, position: &str, year: &str, isbn: &str) -> String {
    pattern
        .replace("{title}", title)
        .replace("{author}", author)
        .replace("{series}", series)
        .replace("{position}", position)
        .replace("{year}", year)
        .replace("{isbn}", isbn)
}

/// Build a target `PathBuf` under `parent` from an unsanitized expanded pattern.
///
/// Splits `expanded` on `'/'`. Each segment is sanitized individually so that
/// the `/` between segments is preserved as a path separator while `/` within
/// a token value is replaced with `_`. The last segment has `.epub` appended
/// if it does not already end with `.epub`.
fn build_target_path(parent: &Path, expanded: &str) -> PathBuf {
    let mut path = parent.to_path_buf();
    let segments: Vec<&str> = expanded.split('/').collect();
    let last = segments.len().saturating_sub(1);
    for (i, seg) in segments.iter().enumerate() {
        let mut sanitized = sanitize(seg.to_string());
        if i == last && !sanitized.ends_with(".epub") {
            sanitized.push_str(".epub");
        }
        path = path.join(sanitized);
    }
    path
}

/// Compute which renames need to happen for `editions` using `pattern`.
///
/// Only editions with `owned == 1` are included. Editions whose computed
/// target path already equals the current `source_path` are skipped (AC-39).
///
/// When `series_pattern` is `Some(p)`, editions that have both `series_name`
/// and `series_position` set use `p` instead of `pattern` (AC-36).
pub fn compute_rename_plan(
    editions: &[EditionRow],
    pattern: &str,
    unknown_fallback: bool,
    series_pattern: Option<&str>,
) -> Vec<RenamePlan> {
    let mut plan = Vec::new();

    for edition in editions {
        if edition.owned != 1 {
            continue;
        }

        // AC-36: use series_pattern when the edition has both series fields.
        let effective_pattern = match series_pattern {
            Some(sp)
                if edition.series_name.is_some() && edition.series_position.is_some() =>
            {
                sp
            }
            _ => pattern,
        };

        let fallback = if unknown_fallback { "Unknown" } else { "" };
        let title = edition.title.as_deref().unwrap_or(fallback);
        let author = edition.authors.as_deref().unwrap_or(fallback);
        let series = edition.series_name.as_deref().unwrap_or("");
        let position = edition.series_position.as_deref().unwrap_or("");
        let year = edition
            .publish_date
            .as_deref()
            .map(extract_year)
            .unwrap_or("");
        let isbn = edition.isbn.as_deref().unwrap_or("");

        let expanded = expand_pattern_raw(effective_pattern, title, author, series, position, year, isbn);

        let old_path = PathBuf::from(&edition.source_path);
        let parent = old_path.parent().unwrap_or_else(|| Path::new("."));
        let new_path = build_target_path(parent, &expanded);

        // Skip if target is the same as source (AC-39).
        if new_path == old_path {
            continue;
        }

        plan.push(RenamePlan {
            edition_id: edition.id,
            old_path,
            new_path,
        });
    }

    plan
}

/// Rename edition files on disk and update `source_path` in the DB.
///
/// Steps:
/// 1. Load owned editions via `db::list_editions` filtered to `owned == 1`.
/// 2. `validate_pattern` — fail before touching any files (AC-42).
/// 3. `compute_rename_plan`.
/// 4. If `dry_run`: print each planned rename and return (AC-43).
/// 5. Execute renames; skip on collision (AC-44); update DB path (AC-45).
///
/// When `series_pattern` is `Some(p)`, editions with both `series_name` and
/// `series_position` set use `p` instead of `pattern` (AC-36).
pub async fn rename_editions(
    pool: &DbPool,
    pattern: &str,
    dry_run: bool,
    unknown_fallback: bool,
    series_pattern: Option<&str>,
) -> anyhow::Result<RenameResult> {
    let all_editions = db::list_editions(pool).await?;
    let owned: Vec<EditionRow> = all_editions
        .into_iter()
        .filter(|e| e.owned == 1)
        .collect();

    // Fail-fast before touching any files (AC-42).
    validate_pattern(pattern)?;
    if let Some(sp) = series_pattern {
        validate_pattern(sp)?;
    }

    let plan = compute_rename_plan(&owned, pattern, unknown_fallback, series_pattern);

    // Editions that are already correctly named count as skipped (AC-39).
    let already_matching = owned.len() - plan.len();

    if dry_run {
        let mut planned = Vec::with_capacity(plan.len());
        for entry in &plan {
            let old_name = entry
                .old_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            let new_name = entry
                .new_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            println!("{old_name} \u{2192} {new_name}");
            planned.push((entry.old_path.clone(), entry.new_path.clone()));
        }
        return Ok(RenameResult {
            renamed: 0,
            skipped: plan.len(),
            planned,
        });
    }

    let mut renamed = 0usize;
    let mut skipped = already_matching;

    for entry in plan {
        // Collision check: target exists and is a different path (AC-44).
        if entry.new_path.exists() && entry.new_path != entry.old_path {
            eprintln!(
                "WARNING: skipping rename — target already exists: {}",
                entry.new_path.display()
            );
            skipped += 1;
            continue;
        }

        // Create intermediate directories for subdirectory patterns (AC-22).
        if let Some(dir) = entry.new_path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        // Rename on disk.
        std::fs::rename(&entry.old_path, &entry.new_path)?;

        // Update DB.
        let new_path_str = entry.new_path.to_str().unwrap_or("");
        db::update_source_path(pool, entry.edition_id, new_path_str).await?;

        renamed += 1;
    }

    Ok(RenameResult { renamed, skipped, planned: vec![] })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_replaces_invalid_chars() {
        assert_eq!(sanitize("A/B:C".to_string()), "A_B_C");
        assert_eq!(sanitize("foo|bar".to_string()), "foo_bar");
    }

    #[test]
    fn test_sanitize_trims_trailing_dots_and_spaces() {
        assert_eq!(sanitize("My Book.".to_string()), "My Book");
        assert_eq!(sanitize("Title   ".to_string()), "Title");
        assert_eq!(sanitize("Book...".to_string()), "Book");
    }

    #[test]
    fn test_sanitize_windows_reserved_nul() {
        assert_eq!(sanitize("NUL".to_string()), "NUL_");
        assert_eq!(sanitize("nul".to_string()), "nul_");
    }

    #[test]
    fn test_sanitize_windows_reserved_com_lpt() {
        assert_eq!(sanitize("COM1".to_string()), "COM1_");
        assert_eq!(sanitize("LPT9".to_string()), "LPT9_");
    }

    #[test]
    fn test_sanitize_non_reserved_not_affected() {
        assert_eq!(sanitize("NULL".to_string()), "NULL");
        assert_eq!(sanitize("COM10".to_string()), "COM10");
        assert_eq!(sanitize("Normal Title".to_string()), "Normal Title");
    }

    #[test]
    fn test_sanitize_truncates_long_string() {
        let long = "A".repeat(150);
        let result = sanitize(long);
        assert_eq!(result.len(), 100);
    }
}
