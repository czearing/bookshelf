use crate::db::{self, DbPool};
use crate::epub;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Summary of a completed import operation.
#[derive(Debug, Default)]
pub struct ImportResult {
    /// Number of EPUB files successfully imported (copied and registered in DB).
    pub imported: usize,
    /// Number of EPUB files skipped (already registered or failed to parse).
    pub skipped: usize,
    /// Warning messages for files that could not be imported.
    pub errors: Vec<String>,
}

/// Resolve the default library directory: `<data_dir>/bookshelf/library`.
///
/// Falls back to `./bookshelf/library` if `dirs::data_dir()` returns `None`.
pub fn default_library_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("bookshelf")
        .join("library")
}

/// Import EPUB files from `source` (a single file or directory) into `library_dir`.
///
/// - If `source` does not exist, returns `Err` immediately (AC-11).
/// - Creates `library_dir` if it does not exist (AC-9).
/// - Collects only `.epub` files; non-EPUB files are silently ignored (AC-10).
/// - For each EPUB: copies to `library_dir`, parses metadata, upserts into DB.
/// - Files already registered (by destination path) are skipped (AC-2, AC-4).
/// - If `do_move` is `true`, removes the source file after a successful import (AC-5).
pub async fn import_path(
    pool: &DbPool,
    source: &Path,
    library_dir: &Path,
    do_move: bool,
) -> anyhow::Result<ImportResult> {
    // AC-11: validate source exists.
    if !source.exists() {
        return Err(anyhow::anyhow!(
            "path does not exist: {}",
            source.display()
        ));
    }

    // AC-9: create library directory if absent.
    std::fs::create_dir_all(library_dir).map_err(|e| {
        anyhow::anyhow!(
            "cannot create library directory {}: {e}",
            library_dir.display()
        )
    })?;

    // Collect EPUB files to process.
    let epub_files: Vec<PathBuf> = if source.is_file() {
        vec![source.to_path_buf()]
    } else {
        WalkDir::new(source)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("epub"))
                    .unwrap_or(false)
            })
            .map(|e| e.path().to_path_buf())
            .collect()
    };

    let mut result = ImportResult::default();

    for source_path in &epub_files {
        // Compute destination path.
        let filename = match source_path.file_name() {
            Some(n) => n,
            None => {
                let msg = format!("cannot determine filename for {}", source_path.display());
                eprintln!("WARNING: {msg}");
                result.errors.push(msg);
                result.skipped += 1;
                continue;
            }
        };
        let dest = library_dir.join(filename);

        // AC-2/4: skip if already registered by destination path.
        // Do not canonicalize — parse_epub stores paths without canonicalization
        // so the lookup key must match exactly what was stored.
        let dest_key = dest.to_string_lossy().to_string();
        match db::get_edition_by_source_path(pool, &dest_key).await {
            Ok(Some(_)) => {
                result.skipped += 1;
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                let msg = format!(
                    "DB lookup error for {}: {e}",
                    dest.display()
                );
                eprintln!("WARNING: {msg}");
                result.errors.push(msg);
                result.skipped += 1;
                continue;
            }
        }

        // Copy source file to library directory.
        if let Err(e) = std::fs::copy(source_path, &dest) {
            let msg = format!(
                "failed to copy {} to {}: {e}",
                source_path.display(),
                dest.display()
            );
            eprintln!("WARNING: {msg}");
            result.errors.push(msg);
            result.skipped += 1;
            continue;
        }

        // AC-12: parse EPUB metadata; on failure remove the copied file and skip.
        let meta = match epub::parse_epub(&dest) {
            Ok(m) => m,
            Err(e) => {
                let msg = format!("invalid EPUB {}: {e}", dest.display());
                eprintln!("WARNING: {msg}");
                result.errors.push(msg);
                let _ = std::fs::remove_file(&dest);
                result.skipped += 1;
                continue;
            }
        };

        // AC-13, AC-14: register edition in DB (owned=1 by default via schema).
        if let Err(e) = db::upsert_edition(pool, &meta).await {
            let msg = format!("DB error for {}: {e}", dest.display());
            eprintln!("WARNING: {msg}");
            result.errors.push(msg);
            let _ = std::fs::remove_file(&dest);
            result.skipped += 1;
            continue;
        }

        // AC-5: remove source file if requested.
        if do_move && source_path != &dest {
            if let Err(e) = std::fs::remove_file(source_path) {
                eprintln!(
                    "WARNING: could not remove source file {}: {e}",
                    source_path.display()
                );
            }
        }

        result.imported += 1;
    }

    Ok(result)
}
