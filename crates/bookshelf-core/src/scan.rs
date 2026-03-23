use crate::db::{self, DbPool};
use crate::epub;
use crate::fuzzy;
use anyhow::Context;
use std::path::Path;
use walkdir::WalkDir;

/// Summary of a completed scan operation.
#[derive(Debug, Default)]
pub struct ScanResult {
    /// Total `.epub` files examined.
    pub scanned: usize,
    /// New rows inserted (not already in DB).
    pub inserted: usize,
    /// Files skipped because extension is not `.epub`.
    pub skipped_non_epub: usize,
    /// Paths that produced parse or DB errors (warnings were printed).
    pub errors: Vec<String>,
}

/// Scan `dir` recursively for `.epub` files, parse OPF metadata,
/// upsert into the database, and link editions to works.
///
/// Returns `Err` if `dir` does not exist or is not a directory (AC-6, AC-7).
/// Individual file errors produce a warning on stderr and are collected in
/// `ScanResult.errors`; scanning continues (AC-20, AC-21).
pub async fn scan_directory(pool: &DbPool, dir: &Path) -> anyhow::Result<ScanResult> {
    if !dir.exists() {
        return Err(anyhow::anyhow!(
            "path does not exist: {}",
            dir.display()
        ));
    }
    if !dir.is_dir() {
        return Err(anyhow::anyhow!(
            "path is not a directory: {}",
            dir.display()
        ));
    }

    let mut result = ScanResult::default();

    for entry in WalkDir::new(dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path().to_path_buf();
        if !entry.file_type().is_file() {
            continue;
        }

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        if ext != "epub" {
            result.skipped_non_epub += 1;
            continue;
        }

        result.scanned += 1;

        // Parse EPUB metadata.
        let meta = match epub::parse_epub(&path) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("WARNING: skipping {}: {e}", path.display());
                result.errors.push(path.display().to_string());
                continue;
            }
        };

        // Check whether this path is already in the DB before inserting.
        let edition_id = match db::upsert_edition(pool, &meta).await {
            Ok(id) => id,
            Err(e) => {
                eprintln!("WARNING: DB error for {}: {e}", path.display());
                result.errors.push(path.display().to_string());
                continue;
            }
        };

        // Detect whether this was a new insertion or an existing row.
        // We count it as "inserted" when upsert_edition returns successfully
        // but the edition has no work_id yet (i.e., it's genuinely new).
        let existing =
            db::get_edition(pool, edition_id)
                .await
                .context("get_edition after upsert")?;

        if let Some(row) = existing {
            if row.work_id.is_none() {
                // Newly inserted (or existing but never work-linked).
                result.inserted += 1;
                if let Err(e) = link_work(pool, edition_id, &meta).await {
                    eprintln!(
                        "WARNING: work-linking failed for {}: {e}",
                        path.display()
                    );
                }
            }
        }
    }

    Ok(result)
}

/// Link an edition to a `works` row.
///
/// Strategy (AC-44, AC-46, AC-47, AC-48):
/// - If the edition has an ISBN: find or create a `works` row by ISBN.
/// - If no ISBN but title+authors present: fuzzy-compare against all
///   no-ISBN editions that already have a `work_id`; share or create.
/// - If neither title nor authors: skip work linking.
async fn link_work(
    pool: &DbPool,
    edition_id: i64,
    meta: &epub::EpubMeta,
) -> anyhow::Result<()> {
    if let Some(isbn) = meta.isbn.as_deref() {
        // ISBN path.
        let work_id = match db::find_work_by_isbn(pool, isbn).await? {
            Some(wid) => wid,
            None => {
                let title = meta.title.as_deref().unwrap_or("Unknown");
                let authors = meta.authors.as_deref().unwrap_or("Unknown");
                db::insert_work(pool, title, authors).await?
            }
        };
        db::set_edition_work_id(pool, edition_id, work_id).await?;
    } else if let (Some(title), Some(authors)) =
        (meta.title.as_deref(), meta.authors.as_deref())
    {
        // No-ISBN fuzzy path.
        let candidates = db::all_editions_for_dedup(pool).await?;
        let matched_work_id = candidates.iter().find_map(|row| {
            let t = row.title.as_deref().unwrap_or("");
            let a = row.authors.as_deref().unwrap_or("");
            if fuzzy::is_same_work(title, authors, t, a) {
                row.work_id
            } else {
                None
            }
        });

        let work_id = match matched_work_id {
            Some(wid) => wid,
            None => db::insert_work(pool, title, authors).await?,
        };
        db::set_edition_work_id(pool, edition_id, work_id).await?;
    }
    // Else: no title/authors — leave work_id NULL.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epub::tests::make_test_epub;
    use tempfile::TempDir;

    async fn open_temp_db() -> (DbPool, tempfile::NamedTempFile) {
        let tmp = tempfile::NamedTempFile::with_suffix(".db").unwrap();
        let pool = db::open(tmp.path()).await.unwrap();
        (pool, tmp)
    }

    #[tokio::test]
    async fn test_scan_nonexistent_dir_returns_err() {
        let (pool, _tmp) = open_temp_db().await;
        let result = scan_directory(&pool, Path::new("/nonexistent/path/xyz")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scan_file_path_returns_err() {
        let (pool, _tmp) = open_temp_db().await;
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let result = scan_directory(&pool, tmp_file.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scan_skips_non_epub_files() {
        let (pool, _tmp) = open_temp_db().await;
        let tmp_dir = TempDir::new().unwrap();

        // Write a non-epub file
        std::fs::write(tmp_dir.path().join("book.pdf"), b"fake pdf").unwrap();
        std::fs::write(tmp_dir.path().join("cover.jpg"), b"fake jpg").unwrap();

        let result = scan_directory(&pool, tmp_dir.path()).await.unwrap();
        assert_eq!(result.scanned, 0);
        assert_eq!(result.inserted, 0);
    }

    // Category 10: symlink loop detection
    // Symlink creation may require elevated privileges on Windows.
    // We gate the test on Unix only, but also check gracefully on Windows
    // by attempting to create the symlink and skipping on PermissionDenied.
    #[tokio::test]
    async fn test_scan_handles_symlink_gracefully() {
        let (pool, _tmp) = open_temp_db().await;
        let tmp_dir = TempDir::new().unwrap();

        // Create a symlink pointing to the parent directory (potential infinite loop).
        let link_path = tmp_dir.path().join("loop_link");

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(tmp_dir.path(), &link_path).unwrap();
            // scan_directory must return Ok (not hang or panic) because follow_links(false).
            let result = scan_directory(&pool, tmp_dir.path()).await;
            assert!(result.is_ok(), "scan_directory must succeed despite symlink loop");
        }

        #[cfg(windows)]
        {
            // On Windows, symlink creation requires elevated privileges (SeCreateSymbolicLinkPrivilege).
            // Try to create it; if it fails for any privilege/OS reason, skip gracefully.
            match std::os::windows::fs::symlink_dir(tmp_dir.path(), &link_path) {
                Ok(()) => {
                    let result = scan_directory(&pool, tmp_dir.path()).await;
                    assert!(result.is_ok(), "scan_directory must succeed despite symlink loop");
                }
                Err(_) => {
                    // Not running as administrator or Developer Mode not enabled — skip.
                    // Still verify that a normal scan of an empty dir succeeds.
                    let result = scan_directory(&pool, tmp_dir.path()).await;
                    assert!(result.is_ok());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_scan_inserts_epub_and_deduplicates() {
        let (pool, _tmp) = open_temp_db().await;
        let tmp_dir = TempDir::new().unwrap();

        let epub = make_test_epub(
            "The Hobbit",
            &["J.R.R. Tolkien"],
            Some("9780261102217"),
            None,
            None,
            None,
            None,
            Some("en"),
            None,
            None,
        );
        let dest = tmp_dir.path().join("hobbit.epub");
        std::fs::copy(epub.path(), &dest).unwrap();

        // First scan
        let r1 = scan_directory(&pool, tmp_dir.path()).await.unwrap();
        assert_eq!(r1.scanned, 1);
        assert_eq!(r1.inserted, 1);

        // Second scan — must not duplicate
        let r2 = scan_directory(&pool, tmp_dir.path()).await.unwrap();
        assert_eq!(r2.scanned, 1);
        let editions = db::list_editions(&pool).await.unwrap();
        assert_eq!(editions.len(), 1);
    }
}
