pub mod author;
pub mod config;
pub mod db;
pub mod download;
pub mod download_sources;
pub mod enrich;
pub mod epub;
pub mod follow;
pub mod fuzzy;
pub mod grab;
pub mod import;
pub mod organize;
pub mod rename;
pub mod scan;
pub mod series;
pub mod sync;
pub mod tag;
pub mod want;

pub use db::DbPool;
pub use download_sources::{DownloadCandidate, DownloadSource};
pub use epub::EpubMeta;

/// Normalize a path to use the OS-native separator before storing in the DB.
///
/// On Windows, `PathBuf::join` appends with `\` but the root may retain `/`
/// when the `PathBuf` was originally constructed from a forward-slash string
/// (e.g. from a TOML config value). This produces mixed paths like
/// `C:/Users/foo/Books\Author\Title.epub` which compare unequal to the
/// all-backslash paths produced by WalkDir, breaking the UNIQUE constraint
/// deduplication and silently inserting phantom duplicates.
pub fn native_path_str(path: &std::path::Path) -> String {
    #[cfg(windows)]
    return path.to_string_lossy().replace('/', "\\");
    #[cfg(not(windows))]
    return path.to_string_lossy().into_owned();
}
