use anyhow::Context;
use std::fs;
use std::path::PathBuf;

/// The ISO 639-2 language codes accepted by `config set language`.
/// Includes both ISO 639-2/B (bibliographic) and ISO 639-2/T (terminological)
/// variants where they differ: "fre"/"fra" for French.
pub const VALID_LANGUAGE_CODES: &[&str] =
    &["eng", "fre", "fra", "ger", "spa", "ita", "por", "dut", "pol", "rus", "chi", "jpn", "kor"];

/// The download source names that can appear in `download_sources`.
pub const VALID_DOWNLOAD_SOURCES: &[&str] =
    &["standard-ebooks", "gutenberg", "arxiv", "internet-archive"];

/// Persistent application configuration stored in `~/.config/bookshelf/config.toml`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AppConfig {
    pub db: Option<std::path::PathBuf>,
    pub library_dir: Option<std::path::PathBuf>,
    /// ISO 639-2 language code, default "eng".
    pub language: String,
    /// Standard Ebooks account email (for authenticated downloads).
    pub se_email: Option<String>,
    /// Standard Ebooks account password (for authenticated downloads).
    pub se_password: Option<String>,
    /// File formats to scan and download. Default: ["epub"].
    #[serde(default = "default_allowed_formats")]
    pub allowed_formats: Vec<String>,
    /// Goodreads CSV export paths to import from during `run`.
    #[serde(default)]
    pub goodreads_csv_paths: Vec<PathBuf>,
    /// Separate directory for downloaded books (defaults to library_dir if not set).
    pub downloads_dir: Option<PathBuf>,
    /// OpenLibrary usernames whose want-to-read lists are imported during `run`.
    #[serde(default)]
    pub ol_usernames: Vec<String>,
    /// How often `bookshelf run` repeats its pipeline, in minutes. Default: 360 (6 hours).
    #[serde(default = "default_run_interval_minutes")]
    pub run_interval_minutes: u64,
    /// Ordered list of download sources to try. Each entry is a source name.
    /// Standard Ebooks is only active when se_email + se_password are both set.
    /// Default: all four sources in priority order.
    #[serde(default = "default_download_sources")]
    pub download_sources: Vec<String>,
    /// HTTP request timeout in seconds for all download source requests. Default: 30.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

fn default_run_interval_minutes() -> u64 {
    360
}

fn default_allowed_formats() -> Vec<String> {
    vec!["epub".to_string()]
}

fn default_download_sources() -> Vec<String> {
    VALID_DOWNLOAD_SOURCES.iter().map(|s| s.to_string()).collect()
}

fn default_request_timeout_secs() -> u64 {
    30
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            db: None,
            library_dir: None,
            language: "eng".to_string(),
            se_email: None,
            se_password: None,
            allowed_formats: default_allowed_formats(),
            goodreads_csv_paths: Vec::new(),
            downloads_dir: None,
            ol_usernames: Vec::new(),
            run_interval_minutes: default_run_interval_minutes(),
            download_sources: default_download_sources(),
            request_timeout_secs: default_request_timeout_secs(),
        }
    }
}

/// Returns the platform-specific path to the config file:
/// `<config_dir>/bookshelf/config.toml`.
pub fn config_file_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("bookshelf")
        .join("config.toml")
}

/// Load the config file. Returns `AppConfig::default()` if the file does not
/// exist. Returns `Err` if the file exists but cannot be parsed.
pub fn load() -> anyhow::Result<AppConfig> {
    load_from_path(&config_file_path())
}

/// Load the config from the given path. Returns `AppConfig::default()` if the
/// file does not exist. Returns `Err` if the file exists but cannot be parsed.
pub(crate) fn load_from_path(path: &std::path::Path) -> anyhow::Result<AppConfig> {
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let cfg: AppConfig = toml::from_str(&contents)
        .with_context(|| format!("failed to parse config file: {}", path.display()))?;
    Ok(cfg)
}

/// Serialize `cfg` to TOML and write it to the config file, creating any
/// missing parent directories.
pub fn save(cfg: &AppConfig) -> anyhow::Result<()> {
    save_to_path(&config_file_path(), cfg)
}

/// Serialize `cfg` to TOML and write it to `path`, creating any missing parent
/// directories.
pub(crate) fn save_to_path(path: &std::path::Path, cfg: &AppConfig) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory: {}", parent.display()))?;
    }
    let contents = toml::to_string(cfg)
        .context("failed to serialize config to TOML")?;
    fs::write(path, contents)
        .with_context(|| format!("failed to write config file: {}", path.display()))?;
    Ok(())
}

/// Load the current config, apply a single key=value update, and save.
///
/// Valid keys: `db`, `library_dir`, `language`.
/// For `language`, the value must be one of the 12 entries in
/// [`VALID_LANGUAGE_CODES`].
pub fn set_key(key: &str, value: &str) -> anyhow::Result<()> {
    set_key_at_path(&config_file_path(), key, value)
}

/// Like `set_key` but reads and writes `path` rather than the default config
/// file path. Used internally by `set_key` and directly by tests.
pub(crate) fn set_key_at_path(path: &std::path::Path, key: &str, value: &str) -> anyhow::Result<()> {
    let mut cfg = load_from_path(path)?;
    match key {
        "db" => {
            cfg.db = Some(PathBuf::from(value));
        }
        "library_dir" => {
            cfg.library_dir = Some(PathBuf::from(value));
        }
        "language" => {
            if !VALID_LANGUAGE_CODES.contains(&value) {
                anyhow::bail!(
                    "invalid language code {:?}: must be one of: {}",
                    value,
                    VALID_LANGUAGE_CODES.join(", ")
                );
            }
            cfg.language = value.to_string();
        }
        "se_email" => {
            cfg.se_email = Some(value.to_string());
        }
        "se_password" => {
            cfg.se_password = Some(value.to_string());
        }
        "allowed_formats" => {
            let formats: Vec<String> = value
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            for fmt in &formats {
                if fmt != "epub" && fmt != "pdf" {
                    anyhow::bail!(
                        "unknown format {:?}: valid formats are epub, pdf",
                        fmt
                    );
                }
            }
            cfg.allowed_formats = formats;
        }
        "downloads_dir" => {
            cfg.downloads_dir = Some(PathBuf::from(value));
        }
        "goodreads_csv_paths" => {
            cfg.goodreads_csv_paths = value
                .split(';')
                .map(|s| PathBuf::from(s.trim()))
                .collect();
        }
        "ol_usernames" => {
            cfg.ol_usernames = value
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }
        "run_interval_minutes" => {
            let minutes: u64 = value.parse().map_err(|_| {
                anyhow::anyhow!("run_interval_minutes must be a positive integer, got {:?}", value)
            })?;
            if minutes == 0 {
                anyhow::bail!("run_interval_minutes must be greater than 0");
            }
            cfg.run_interval_minutes = minutes;
        }
        "download_sources" => {
            let sources: Vec<String> = value
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            for src in &sources {
                if !VALID_DOWNLOAD_SOURCES.contains(&src.as_str()) {
                    anyhow::bail!(
                        "unknown download source {:?}: valid sources are {}",
                        src,
                        VALID_DOWNLOAD_SOURCES.join(", ")
                    );
                }
            }
            cfg.download_sources = sources;
        }
        "request_timeout_secs" => {
            let secs: u64 = value.parse().map_err(|_| {
                anyhow::anyhow!("request_timeout_secs must be a positive integer, got {:?}", value)
            })?;
            if secs == 0 {
                anyhow::bail!("request_timeout_secs must be greater than 0");
            }
            cfg.request_timeout_secs = secs;
        }
        _ => {
            anyhow::bail!(
                "unknown key {:?}: valid keys are db, library_dir, language, se_email, se_password, allowed_formats, downloads_dir, goodreads_csv_paths, ol_usernames, run_interval_minutes, download_sources, request_timeout_secs",
                key
            );
        }
    }
    save_to_path(path, &cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_language_is_eng() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.language, "eng");
        assert!(cfg.db.is_none());
        assert!(cfg.library_dir.is_none());
    }

    #[test]
    fn test_valid_language_codes_count() {
        assert_eq!(VALID_LANGUAGE_CODES.len(), 13);
    }

    #[test]
    fn test_valid_language_codes_include_eng() {
        assert!(VALID_LANGUAGE_CODES.contains(&"eng"));
        assert!(VALID_LANGUAGE_CODES.contains(&"jpn"));
        assert!(!VALID_LANGUAGE_CODES.contains(&"en"));
        assert!(!VALID_LANGUAGE_CODES.contains(&"ENG"));
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent").join("config.toml");
        let cfg = load_from_path(&path).unwrap();
        assert_eq!(cfg.language, "eng");
        assert!(cfg.db.is_none());
        assert!(cfg.library_dir.is_none());
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = AppConfig {
            db: Some(PathBuf::from("/tmp/books.db")),
            library_dir: Some(PathBuf::from("/tmp/books")),
            language: "fre".to_string(),
            se_email: None,
            se_password: None,
            ..Default::default()
        };
        save_to_path(&path, &cfg).unwrap();
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.language, "fre");
        assert_eq!(loaded.db, Some(PathBuf::from("/tmp/books.db")));
        assert_eq!(loaded.library_dir, Some(PathBuf::from("/tmp/books")));
    }

    #[test]
    fn test_save_to_path_creates_parent_directory() {
        let dir = TempDir::new().unwrap();
        // nested subdirectory that does not yet exist
        let path = dir.path().join("subdir").join("config.toml");
        let cfg = AppConfig::default();
        save_to_path(&path, &cfg).unwrap();
        assert!(path.exists(), "save_to_path must create missing parent directories");
    }

    #[test]
    fn test_set_key_language_valid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        // Call the real set_key_at_path so the load → validate → save chain
        // is exercised end-to-end against a temp file.
        set_key_at_path(&path, "language", "fre").unwrap();
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.language, "fre");
    }

    #[test]
    fn test_set_key_language_invalid() {
        // "xyz" is not in VALID_LANGUAGE_CODES — set_key must return Err.
        // set_key calls load() internally (returns default when no config
        // file exists) then validates the code before any write, so this is
        // safe to call against the real function.
        let result = set_key("language", "xyz");
        assert!(result.is_err(), "set_key must reject unknown language codes");
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("invalid language code"), "error must mention 'invalid language code'");
    }

    #[test]
    fn test_set_key_unknown_key() {
        // set_key with an unrecognised key must return Err.
        // The unknown-key branch is reached before any write, so calling the
        // real function is safe even in a test environment.
        let result = set_key("badkey", "somevalue");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("unknown key"), "error message must mention 'unknown key'");
    }

    #[test]
    fn test_set_key_db_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        set_key_at_path(&path, "db", "/some/path/to/db.sqlite3").unwrap();
        let cfg = load_from_path(&path).unwrap();
        assert_eq!(cfg.db, Some(PathBuf::from("/some/path/to/db.sqlite3")));
    }

    #[test]
    fn test_set_key_library_dir() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        set_key_at_path(&path, "library_dir", "/some/library/dir").unwrap();
        let cfg = load_from_path(&path).unwrap();
        assert_eq!(cfg.library_dir, Some(PathBuf::from("/some/library/dir")));
    }

    #[test]
    fn test_default_download_sources_contains_all_four() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.download_sources, vec![
            "standard-ebooks", "gutenberg", "arxiv", "internet-archive"
        ]);
    }

    #[test]
    fn test_set_key_download_sources_valid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        set_key_at_path(&path, "download_sources", "gutenberg, arxiv").unwrap();
        let cfg = load_from_path(&path).unwrap();
        assert_eq!(cfg.download_sources, vec!["gutenberg", "arxiv"]);
    }

    #[test]
    fn test_set_key_download_sources_invalid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let result = set_key_at_path(&path, "download_sources", "gutenberg, z-library");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("unknown download source"), "error must mention 'unknown download source'");
    }

    #[test]
    fn test_set_key_request_timeout_secs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        set_key_at_path(&path, "request_timeout_secs", "60").unwrap();
        let cfg = load_from_path(&path).unwrap();
        assert_eq!(cfg.request_timeout_secs, 60);
    }

    #[test]
    fn test_set_key_request_timeout_secs_zero_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let result = set_key_at_path(&path, "request_timeout_secs", "0");
        assert!(result.is_err());
    }
}
