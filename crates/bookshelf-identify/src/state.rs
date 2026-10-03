//! Persistent results, so a long run survives interruption and later runs only look
//! at files that are new or changed. One SQLite file; every book's outcome is saved
//! the moment it is known, and the same table is the review queue.

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Mutex};

/// Bump when identification or matching rules change in a way that could alter an
/// outcome. Stored results from other versions are re-examined, never trusted.
pub const RULES_VERSION: u32 = 1;

/// Just enough of a book's outcome to plan, tag and report it without looking it up again.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StoredRecord {
    pub status: String,
    pub reason: String,
    pub destination: Option<String>,
    pub google_id: Option<String>,
    pub provider: usize,
    pub warnings: Vec<String>,
    pub title: String,
    pub declared_title: String,
    pub authors: Vec<String>,
    pub isbns: Vec<String>,
    pub publisher: Option<String>,
    pub published_date: Option<String>,
    pub series: Option<String>,
    pub series_index: Option<String>,
    pub has_cover: bool,
    pub work: Option<StoredWork>,
    pub edition_status: String,
    pub edition_identifiers: Vec<String>,
    pub edition_publisher: Option<String>,
    pub edition_published_date: Option<String>,
    pub language: Option<String>,
    pub language_declared: Vec<String>,
    pub language_status: String,
    /// Provider cover addresses of the accepted candidates, best first.
    #[serde(default)]
    pub cover_urls: Vec<String>,
    /// A few candidate summaries, for the review queue.
    pub candidates: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StoredWork {
    pub title: String,
    pub authors: Vec<String>,
    pub language: Option<String>,
    pub supporting_ids: Vec<String>,
}

pub struct State {
    conn: Mutex<Connection>,
}

#[derive(Debug, Default)]
pub struct Counts {
    pub total: u64,
    pub by_status: Vec<(String, u64)>,
}

impl State {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).context("Cannot open state database")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                status TEXT NOT NULL,
                rules_version INTEGER NOT NULL,
                checked_at INTEGER NOT NULL,
                record TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS files_status ON files(status);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// A stored outcome, only when the file is unchanged and the rules are current.
    pub fn lookup(&self, path: &str, size: u64, mtime_ns: i128) -> Result<Option<StoredRecord>> {
        let conn = self.conn.lock().expect("state lock");
        let row: Option<(String, i64, i64, u32)> = conn
            .query_row(
                "SELECT record, size, mtime_ns, rules_version FROM files WHERE path = ?1",
                params![path],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        Ok(
            row.and_then(|(record, stored_size, stored_mtime, version)| {
                (version == RULES_VERSION
                    && stored_size as u64 == size
                    && i128::from(stored_mtime) == mtime_ns)
                    .then(|| serde_json::from_str(&record).ok())
                    .flatten()
            }),
        )
    }

    pub fn save(&self, path: &str, size: u64, mtime_ns: i128, record: &StoredRecord) -> Result<()> {
        let json = serde_json::to_string(record)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        self.conn.lock().expect("state lock").execute(
            "INSERT INTO files (path, size, mtime_ns, status, rules_version, checked_at, record)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(path) DO UPDATE SET size = ?2, mtime_ns = ?3, status = ?4,
                rules_version = ?5, checked_at = ?6, record = ?7",
            params![
                path,
                size as i64,
                mtime_ns as i64,
                record.status,
                RULES_VERSION,
                now,
                json
            ],
        )?;
        Ok(())
    }

    pub fn forget(&self, path: &str) -> Result<()> {
        self.conn
            .lock()
            .expect("state lock")
            .execute("DELETE FROM files WHERE path = ?1", params![path])?;
        Ok(())
    }

    pub fn counts(&self) -> Result<Counts> {
        let conn = self.conn.lock().expect("state lock");
        let mut statement =
            conn.prepare("SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY 2 DESC")?;
        let by_status: Vec<(String, u64)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)))?
            .collect::<std::result::Result<_, _>>()?;
        Ok(Counts {
            total: by_status.iter().map(|(_, n)| n).sum(),
            by_status,
        })
    }

    /// Paths and reasons of everything awaiting a human, oldest first.
    pub fn review_queue(&self, limit: usize) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock().expect("state lock");
        let mut statement = conn.prepare(
            "SELECT path, record FROM files WHERE status IN ('review', 'error')
             ORDER BY checked_at LIMIT ?1",
        )?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .map(|(path, record)| {
                let reason = serde_json::from_str::<StoredRecord>(&record)
                    .map(|r| r.reason)
                    .unwrap_or_default();
                (path, reason)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_round_trip_and_are_only_trusted_for_unchanged_files_under_current_rules() {
        let dir = tempfile::tempdir().unwrap();
        let state = State::open(&dir.path().join("state.db")).unwrap();
        let record = StoredRecord {
            status: "planned".into(),
            title: "Dune".into(),
            authors: vec!["Frank Herbert".into()],
            ..Default::default()
        };
        state.save("a.epub", 100, 5, &record).unwrap();
        assert_eq!(
            state.lookup("a.epub", 100, 5).unwrap().unwrap().title,
            "Dune"
        );
        assert!(
            state.lookup("a.epub", 101, 5).unwrap().is_none(),
            "size changed"
        );
        assert!(
            state.lookup("a.epub", 100, 6).unwrap().is_none(),
            "mtime changed"
        );
        assert!(
            state.lookup("b.epub", 100, 5).unwrap().is_none(),
            "unknown file"
        );
        // A result written under other rules is never reused.
        state
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE files SET rules_version = rules_version + 1", [])
            .unwrap();
        assert!(state.lookup("a.epub", 100, 5).unwrap().is_none());
        // Queue and counts reflect saved statuses; saving again replaces, never duplicates.
        let review = StoredRecord {
            status: "review".into(),
            reason: "No match".into(),
            ..Default::default()
        };
        state.save("r.epub", 1, 1, &review).unwrap();
        state.save("r.epub", 1, 1, &review).unwrap();
        assert_eq!(state.counts().unwrap().total, 2);
        assert_eq!(
            state.review_queue(10).unwrap(),
            [("r.epub".into(), "No match".into())]
        );
    }
}
