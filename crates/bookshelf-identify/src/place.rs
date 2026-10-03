//! Replaces a library file with its verified, tagged copy, keeping the original
//! in a backup folder and recording every swap in a journal so it can be undone.

use crate::{
    epub::{self, Fingerprint},
    rename::move_no_replace,
    tagwrite::{self, Tags},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const BACKUP_DIR: &str = ".book-manager-backup";
const JOURNAL: &str = "journal.jsonl";

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
    time: u64,
    original: PathBuf,
    backup: PathBuf,
    new: PathBuf,
    sha256_before: String,
    sha256_after: String,
    changes: Vec<String>,
}

pub fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Writes a tagged copy of `source` at `destination` (which may equal `source`)
/// and moves the original into the backup folder. On any failure the original
/// is left, or put back, exactly where it was.
pub fn tag_in_place(
    source: &Path,
    destination: &Path,
    expected: &Fingerprint,
    tags: &Tags,
    cover: Option<&[u8]>,
) -> Result<Vec<String>> {
    ensure!(
        epub::fingerprint(source)? == *expected,
        "Source changed since identification; not tagged"
    );
    let directory = source.parent().context("Source has no directory")?;
    ensure!(
        destination.parent() == Some(directory),
        "Tagging must stay in the source directory"
    );
    if source != destination {
        ensure!(!destination.try_exists()?, "Destination already exists");
    }
    let backup_dir = directory.join(BACKUP_DIR);
    fs::create_dir_all(&backup_dir)?;
    let before = sha256(source)?;
    let backup = backup_dir.join(format!("{before}.epub"));
    ensure!(
        !backup.exists(),
        "A backup of this exact file already exists"
    );
    let staged = directory.join(format!(".{before}.tagging"));
    let outcome = tagwrite::write(source, &staged, tags, cover)?;
    let after = match sha256(&staged) {
        Ok(hash) => hash,
        Err(error) => {
            let _ = fs::remove_file(&staged);
            return Err(error);
        }
    };
    if let Err(error) = move_no_replace(source, &backup) {
        let _ = fs::remove_file(&staged);
        return Err(error.context("Cannot back up the original; nothing changed"));
    }
    if let Err(error) = move_no_replace(&staged, destination) {
        let restored = move_no_replace(&backup, source);
        let _ = fs::remove_file(&staged);
        return Err(error.context(match restored {
            Ok(()) => "Cannot place the tagged file; original restored".to_owned(),
            Err(e) => format!(
                "Cannot place the tagged file AND restore failed ({e}); original is at {}",
                backup.display()
            ),
        }));
    }
    let entry = Entry {
        time: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        original: source.to_owned(),
        backup,
        new: destination.to_owned(),
        sha256_before: before,
        sha256_after: after,
        changes: outcome.changes.clone(),
    };
    let mut journal = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(backup_dir.join(JOURNAL))?;
    writeln!(journal, "{}", serde_json::to_string(&entry)?)?;
    journal.sync_all()?;
    Ok(outcome.changes)
}

/// Deletes backups whose tagged file is still byte-for-byte what tagging produced
/// and whose journal entry is at least `min_age_secs` old, then drops those entries
/// from the journal. After this, those books can no longer be undone. Returns the
/// number of backups removed and the bytes freed.
pub fn prune(backup_dir: &Path, min_age_secs: u64) -> Result<(usize, u64)> {
    let journal = backup_dir.join(JOURNAL);
    let text = fs::read_to_string(&journal).context("No journal found")?;
    let entries: Vec<Entry> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .context("Corrupt journal")?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let (mut removed, mut freed) = (0usize, 0u64);
    let mut kept = Vec::new();
    for entry in entries {
        let old_enough = now.saturating_sub(entry.time) >= min_age_secs;
        let verified = old_enough
            && entry.new.exists()
            && sha256(&entry.new).is_ok_and(|hash| hash == entry.sha256_after);
        if verified && entry.backup.exists() {
            let size = fs::metadata(&entry.backup).map_or(0, |m| m.len());
            if fs::remove_file(&entry.backup).is_ok() {
                removed += 1;
                freed += size;
                continue;
            }
        }
        kept.push(entry);
    }
    let rewritten: String = kept
        .iter()
        .map(|e| {
            serde_json::to_string(e).map(|line| {
                line + "
"
            })
        })
        .collect::<std::result::Result<_, _>>()?;
    let staged = backup_dir.join("journal.jsonl.tmp");
    fs::write(&staged, rewritten)?;
    fs::rename(&staged, &journal)?;
    Ok((removed, freed))
}

/// Reverses every journalled swap, newest first. Files edited since tagging are
/// reported and left alone.
pub fn undo(backup_dir: &Path) -> Result<Vec<String>> {
    let text = fs::read_to_string(backup_dir.join(JOURNAL)).context("No journal found")?;
    let mut entries: Vec<Entry> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .context("Corrupt journal")?;
    entries.reverse();
    let mut report = Vec::new();
    for entry in entries {
        if !entry.backup.exists() {
            report.push(format!(
                "skipped (already restored): {}",
                entry.original.display()
            ));
            continue;
        }
        if sha256(&entry.backup)? != entry.sha256_before {
            bail!(
                "Backup {} no longer matches its recorded hash",
                entry.backup.display()
            );
        }
        if entry.new.exists() {
            if sha256(&entry.new)? != entry.sha256_after {
                report.push(format!(
                    "skipped (edited since tagging): {}",
                    entry.new.display()
                ));
                continue;
            }
            let parked = entry
                .backup
                .with_file_name(format!("undone-{}.epub", entry.sha256_after));
            move_no_replace(&entry.new, &parked)?;
        }
        if entry.original.exists() {
            report.push(format!(
                "skipped (original path now occupied): {}",
                entry.original.display()
            ));
            continue;
        }
        move_no_replace(&entry.backup, &entry.original)?;
        report.push(format!("restored: {}", entry.original.display()));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(dir: &Path, name: &str, age: u64, edited: bool) -> Entry {
        let new = dir.join(format!("{name}-new.epub"));
        let backup = dir.join(format!("{name}-backup.epub"));
        fs::write(&new, b"tagged bytes").unwrap();
        fs::write(&backup, b"original bytes").unwrap();
        let sha256_after = sha256(&new).unwrap();
        if edited {
            fs::write(&new, b"edited by the user afterwards").unwrap();
        }
        Entry {
            time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - age,
            original: dir.join(format!("{name}.epub")),
            backup,
            new,
            sha256_before: "x".into(),
            sha256_after,
            changes: vec![],
        }
    }

    #[test]
    fn only_old_entries_whose_tagged_file_is_untouched_lose_their_backup() {
        let dir = tempfile::tempdir().unwrap();
        let day = 86_400;
        let entries = [
            entry(dir.path(), "old-ok", 40 * day, false),
            entry(dir.path(), "recent", day, false),
            entry(dir.path(), "old-edited", 40 * day, true),
        ];
        let lines: String = entries
            .iter()
            .map(|e| {
                serde_json::to_string(e).unwrap()
                    + "
"
            })
            .collect();
        fs::write(dir.path().join(JOURNAL), lines).unwrap();
        let (removed, freed) = prune(dir.path(), 30 * day).unwrap();
        assert_eq!((removed, freed), (1, 14));
        assert!(!entries[0].backup.exists(), "verified and old: removed");
        assert!(entries[1].backup.exists(), "too recent: kept");
        assert!(entries[2].backup.exists(), "edited since tagging: kept");
        let journal = fs::read_to_string(dir.path().join(JOURNAL)).unwrap();
        assert_eq!(
            journal.lines().count(),
            2,
            "pruned entry leaves the journal"
        );
    }
}
