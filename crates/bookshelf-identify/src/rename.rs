use crate::epub::{self, Fingerprint};
use anyhow::{Context, Result, ensure};
use std::path::{Path, PathBuf};

pub fn destination(source: &Path, title: &str, authors: &[String]) -> Result<PathBuf> {
    let author = clean(&authors.join(", "));
    let title = clean(title);
    ensure!(
        !author.is_empty() && !title.is_empty(),
        "Cannot form a safe filename"
    );
    let author = truncate(&author, 65);
    let title = truncate(&title, 180 - author.encode_utf16().count() - 3);
    Ok(source.with_file_name(format!("{author} - {title}.epub")))
}

fn clean(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_control() || r#"<>:"/\|?*"#.contains(c) {
                ' '
            } else {
                c
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches([' ', '.'])
        .to_owned()
}

fn truncate(value: &str, max: usize) -> String {
    let mut units = 0;
    value
        .chars()
        .take_while(|c| {
            units += c.len_utf16();
            units <= max
        })
        .collect::<String>()
        .trim_end_matches([' ', '.'])
        .to_owned()
}

pub fn apply(source: &Path, destination: &Path, expected: &Fingerprint) -> Result<()> {
    ensure!(
        epub::fingerprint(source)? == *expected,
        "Source changed since identification; not renamed"
    );
    ensure!(
        source.parent() == destination.parent(),
        "Renaming must stay in the source directory"
    );
    if source == destination {
        return Ok(());
    }
    move_no_replace(source, destination).context("Rename failed; destination is never overwritten")
}

#[cfg(windows)]
pub(crate) fn move_no_replace(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
    let source: Vec<_> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<_> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // Both paths are NUL-terminated; zero flags prohibit replacing an existing destination.
    if unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn move_no_replace(source: &Path, destination: &Path) -> Result<()> {
    std::fs::hard_link(source, destination)?;
    std::fs::remove_file(source).context(
        "Destination link created but source removal failed; both paths remain, remove one after review",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn refuses_overwrite_and_changed_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("before.epub");
        let destination = dir.path().join("after.epub");
        fs::write(&source, b"original").unwrap();
        let fingerprint = epub::fingerprint(&source).unwrap();
        fs::write(&destination, b"untouched").unwrap();
        assert!(apply(&source, &destination, &fingerprint).is_err());
        assert_eq!(fs::read(&source).unwrap(), b"original");
        assert_eq!(fs::read(&destination).unwrap(), b"untouched");
        fs::remove_file(&destination).unwrap();
        fs::write(&source, b"changed length").unwrap();
        assert!(apply(&source, &destination, &fingerprint).is_err());
        assert!(source.exists());
    }

    #[test]
    fn rename_preserves_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("before.epub");
        let destination = dir.path().join("after.epub");
        fs::write(&source, b"identical contents").unwrap();
        let fingerprint = epub::fingerprint(&source).unwrap();
        apply(&source, &destination, &fingerprint).unwrap();
        assert!(!source.exists());
        assert_eq!(fs::read(&destination).unwrap(), b"identical contents");
    }

    #[test]
    fn filenames_are_portable_and_unicode_safe() {
        assert_eq!(clean("A/B: C?* \"D\"."), "A B C D");
        assert_eq!(truncate("a😀b", 3), "a😀");
    }
}
