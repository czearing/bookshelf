use crate::{
    epub, google, matching, place,
    provider::{OpenLibrary, Provider},
    rename,
    state::{State, StoredRecord, StoredWork},
    tagwrite::Tags,
};

use anyhow::{Context, Result, bail, ensure};
use futures::{StreamExt, stream};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Debug, clap::Parser)]
#[command(
    name = "identify",
    version,
    about = "Identify EPUBs with Google Books and safely rename them. Preview by default."
)]
pub struct Args {
    /// EPUB files or directories. Directories are scanned recursively without following symlinks.
    #[arg(required_unless_present_any = ["undo", "status", "queue", "approve", "skip", "prune_backups"])]
    paths: Vec<PathBuf>,
    /// Keep every result in this SQLite file: an interrupted run resumes where it
    /// stopped and later runs only look at new or changed files.
    #[arg(long, value_name = "FILE")]
    state: Option<PathBuf>,
    /// With --state, look up books again even if they were already sent to review.
    #[arg(long, requires = "state")]
    recheck: bool,
    /// Print how many books are in each status in the --state file, then exit.
    #[arg(long, requires = "state")]
    status: bool,
    /// Confirm the identity of one reviewed book (with --title and --author). The
    /// decision is remembered; a later --apply renames and tags it.
    #[arg(long, requires_all = ["state", "title", "author"], value_name = "FILE")]
    approve: Option<PathBuf>,
    /// Title for --approve.
    #[arg(long, requires = "approve")]
    title: Option<String>,
    /// Author for --approve (repeat for several).
    #[arg(long, requires = "approve")]
    author: Vec<String>,
    /// Take one book out of the review queue without changing it.
    #[arg(long, requires = "state", value_name = "FILE")]
    skip: Option<PathBuf>,
    /// Print the first N books awaiting review from the --state file, then exit.
    #[arg(long, requires = "state", value_name = "N")]
    queue: Option<usize>,
    /// Actually rename uniquely matched books in their existing directories.
    #[arg(long)]
    apply: bool,
    /// With --apply, also write the identified title, authors and (for a supported
    /// edition) ISBN, publisher and date into each EPUB, and add a cover when the
    /// book has none. Originals are kept in .book-manager-backup next to the book.
    #[arg(long, requires = "apply")]
    tag: bool,
    /// Delete backups older than --older-than-days whose tagged file is untouched
    /// since tagging (verified by hash). Those books can then no longer be undone.
    #[arg(long, value_name = "BACKUP_DIR", conflicts_with_all = ["apply", "tag", "undo"])]
    prune_backups: Option<PathBuf>,
    /// Minimum age for --prune-backups.
    #[arg(long, default_value_t = 30, requires = "prune_backups")]
    older_than_days: u64,
    /// Reverse every tagging recorded in a .book-manager-backup folder, newest first.
    #[arg(long, value_name = "BACKUP_DIR", conflicts_with_all = ["apply", "tag"])]
    undo: Option<PathBuf>,
    /// Emit a JSON report instead of human-readable output.
    #[arg(long)]
    json: bool,
    /// Bibliographic provider. OpenLibrary needs no key or quota.
    #[arg(long, value_enum, default_value_t = Source::Google)]
    provider: Source,
    /// Send `isbn:` queries to Google (off by default: they currently return nothing).
    #[arg(long)]
    google_isbn_search: bool,
    /// Never name a book from its own tags and title page when no provider has it.
    #[arg(long)]
    require_provider_match: bool,
    /// Fetch a separate record per Google candidate (default: reuse the search result).
    #[arg(long)]
    google_details: bool,
    /// Width in pixels to request covers at (Google returns the original if smaller).
    #[arg(long, default_value_t = 1200, value_parser = clap::value_parser!(u32).range(200..=2500))]
    cover_width: u32,
    /// Narrowest cover accepted. A book with no good cover gets none rather than a poor one.
    #[arg(long, default_value_t = 500)]
    min_cover_width: u32,
    /// Also replace an existing cover narrower than --min-cover-width, but only when the
    /// provider's image is provably the same artwork at a higher resolution.
    #[arg(long)]
    upgrade_covers: bool,
    /// Maximum books processed concurrently.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=64))]
    jobs: u16,
    /// Maximum Google Books request starts per second, shared by all workers.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..=20))]
    requests_per_second: u32,
    /// Cache location (default: OS cache directory/book-manager/google-books-v1).
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// Replace cached results with fresh Google Books responses.
    #[arg(long, conflicts_with = "offline")]
    refresh: bool,
    /// Use only unexpired cached results; never contact Google.
    #[arg(long)]
    offline: bool,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum Source {
    Google,
    Openlibrary,
    /// OpenLibrary first; Google only for books OpenLibrary could not settle.
    Both,
    /// Google first (metadata and covers); OpenLibrary, which has no quota, only for
    /// books Google could not settle.
    GoogleFirst,
}

#[derive(Debug, Serialize)]
struct Record {
    source: PathBuf,
    destination: Option<PathBuf>,
    status: &'static str,
    reason: String,
    google_id: Option<String>,
    metadata: Option<epub::Metadata>,
    candidates: Vec<matching::Candidate>,
    work: Option<matching::WorkIdentity>,
    edition: Option<matching::Edition>,
    language: Option<crate::language::Assessment>,
    warnings: Vec<String>,
    #[serde(skip)]
    fingerprint: Option<epub::Fingerprint>,
    /// Index of the provider whose lookup settled this book (for cover download).
    #[serde(skip)]
    provider: usize,
}

impl Record {
    fn error(source: PathBuf, error: impl std::fmt::Display) -> Self {
        Self {
            source,
            destination: None,
            status: "error",
            reason: error.to_string(),
            google_id: None,
            metadata: None,
            candidates: Vec::new(),
            work: None,
            edition: None,
            language: None,
            warnings: Vec::new(),
            fingerprint: None,
            provider: 0,
        }
    }
}

#[derive(Serialize)]
struct Report {
    mode: &'static str,
    total: usize,
    planned: usize,
    renamed: usize,
    tagged: usize,
    unchanged: usize,
    duplicates: usize,
    review: usize,
    errors: usize,
    /// Live provider requests made in this run (cache hits excluded).
    requests: u64,
    records: Vec<Record>,
}

pub async fn run(args: Args) -> Result<i32> {
    if let Some(dir) = &args.prune_backups {
        let (removed, freed) = place::prune(dir, args.older_than_days * 86_400)?;
        println!(
            "removed {removed} backups, freed {:.1} MB",
            freed as f64 / 1e6
        );
        return Ok(0);
    }
    if let Some(dir) = &args.undo {
        for line in place::undo(dir)? {
            println!("{line}");
        }
        return Ok(0);
    }
    let state = args
        .state
        .as_deref()
        .map(State::open)
        .transpose()?
        .map(std::sync::Arc::new);
    if let Some(state) = &state {
        if args.status {
            let counts = state.counts()?;
            println!("{} books recorded", counts.total);
            for (status, count) in counts.by_status {
                println!("  {status:<10} {count}");
            }
            return Ok(0);
        }
        if let Some(path) = &args.skip {
            let (path, fingerprint, mut stored) = stored_for(state, path)?;
            stored.status = "skipped".into();
            stored.reason = "Skipped by the user".into();
            state.save(&path, fingerprint.len, nanos(&fingerprint), &stored)?;
            println!("skipped: {path}");
            return Ok(0);
        }
        if let Some(path) = &args.approve {
            let (path, fingerprint, mut stored) = stored_for(state, path)?;
            let title = args.title.clone().unwrap_or_default();
            ensure!(
                !title.trim().is_empty() && args.author.iter().any(|a| !a.trim().is_empty()),
                "--approve needs a --title and at least one --author"
            );
            let destination = rename::destination(Path::new(&path), &title, &args.author)?;
            stored.status = if destination == Path::new(&path) {
                "unchanged"
            } else {
                "planned"
            }
            .into();
            stored.reason = "Confirmed by the user".into();
            stored.destination = Some(destination.to_string_lossy().into_owned());
            stored.work = Some(StoredWork {
                title: title.trim().to_owned(),
                authors: args.author.iter().map(|a| a.trim().to_owned()).collect(),
                language: stored.language.clone(),
                supporting_ids: Vec::new(),
            });
            state.save(&path, fingerprint.len, nanos(&fingerprint), &stored)?;
            println!("approved: {path} -> {}", destination.display());
            return Ok(0);
        }
        if let Some(limit) = args.queue {
            for (path, reason) in state.review_queue(limit)? {
                println!(
                    "{path}
  {reason}"
                );
            }
            return Ok(0);
        }
    }
    let base = match args.cache_dir {
        Some(path) => path,
        None => dirs::cache_dir()
            .context("Cannot locate OS cache directory; supply --cache-dir")?
            .join("book-manager"),
    };
    // With a single provider an explicit --cache-dir is used as given (historic
    // behaviour); the chain keeps one subfolder per provider.
    let single = !matches!(args.provider, Source::Both | Source::GoogleFirst);
    let folder = |name: &str, default: &str| {
        if single && base.ends_with("book-manager") {
            base.join(default)
        } else if single {
            base.clone()
        } else {
            base.join(name)
        }
    };
    let google_cache = folder("google", "google-books-v1");
    let ol_cache = folder("openlibrary", "openlibrary-v1");
    let make_google = || -> Result<Provider> {
        Ok(Provider::Google(
            google::Google::new(
                google_cache.clone(),
                args.offline,
                args.refresh,
                args.requests_per_second,
            )?
            .with_isbn_search(args.google_isbn_search)
            .with_detail_requests(args.google_details)
            .with_cover_width(args.cover_width),
        ))
    };
    let make_ol = || -> Result<Provider> {
        Ok(Provider::OpenLibrary(OpenLibrary::new(
            ol_cache.clone(),
            args.offline,
            args.refresh,
            args.requests_per_second,
        )?))
    };
    let providers: Vec<Provider> = match args.provider {
        Source::Google => vec![make_google()?],
        Source::Openlibrary => vec![make_ol()?],
        Source::Both => vec![make_ol()?, make_google()?],
        Source::GoogleFirst => vec![make_google()?, make_ol()?],
    };
    let paths = args.paths;
    let (files, mut records) = tokio::task::spawn_blocking(move || discover(paths)).await?;
    ensure!(
        !files.is_empty() || !records.is_empty(),
        "No EPUB files found"
    );
    let processed = stream::iter(files.into_iter().map(|path| {
        let providers = providers.clone();
        let state = state.clone();
        let recheck = args.recheck;
        let keep_evidence = args.json;
        let allow_self_consistent = !args.require_provider_match;
        async move {
            match process(
                path.clone(),
                &providers,
                state,
                recheck,
                keep_evidence,
                allow_self_consistent,
            )
            .await
            {
                Ok(record) => record,
                Err(error) => Record::error(path, format!("{error:#}")),
            }
        }
    }))
    .buffer_unordered(usize::from(args.jobs))
    .collect::<Vec<_>>()
    .await;
    records.extend(processed);
    records.sort_by(|a, b| a.source.cmp(&b.source));
    preflight(&mut records);
    let covers = CoverOptions {
        min_width: args.min_cover_width,
        upgrade: args.upgrade_covers,
    };
    let mut jobs: HashMap<PathBuf, TagJob> = HashMap::new();
    if args.apply && args.tag {
        for record in records
            .iter_mut()
            .filter(|r| matches!(r.status, "planned" | "unchanged"))
        {
            let settled = record.provider.min(providers.len() - 1);
            let provider = &providers[settled];
            let others: Vec<Provider> = providers
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != settled)
                .map(|(_, p)| p.clone())
                .collect();
            match tag_job(record, provider, &others, &covers).await {
                Ok(Some(job)) => {
                    record.warnings.extend(job.notes.iter().cloned());
                    jobs.insert(record.source.clone(), job);
                }
                Ok(None) => {}
                Err(error) => record.warnings.push(format!("Tags not written: {error:#}")),
            }
        }
    }
    if args.apply {
        let state = state.clone();
        records = tokio::task::spawn_blocking(move || {
            for record in &mut records {
                if !matches!(record.status, "planned" | "unchanged") {
                    continue;
                }
                let fingerprint = record.fingerprint.as_ref().expect("fingerprint");
                let destination = record.destination.clone().expect("destination");
                let renaming = record.status == "planned";
                let job = jobs.remove(&record.source);
                let cover_added = job.as_ref().is_some_and(|j| j.cover.is_some());
                let result = match job {
                    Some(job) => place::tag_in_place(
                        &record.source,
                        &destination,
                        fingerprint,
                        &job.tags,
                        job.cover.as_deref(),
                    )
                    .map(|changes| {
                        record
                            .warnings
                            .push(format!("Tagged: {}", changes.join("; ")));
                        true
                    }),
                    None if renaming => {
                        rename::apply(&record.source, &destination, fingerprint).map(|()| false)
                    }
                    None => continue,
                };
                match result {
                    Ok(tagged) => {
                        record.status = match (renaming, tagged) {
                            (true, _) => "renamed",
                            (false, true) => "tagged",
                            (false, false) => "unchanged",
                        };
                        // The file now lives at its final name with its final content:
                        // remember it there as settled, and forget the old path.
                        if let Some(state) = &state {
                            let mut stored = record_to_stored(record);
                            stored.status = "unchanged".into();
                            stored.destination = Some(destination.to_string_lossy().into_owned());
                            if tagged && let Some(work) = &record.work {
                                stored.title = work.title.clone();
                                stored.declared_title = work.title.clone();
                                stored.authors = work.authors.clone();
                                stored.has_cover |= cover_added;
                            }
                            if let Ok(fresh) = epub::fingerprint(&destination) {
                                let _ = state.save(
                                    &destination.to_string_lossy(),
                                    fresh.len,
                                    nanos(&fresh),
                                    &stored,
                                );
                            }
                            if destination != record.source {
                                let _ = state.forget(&record.source.to_string_lossy());
                            }
                        }
                    }
                    Err(error) => {
                        record.status = "error";
                        record.reason = format!("{error:#}");
                    }
                }
            }
            records
        })
        .await?;
    }
    let count = |status| records.iter().filter(|r| r.status == status).count();
    let report = Report {
        mode: if args.apply { "apply" } else { "preview" },
        total: records.len(),
        planned: count("planned"),
        renamed: count("renamed"),
        tagged: count("tagged"),
        unchanged: count("unchanged"),
        duplicates: count("duplicate"),
        review: count("review"),
        errors: count("error"),
        requests: crate::provider::REQUESTS.load(std::sync::atomic::Ordering::Relaxed),
        records,
    };
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    if args.json {
        serde_json::to_writer_pretty(&mut stdout, &report)?;
        writeln!(stdout)?;
    } else {
        for record in &report.records {
            write!(stdout, "[{}] {}", record.status, record.source.display())?;
            if let Some(destination) = &record.destination {
                write!(
                    stdout,
                    " -> {}",
                    destination
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                )?;
            }
            writeln!(stdout, "\n  {}", record.reason)?;
            if let Some(language) = &record.language {
                writeln!(
                    stdout,
                    "  Language: {} ({})",
                    language.language.as_deref().unwrap_or("unknown"),
                    language.status
                )?;
            }
            for warning in &record.warnings {
                writeln!(stdout, "  Warning: {warning}")?;
            }
            if record.status == "review" {
                for candidate in record.candidates.iter().take(5) {
                    writeln!(
                        stdout,
                        "  Google {}: {}",
                        serde_json::to_string(&candidate.google_id)?,
                        candidate.evidence
                    )?;
                }
            }
        }
        writeln!(
            stdout,
            "\n{}: {} total, {} planned, {} renamed, {} unchanged, {} review, {} errors",
            report.mode,
            report.total,
            report.planned,
            report.renamed,
            report.unchanged,
            report.review,
            report.errors
        )?;
        if !args.apply && report.planned > 0 {
            writeln!(stdout, "Preview only. Re-run with --apply to rename.")?;
        }
    }
    stdout.flush()?;
    Ok(if report.errors > 0 || report.review > 0 {
        1
    } else {
        0
    })
}

fn discover(paths: Vec<PathBuf>) -> (Vec<PathBuf>, Vec<Record>) {
    let mut files = HashSet::new();
    let mut errors = Vec::new();
    for path in paths {
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                errors.push(Record::error(path, error));
                continue;
            }
        };
        if metadata.file_type().is_symlink() {
            errors.push(Record::error(path, "Symbolic-link inputs are not followed"));
            continue;
        }
        if metadata.is_file() && !is_epub(&path) {
            errors.push(Record::error(path, "Only native EPUB files are supported"));
            continue;
        }
        let walker = WalkDir::new(&path)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| entry.file_name() != place::BACKUP_DIR);
        for entry in walker {
            match entry {
                Ok(entry) if entry.file_type().is_file() && is_epub(entry.path()) => {
                    match entry.path().canonicalize() {
                        Ok(path) => {
                            files.insert(path);
                        }
                        Err(error) => errors.push(Record::error(entry.path().to_owned(), error)),
                    }
                }
                Ok(_) => {}
                Err(error) => errors.push(Record::error(
                    error.path().unwrap_or(&path).to_owned(),
                    &error,
                )),
            }
        }
    }
    (files.into_iter().collect(), errors)
}

fn is_epub(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("epub"))
}

async fn process(
    path: PathBuf,
    providers: &[Provider],
    state: Option<std::sync::Arc<State>>,
    recheck: bool,
    keep_evidence: bool,
    allow_self_consistent: bool,
) -> Result<Record> {
    let stat_path = path.clone();
    let before = tokio::task::spawn_blocking(move || epub::fingerprint(&stat_path)).await??;
    if let Some(state) = &state
        && let Some(stored) = state.lookup(&path.to_string_lossy(), before.len, nanos(&before))?
        && (matches!(stored.status.as_str(), "planned" | "unchanged") || !recheck)
    {
        return Ok(record_from_stored(path, stored, before));
    }
    let read_path = path.clone();
    let (metadata, fingerprint) =
        tokio::task::spawn_blocking(move || epub::read(&read_path)).await??;
    // Providers are tried in order and only for a book the earlier ones could not
    // settle. A provider that fails (daily quota, outage, rate block) is noted and the
    // next one is tried; the book errors only when every provider fails.
    let mut identification: Option<matching::Identification> = None;
    let mut provider = 0;
    let mut failures: Vec<String> = Vec::new();
    for (index, next) in providers.iter().enumerate() {
        if identification.as_ref().is_some_and(|i| i.work.is_some()) {
            break;
        }
        match matching::identify_with(next, providers, &metadata).await {
            Ok(found) => {
                if identification.is_none() || found.work.is_some() {
                    identification = Some(found);
                    provider = index;
                }
            }
            Err(error) => failures.push(format!("{} lookup failed: {error:#}", next.name())),
        }
    }
    let mut identification = match identification {
        Some(found) => found,
        None => bail!("{}", failures.join("; ")),
    };
    // Every provider answered and none has this book: the file's own consistent
    // tags and title page may name it. A provider that merely failed never counts.
    if identification.work.is_none()
        && failures.is_empty()
        && allow_self_consistent
        && let Some(named) = matching::self_consistent(&metadata, &identification.candidates)
    {
        identification = named;
    }
    identification.warnings.extend(failures);
    let destination = identification
        .work
        .as_ref()
        .map(|work| rename::destination(&path, &work.title, &work.authors))
        .transpose()?;
    let status = match &destination {
        Some(destination) if destination == &path => "unchanged",
        Some(_) => "planned",
        None => "review",
    };
    let record = Record {
        source: path,
        destination,
        status,
        reason: identification.reason,
        google_id: identification.edition.google_id.clone(),
        metadata: Some(metadata),
        candidates: identification.candidates,
        work: identification.work,
        edition: Some(identification.edition),
        language: Some(identification.language),
        warnings: identification.warnings,
        fingerprint: Some(fingerprint),
        provider,
    };
    let mut record = record;
    if let Some(state) = &state
        && let Some(fingerprint) = &record.fingerprint
    {
        // Saved the moment it is known, so an interrupted run loses nothing.
        state.save(
            &record.source.to_string_lossy(),
            fingerprint.len,
            nanos(fingerprint),
            &record_to_stored(&record),
        )?;
        // Everything needed later is in the state file; do not hold a whole large
        // library's evidence in memory.
        if !keep_evidence {
            if let Some(metadata) = &mut record.metadata {
                metadata.content.documents = Vec::new();
                metadata.content.language.samples = Vec::new();
            }
            record.candidates.truncate(5);
        }
    }
    Ok(record)
}

fn nanos(fingerprint: &epub::Fingerprint) -> i128 {
    fingerprint
        .modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i128)
}

fn record_to_stored(record: &Record) -> StoredRecord {
    let metadata = record.metadata.as_ref();
    let edition = record.edition.as_ref();
    let language = record.language.as_ref();
    StoredRecord {
        status: record.status.to_owned(),
        reason: record.reason.clone(),
        destination: record
            .destination
            .as_ref()
            .map(|d| d.to_string_lossy().into_owned()),
        google_id: record.google_id.clone(),
        provider: record.provider,
        warnings: record.warnings.clone(),
        title: metadata.map(|m| m.title.clone()).unwrap_or_default(),
        declared_title: metadata
            .map(|m| m.declared_title.clone())
            .unwrap_or_default(),
        authors: metadata.map(|m| m.authors.clone()).unwrap_or_default(),
        isbns: metadata.map(|m| m.isbns.clone()).unwrap_or_default(),
        publisher: metadata.and_then(|m| m.publisher.clone()),
        published_date: metadata.and_then(|m| m.published_date.clone()),
        series: metadata.and_then(|m| m.series.clone()),
        series_index: metadata.and_then(|m| m.series_index.clone()),
        has_cover: metadata.is_some_and(|m| m.has_cover),
        work: record.work.as_ref().map(|w| StoredWork {
            title: w.title.clone(),
            authors: w.authors.clone(),
            language: w.language.clone(),
            supporting_ids: w.supporting_google_ids.clone(),
        }),
        edition_status: edition.map(|e| e.status.clone()).unwrap_or_default(),
        edition_identifiers: edition.map(|e| e.identifiers.clone()).unwrap_or_default(),
        edition_publisher: edition.and_then(|e| e.publisher.clone()),
        edition_published_date: edition.and_then(|e| e.published_date.clone()),
        language: language.and_then(|l| l.language.clone()),
        language_declared: language.map(|l| l.declared.clone()).unwrap_or_default(),
        language_status: language.map(|l| l.status.clone()).unwrap_or_default(),
        cover_urls: ranked_cover_urls(&record.candidates)
            .into_iter()
            .take(5)
            .collect(),
        candidates: record
            .candidates
            .iter()
            .take(5)
            .map(|c| (c.google_id.clone(), c.evidence.clone()))
            .collect(),
    }
}

fn record_from_stored(
    path: PathBuf,
    stored: StoredRecord,
    fingerprint: epub::Fingerprint,
) -> Record {
    let status: &'static str = match stored.status.as_str() {
        "planned" => "planned",
        "unchanged" => "unchanged",
        "error" => "error",
        "skipped" => "skipped",
        "duplicate" => "duplicate",
        _ => "review",
    };
    let metadata = epub::Metadata {
        title: stored.title.clone(),
        declared_title: stored.declared_title.clone(),
        authors: stored.authors.clone(),
        isbns: stored.isbns.clone(),
        publisher: stored.publisher.clone(),
        published_date: stored.published_date.clone(),
        series: stored.series.clone(),
        series_index: stored.series_index.clone(),
        has_cover: stored.has_cover,
        ..Default::default()
    };
    let mut candidates: Vec<matching::Candidate> = stored
        .candidates
        .iter()
        .map(|(id, evidence)| {
            matching::Candidate::stored(id.clone(), evidence.clone(), None, false)
        })
        .collect();
    for (index, url) in stored.cover_urls.iter().enumerate() {
        candidates.insert(
            index,
            matching::Candidate::stored(
                stored.google_id.clone().unwrap_or_default(),
                "stored accepted candidate".into(),
                Some(url.clone()),
                true,
            ),
        );
    }
    Record {
        source: path,
        destination: stored.destination.map(PathBuf::from),
        status,
        reason: stored.reason,
        google_id: stored.google_id,
        metadata: Some(metadata),
        candidates,
        work: stored.work.map(|w| matching::WorkIdentity {
            title: w.title,
            authors: w.authors,
            language: w.language,
            confidence: "corroborated_work",
            evidence: vec!["Reused from the state database".into()],
            supporting_google_ids: w.supporting_ids,
        }),
        edition: Some(matching::Edition {
            status: stored.edition_status,
            identifiers: stored.edition_identifiers,
            publisher: stored.edition_publisher,
            published_date: stored.edition_published_date,
            ..Default::default()
        }),
        language: Some(crate::language::Assessment {
            language: stored.language,
            declared: stored.language_declared,
            detected: None,
            status: stored.language_status,
            samples: Vec::new(),
        }),
        warnings: stored.warnings,
        fingerprint: Some(fingerprint),
        provider: stored.provider,
    }
}

/// The same destination wanted by several files, or already occupied. No outcome
/// needs a human and nothing is ever overwritten or deleted:
/// - the first file (by path) takes the plain name;
/// - a file byte-identical to the one that holds the name is marked `duplicate`
///   and left exactly where it is;
/// - a different file of the same work gets the next free numbered name, "(2)".
fn preflight(records: &mut [Record]) {
    let mut groups = HashMap::<String, Vec<usize>>::new();
    for (index, record) in records.iter().enumerate() {
        if record.status == "planned" {
            let key = record
                .destination
                .as_ref()
                .expect("planned destination")
                .to_string_lossy()
                .to_lowercase();
            groups.entry(key).or_default().push(index);
        }
    }
    let mut claimed: std::collections::HashSet<String> = records
        .iter()
        .filter(|r| r.status == "planned")
        .filter_map(|r| r.destination.as_ref())
        .map(|d| d.to_string_lossy().to_lowercase())
        .collect();
    let mut keys: Vec<_> = groups.keys().cloned().collect();
    keys.sort();
    for key in keys {
        let members = &groups[&key];
        let destination = records[members[0]]
            .destination
            .clone()
            .expect("destination");
        let existing = match destination.try_exists() {
            Ok(true) => Some(destination.clone()),
            Ok(false) => None,
            Err(error) => {
                for &index in members {
                    records[index].status = "error";
                    records[index].reason = format!("Cannot inspect destination: {error}");
                }
                continue;
            }
        };
        if existing.is_none() && members.len() == 1 {
            continue;
        }
        // Who holds the plain name: a file already there, else the first member.
        let (holder, rest): (PathBuf, &[usize]) = match &existing {
            Some(path) => (path.clone(), &members[..]),
            None => (records[members[0]].source.clone(), &members[1..]),
        };
        let holder_hash = place::sha256(&holder).ok();
        let mut taken: Vec<(String, PathBuf)> = Vec::new();
        for &index in rest {
            let source = records[index].source.clone();
            let hash = place::sha256(&source).ok();
            if hash.is_some() && hash == holder_hash {
                records[index].status = "duplicate";
                records[index].reason =
                    format!("Identical copy of {}; left where it is", holder.display());
                continue;
            }
            if let Some((_, first)) = taken
                .iter()
                .find(|(other, _)| hash.as_deref() == Some(other.as_str()))
            {
                records[index].status = "duplicate";
                records[index].reason =
                    format!("Identical copy of {}; left where it is", first.display());
                continue;
            }
            let numbered = (2..)
                .map(|n| numbered_name(&destination, n))
                .find(|candidate| {
                    let name = candidate.to_string_lossy().to_lowercase();
                    !claimed.contains(&name) && !candidate.exists()
                })
                .expect("an unused numbered name exists");
            claimed.insert(numbered.to_string_lossy().to_lowercase());
            records[index].reason = format!(
                "{} (another file of the same book already holds the plain name)",
                records[index].reason
            );
            records[index].destination = Some(numbered);
            if let Some(hash) = hash {
                taken.push((hash, source));
            }
        }
    }
}

/// "Author - Title.epub" with " (n)" before the extension.
fn numbered_name(destination: &Path, n: u32) -> PathBuf {
    let stem = destination
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ext = destination
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    destination.with_file_name(format!("{stem} ({n}){ext}"))
}

struct TagJob {
    tags: Tags,
    cover: Option<Vec<u8>>,
    /// Things worth reporting that did not stop the tag (for example a skipped cover).
    notes: Vec<String>,
}

/// Decides what, if anything, to write into a corroborated book. Edition fields
/// (ISBN, publisher, date) are written only for a supported edition; a cover is
/// added only when the book has none.
async fn tag_job(
    record: &Record,
    provider: &Provider,
    others: &[Provider],
    covers: &CoverOptions,
) -> Result<Option<TagJob>> {
    let (Some(work), Some(metadata)) = (&record.work, &record.metadata) else {
        return Ok(None);
    };
    let mut tags = Tags {
        title: work.title.clone(),
        authors: work.authors.clone(),
        ..Tags::default()
    };
    if let Some(language) = record.language.as_ref().filter(|l| l.status == "corrected") {
        tags.language = language.language.clone();
        tags.replace_language = true;
    }
    if let Some(edition) = record.edition.as_ref().filter(|e| e.status == "supported") {
        tags.isbn13 = edition
            .identifiers
            .iter()
            .find_map(|id| epub::normalize_isbn(id));
        tags.publisher = edition.publisher.clone();
        tags.published_date = edition.published_date.clone();
    }
    // Quality over quantity: a cover is added when the book has none, or (opt-in)
    // replaces a small one with the same artwork at a higher resolution. A book with
    // no good cover is left without one rather than given a poor one.
    let mut notes = Vec::new();
    let urls = ranked_cover_urls(&record.candidates);
    let needs = crate::cover::CoverRequirements {
        min_width: covers.min_width,
    };
    let cover = if urls.is_empty() {
        None
    } else if !metadata.has_cover {
        best_cover(provider, &urls, &needs, None, &mut notes).await
    } else if covers.upgrade {
        let local_path = record.source.clone();
        let local = tokio::task::spawn_blocking(move || epub::read(&local_path))
            .await?
            .ok()
            .and_then(|(m, _)| m.cover);
        match local {
            Some(local) if local.width < covers.min_width => {
                best_cover(provider, &urls, &needs, Some(&local), &mut notes).await
            }
            _ => None,
        }
    } else {
        None
    };
    // No good cover from the provider that identified the book: ask the others for
    // the same work and put whatever they offer through the very same quality gate.
    let mut cover = cover;
    if cover.is_none() && !metadata.has_cover {
        for other in others {
            let path = record.source.clone();
            let Ok(Ok((full, _))) = tokio::task::spawn_blocking(move || epub::read(&path)).await
            else {
                break;
            };
            match matching::identify(other, &full).await {
                Ok(found) if found.work.is_some() => {
                    let urls = ranked_cover_urls(&found.candidates);
                    if let Some(bytes) = best_cover(other, &urls, &needs, None, &mut notes).await {
                        notes.push(format!("Cover taken from {}", other.name()));
                        cover = Some(bytes);
                        break;
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    notes.push(format!("{} cover lookup failed: {error:#}", other.name()))
                }
            }
        }
    }
    if let Some(series) = &metadata.series {
        tags.series = Some((series.clone(), metadata.series_index.clone()));
    }
    let declared_clean = metadata.declared_title == tags.title;
    let already = !tags.replace_language
        && declared_clean
        && metadata.title == tags.title
        && metadata.authors == tags.authors
        && cover.is_none()
        && tags
            .isbn13
            .as_ref()
            .is_none_or(|i| metadata.isbns.contains(i))
        && tags
            .publisher
            .as_ref()
            .is_none_or(|p| metadata.publisher.as_ref() == Some(p))
        && tags
            .published_date
            .as_ref()
            .is_none_or(|d| metadata.published_date.as_ref() == Some(d));
    Ok((!already).then_some(TagJob { tags, cover, notes }))
}

/// The stored record for a file the user is acting on. The file must already have
/// been examined and must be unchanged since.
fn stored_for(state: &State, path: &Path) -> Result<(String, epub::Fingerprint, StoredRecord)> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("Cannot find {}", path.display()))?;
    let key = canonical.to_string_lossy().into_owned();
    let fingerprint = epub::fingerprint(&canonical)?;
    let stored = state
        .lookup(&key, fingerprint.len, nanos(&fingerprint))?
        .context(
            "This file is not in the state file (or changed since); run the tool on it first",
        )?;
    Ok((key, fingerprint, stored))
}

struct CoverOptions {
    min_width: u32,
    upgrade: bool,
}

/// Cover addresses of the accepted candidates, best edition first: the edition that
/// agrees with the file's ISBN or publisher leads, and tie-in editions (a film or
/// television cover on a plain novel) come last.
fn ranked_cover_urls(candidates: &[matching::Candidate]) -> Vec<String> {
    let tie_in = |c: &matching::Candidate| {
        let text = format!(
            " {} {} ",
            crate::text::normalized(&c.title),
            crate::text::normalized(c.subtitle.as_deref().unwrap_or(""))
        );
        [
            " tie in ",
            " movie ",
            " film ",
            " hbo ",
            " netflix ",
            " television ",
            " motion picture ",
        ]
        .iter()
        .any(|marker| text.contains(marker))
    };
    let mut ranked: Vec<(usize, &matching::Candidate)> = candidates
        .iter()
        .enumerate()
        .filter(|(_, c)| c.accepted && c.cover_url.is_some())
        .collect();
    ranked.sort_by_key(|(index, c)| (tie_in(c), !c.isbn_agrees, !c.edition_supported, *index));
    let mut urls: Vec<String> = Vec::new();
    for (_, candidate) in ranked {
        if let Some(url) = &candidate.cover_url
            && !urls.contains(url)
        {
            urls.push(url.clone());
        }
    }
    urls
}

/// Downloads the ranked covers (at most four), keeps those that pass the quality gate
/// and, when `same_as` is given, are the same artwork as the cover already in the
/// book, and returns the widest. Stops early once one is as good as it can be.
async fn best_cover(
    provider: &Provider,
    urls: &[String],
    needs: &crate::cover::CoverRequirements,
    same_as: Option<&crate::cover::Signature>,
    notes: &mut Vec<String>,
) -> Option<Vec<u8>> {
    let mut best: Option<(u32, Vec<u8>)> = None;
    for url in urls.iter().take(4) {
        let bytes = match provider.cover_bytes(url).await {
            Ok(bytes) => bytes,
            Err(error) => {
                notes.push(format!("Cover not used: {error:#}"));
                continue;
            }
        };
        let checked = {
            let bytes = bytes.clone();
            let needs = crate::cover::CoverRequirements {
                min_width: needs.min_width,
            };
            tokio::task::spawn_blocking(move || {
                crate::cover::judge_cover(&bytes, &needs)
            })
            .await
        };
        let signature = match checked {
            Ok(Ok(signature)) => signature,
            Ok(Err(error)) => {
                notes.push(format!("Cover not used: {error:#}"));
                continue;
            }
            Err(_) => continue,
        };
        if let Some(local) = same_as
            && !crate::cover::compare(local, &signature).strong
        {
            notes.push("Cover not used: not the same artwork as the book's own".to_owned());
            continue;
        }
        let width = signature.width;
        if best.as_ref().is_none_or(|(w, _)| width > *w) {
            notes.push(format!(
                "Cover candidate {}x{}",
                signature.width, signature.height
            ));
            best = Some((width, bytes));
        }
        if width >= 1000 {
            break;
        }
    }
    if best.is_none() {
        notes.push("No cover met the quality requirements; none added".to_owned());
    }
    best.map(|(_, bytes)| bytes)
}

#[cfg(test)]
mod cover_ranking_tests {
    use super::*;

    fn candidate(id: &str, title: &str, url: &str, isbn: bool) -> matching::Candidate {
        let mut c = matching::Candidate::stored(id.into(), String::new(), Some(url.into()), true);
        c.title = title.into();
        c.isbn_agrees = isbn;
        c
    }

    #[test]
    fn the_matching_edition_leads_and_film_tie_ins_come_last() {
        let list = [
            candidate("a", "Dune (Movie Tie-In)", "https://x/a", false),
            candidate("b", "Dune", "https://x/b", false),
            candidate("c", "Dune", "https://x/c", true),
            candidate("d", "Dune", "https://x/b", false), // same address: listed once
        ];
        assert_eq!(
            ranked_cover_urls(&list),
            ["https://x/c", "https://x/b", "https://x/a"]
        );
        let mut rejected = candidate("e", "Dune", "https://x/e", true);
        rejected.accepted = false;
        assert!(
            ranked_cover_urls(&[rejected]).is_empty(),
            "unaccepted editions never donate a cover"
        );
    }
}
