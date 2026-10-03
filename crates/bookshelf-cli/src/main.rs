use anyhow::Context;
use bookshelf_core::{db, enrich, follow, grab, import, organize, rename, scan, series, sync, tag, want};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "bookshelf", about = "EPUB library manager")]
struct Cli {
    /// Override the database file path
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    /// Override the library directory for import and download
    #[arg(long, global = true)]
    library_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan a directory recursively for EPUB files and populate the library
    Scan(ScanArgs),
    /// Verify each book's identity (Google Books, Open Library, text witnesses), then
    /// safely retag, add covers and rename. Preview by default; run `scan` afterwards
    Identify(bookshelf_identify::cli::Args),
    /// List all books in the library
    List(ListArgs),
    /// Fuzzy-search the library by title or author
    Search(SearchArgs),
    /// Show full metadata for a book by ID
    Info(InfoArgs),
    /// Fetch metadata from OpenLibrary and Google Books for un-enriched books
    Enrich(EnrichArgs),
    /// Manage the want list (import, add, list, enrich)
    Want(WantArgs),
    /// Compare want list against owned editions and output the grab list
    Grab(GrabArgs),
    /// Display library statistics
    Stats,
    /// Track authors on OpenLibrary and populate the want list with their works
    Follow(FollowArgs),
    /// Display series information and fill gaps from OpenLibrary
    Series(SeriesArgs),
    /// Import EPUB files into the managed library folder
    Import(ImportArgs),
    /// Write DB metadata back into EPUB OPF files
    Tag(TagArgs),
    /// Rename EPUB files using a token pattern
    Rename(RenameArgs),
    /// Sync want list from authors, series, and Goodreads
    Sync(SyncArgs),
    /// Show or update persistent configuration
    Config(ConfigArgs),
    /// Reorganize EPUBs into {library}/{Author}/{Title}.epub
    Organize(OrganizeArgs),
    /// Mark a book as finished
    Read(StatusArgs),
    /// Mark a book as currently reading
    Reading(StatusArgs),
    /// Clear reading status
    Unread(StatusArgs),
    /// Find duplicate books in the library
    Dedupe(DedupeArgs),
    /// Detect and fill gaps in partially-owned series
    SeriesGaps(SeriesGapsArgs),
    /// Remove a book from the library database
    Remove(RemoveArgs),
    /// Manually update metadata fields for a book
    Update(UpdateArgs),
    /// Run the full automation pipeline: import lists, download, enrich, organize
    Run(RunArgs),
    /// Database maintenance: backup and integrity check
    Db(DbArgs),
    /// Manage the bookshelf Windows service (install, uninstall, start, stop)
    Service(ServiceArgs),
}

#[derive(Parser)]
struct ScanArgs {
    /// Directory to scan recursively for .epub files
    path: PathBuf,
    /// Skip automatic enrichment of newly scanned books
    #[arg(long)]
    no_enrich: bool,
}

#[derive(Parser)]
struct OrganizeArgs {
    /// Show planned moves without executing
    #[arg(long)]
    dry_run: bool,
    /// Overwrite destination if it already exists
    #[arg(long)]
    force: bool,
}

#[derive(Parser)]
struct StatusArgs {
    id: i64,
}

#[derive(Parser)]
struct DedupeArgs {
    /// Automatically keep the enriched copy and remove the other (requires --yes to confirm)
    #[arg(long)]
    auto: bool,
    /// Skip confirmation prompt (use with --auto)
    #[arg(long)]
    yes: bool,
}

#[derive(Parser)]
struct RemoveArgs {
    /// Database ID of the book to remove (see `bookshelf list`)
    id: i64,
    /// Also delete the file from disk
    #[arg(long)]
    delete_file: bool,
}

#[derive(Parser)]
struct UpdateArgs {
    /// Database ID of the book to update (see `bookshelf list`)
    id: i64,
    /// Set the title
    #[arg(long)]
    title: Option<String>,
    /// Set the authors (comma-separated)
    #[arg(long)]
    authors: Option<String>,
    /// Set the language code
    #[arg(long)]
    language: Option<String>,
    /// Set the source file path (use after manual file renames)
    #[arg(long)]
    source_path: Option<String>,
}

#[derive(Parser, Default)]
struct RunArgs {
    /// Show what would happen without making changes
    #[arg(long)]
    dry_run: bool,
    /// Skip the download step
    #[arg(long)]
    no_download: bool,
    /// Skip the organize step
    #[arg(long)]
    no_organize: bool,
    /// Run the pipeline once and exit instead of looping
    #[arg(long)]
    once: bool,
}

#[derive(Parser)]
struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommands,
}

#[derive(Subcommand)]
enum ServiceCommands {
    /// Install bookshelf as a Windows background service (requires Administrator)
    Install,
    /// Remove the bookshelf Windows service (requires Administrator)
    Uninstall,
    /// Start the installed Windows service
    Start,
    /// Stop the running Windows service
    Stop,
    /// Run as a Windows service — invoked by the Service Control Manager, not by users
    #[command(hide = true)]
    Run,
}

#[derive(Parser)]
struct DbArgs {
    #[command(subcommand)]
    command: DbCommands,
}

#[derive(Subcommand)]
enum DbCommands {
    /// Create a backup copy of the database (uses VACUUM INTO — dest must not exist)
    Backup(DbBackupArgs),
    /// Run SQLite integrity check
    Check,
}

#[derive(Parser)]
struct DbBackupArgs {
    /// Destination path for the backup file (e.g. ~/backups/library-2026-01-01.db)
    dest: PathBuf,
}

#[derive(Parser)]
struct SeriesGapsArgs {
    /// Add missing books to the want list automatically
    #[arg(long)]
    add: bool,
    /// Show what would be added without writing
    #[arg(long)]
    dry_run: bool,
}

#[derive(Parser)]
struct SearchArgs {
    /// Search query (fuzzy matched against title and authors)
    query: String,
}

#[derive(Parser)]
struct InfoArgs {
    /// Edition ID
    id: i64,
}

#[derive(Parser)]
struct EnrichArgs {
    /// Only enrich the edition with this database ID
    #[arg(long)]
    id: Option<i64>,
    /// Print planned changes without writing to the database
    #[arg(long)]
    dry_run: bool,
}

#[derive(Parser)]
struct ListArgs {
    /// Filter by language code (e.g. "en", "eng")
    #[arg(long)]
    language: Option<String>,
    /// Filter by series name (substring match)
    #[arg(long)]
    series: Option<String>,
    /// Filter by author name (substring match)
    #[arg(long)]
    author: Option<String>,
    /// Filter by tag
    #[arg(long)]
    tag: Option<String>,
    /// Only show books that haven't been enriched yet
    #[arg(long)]
    not_enriched: bool,
    /// Sort by: title (default), author, series
    #[arg(long, default_value = "title")]
    sort: String,
    /// Filter by reading status: read, reading, unread
    #[arg(long)]
    status: Option<String>,
}

// ---------------------------------------------------------------------------
// Want subcommand clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct WantArgs {
    #[command(subcommand)]
    command: WantCommands,
}

#[derive(Subcommand)]
enum WantCommands {
    /// Import books from a source into the want list
    Import(WantImportArgs),
    /// Manually add a book to the want list
    Add(WantAddArgs),
    /// List all want list entries
    List(WantListArgs),
    /// Enrich want list entries by resolving missing ISBN-13 values
    Enrich,
    /// Remove an entry from the want list by ID
    Remove(WantRemoveArgs),
}

#[derive(Parser)]
struct WantImportArgs {
    #[command(subcommand)]
    command: WantImportCommands,
}

#[derive(Subcommand)]
enum WantImportCommands {
    /// Import from a Goodreads CSV export
    Goodreads(GoodreadsArgs),
    /// Import from an OpenLibrary want-to-read list
    Openlibrary(OpenlibraryArgs),
    /// Import from a plain text file
    Text(TextArgs),
}

#[derive(Parser)]
struct GoodreadsArgs {
    /// Path to the Goodreads CSV export file
    path: PathBuf,
    /// Import from all shelves instead of only to-read
    #[arg(long)]
    all_shelves: bool,
}

#[derive(Parser)]
struct OpenlibraryArgs {
    /// OpenLibrary username
    username: String,
}

#[derive(Parser)]
struct TextArgs {
    /// Path to the plain text file
    path: PathBuf,
}

#[derive(Parser)]
struct WantAddArgs {
    /// Title of the book
    title: String,
    #[arg(long)]
    author: Option<String>,
    #[arg(long)]
    isbn: Option<String>,
    #[arg(long, default_value = "5")]
    priority: i64,
    #[arg(long)]
    notes: Option<String>,
}

#[derive(Parser)]
struct WantListArgs {
    /// Filter by source (goodreads_csv, openlibrary, manual, text_file)
    #[arg(long)]
    source: Option<String>,
}

#[derive(Parser)]
struct WantRemoveArgs {
    /// ID of the want list entry to remove
    id: i64,
}

#[derive(Parser)]
struct GrabArgs {
    /// Output format: text (default), json, or csv
    #[arg(long, default_value = "text")]
    output: String,
    /// Only include want list entries with priority >= N
    #[arg(long)]
    min_priority: Option<i64>,
    /// Optional subcommand (e.g. download)
    #[command(subcommand)]
    command: Option<GrabCommands>,
}

#[derive(Subcommand)]
enum GrabCommands {
    /// Download EPUB files for every item in the grab list
    Download(DownloadArgs),
}

#[derive(Parser)]
struct DownloadArgs {
    /// Show what would be downloaded without writing files
    #[arg(long)]
    dry_run: bool,
    /// Restrict to a single source: "standard-ebooks" or "internet-archive"
    #[arg(long)]
    source: Option<String>,
}

// ---------------------------------------------------------------------------
// Import / Tag / Rename clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct ImportArgs {
    /// Path to a single .epub file or a directory of .epub files
    path: PathBuf,
    /// Move the source file(s) instead of copying
    #[arg(long = "move")]
    move_file: bool,
}

#[derive(Parser)]
struct TagArgs {
    /// Edition ID to tag (omit when using --all)
    id: Option<i64>,
    /// Tag all owned enriched editions
    #[arg(long)]
    all: bool,
    /// Show what would be changed without writing
    #[arg(long)]
    dry_run: bool,
}

#[derive(Parser)]
struct RenameArgs {
    /// Token pattern for filenames (e.g. "{author} - {title}")
    #[arg(long)]
    pattern: Option<String>,
    /// Show planned renames without applying them
    #[arg(long)]
    dry_run: bool,
}

// ---------------------------------------------------------------------------
// Follow subcommand clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct FollowArgs {
    #[command(subcommand)]
    command: FollowCommands,
}

#[derive(Subcommand)]
enum FollowCommands {
    /// Add an author to the follow list and import their catalog
    Add(FollowAddArgs),
    /// Remove an author from the follow list
    Remove(FollowRemoveArgs),
    /// List followed authors
    List,
    /// Re-sync all followed authors from OpenLibrary
    Sync,
}

#[derive(Parser)]
struct FollowAddArgs {
    /// Author name to follow
    author: String,
}

#[derive(Parser)]
struct FollowRemoveArgs {
    /// Author name to unfollow
    author: String,
}

// ---------------------------------------------------------------------------
// Series subcommand clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct SeriesArgs {
    /// Subcommand (omit to show series status)
    #[command(subcommand)]
    command: Option<SeriesCommands>,
}

#[derive(Subcommand)]
enum SeriesCommands {
    /// Fill gaps in owned series by querying OpenLibrary
    Fill,
    /// List series status in machine-readable format
    List(SeriesListArgs),
}

#[derive(Parser)]
struct SeriesListArgs {
    /// Output format: text (default), json, or csv
    #[arg(long, default_value = "text")]
    output: String,
}

// ---------------------------------------------------------------------------
// Sync subcommand clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct SyncArgs {
    #[arg(long)]
    goodreads: Option<PathBuf>,
    /// Import from all shelves instead of only to-read (used with --goodreads)
    #[arg(long)]
    all_shelves: bool,
    #[command(subcommand)]
    command: Option<SyncCommands>,
}

#[derive(Subcommand)]
enum SyncCommands {
    /// Detect new releases from followed authors
    Authors,
    /// Re-run series gap detection
    Series,
    /// Re-import a Goodreads CSV export
    Goodreads(SyncGoodreadsArgs),
}

#[derive(Parser)]
struct SyncGoodreadsArgs {
    /// Path to the Goodreads CSV export file
    path: PathBuf,
    /// Import from all shelves instead of only to-read
    #[arg(long)]
    all_shelves: bool,
}

// ---------------------------------------------------------------------------
// Config subcommand clap structs
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommands,
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Show current resolved configuration
    Show,
    /// Set a configuration key to a value
    Set(ConfigSetArgs),
}

#[derive(Parser)]
struct ConfigSetArgs {
    key: String,
    value: String,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {e:?}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Windows service entry-point (must be at crate root, outside any function).
// The define_windows_service! macro generates an extern "system" function that
// Windows calls as the service entry point.
// ---------------------------------------------------------------------------
#[cfg(windows)]
fn svc_main(args: Vec<std::ffi::OsString>) {
    windows_svc::service_entry(args);
}
#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, svc_main);

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Service commands must be handled before the tokio runtime starts.
    // `service run` hands control to the Windows SCM dispatcher, which is a
    // blocking call that never returns until the service is stopped.
    #[cfg(windows)]
    if let Commands::Service(ref svc_args) = cli.command {
        return windows_svc::handle(svc_args);
    }

    tracing_subscriber::fmt::init();
    tokio::runtime::Runtime::new()?.block_on(async_run(cli))
}

async fn async_run(cli: Cli) -> anyhow::Result<()> {
    let app_config = bookshelf_core::config::load().unwrap_or_default();

    let db_path = cli.db
        .or_else(|| app_config.db.clone())
        .unwrap_or_else(db::default_db_path);

    let library_dir = cli.library_dir
        .or_else(|| app_config.library_dir.clone())
        .unwrap_or_else(import::default_library_dir);

    let language = app_config.language.clone();

    // Handle Config subcommand before opening the database — config set must
    // not require the DB to exist.
    if let Commands::Config(ref args) = cli.command {
        return cmd_config(args, &db_path, &library_dir, &language).await;
    }

    // Identification works on files and keeps its own resumable state, so it does not
    // need the database; `scan` afterwards ingests the corrected books.
    if let Commands::Identify(args) = cli.command {
        let code = bookshelf_identify::cli::run(args).await?;
        std::process::exit(code);
    }

    let pool = db::open(&db_path)
        .await
        .with_context(|| format!("cannot open database at {}", db_path.display()))?;

    match cli.command {
        Commands::Identify(_) => unreachable!("handled above"),
        Commands::Scan(args) => cmd_scan(&pool, &args, &library_dir).await?,
        Commands::List(args) => cmd_list(&pool, &args).await?,
        Commands::Search(args) => cmd_search(&pool, &args.query).await?,
        Commands::Info(args) => cmd_info(&pool, args.id).await?,
        Commands::Enrich(args) => cmd_enrich(&pool, &args).await?,
        Commands::Want(args) => cmd_want(&pool, args).await?,
        Commands::Grab(args) => cmd_grab(&pool, args, &library_dir, &app_config).await?,
        Commands::Stats => cmd_stats(&pool).await?,
        Commands::Follow(args) => cmd_follow(&pool, args, &language).await?,
        Commands::Series(args) => cmd_series(&pool, args, &language).await?,
        Commands::Import(args) => cmd_import(&pool, args, &library_dir).await?,
        Commands::Tag(args) => cmd_tag(&pool, args).await?,
        Commands::Rename(args) => cmd_rename(&pool, args).await?,
        Commands::Sync(args) => cmd_sync(&pool, args, &language).await?,
        Commands::Config(_) => unreachable!("handled above"),
        Commands::Organize(args) => cmd_organize(&pool, &args, &library_dir).await?,
        Commands::Read(args) => cmd_mark_read(&pool, args.id).await?,
        Commands::Reading(args) => cmd_mark_reading(&pool, args.id).await?,
        Commands::Unread(args) => cmd_mark_unread(&pool, args.id).await?,
        Commands::Dedupe(args) => cmd_dedupe(&pool, &args).await?,
        Commands::SeriesGaps(args) => cmd_series_gaps(&pool, &args).await?,
        Commands::Remove(args) => cmd_remove(&pool, &args).await?,
        Commands::Update(args) => cmd_update(&pool, &args).await?,
        Commands::Run(args) => cmd_run(&pool, &library_dir, &args, &app_config).await?,
        Commands::Db(args) => cmd_db(&pool, args).await?,
        Commands::Service(_) => {
            #[cfg(windows)]
            unreachable!("service commands are handled before async_run");
            #[cfg(not(windows))]
            {
                eprintln!("Service management is only available on Windows.");
                eprintln!("Use 'bookshelf run' to start the daemon.");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

async fn cmd_scan(pool: &db::DbPool, args: &ScanArgs, _library_dir: &Path) -> anyhow::Result<()> {
    let path = &args.path;
    if !path.exists() {
        eprintln!("Error: path does not exist: {}", path.display());
        std::process::exit(1);
    }
    if !path.is_dir() {
        eprintln!("Error: path is not a directory: {}", path.display());
        std::process::exit(1);
    }

    let app_config = bookshelf_core::config::load().unwrap_or_default();
    let allowed: Vec<&str> = app_config.allowed_formats.iter().map(|s| s.as_str()).collect();
    let result = scan::scan_directory(pool, path, &allowed).await?;
    println!(
        "Scanned {} files, inserted {} new books. ({} errors)",
        result.scanned,
        result.inserted,
        result.errors.len()
    );

    if !args.no_enrich && !result.new_edition_ids.is_empty() {
        let n = result.new_edition_ids.len();
        println!("Enriching {} new book{}...", n, if n == 1 { "" } else { "s" });
        let new_editions = db::get_editions_by_ids(pool, &result.new_edition_ids).await?;
        let client = reqwest::Client::new();
        let fake_args = EnrichArgs { id: None, dry_run: false };
        enrich_editions_list(pool, &new_editions, &fake_args, &client).await?;
    }

    Ok(())
}

async fn cmd_list(pool: &db::DbPool, args: &ListArgs) -> anyhow::Result<()> {
    let valid_sorts = ["title", "author", "series"];
    if !valid_sorts.contains(&args.sort.as_str()) {
        anyhow::bail!(
            "Invalid sort '{}'. Valid options: title, author, series",
            args.sort
        );
    }

    let editions = db::list_editions_filtered(
        pool,
        args.language.as_deref(),
        args.series.as_deref(),
        args.author.as_deref(),
        args.tag.as_deref(),
        args.not_enriched,
        &args.sort,
        args.status.as_deref(),
    )
    .await?;

    if editions.is_empty() {
        println!("No books found.");
        return Ok(());
    }

    print_edition_table(&editions);
    println!("\n{} book{}", editions.len(), if editions.len() == 1 { "" } else { "s" });
    Ok(())
}

/// Print editions in a fixed-width table.
fn print_edition_table(editions: &[db::EditionRow]) {
    // Compute column widths
    let id_w = 4usize;
    let title_w = editions
        .iter()
        .map(|e| e.title.as_deref().unwrap_or("").chars().count())
        .max()
        .unwrap_or(5)
        .max(5)
        .min(40);
    let author_w = editions
        .iter()
        .map(|e| e.authors.as_deref().unwrap_or("").chars().count())
        .max()
        .unwrap_or(7)
        .max(7)
        .min(30);
    let series_w = editions
        .iter()
        .map(|e| match (&e.series_name, &e.series_position) {
            (Some(s), Some(p)) => format!("{} #{}", s, p).chars().count(),
            (Some(s), None) => s.chars().count(),
            _ => 0,
        })
        .max()
        .unwrap_or(6)
        .max(6)
        .min(30);
    let lang_w = 4usize;
    let st_w = 2usize;

    let header = format!(
        "{:>id_w$}  {:<title_w$}  {:<author_w$}  {:<series_w$}  {:<lang_w$}  {:<st_w$}",
        "ID",
        "Title",
        "Authors",
        "Series",
        "Lang",
        "St",
        id_w = id_w,
        title_w = title_w,
        author_w = author_w,
        series_w = series_w,
        lang_w = lang_w,
        st_w = st_w,
    );
    let separator = format!(
        "{}  {}  {}  {}  {}  {}",
        "-".repeat(id_w),
        "-".repeat(title_w),
        "-".repeat(author_w),
        "-".repeat(series_w),
        "-".repeat(lang_w),
        "-".repeat(st_w),
    );
    println!("{}", header);
    println!("{}", separator);

    for e in editions {
        let title = e.title.as_deref().unwrap_or("(no title)");
        let authors = e.authors.as_deref().unwrap_or("");
        let series_str = match (&e.series_name, &e.series_position) {
            (Some(s), Some(p)) => format!("{} #{}", s, p),
            (Some(s), None) => s.clone(),
            _ => String::new(),
        };
        let lang = e.language.as_deref().unwrap_or("");
        let st = match e.reading_status.as_deref() {
            Some("read") => "rd",
            Some("reading") => "rg",
            _ => "",
        };

        let title_disp = truncate(title, title_w);
        let author_disp = truncate(authors, author_w);
        let series_disp = truncate(&series_str, series_w);

        println!(
            "{:>id_w$}  {:<title_w$}  {:<author_w$}  {:<series_w$}  {:<lang_w$}  {:<st_w$}",
            e.id,
            title_disp,
            author_disp,
            series_disp,
            lang,
            st,
            id_w = id_w,
            title_w = title_w,
            author_w = author_w,
            series_w = series_w,
            lang_w = lang_w,
            st_w = st_w,
        );
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len.saturating_sub(1)).collect();
        format!("{}…", truncated)
    }
}

async fn cmd_search(pool: &db::DbPool, query: &str) -> anyhow::Result<()> {
    let editions = db::search_editions(pool, query).await?;

    if editions.is_empty() {
        println!("No results for \"{}\".", query);
        return Ok(());
    }

    print_edition_table(&editions);
    println!("\n{} result(s) for \"{}\"", editions.len(), query);
    Ok(())
}

async fn cmd_info(pool: &db::DbPool, id: i64) -> anyhow::Result<()> {
    let row = db::get_edition(pool, id).await?;
    let Some(row) = row else {
        eprintln!("Error: no book with id {id}");
        std::process::exit(1);
    };

    println!("id:               {}", row.id);
    if let Some(v) = &row.title {
        println!("title:            {v}");
    }
    if let Some(v) = &row.authors {
        println!("authors:          {v}");
    }
    if let Some(v) = &row.isbn {
        println!("isbn:             {v}");
    }
    if let Some(v) = &row.series_name {
        println!("series_name:      {v}");
    }
    if let Some(v) = &row.series_position {
        println!("series_position:  {v}");
    }
    if let Some(v) = &row.publisher {
        println!("publisher:        {v}");
    }
    if let Some(v) = &row.publish_date {
        println!("publish_date:     {v}");
    }
    if let Some(v) = &row.language {
        println!("language:         {v}");
    }
    if let Some(v) = &row.description {
        println!("description:      {v}");
    }
    if let Some(v) = &row.cover_image_path {
        println!("cover_image_path: {v}");
    }
    println!("source_path:      {}", row.source_path);
    if let Some(v) = row.work_id {
        println!("work_id:          {v}");
    }
    println!("owned:            {}", row.owned);
    if let Some(v) = &row.enriched_at {
        println!("enriched_at:      {v}");
    }
    if let Some(v) = &row.tagged_at {
        println!("tagged_at:        {v}");
    }
    Ok(())
}

/// Returns true if a series name looks like a publisher/edition series rather
/// than an authorial/fictional series. These should not be stored as the book's
/// series (e.g. "Barnes & Noble Classics Series", "Penguin Popular Classics").
fn is_publisher_series(name: &str) -> bool {
    let lower = name.to_lowercase();
    const PUBLISHER_KEYWORDS: &[&str] = &[
        "classics series",
        "barnes & noble",
        "penguin",
        "norton",
        "oxford",
        "dover",
        "everyman",
        "heritage",
        "collector's library",
        "wordsworth",
        "planet",
        "signet classic",
        "bantam classic",
        "modern library",
        "enriched classic",
    ];
    PUBLISHER_KEYWORDS.iter().any(|kw| lower.contains(kw))
}

async fn enrich_editions_list(
    pool: &db::DbPool,
    editions: &[db::EditionRow],
    args: &EnrichArgs,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let app_config = bookshelf_core::config::load().unwrap_or_default();
    let lang_filter: Option<&str> = if app_config.language.is_empty() { None } else { Some(app_config.language.as_str()) };
    let n = editions.len();

    let mut success = 0usize;
    let mut not_found = 0usize;
    let mut errors = 0usize;

    for (i, row) in editions.iter().enumerate() {
        // Rate-limit: pause between books to avoid triggering OpenLibrary/Google Books limits.
        // OL's unauthenticated limit is ~100 req/min; each book makes up to 8 calls.
        if i > 0 && !args.dry_run {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let display_title = row.title.as_deref().unwrap_or("(no title)");
        print!("[{}/{}] #{} '{}'... ", i + 1, n, row.id, display_title);

        // If no title but has ISBN, attempt an ISBN-only OL lookup to fill in title/authors.
        if row.title.as_deref().map(|t| t.is_empty()).unwrap_or(true) {
            if let Some(ref isbn) = row.isbn {
                match enrich::enrich_from_openlibrary_isbn(&client, isbn, enrich::OPENLIBRARY_BASE).await {
                    Ok(Some(update)) => {
                        if let Err(e) = db::apply_enrichment(&pool, row.id, &update).await {
                            eprintln!("DB error applying ISBN enrichment for #{}: {e}", row.id);
                        } else {
                            println!("ok (isbn-only lookup)");
                            success += 1;
                        }
                    }
                    _ => {
                        println!("skip (no title, isbn lookup failed)");
                        not_found += 1;
                        let _ = db::mark_enrichment_attempted(pool, row.id).await;
                    }
                }
            } else {
                println!("skip (no title or isbn)");
                not_found += 1;
                let _ = db::mark_enrichment_attempted(pool, row.id).await;
            }
            continue;
        }

        let title = row.title.as_deref().unwrap_or("").to_string();
        // Strip parenthetical adaptation/editing credits before searching
        // e.g. "Charles Dickens (Adapted by Bob)" → "Charles Dickens"
        let authors_raw = row.authors.as_deref().unwrap_or("");
        let authors_str = authors_raw
            .split_once(" (")
            .map(|(before, _)| before.trim())
            .unwrap_or(authors_raw);

        let mut isbn = row.isbn.clone();

        // Phase A-1: if no isbn, try title+author search to find one
        if isbn.is_none() {
            match enrich::find_isbn_by_title_author_lang(
                &client,
                &title,
                authors_str,
                enrich::OPENLIBRARY_BASE,
                lang_filter,
            )
            .await
            {
                Ok(Some(found_isbn)) => {
                    tracing::info!(
                        "Found ISBN {} for '{}' via title+author search",
                        found_isbn,
                        title
                    );
                    if !args.dry_run {
                        let isbn_update = bookshelf_core::db::EnrichmentUpdate {
                            isbn: Some(found_isbn.clone()),
                            ..Default::default()
                        };
                        if let Err(e) = db::apply_enrichment(pool, row.id, &isbn_update).await {
                            eprintln!("WARNING: failed to persist discovered ISBN for id={}: {e}", row.id);
                        }
                    }
                    isbn = Some(found_isbn);
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!("WARNING: title+author search failed for id={}: {e}", row.id);
                }
            }
        }

        // Phase A-2: ISBN-based enrichment (OL + GB fallback)
        let mut isbn_update: Option<bookshelf_core::db::EnrichmentUpdate> = if let Some(ref isbn_val) = isbn {
            let ol_result = enrich::enrich_from_openlibrary_isbn(
                &client,
                isbn_val,
                enrich::OPENLIBRARY_BASE,
            )
            .await;

            match ol_result {
                Ok(Some(u)) => {
                    if !args.dry_run {
                        if let Some(ol_id) = &u.ol_work_id {
                            if let Some(wid) = row.work_id {
                                if let Err(e) = db::update_work_ol_id(pool, wid, ol_id).await {
                                    eprintln!("WARNING: work OL-ID update failed for id={}: {e}", row.id);
                                }
                            }
                        }
                    }
                    Some(u)
                }
                Ok(None) => {
                    // Try alternative ISBN via title+author search, then GB fallback
                    let alt_isbn = enrich::find_isbn_by_title_author_lang(
                        &client,
                        &title,
                        authors_str,
                        enrich::OPENLIBRARY_BASE,
                        lang_filter,
                    )
                    .await
                    .unwrap_or_default();

                    let ol_via_alt = if let Some(ref alt) = alt_isbn {
                        enrich::enrich_from_openlibrary_isbn(
                            &client,
                            alt,
                            enrich::OPENLIBRARY_BASE,
                        )
                        .await
                        .ok()
                        .flatten()
                    } else {
                        None
                    };

                    if ol_via_alt.is_some() {
                        ol_via_alt
                    } else {
                        enrich::enrich_from_google_books_isbn(
                            &client,
                            isbn_val,
                            enrich::GOOGLE_BOOKS_BASE,
                        )
                        .await
                        .unwrap_or(None)
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: OpenLibrary failed for ISBN {isbn_val}: {e}");
                    errors += 1;
                    // Mark attempted so we don't retry
                    if !args.dry_run {
                        let empty = bookshelf_core::db::EnrichmentUpdate {
                            enrichment_attempted: 1,
                            ..Default::default()
                        };
                        let _ = db::apply_enrichment(pool, row.id, &empty).await;
                    }
                    continue;
                }
            }
        } else {
            None
        };

        // Phase A-3: if OL gave us a work_id but no description, fetch from the Work endpoint.
        if let Some(ref mut u) = isbn_update {
            if u.description.is_none() {
                if let Some(ref wk) = u.ol_work_id {
                    if let Ok(Some(desc)) = enrich::fetch_ol_work_description(
                        &client,
                        wk,
                        enrich::OPENLIBRARY_BASE,
                    ).await {
                        u.description = Some(desc);
                    }
                }
            }
        }

        // Phase B: search.json enrichment for series/language supplement
        let search_update = match enrich::enrich_from_search_lang(
            &client,
            &title,
            authors_str,
            enrich::OL_BASE_URL,
            lang_filter,
        )
        .await
        {
            Ok(u) => u,
            Err(e) => {
                eprintln!("search warn: {e}");
                None
            }
        };

        // Phase C: merge — isbn_update is primary; fill gaps from search_update
        let mut final_update = match (isbn_update, search_update) {
            (Some(mut primary), Some(supplement)) => {
                // Fill any missing fields from search supplement
                if primary.authors.is_none() {
                    primary.authors = supplement.authors;
                }
                if primary.series_name.is_none() {
                    primary.series_name = supplement.series_name;
                }
                if primary.series_position.is_none() {
                    primary.series_position = supplement.series_position;
                }
                if primary.language.is_none() {
                    primary.language = supplement.language;
                }
                // Prefer the supplement (search.json) title when it is a
                // better match for the stored title:
                // 1. Same title, different case (e.g. "pride and prejudice" → proper case)
                // 2. Phase A returned an off-title (different edition/annotation) but
                //    Phase B's title matches the stored title case-insensitively.
                if let Some(ref supp_t) = supplement.title {
                    let stored_lc = row.title.as_deref().unwrap_or("").to_lowercase();
                    let supp_lc = supp_t.to_lowercase();
                    let use_supp = if let Some(ref pri_t) = primary.title {
                        let pri_lc = pri_t.to_lowercase();
                        // Case 1: same content, different case
                        (supp_lc == pri_lc && supp_t != pri_t)
                        // Case 2: Phase B matches stored but Phase A doesn't
                        || (supp_lc == stored_lc && pri_lc != stored_lc)
                    } else {
                        false
                    };
                    if use_supp {
                        primary.title = supplement.title;
                    }
                }
                primary
            }
            (Some(primary), None) => primary,
            (None, Some(supplement)) => supplement,
            (None, None) => {
                // If a language is configured, apply it even when OL has no
                // metadata — it's the one thing we know from context.
                if let Some(pref_lang) = lang_filter {
                    println!("ok (language from config)");
                    success += 1;
                    if !args.dry_run {
                        let lang_only = bookshelf_core::db::EnrichmentUpdate {
                            language: Some(pref_lang.to_string()),
                            enrichment_attempted: 1,
                            ..Default::default()
                        };
                        let _ = db::apply_enrichment(pool, row.id, &lang_only).await;
                    }
                } else {
                    println!("not found");
                    not_found += 1;
                    if !args.dry_run {
                        let empty = bookshelf_core::db::EnrichmentUpdate {
                            enrichment_attempted: 1,
                            ..Default::default()
                        };
                        let _ = db::apply_enrichment(pool, row.id, &empty).await;
                    }
                }
                continue;
            }
        };

        // If a language preference is configured, always trust it over OL's
        // language field (OL returns all editions' languages, not just the
        // preferred one, so the first entry is often wrong).
        if let Some(pref_lang) = lang_filter {
            if final_update.language.as_deref() != Some(pref_lang) {
                final_update.language = Some(pref_lang.to_string());
            }
        }

        // Phase D: if still no description, search OL by title+author for the work record.
        if final_update.description.is_none() {
            if let Ok(Some(desc)) = enrich::fetch_description_by_title_author(
                &client,
                &title,
                authors_str,
                enrich::OPENLIBRARY_BASE,
                lang_filter,
            ).await {
                final_update.description = Some(desc);
            }
        }

        // Phase E: Google Books title+author fallback for descriptions.
        if final_update.description.is_none() {
            if let Ok(Some(desc)) = enrich::fetch_description_from_google_books(
                &client,
                &title,
                authors_str,
                enrich::GOOGLE_BOOKS_BASE,
            ).await {
                final_update.description = Some(desc);
            }
        }

        // Strip parenthetical publisher/edition notes from the enriched title.
        // e.g. "Dracula (Barnes & Noble Classics Series)" → "Dracula"
        if let Some(ref t) = final_update.title.clone() {
            if let Some((before, _)) = t.split_once(" (") {
                let clean = before.trim().to_string();
                if !clean.is_empty() {
                    final_update.title = Some(clean);
                }
            }
        }

        // Drop publisher/edition series (e.g. "Barnes & Noble Classics Series").
        // Only keep series that look like authorial/fictional series.
        if let Some(ref s) = final_update.series_name.clone() {
            if is_publisher_series(s) {
                final_update.series_name = None;
                final_update.series_position = None;
            }
        }

        // Phase D: write or dry-run
        if args.dry_run {
            println!("would update:");
            if let Some(ref v) = final_update.title {
                println!("  title: {v}");
            }
            if let Some(ref v) = final_update.authors {
                println!("  authors: {v}");
            }
            if let Some(ref v) = final_update.series_name {
                println!("  series_name: {v}");
            }
            if let Some(ref v) = final_update.series_position {
                println!("  series_position: {v}");
            }
            if let Some(ref v) = final_update.language {
                println!("  language: {v}");
            }
            success += 1;
        } else {
            match db::apply_enrichment(pool, row.id, &final_update).await {
                Ok(()) => {
                    // Force-set language to the configured preference if set,
                    // since apply_enrichment uses COALESCE and won't overwrite
                    // a previously wrong value.
                    if let Some(pref_lang) = lang_filter {
                        let _ = db::set_language(pool, row.id, pref_lang).await;
                    }
                    // Fix up stored title:
                    // 1. Strip parenthetical junk (apply_enrichment COALESCE won't overwrite).
                    // 2. Use OL's properly-cased title when stored title differs only in case
                    //    (e.g. "Pride and prejudice" → "Pride and Prejudice").
                    let stored_title = row.title.as_deref().unwrap_or("");
                    let target_title: Option<String> = if stored_title.contains(" (") {
                        stored_title.split_once(" (").and_then(|(before, _)| {
                            let clean = before.trim().to_string();
                            if clean.is_empty() { None } else { Some(clean) }
                        })
                    } else if let Some(ref enriched) = final_update.title {
                        // Same title, different case — prefer OL's version
                        if !stored_title.is_empty()
                            && enriched.to_lowercase() == stored_title.to_lowercase()
                            && enriched != stored_title
                        {
                            Some(enriched.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if let Some(t) = target_title {
                        let _ = db::force_set_title(pool, row.id, &t).await;
                    }
                    // Fix up stored authors:
                    // 1. OL provided an author and it differs from stored → use OL value
                    // 2. Stored has parenthetical junk "(Adapted by ...)" → strip it
                    let stored_authors = row.authors.as_deref().unwrap_or("");
                    let clean_authors: Option<String> = if let Some(ref enriched) = final_update.authors {
                        // Only prefer the OL-returned name when it's in Latin script;
                        // non-Latin transliterations (e.g. "Πλάτων") are worse than
                        // the English name already stored in the EPUB.
                        let enriched_is_latin = enriched.chars().all(|c| c.is_ascii() || matches!(c, '\u{00C0}'..='\u{024F}'));
                        if enriched_is_latin && enriched != stored_authors {
                            Some(enriched.clone())
                        } else {
                            None
                        }
                    } else if stored_authors.contains('(') {
                        stored_authors
                            .split_once(" (")
                            .map(|(before, _)| before.trim().to_string())
                    } else {
                        None
                    };
                    if let Some(clean) = clean_authors {
                        let _ = db::force_set_authors(pool, row.id, &clean).await;
                    }
                    // Clear stored publisher series.
                    let stored_series = row.series_name.as_deref().unwrap_or("");
                    if !stored_series.is_empty() && is_publisher_series(stored_series) {
                        let _ = db::force_clear_series(pool, row.id).await;
                    }
                    println!("ok");
                    success += 1;
                }
                Err(e) => {
                    eprintln!("WARNING: DB update failed for id={}: {e}", row.id);
                    errors += 1;
                }
            }
        }
    }

    println!("\nDone. Enriched: {}, Not found: {}, Errors: {}", success, not_found, errors);
    Ok(())
}

async fn cmd_enrich(pool: &db::DbPool, args: &EnrichArgs) -> anyhow::Result<()> {
    let client = reqwest::Client::new();

    let editions: Vec<db::EditionRow> = if let Some(id) = args.id {
        match db::get_edition_by_id(pool, id).await? {
            Some(e) => vec![e],
            None => {
                println!("No edition found with ID {}", id);
                return Ok(());
            }
        }
    } else {
        let all = db::editions_needing_enrichment(pool).await?;
        if all.is_empty() {
            println!("All books are already enriched.");
            return Ok(());
        }
        all
    };

    let n = editions.len();
    println!("Enriching {} book{}...", n, if n == 1 { "" } else { "s" });
    enrich_editions_list(pool, &editions, args, &client).await
}

async fn cmd_organize(pool: &db::DbPool, args: &OrganizeArgs, library_dir: &Path) -> anyhow::Result<()> {
    let result = organize::organize_editions(pool, library_dir, args.dry_run, args.force).await?;
    if !args.dry_run {
        println!(
            "\nMoved: {}, Skipped: {}, Errors: {}",
            result.moved, result.skipped, result.errors.len()
        );
        for e in &result.errors {
            eprintln!("  ERROR: {}", e);
        }
    }
    Ok(())
}

async fn cmd_mark_read(pool: &db::DbPool, id: i64) -> anyhow::Result<()> {
    db::mark_read(pool, id).await?;
    println!("Marked #{} as read.", id);
    Ok(())
}

async fn cmd_mark_reading(pool: &db::DbPool, id: i64) -> anyhow::Result<()> {
    db::mark_reading(pool, id).await?;
    println!("Marked #{} as reading.", id);
    Ok(())
}

async fn cmd_mark_unread(pool: &db::DbPool, id: i64) -> anyhow::Result<()> {
    db::mark_unread(pool, id).await?;
    println!("Cleared reading status for #{}.", id);
    Ok(())
}

// ---------------------------------------------------------------------------
// Want command handlers
// ---------------------------------------------------------------------------

async fn cmd_want(pool: &db::DbPool, args: WantArgs) -> anyhow::Result<()> {
    match args.command {
        WantCommands::Import(import_args) => cmd_want_import(pool, import_args).await?,
        WantCommands::Add(add_args) => cmd_want_add(pool, add_args).await?,
        WantCommands::List(list_args) => cmd_want_list(pool, list_args).await?,
        WantCommands::Enrich => cmd_want_enrich(pool).await?,
        WantCommands::Remove(remove_args) => cmd_want_remove(pool, remove_args).await?,
    }
    Ok(())
}

async fn cmd_want_import(pool: &db::DbPool, args: WantImportArgs) -> anyhow::Result<()> {
    match args.command {
        WantImportCommands::Goodreads(a) => {
            cmd_want_import_goodreads(pool, a.path, a.all_shelves).await?
        }
        WantImportCommands::Openlibrary(a) => {
            cmd_want_import_openlibrary(pool, a.username).await?
        }
        WantImportCommands::Text(a) => cmd_want_import_text(pool, a.path).await?,
    }
    Ok(())
}

async fn cmd_want_import_goodreads(
    pool: &db::DbPool,
    path: PathBuf,
    all_shelves: bool,
) -> anyhow::Result<()> {
    if let Err(e) = want::import_goodreads_csv(pool, &path, all_shelves).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    Ok(())
}

async fn cmd_want_import_openlibrary(
    pool: &db::DbPool,
    username: String,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    if let Err(e) =
        want::import_openlibrary(pool, &client, &username, enrich::OPENLIBRARY_BASE).await
    {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

async fn cmd_want_import_text(pool: &db::DbPool, path: PathBuf) -> anyhow::Result<()> {
    if let Err(e) = want::import_text_file(pool, &path).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    Ok(())
}

async fn cmd_want_add(pool: &db::DbPool, args: WantAddArgs) -> anyhow::Result<()> {
    let result = want::add_manual(
        pool,
        &args.title,
        args.author.as_deref(),
        args.isbn.as_deref(),
        args.priority,
        args.notes.as_deref(),
    )
    .await;

    match result {
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        Ok(want::AddResult::AlreadyOwned) => {
            println!("Already owned: {}", args.title);
        }
        Ok(want::AddResult::AlreadyInWantList) => {
            println!("Already in want list: {}", args.title);
        }
        Ok(want::AddResult::Inserted) => {
            println!("Added to want list: {}", args.title);
        }
    }
    Ok(())
}

async fn cmd_want_list(pool: &db::DbPool, args: WantListArgs) -> anyhow::Result<()> {
    if let Some(ref src) = args.source {
        if !db::VALID_SOURCES.contains(&src.as_str()) {
            eprintln!(
                "Error: invalid source '{src}'. Valid values: {}",
                db::VALID_SOURCES.join(", ")
            );
            std::process::exit(1);
        }
    }

    let rows = db::list_want(pool, args.source.as_deref(), None).await?;
    if rows.is_empty() {
        println!("No entries in want list.");
        return Ok(());
    }

    for row in &rows {
        let author = row.author.as_deref().unwrap_or("(none)");
        let attempts_str = if row.download_attempts > 0 {
            format!("  attempts:{}", row.download_attempts)
        } else {
            String::new()
        };
        println!(
            "{}  {}  [{}]  priority:{}  source:{}{}",
            row.id, row.title, author, row.priority, row.source, attempts_str
        );
    }
    Ok(())
}

async fn cmd_want_remove(pool: &db::DbPool, args: WantRemoveArgs) -> anyhow::Result<()> {
    // Get the title before deleting so we can print it.
    let row = db::get_want(pool, args.id).await?;
    let Some(row) = row else {
        eprintln!("Error: no want list entry with id {}", args.id);
        std::process::exit(1);
    };
    let title = row.title.clone();
    want::remove_want(pool, args.id).await?;
    println!("Removed: {title}");
    Ok(())
}

async fn cmd_want_enrich(pool: &db::DbPool) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let (enriched, eligible) =
        want::enrich_want_list(pool, &client, enrich::OPENLIBRARY_BASE).await?;
    println!("Enriched {enriched} of {eligible} want list entries.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Grab command handler
// ---------------------------------------------------------------------------

async fn cmd_grab(pool: &db::DbPool, args: GrabArgs, library_dir: &std::path::Path, app_config: &bookshelf_core::config::AppConfig) -> anyhow::Result<()> {
    // If the download subcommand was given, delegate to cmd_grab_download.
    if let Some(GrabCommands::Download(ref dl_args)) = args.command {
        return cmd_grab_download(pool, library_dir, dl_args, app_config).await;
    }

    const VALID_OUTPUTS: &[&str] = &["text", "json", "csv"];
    if !VALID_OUTPUTS.contains(&args.output.as_str()) {
        eprintln!(
            "Error: invalid --output '{}'. Valid values: text, json, csv",
            args.output
        );
        std::process::exit(1);
    }

    // Check if the want list is completely empty.
    let all_want = db::all_want_entries(pool).await?;
    if all_want.is_empty() {
        match args.output.as_str() {
            "json" => println!("[]"),
            "csv" => {
                println!("priority,title,author,isbn13,source,notes");
            }
            _ => println!("Want list is empty."),
        }
        return Ok(());
    }

    let entries = grab::compute_grab_list(pool, args.min_priority).await?;

    if entries.is_empty() {
        match args.output.as_str() {
            "json" => println!("[]"),
            "csv" => {
                println!("priority,title,author,isbn13,source,notes");
            }
            _ => println!("All wanted books are already owned."),
        }
        return Ok(());
    }

    match args.output.as_str() {
        "json" => {
            let json = grab::format_json(&entries)?;
            println!("{json}");
        }
        "csv" => {
            let csv = grab::format_csv(&entries)?;
            print!("{csv}");
        }
        _ => {
            let text = grab::format_text(&entries);
            print!("{text}");
        }
    }

    Ok(())
}

async fn cmd_stats(pool: &db::DbPool) -> anyhow::Result<()> {
    let s = db::library_stats(pool).await?;
    println!("Library Statistics");
    println!("{}", "\u{2500}".repeat(30));
    println!("Books in library:    {:>6}", s.books_in_library);
    println!("  With ISBN:         {:>6}", s.with_isbn);
    println!("  In a series:       {:>6}", s.in_a_series);
    println!("  Enriched:          {:>6}", s.enriched);
    println!("  Not yet enriched:  {:>6}", s.not_yet_enriched);
    println!("  Without metadata:  {:>6}", s.without_metadata);
    println!();
    println!("Reading Status");
    println!("{}", "\u{2500}".repeat(30));
    println!("  Read:              {:>6}", s.status_read);
    println!("  Reading:           {:>6}", s.status_reading);
    println!("  Unread:            {:>6}", s.status_unread);
    println!();
    if !s.lang_counts.is_empty() {
        println!("Languages");
        println!("{}", "\u{2500}".repeat(30));
        for (lang, cnt) in &s.lang_counts {
            println!("  {:<20} {:>6}", lang, cnt);
        }
        println!();
    }
    // Series completeness
    let series_editions = db::editions_with_series(pool).await?;
    let views = bookshelf_core::series::compute_series_views(&series_editions);
    if !views.is_empty() {
        println!("Series");
        println!("{}", "\u{2500}".repeat(30));
        let complete = views.iter().filter(|v| v.entries.iter().all(|e| e.owned)).count();
        let incomplete = views.len() - complete;
        println!("  Total series:      {:>6}", views.len());
        println!("  Complete:          {:>6}", complete);
        println!("  Incomplete:        {:>6}", incomplete);
        println!();
    }
    println!("Want List");
    println!("{}", "\u{2500}".repeat(30));
    println!("Total entries:       {:>6}", s.want_total);
    println!("  With ISBN:         {:>6}", s.want_with_isbn);
    println!("  By source:");
    println!("    Goodreads CSV:   {:>6}", s.want_by_goodreads_csv);
    println!("    Manual:          {:>6}", s.want_by_manual);
    println!("    OpenLibrary:     {:>6}", s.want_by_openlibrary);
    println!("    Text file:       {:>6}", s.want_by_text_file);
    println!("    Author follow:   {:>6}", s.want_by_author_follow);
    println!("    Series fill:     {:>6}", s.want_by_series_fill);
    println!();
    println!("Grab List (not owned):{:>5}", s.grab_count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Follow command handlers
// ---------------------------------------------------------------------------

async fn cmd_follow(pool: &db::DbPool, args: FollowArgs, language: &str) -> anyhow::Result<()> {
    match args.command {
        FollowCommands::Add(a) => cmd_follow_add(pool, a, language).await?,
        FollowCommands::Remove(a) => cmd_follow_remove(pool, a).await?,
        FollowCommands::List => cmd_follow_list(pool).await?,
        FollowCommands::Sync => cmd_follow_sync(pool, language).await?,
    }
    Ok(())
}

async fn cmd_follow_add(pool: &db::DbPool, args: FollowAddArgs, language: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match follow::follow_add(pool, &client, &args.author, enrich::OPENLIBRARY_BASE, Some(language)).await {
        Ok(follow::FollowAddResult::Added { works_queued }) => {
            println!("Now following '{}'. Queued {works_queued} works.", args.author);
        }
        Ok(follow::FollowAddResult::AlreadyFollowed) => {
            println!("Already following '{}'.", args.author);
        }
        Ok(follow::FollowAddResult::AuthorNotFound) => {
            println!("Author '{}' not found on OpenLibrary.", args.author);
        }
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn cmd_follow_remove(pool: &db::DbPool, args: FollowRemoveArgs) -> anyhow::Result<()> {
    let found = follow::follow_remove(pool, &args.author).await?;
    if found {
        println!("Unfollowed '{}'.", args.author);
    } else {
        eprintln!("Error: '{}' is not in the follow list.", args.author);
        std::process::exit(1);
    }
    Ok(())
}

async fn cmd_follow_list(pool: &db::DbPool) -> anyhow::Result<()> {
    let authors = follow::follow_list(pool).await?;
    if authors.is_empty() {
        println!("No authors followed.");
        return Ok(());
    }
    for row in &authors {
        println!(
            "{} (last synced: {})",
            row.name,
            row.last_synced.as_deref().unwrap_or("never")
        );
    }
    Ok(())
}

async fn cmd_follow_sync(pool: &db::DbPool, language: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match follow::follow_sync(pool, &client, enrich::OPENLIBRARY_BASE, Some(language)).await {
        Ok(0) => {} // message already printed inside follow_sync
        Ok(n) => println!("Synced {n} author(s)."),
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Series command handlers
// ---------------------------------------------------------------------------

async fn cmd_series(pool: &db::DbPool, args: SeriesArgs, language: &str) -> anyhow::Result<()> {
    match args.command {
        None => cmd_series_show(pool).await?,
        Some(SeriesCommands::Fill) => cmd_series_fill(pool, language).await?,
        Some(SeriesCommands::List(a)) => cmd_series_list(pool, a).await?,
    }
    Ok(())
}

async fn cmd_series_show(pool: &db::DbPool) -> anyhow::Result<()> {
    let editions = db::editions_with_series(pool).await?;
    let views = series::compute_series_views(&editions);
    print!("{}", series::format_series_text(&views));
    Ok(())
}

async fn cmd_series_fill(pool: &db::DbPool, language: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match series::series_fill(pool, &client, enrich::OPENLIBRARY_BASE, Some(language)).await {
        Ok(0) => {} // message already printed inside series_fill
        Ok(n) => println!("Queued {n} missing works."),
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn cmd_series_list(pool: &db::DbPool, args: SeriesListArgs) -> anyhow::Result<()> {
    const VALID_OUTPUTS: &[&str] = &["text", "json", "csv"];
    if !VALID_OUTPUTS.contains(&args.output.as_str()) {
        eprintln!(
            "Error: invalid --output '{}'. Valid values: text, json, csv",
            args.output
        );
        std::process::exit(1);
    }

    let editions = db::editions_with_series(pool).await?;
    let views = series::compute_series_views(&editions);

    match args.output.as_str() {
        "json" => println!("{}", series::format_series_json(&views)?),
        "csv" => print!("{}", series::format_series_csv(&views)?),
        _ => print!("{}", series::format_series_text(&views)),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Import / Tag / Rename / Grab-Download command handlers
// ---------------------------------------------------------------------------

async fn cmd_import(
    pool: &db::DbPool,
    args: ImportArgs,
    library_dir: &std::path::Path,
) -> anyhow::Result<()> {
    match import::import_path(pool, &args.path, library_dir, args.move_file).await {
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        Ok(result) => {
            println!(
                "Imported {} file(s), skipped {} file(s). ({} error(s))",
                result.imported,
                result.skipped,
                result.errors.len()
            );
        }
    }
    Ok(())
}

async fn cmd_tag(pool: &db::DbPool, args: TagArgs) -> anyhow::Result<()> {
    match (args.id, args.all) {
        (Some(_), true) => {
            eprintln!("Error: --all and a specific ID cannot be combined");
            std::process::exit(1);
        }
        (None, false) => {
            eprintln!("Error: provide an edition ID or --all");
            std::process::exit(1);
        }
        (Some(id), false) => {
            if let Err(e) = tag::tag_edition(pool, id, args.dry_run).await {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
        (None, true) => {
            match tag::tag_all(pool, args.dry_run).await {
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
                Ok(_result) => {}
            }
        }
    }
    Ok(())
}

async fn cmd_rename(pool: &db::DbPool, args: RenameArgs) -> anyhow::Result<()> {
    // When --pattern is supplied, use it for all editions with no series override.
    // When absent, use "{author} - {title}" as the base pattern and
    // "{author} - {series} {position} - {title}" for editions that have series info (AC-36).
    let (pattern, unknown_fallback, series_pat) = if let Some(ref p) = args.pattern {
        (p.clone(), false, None)
    } else {
        (
            "{author} - {title}".to_string(),
            true,
            Some("{author} - {series} {position} - {title}"),
        )
    };

    match rename::rename_editions(pool, &pattern, args.dry_run, unknown_fallback, series_pat).await {
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        Ok(result) => {
            if !args.dry_run {
                println!("Renamed {} file(s), skipped {}.", result.renamed, result.skipped);
            }
        }
    }
    Ok(())
}

/// Build a `reqwest::Client` with the configured timeout.
fn build_http_client(app_config: &bookshelf_core::config::AppConfig) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(app_config.request_timeout_secs))
        .build()
        .expect("failed to build HTTP client")
}

/// Build SE credentials from config, printing a warning if only one half is set.
fn build_se_creds(
    app_config: &bookshelf_core::config::AppConfig,
    warn_prefix: &str,
) -> Option<(String, String)> {
    match (&app_config.se_email, &app_config.se_password) {
        (Some(email), Some(password)) => Some((email.clone(), password.clone())),
        (Some(_), None) | (None, Some(_)) => {
            eprintln!("{warn_prefix}se_email and se_password must both be set for Standard Ebooks auth");
            None
        }
        (None, None) => None,
    }
}

/// Construct the ordered list of download sources from `app_config.download_sources`.
/// SE is skipped silently when it appears in the list but no credentials are configured.
fn build_sources_from_config(
    app_config: &bookshelf_core::config::AppConfig,
    client: &reqwest::Client,
    se_creds: Option<(String, String)>,
) -> Vec<Box<dyn bookshelf_core::download_sources::DownloadSource>> {
    use bookshelf_core::download_sources::{
        ArxivSource, DownloadSource, GutenbergSource, InternetArchiveSource, StandardEbooksSource,
    };

    let mut sources: Vec<Box<dyn DownloadSource>> = Vec::new();
    for name in &app_config.download_sources {
        match name.as_str() {
            "standard-ebooks" => {
                if let Some(creds) = se_creds.clone() {
                    sources.push(Box::new(StandardEbooksSource::new(
                        client.clone(),
                        "https://standardebooks.org",
                        Some(creds),
                    )));
                }
            }
            "gutenberg" => {
                sources.push(Box::new(GutenbergSource::new(client.clone(), "https://gutendex.com")));
            }
            "arxiv" => {
                sources.push(Box::new(ArxivSource::new(client.clone(), "https://export.arxiv.org")));
            }
            "internet-archive" => {
                sources.push(Box::new(InternetArchiveSource::new(
                    client.clone(),
                    "https://archive.org",
                    app_config.allowed_formats.clone(),
                )));
            }
            other => {
                eprintln!("WARNING: unknown download source in config: '{other}' — skipping");
            }
        }
    }
    sources
}

async fn cmd_grab_download(
    pool: &db::DbPool,
    library_dir: &std::path::Path,
    args: &DownloadArgs,
    app_config: &bookshelf_core::config::AppConfig,
) -> anyhow::Result<()> {
    use bookshelf_core::download_sources::{
        ArxivSource, DownloadSource, GutenbergSource, InternetArchiveSource, StandardEbooksSource,
    };

    let client = build_http_client(app_config);
    let se_creds = build_se_creds(app_config, "WARNING: ");

    let sources: Vec<Box<dyn DownloadSource>> = match args.source.as_deref() {
        None => build_sources_from_config(app_config, &client, se_creds),
        Some("standard-ebooks") => vec![
            Box::new(StandardEbooksSource::new(client.clone(), "https://standardebooks.org", se_creds)),
        ],
        Some("gutenberg") => vec![
            Box::new(GutenbergSource::new(client.clone(), "https://gutendex.com")),
        ],
        Some("arxiv") => vec![
            Box::new(ArxivSource::new(client.clone(), "https://export.arxiv.org")),
        ],
        Some("internet-archive") => vec![
            Box::new(InternetArchiveSource::new(client.clone(), "https://archive.org", app_config.allowed_formats.clone())),
        ],
        Some(invalid) => {
            eprintln!(
                "Error: invalid --source '{}'. Valid values: standard-ebooks, gutenberg, arxiv, internet-archive",
                invalid
            );
            std::process::exit(1);
        }
    };

    let result = bookshelf_core::download::download_want_list(
        pool,
        library_dir,
        &sources,
        args.dry_run,
    ).await?;

    for event in &result.events {
        match event {
            bookshelf_core::download::DownloadEvent::Downloaded { title, source_name } => {
                println!("Downloaded: {} [{}]", title, source_name);
            }
            bookshelf_core::download::DownloadEvent::WouldDownload { title, source_name } => {
                println!("[DRY RUN] Would download: {} [{}]", title, source_name);
            }
            bookshelf_core::download::DownloadEvent::NotFound { title } => {
                if args.dry_run {
                    println!("[DRY RUN] Not found: {}", title);
                } else {
                    println!("Not found: {}", title);
                }
            }
        }
    }

    if args.dry_run {
        println!("{} would download, {} not found.", result.would_download, result.not_found);
    } else {
        println!("{} downloaded, {} not found.", result.downloaded, result.not_found);
    }

    // Auto-enrich any newly downloaded books (enrichment_attempted = 0).
    if !args.dry_run && result.downloaded > 0 {
        let unenriched = db::editions_needing_enrichment(pool).await?;
        if !unenriched.is_empty() {
            println!("Enriching {} new book{}...", unenriched.len(), if unenriched.len() == 1 { "" } else { "s" });
            let enrich_args = EnrichArgs { id: None, dry_run: false };
            enrich_editions_list(pool, &unenriched, &enrich_args, &client).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sync command handlers
// ---------------------------------------------------------------------------

async fn cmd_sync(pool: &db::DbPool, args: SyncArgs, language: &str) -> anyhow::Result<()> {
    match args.command {
        Some(SyncCommands::Authors) => cmd_sync_authors(pool, language).await?,
        Some(SyncCommands::Series) => cmd_sync_series(pool, language).await?,
        Some(SyncCommands::Goodreads(a)) => {
            cmd_sync_goodreads(pool, a.path, a.all_shelves).await?
        }
        None => {
            // Master sync: goodreads (if path given), then series, then authors.
            let client = reqwest::Client::new();
            let goodreads_path = args.goodreads.as_deref();
            let result = sync::sync_all(
                pool,
                &client,
                enrich::OPENLIBRARY_BASE,
                goodreads_path,
                args.all_shelves,
                Some(language),
            )
            .await;

            let mut any_error = false;

            if let Some(gr) = result.goodreads {
                match gr {
                    Ok(summary) => {
                        println!(
                            "{} new entries imported, {} skipped",
                            summary.imported, summary.skipped_owned
                        );
                    }
                    Err(e) => {
                        eprintln!("Error (goodreads): {e}");
                        any_error = true;
                    }
                }
            }

            match result.series {
                Some(Ok(n)) => println!("{n} missing entries queued"),
                Some(Err(e)) => {
                    eprintln!("Error (series): {e}");
                    any_error = true;
                }
                None => {}
            }

            match result.authors {
                Some(Ok(outcome)) => {
                    if outcome.results.is_empty() {
                        println!("No followed authors to sync.");
                    } else {
                        for r in &outcome.results {
                            println!("{}: {} new works queued", r.name, r.works_queued);
                        }
                    }
                    if outcome.had_error {
                        any_error = true;
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Error (authors): {e}");
                    any_error = true;
                }
                None => {}
            }

            if any_error {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

async fn cmd_sync_authors(pool: &db::DbPool, language: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match follow::sync_authors_detailed(pool, &client, enrich::OPENLIBRARY_BASE, Some(language)).await {
        Ok(outcome) => {
            if outcome.results.is_empty() {
                println!("No followed authors to sync.");
            } else {
                for r in &outcome.results {
                    println!("{}: {} new works queued", r.name, r.works_queued);
                }
            }
            if outcome.had_error {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn cmd_sync_series(pool: &db::DbPool, language: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match series::series_fill(pool, &client, enrich::OPENLIBRARY_BASE, Some(language)).await {
        Ok(n) => println!("{n} missing entries queued"),
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn cmd_sync_goodreads(
    pool: &db::DbPool,
    path: PathBuf,
    all_shelves: bool,
) -> anyhow::Result<()> {
    match want::import_goodreads_csv(pool, &path, all_shelves).await {
        Ok(summary) => {
            println!(
                "{} new entries imported, {} skipped",
                summary.imported, summary.skipped_owned
            );
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Dedupe command handler
// ---------------------------------------------------------------------------

async fn cmd_dedupe(pool: &db::DbPool, args: &DedupeArgs) -> anyhow::Result<()> {
    let groups = db::find_duplicate_editions(pool).await?;

    if groups.is_empty() {
        println!("No duplicates found.");
        return Ok(());
    }

    let mut total_removed = 0usize;

    for group in &groups {
        let title = group[0].title.as_deref().unwrap_or("(no title)");
        let authors = group[0].authors.as_deref().unwrap_or("(no authors)");
        println!("DUPLICATE: \"{}\" by {}", title, authors);
        for (i, ed) in group.iter().enumerate() {
            let enriched_flag = if ed.enriched_at.is_some() { "enriched" } else { "not enriched" };
            let read_flag = match ed.reading_status.as_deref() {
                Some("read") => ", read",
                Some("reading") => ", reading",
                _ => "",
            };
            println!("  #{}  {}  [{}{read_flag}]", i + 1, ed.source_path, enriched_flag);
        }

        if args.auto {
            // Pick winner: prefer enriched; on tie prefer lower id.
            let winner = group
                .iter()
                .max_by_key(|e| (e.enriched_at.is_some() as i64, -e.id))
                .unwrap();
            let to_remove: Vec<&db::EditionRow> =
                group.iter().filter(|e| e.id != winner.id).collect();

            println!(
                "  -> Keep: #{} ({})",
                winner.id,
                winner.source_path
            );
            for ed in &to_remove {
                println!("  -> Remove: #{} ({})", ed.id, ed.source_path);
            }

            if args.yes {
                for ed in &to_remove {
                    let remove_path = std::path::Path::new(&ed.source_path);
                    let keep_path = std::path::Path::new(&winner.source_path);
                    // Only delete the file if it resolves to a DIFFERENT physical
                    // path from the winner's file.  On Windows, canonicalize may
                    // return "\\?\" prefix paths that differ lexically but refer to
                    // the exact same file — deleting them would remove the winner's
                    // file too.
                    let same_file = remove_path.canonicalize().ok()
                        == keep_path.canonicalize().ok();
                    if !same_file && remove_path.exists() {
                        if let Err(e) = std::fs::remove_file(remove_path) {
                            eprintln!("WARNING: could not delete file {}: {e}", ed.source_path);
                        }
                    }
                    db::delete_edition(pool, ed.id).await?;
                    total_removed += 1;
                }
            } else {
                println!("  (pass --yes to execute removal)");
            }
        }
    }

    if args.auto && args.yes {
        println!("Removed {} duplicate(s).", total_removed);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SeriesGaps command handler
// ---------------------------------------------------------------------------

async fn cmd_series_gaps(pool: &db::DbPool, args: &SeriesGapsArgs) -> anyhow::Result<()> {
    let editions = db::editions_in_series(pool).await?;
    let gaps = series::compute_series_gaps(&editions);

    if gaps.is_empty() {
        println!("No series gaps found.");
        return Ok(());
    }

    for gap in &gaps {
        let missing_str: Vec<String> = gap.missing_positions.iter().map(|p| format!("#{}", p)).collect();
        println!(
            "\"{}\" by {} — missing: {}",
            gap.series_name,
            gap.authors,
            missing_str.join(", ")
        );

        if args.add || args.dry_run {
            for pos in &gap.missing_positions {
                let title = format!("{} #{}", gap.series_name, pos);
                let authors = &gap.authors;

                // Check if already in the want list.
                let exists = db::want_exists_by_title_author(pool, &title, authors).await?;
                if exists {
                    continue;
                }

                if args.dry_run {
                    println!(
                        "  [DRY RUN] Would add to want list: \"{}\" by {}",
                        title, authors
                    );
                } else {
                    db::insert_want(pool, &title, Some(authors.as_str()), None, "series_gap", None, 5, None).await?;
                    println!("  Added: \"{}\" to want list", title);
                }
            }
        }
    }

    Ok(())
}

async fn cmd_remove(pool: &db::DbPool, args: &RemoveArgs) -> anyhow::Result<()> {
    let edition = match db::get_edition_by_id(pool, args.id).await? {
        Some(e) => e,
        None => {
            eprintln!("Error: no book with ID {}", args.id);
            std::process::exit(1);
        }
    };

    let title = edition.title.as_deref().unwrap_or("(untitled)");
    let path = std::path::PathBuf::from(&edition.source_path);

    db::delete_edition(pool, args.id).await?;

    if args.delete_file {
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                anyhow::anyhow!("removed from DB but could not delete file {}: {e}", path.display())
            })?;
            println!("Removed: '{}' (file deleted)", title);
        } else {
            eprintln!("Warning: file not found on disk: {}", path.display());
            println!("Removed: '{}' (DB entry only — file was already missing)", title);
        }
    } else {
        println!("Removed: '{}' (DB entry only)", title);
    }

    Ok(())
}

async fn cmd_update(pool: &db::DbPool, args: &UpdateArgs) -> anyhow::Result<()> {
    let edition = match db::get_edition_by_id(pool, args.id).await? {
        Some(e) => e,
        None => {
            eprintln!("Error: no book with ID {}", args.id);
            std::process::exit(1);
        }
    };

    if args.title.is_none() && args.authors.is_none() && args.language.is_none() && args.source_path.is_none() {
        eprintln!("No fields specified. Use --title, --authors, or --language.");
        std::process::exit(1);
    }

    let title_display = edition.title.as_deref().unwrap_or("(untitled)");

    if let Some(ref t) = args.title {
        db::force_set_title(pool, args.id, t).await?;
        println!("Updated title: '{}'", t);
    }
    if let Some(ref a) = args.authors {
        db::force_set_authors(pool, args.id, a).await?;
        println!("Updated authors: '{}'", a);
    }
    if let Some(ref lang) = args.language {
        db::set_language(pool, args.id, lang).await?;
        println!("Updated language: '{}'", lang);
    }
    if let Some(ref sp) = args.source_path {
        db::force_set_source_path(pool, args.id, sp).await?;
        println!("Updated source_path: '{}'", sp);
    }

    if args.title.is_none() {
        println!("(book: '{}')", title_display);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Config command handler
// ---------------------------------------------------------------------------

async fn cmd_config(
    args: &ConfigArgs,
    db_path: &std::path::Path,
    library_dir: &std::path::Path,
    language: &str,
) -> anyhow::Result<()> {
    match &args.command {
        ConfigCommands::Show => {
            let cfg = bookshelf_core::config::load().unwrap_or_default();
            let cfg_path = bookshelf_core::config::config_file_path();
            println!("# bookshelf configuration");
            println!("# config file: {}", cfg_path.display());
            println!();
            println!("# --- Paths ---");
            println!("db                   = \"{}\"", db_path.display());
            println!("library_dir          = \"{}\"", library_dir.display());
            if let Some(ref d) = cfg.downloads_dir {
                println!("downloads_dir        = \"{}\"", d.display());
            } else {
                println!("downloads_dir        = (same as library_dir)");
            }
            println!();
            println!("# --- Library ---");
            println!("language             = \"{}\"", language);
            println!("allowed_formats      = [{}]", cfg.allowed_formats.iter().map(|f| format!("\"{f}\"")).collect::<Vec<_>>().join(", "));
            println!();
            println!("# --- Imports ---");
            if cfg.goodreads_csv_paths.is_empty() {
                println!("goodreads_csv_paths  = []");
            } else {
                let paths: Vec<String> = cfg.goodreads_csv_paths.iter().map(|p| format!("\"{}\"", p.display())).collect();
                println!("goodreads_csv_paths  = [{}]", paths.join(", "));
            }
            if cfg.ol_usernames.is_empty() {
                println!("ol_usernames         = []");
            } else {
                let names: Vec<String> = cfg.ol_usernames.iter().map(|u| format!("\"{u}\"")).collect();
                println!("ol_usernames         = [{}]", names.join(", "));
            }
            println!();
            println!("# --- Standard Ebooks ---");
            println!("se_email             = {}", cfg.se_email.as_deref().map(|_| "(set)").unwrap_or("(not set)"));
            println!("se_password          = {}", cfg.se_password.as_deref().map(|_| "(set)").unwrap_or("(not set)"));
            println!();
            println!("# --- Download ---");
            let sources_display: Vec<String> = cfg.download_sources.iter().map(|s| format!("\"{s}\"")).collect();
            println!("download_sources     = [{}]", sources_display.join(", "));
            println!("request_timeout_secs = {}", cfg.request_timeout_secs);
            println!();
            println!("# --- Daemon ---");
            println!("run_interval_minutes = {} ({} hours)", cfg.run_interval_minutes, cfg.run_interval_minutes / 60);
        }
        ConfigCommands::Set(set_args) => {
            bookshelf_core::config::set_key(&set_args.key, &set_args.value)?;
            println!("Set {} = {}", set_args.key, set_args.value);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Run command — full automation pipeline
// ---------------------------------------------------------------------------

async fn cmd_run(
    pool: &db::DbPool,
    library_dir: &std::path::Path,
    args: &RunArgs,
    app_config: &bookshelf_core::config::AppConfig,
) -> anyhow::Result<()> {
    // -----------------------------------------------------------------------
    // Startup validation — fail fast with a useful message
    // -----------------------------------------------------------------------
    if !library_dir.exists() {
        anyhow::bail!(
            "library_dir '{}' does not exist.\nSet it with: bookshelf config set library_dir <path>",
            library_dir.display()
        );
    }
    if !library_dir.is_dir() {
        anyhow::bail!(
            "library_dir '{}' is not a directory.",
            library_dir.display()
        );
    }
    for csv_path in &app_config.goodreads_csv_paths {
        if !csv_path.exists() {
            eprintln!("WARNING: configured goodreads_csv_path not found: {}", csv_path.display());
        }
    }

    if args.once {
        return run_pipeline_once(pool, library_dir, args, app_config).await;
    }

    // -----------------------------------------------------------------------
    // Daemon mode — single-instance PID file + graceful shutdown
    // -----------------------------------------------------------------------
    let interval_mins = app_config.run_interval_minutes;

    // PID file: warn if another instance may be running.
    let pid_path = db::default_db_path()
        .parent()
        .unwrap_or(Path::new("."))
        .join("daemon.pid");
    if pid_path.exists() {
        eprintln!(
            "WARNING: PID file already exists at {}.\n\
             Another daemon may be running. Delete that file if you are sure no other instance is active.",
            pid_path.display()
        );
    }
    if let Err(e) = std::fs::write(&pid_path, std::process::id().to_string()) {
        eprintln!("WARNING: could not write PID file: {e}");
    }

    // Graceful-shutdown channel: signal handler sends true, loop checks.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            } else {
                tokio::signal::ctrl_c().await.ok();
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.ok();
        eprintln!("\nShutdown signal received — finishing current run then stopping.");
        let _ = shutdown_tx.send(true);
    });

    println!(
        "bookshelf daemon starting — pipeline runs every {} minute{}. Ctrl+C to stop.",
        interval_mins,
        if interval_mins == 1 { "" } else { "s" }
    );

    run_daemon_loop(pool, library_dir, args, app_config, shutdown_rx).await?;

    let _ = std::fs::remove_file(&pid_path);
    println!("bookshelf daemon stopped.");
    Ok(())
}

/// Core pipeline loop shared by the console daemon (`bookshelf run`) and the
/// Windows service. Runs `run_pipeline_once` on the configured interval until
/// `shutdown_rx` receives `true`.
async fn run_daemon_loop(
    pool: &db::DbPool,
    library_dir: &Path,
    run_args: &RunArgs,
    app_config: &bookshelf_core::config::AppConfig,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let interval_mins = app_config.run_interval_minutes;
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        if let Err(e) = run_pipeline_once(pool, library_dir, run_args, app_config).await {
            eprintln!("Pipeline error (will retry next cycle): {e:#}");
        }
        if *shutdown_rx.borrow() {
            break;
        }
        let next = chrono::Local::now() + chrono::Duration::minutes(interval_mins as i64);
        println!("Next run at {}. Sleeping...\n", next.format("%Y-%m-%d %H:%M:%S"));
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(interval_mins * 60)) => {}
            _ = shutdown_rx.changed() => { break; }
        }
    }
    Ok(())
}

async fn run_pipeline_once(
    pool: &db::DbPool,
    library_dir: &std::path::Path,
    args: &RunArgs,
    app_config: &bookshelf_core::config::AppConfig,
) -> anyhow::Result<()> {
    let dry = args.dry_run;
    let client = build_http_client(app_config);
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let language = app_config.language.clone();

    println!("=== bookshelf run [{}]{} ===", timestamp, if dry { " [DRY RUN]" } else { "" });
    println!();

    // -----------------------------------------------------------------------
    // Step 0: Recover editions whose files have gone missing.
    // This catches books deleted by the path-normalisation dedup bug, manual
    // file moves, or external removals. We re-add them to the want list so
    // the download step can restore them, then remove the ghost DB entries.
    // -----------------------------------------------------------------------
    if !dry {
        match db::list_missing_file_editions(pool).await {
            Err(e) => eprintln!("WARNING: could not check for missing files: {e}"),
            Ok(missing) if !missing.is_empty() => {
                println!("[recovery] {} book{} missing from disk — re-queuing for download...",
                    missing.len(), if missing.len() == 1 { "" } else { "s" });
                for edition in &missing {
                    let title = edition.title.as_deref().unwrap_or("");
                    let author = edition.authors.as_deref();
                    if title.is_empty() {
                        // No title: nothing to search for — just remove the ghost entry.
                        let _ = db::delete_edition(pool, edition.id).await;
                        continue;
                    }
                    // Add to want list if not already there.
                    let already = match author {
                        Some(a) => db::want_exists_by_title_author(pool, title, a).await.unwrap_or(false),
                        None    => false,
                    };
                    if !already {
                        let _ = db::insert_want(pool, title, author, edition.isbn.as_deref(),
                            "manual", None, 5, Some("recovered: file missing from disk")).await;
                        println!("  Re-queued: '{}' by {}", title, author.unwrap_or("(unknown)"));
                    }
                    // Remove the ghost DB entry so a fresh download creates a clean record.
                    let _ = db::delete_edition(pool, edition.id).await;
                }
            }
            Ok(_) => {} // nothing missing
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 1: Import Goodreads CSV files
    // -----------------------------------------------------------------------
    if app_config.goodreads_csv_paths.is_empty() {
        println!("[skip] No goodreads_csv_paths configured.");
    } else {
        println!("[1/5] Importing Goodreads lists...");
        let mut total_new = 0usize;
        let mut total_already = 0usize;
        let mut total_owned = 0usize;
        for csv_path in &app_config.goodreads_csv_paths {
            print!("  {} ... ", csv_path.display());
            if !csv_path.exists() {
                println!("SKIP (file not found)");
                continue;
            }
            if dry {
                println!("would import");
                continue;
            }
            match want::import_goodreads_csv(pool, csv_path, false).await {
                Ok(s) => {
                    println!("{} new, {} already wanted, {} owned", s.imported, s.already_wanted, s.skipped_owned);
                    total_new += s.imported;
                    total_already += s.already_wanted;
                    total_owned += s.skipped_owned;
                }
                Err(e) => eprintln!("ERROR: {e}"),
            }
        }
        if !dry {
            println!("  Total: {} new want entries, {} refreshed, {} skipped (owned)", total_new, total_already, total_owned);
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 2: Import OpenLibrary want lists
    // -----------------------------------------------------------------------
    if app_config.ol_usernames.is_empty() {
        println!("[skip] No ol_usernames configured.");
    } else {
        println!("[2/5] Importing OpenLibrary want lists...");
        for username in &app_config.ol_usernames {
            print!("  {} ... ", username);
            if dry {
                println!("would import");
                continue;
            }
            match want::import_openlibrary(pool, &client, username, enrich::OPENLIBRARY_BASE).await {
                Ok(s) => println!("{} new, {} already wanted, {} owned", s.imported, s.already_wanted, s.skipped_owned),
                Err(e) => eprintln!("ERROR: {e}"),
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 2b: Sync followed authors → want list
    // -----------------------------------------------------------------------
    {
        let followed = follow::follow_list(pool).await.unwrap_or_default();
        if followed.is_empty() {
            println!("[skip] No followed authors configured.");
        } else {
            println!("[2b] Syncing followed authors ({} author{})...",
                followed.len(), if followed.len() == 1 { "" } else { "s" });
            if dry {
                println!("  [DRY RUN] Would sync followed authors.");
            } else {
                match follow::follow_sync(pool, &client, enrich::OPENLIBRARY_BASE, Some(&language)).await {
                    Ok(added) if added > 0 => println!("  Queued {} new book{} from followed authors.", added, if added == 1 { "" } else { "s" }),
                    Ok(_) => println!("  No new books from followed authors."),
                    Err(e) => eprintln!("  ERROR syncing followed authors: {e}"),
                }
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 3: Download want list
    // -----------------------------------------------------------------------
    // title_overrides carries (dest_path, want_title, want_authors) for each
    // downloaded book so we can re-apply them after enrich (which may clobber
    // them with bad data from low-quality ISBNs).
    let mut title_overrides: Vec<(String, String, String)> = Vec::new();

    if args.no_download {
        println!("[skip] Download step skipped (--no-download).");
    } else {
        println!("[3/5] Downloading want list...");

        let download_dir = app_config.downloads_dir.as_deref().unwrap_or(library_dir);

        let se_creds = build_se_creds(app_config, "  WARNING: ");
        let sources = build_sources_from_config(app_config, &client, se_creds);

        match bookshelf_core::download::download_want_list(pool, download_dir, &sources, dry).await {
            Err(e) => eprintln!("  ERROR during download: {e}"),
            Ok(result) => {
                title_overrides = result.title_overrides;
                for event in &result.events {
                    match event {
                        bookshelf_core::download::DownloadEvent::Downloaded { title, source_name } => {
                            println!("  Downloaded: {} [{}]", title, source_name);
                        }
                        bookshelf_core::download::DownloadEvent::WouldDownload { title, source_name } => {
                            println!("  [DRY RUN] Would download: {} [{}]", title, source_name);
                        }
                        bookshelf_core::download::DownloadEvent::NotFound { title } => {
                            println!("  Not found: {}", title);
                        }
                    }
                }
                if dry {
                    println!("  {} would download, {} not found.", result.would_download, result.not_found);
                } else {
                    println!("  {} downloaded, {} not found.", result.downloaded, result.not_found);
                }
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 4: Scan library_dir for any new files not yet in DB
    // -----------------------------------------------------------------------
    println!("[4/5] Scanning library for new files...");
    let formats_ref: Vec<&str> = app_config.allowed_formats.iter().map(|s| s.as_str()).collect();
    if dry {
        println!("  [DRY RUN] Would scan: {}", library_dir.display());
    } else {
        match scan::scan_directory(pool, library_dir, &formats_ref).await {
            Err(e) => eprintln!("  ERROR scanning {}: {e}", library_dir.display()),
            Ok(scan_result) => {
                println!("  Scanned {} files, {} new, {} errors", scan_result.scanned, scan_result.inserted, scan_result.errors.len());

                // Enrich any books that haven't been enriched yet
                match db::editions_needing_enrichment(pool).await {
                    Err(e) => eprintln!("  ERROR fetching unenriched editions: {e}"),
                    Ok(unenriched) if unenriched.is_empty() => println!("  All books already enriched."),
                    Ok(unenriched) => {
                        println!("  Enriching {} unenriched book{}...", unenriched.len(), if unenriched.len() == 1 { "" } else { "s" });
                        let enrich_args = EnrichArgs { id: None, dry_run: false };
                        if let Err(e) = enrich_editions_list(pool, &unenriched, &enrich_args, &client).await {
                            eprintln!("  ERROR during enrichment: {e}");
                        }
                    }
                }

                // Re-apply want-list title/authors after enrich — enrich may have
                // overwritten them with garbage from low-quality IA ISBNs.
                for (dest_path, want_title, want_authors) in &title_overrides {
                    let _ = db::overwrite_edition_metadata(pool, dest_path, want_title, want_authors).await;
                }
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 5: Organize library
    // -----------------------------------------------------------------------
    if args.no_organize {
        println!("[skip] Organize step skipped (--no-organize).");
    } else {
        println!("[5/5] Organizing library...");
        if dry {
            println!("  [DRY RUN] Would organize: {}", library_dir.display());
        } else {
            match organize::organize_editions(pool, library_dir, false, false).await {
                Ok(result) => println!("  {} moved, {} skipped, {} errors", result.moved, result.skipped, result.errors.len()),
                Err(e) => eprintln!("  ERROR: {e}"),
            }
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 6: Series gaps — add missing series books to want list
    // -----------------------------------------------------------------------
    println!("[6/6] Checking series gaps...");
    if dry {
        println!("  [DRY RUN] Would check series gaps.");
    } else {
        match series::series_fill(pool, &client, enrich::OL_BASE_URL, Some(&language)).await {
            Ok(added) if added > 0 => println!("  Added {} missing series book{} to want list.", added, if added == 1 { "" } else { "s" }),
            Ok(_) => println!("  No series gaps found."),
            Err(e) => eprintln!("  ERROR checking series gaps: {e}"),
        }
    }
    println!();

    println!("=== Done ===");
    Ok(())
}

// ---------------------------------------------------------------------------
// Db command handler
// ---------------------------------------------------------------------------

async fn cmd_db(pool: &db::DbPool, args: DbArgs) -> anyhow::Result<()> {
    match args.command {
        DbCommands::Backup(b) => {
            if b.dest.exists() {
                anyhow::bail!(
                    "destination '{}' already exists — supply a new path to avoid overwriting a backup.",
                    b.dest.display()
                );
            }
            print!("Backing up to {}... ", b.dest.display());
            db::backup(pool, &b.dest).await?;
            let size_kb = std::fs::metadata(&b.dest)
                .map(|m| m.len() / 1024)
                .unwrap_or(0);
            println!("done ({size_kb} KB)");
        }
        DbCommands::Check => {
            print!("Running integrity check... ");
            match db::integrity_check(pool).await? {
                true  => println!("OK"),
                false => {
                    eprintln!("FAILED — run `sqlite3 <db-path> 'PRAGMA integrity_check'` for details.");
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}

// No unit tests in main.rs — integration tests cover CLI logic indirectly.
// Public functions are all in bookshelf-core which has its own tests.

// ---------------------------------------------------------------------------
// Windows service implementation
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod windows_svc {
    use super::*;
    use std::ffi::OsString;
    use std::time::Duration;
    use windows_service::{
        service::{
            ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl,
            ServiceExitCode, ServiceInfo, ServiceStartType, ServiceState, ServiceStatus,
            ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    pub const SERVICE_NAME: &str = "bookshelf";
    const SERVICE_DISPLAY_NAME: &str = "Bookshelf Library Manager";

    /// Dispatch a `bookshelf service <subcommand>` call.
    pub fn handle(args: &ServiceArgs) -> anyhow::Result<()> {
        match &args.command {
            ServiceCommands::Install   => install(),
            ServiceCommands::Uninstall => uninstall(),
            ServiceCommands::Start     => start(),
            ServiceCommands::Stop      => stop(),
            ServiceCommands::Run       => run_as_service(),
        }
    }

    // -----------------------------------------------------------------------
    // Entry point called by the Windows SCM via define_windows_service!
    // -----------------------------------------------------------------------

    pub fn service_entry(_arguments: Vec<OsString>) {
        if let Err(e) = run_service_inner() {
            eprintln!("bookshelf service fatal error: {e:#}");
        }
    }

    fn run_service_inner() -> anyhow::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Register the SCM control handler. Stop/Shutdown trigger the same
        // shutdown channel that run_daemon_loop listens on.
        let status_handle = service_control_handler::register(SERVICE_NAME, {
            let shutdown_tx = shutdown_tx.clone();
            move |ctrl| match ctrl {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    eprintln!("bookshelf service: stop requested by SCM.");
                    let _ = shutdown_tx.send(true);
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        })
        .context("register service control handler")?;

        let report_state = |state: ServiceState| {
            let accepted = if state == ServiceState::Running {
                ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
            } else {
                ServiceControlAccept::empty()
            };
            let _ = status_handle.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: state,
                controls_accepted: accepted,
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: None,
            });
        };

        report_state(ServiceState::StartPending);

        // Load config and open the database — same logic as async_run.
        let app_config = bookshelf_core::config::load().unwrap_or_default();
        let db_path    = app_config.db.clone().unwrap_or_else(db::default_db_path);
        let library_dir = app_config.library_dir.clone().unwrap_or_else(import::default_library_dir);

        let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
        let pool = rt
            .block_on(db::open(&db_path))
            .with_context(|| format!("cannot open database at {}", db_path.display()))?;

        report_state(ServiceState::Running);
        eprintln!(
            "bookshelf service running — pipeline every {} min.",
            app_config.run_interval_minutes
        );

        // Run the shared daemon loop until SCM sends Stop/Shutdown.
        let run_args = RunArgs::default();
        if let Err(e) = rt.block_on(run_daemon_loop(&pool, &library_dir, &run_args, &app_config, shutdown_rx)) {
            eprintln!("bookshelf service pipeline error: {e:#}");
        }

        report_state(ServiceState::Stopped);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // `bookshelf service run`  — start the SCM dispatcher
    // -----------------------------------------------------------------------

    fn run_as_service() -> anyhow::Result<()> {
        windows_service::service_dispatcher::start(SERVICE_NAME, super::ffi_service_main)
            .context(
                "service dispatcher failed.\n\
                 This command must be run by the Windows Service Control Manager, not manually.\n\
                 Install and start the service first: bookshelf service install && bookshelf service start",
            )
    }

    // -----------------------------------------------------------------------
    // `bookshelf service install`
    // -----------------------------------------------------------------------

    fn install() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(
            None::<&str>,
            ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
        )
        .context("open service manager (run as Administrator)")?;

        let exe = std::env::current_exe().context("resolve current executable path")?;

        let service_info = ServiceInfo {
            name:             OsString::from(SERVICE_NAME),
            display_name:     OsString::from(SERVICE_DISPLAY_NAME),
            service_type:     ServiceType::OWN_PROCESS,
            start_type:       ServiceStartType::AutoStart,
            error_control:    ServiceErrorControl::Normal,
            executable_path:  exe,
            launch_arguments: vec![OsString::from("service"), OsString::from("run")],
            dependencies:     vec![],
            account_name:     None,  // LocalSystem
            account_password: None,
        };

        manager
            .create_service(&service_info, ServiceAccess::CHANGE_CONFIG)
            .context("create service (run as Administrator)")?;

        println!("Service '{SERVICE_NAME}' installed successfully.");
        println!();
        println!("  Start now:    bookshelf service start");
        println!("  Stop:         bookshelf service stop");
        println!("  Uninstall:    bookshelf service uninstall");
        println!();
        println!("  Config file:  {}", bookshelf_core::config::config_file_path().display());
        println!("  Edit it with: bookshelf config set <key> <value>");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // `bookshelf service uninstall`
    // -----------------------------------------------------------------------

    fn uninstall() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .context("open service manager (run as Administrator)")?;

        let service = manager
            .open_service(
                SERVICE_NAME,
                ServiceAccess::STOP | ServiceAccess::DELETE | ServiceAccess::QUERY_STATUS,
            )
            .context("open service — is it installed? (run as Administrator)")?;

        // Stop if running so deletion doesn't leave a zombie entry.
        let status = service.query_status().context("query service status")?;
        if status.current_state != ServiceState::Stopped {
            service.stop().context("stop service before deletion")?;
            std::thread::sleep(Duration::from_secs(2));
        }

        service.delete().context("delete service")?;
        println!("Service '{SERVICE_NAME}' removed.");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // `bookshelf service start`
    // -----------------------------------------------------------------------

    fn start() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .context("open service manager")?;
        let service = manager
            .open_service(SERVICE_NAME, ServiceAccess::START)
            .context("open service — is it installed? run: bookshelf service install")?;
        service
            .start(&[] as &[&std::ffi::OsStr])
            .context("start service")?;
        println!("Service '{SERVICE_NAME}' start requested.");
        println!("Check status in Task Manager > Services or: sc query {SERVICE_NAME}");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // `bookshelf service stop`
    // -----------------------------------------------------------------------

    fn stop() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .context("open service manager")?;
        let service = manager
            .open_service(SERVICE_NAME, ServiceAccess::STOP)
            .context("open service — is it installed? run: bookshelf service install")?;
        service.stop().context("stop service")?;
        println!("Service '{SERVICE_NAME}' stop requested — finishing current pipeline run before exiting.");
        Ok(())
    }
}
