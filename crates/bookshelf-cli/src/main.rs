use anyhow::Context;
use bookshelf_core::{db, enrich, follow, fuzzy, grab, scan, series, want};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bookshelf", about = "EPUB library manager")]
struct Cli {
    /// Override the database file path
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan a directory recursively for EPUB files and populate the library
    Scan(ScanArgs),
    /// List all books in the library
    List,
    /// Fuzzy-search the library by title or author
    Search(SearchArgs),
    /// Show full metadata for a book by ID
    Info(InfoArgs),
    /// Fetch metadata from OpenLibrary and Google Books for un-enriched books
    Enrich,
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
}

#[derive(Parser)]
struct ScanArgs {
    /// Directory to scan recursively for .epub files
    path: PathBuf,
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

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {e:?}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt::init();
    tokio::runtime::Runtime::new()?.block_on(async_run(cli))
}

async fn async_run(cli: Cli) -> anyhow::Result<()> {
    let db_path = cli.db.unwrap_or_else(db::default_db_path);
    let pool = db::open(&db_path)
        .await
        .with_context(|| format!("cannot open database at {}", db_path.display()))?;

    match cli.command {
        Commands::Scan(args) => cmd_scan(&pool, &args.path).await?,
        Commands::List => cmd_list(&pool).await?,
        Commands::Search(args) => cmd_search(&pool, &args.query).await?,
        Commands::Info(args) => cmd_info(&pool, args.id).await?,
        Commands::Enrich => cmd_enrich(&pool).await?,
        Commands::Want(args) => cmd_want(&pool, args).await?,
        Commands::Grab(args) => cmd_grab(&pool, args).await?,
        Commands::Stats => cmd_stats(&pool).await?,
        Commands::Follow(args) => cmd_follow(&pool, args).await?,
        Commands::Series(args) => cmd_series(&pool, args).await?,
    }

    Ok(())
}

async fn cmd_scan(pool: &db::DbPool, path: &std::path::Path) -> anyhow::Result<()> {
    if !path.exists() {
        eprintln!("Error: path does not exist: {}", path.display());
        std::process::exit(1);
    }
    if !path.is_dir() {
        eprintln!("Error: path is not a directory: {}", path.display());
        std::process::exit(1);
    }

    let result = scan::scan_directory(pool, path).await?;
    println!(
        "Scanned {} files, inserted {} new books. ({} errors)",
        result.scanned,
        result.inserted,
        result.errors.len()
    );
    Ok(())
}

async fn cmd_list(pool: &db::DbPool) -> anyhow::Result<()> {
    let editions = db::list_editions(pool).await?;
    if editions.is_empty() {
        println!("No books in library.");
        return Ok(());
    }
    for row in &editions {
        let title = row.title.as_deref().unwrap_or("(no title)");
        let authors = row.authors.as_deref().unwrap_or("(no authors)");
        println!("{}  {}  [{}]", row.id, title, authors);
    }
    Ok(())
}

async fn cmd_search(pool: &db::DbPool, query: &str) -> anyhow::Result<()> {
    let editions = db::list_editions(pool).await?;
    let mut scored: Vec<_> = editions
        .iter()
        .filter_map(|row| {
            let title = row.title.as_deref().unwrap_or("");
            let authors = row.authors.as_deref().unwrap_or("");
            if fuzzy::matches_search(title, authors, query) {
                Some((fuzzy::search_score(title, authors, query), row))
            } else {
                None
            }
        })
        .collect();

    if scored.is_empty() {
        println!("No results for query.");
        return Ok(());
    }

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    for (score, row) in &scored {
        let title = row.title.as_deref().unwrap_or("(no title)");
        let authors = row.authors.as_deref().unwrap_or("(no authors)");
        println!("[{:.3}] {}  {}  [{}]", score, row.id, title, authors);
    }
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
    Ok(())
}

async fn cmd_enrich(pool: &db::DbPool) -> anyhow::Result<()> {
    let editions = db::editions_needing_enrichment(pool).await?;
    if editions.is_empty() {
        println!("All books already enriched.");
        return Ok(());
    }

    let client = reqwest::Client::new();

    for row in &editions {
        let mut isbn = row.isbn.clone();

        // AC-41: if no isbn, try title+author search
        if isbn.is_none() {
            match (row.title.as_deref(), row.authors.as_deref()) {
                (Some(title), Some(authors)) => {
                    match enrich::find_isbn_by_title_author(
                        &client,
                        title,
                        authors,
                        enrich::OPENLIBRARY_BASE,
                    )
                    .await
                    {
                        Ok(Some(found_isbn)) => {
                            tracing::info!(
                                "Found ISBN {} for '{}' via title+author search",
                                found_isbn,
                                title
                            );
                            // AC-41: persist the discovered ISBN to the DB before proceeding
                            let isbn_update = bookshelf_core::db::EnrichmentUpdate {
                                isbn: Some(found_isbn.clone()),
                                ..Default::default()
                            };
                            if let Err(e) =
                                db::apply_enrichment(pool, row.id, &isbn_update).await
                            {
                                eprintln!(
                                    "WARNING: failed to persist discovered ISBN for id={}: {e}",
                                    row.id
                                );
                            }
                            isbn = Some(found_isbn);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            eprintln!(
                                "WARNING: title+author search failed for id={}: {e}",
                                row.id
                            );
                        }
                    }
                }
                _ => {
                    eprintln!(
                        "WARNING: skipping id={}: missing isbn/title/authors",
                        row.id
                    );
                    continue;
                }
            }
        }

        // AC-42: if still no isbn and missing title/authors
        let isbn = match &isbn {
            Some(i) => i.clone(),
            None => {
                eprintln!(
                    "WARNING: skipping id={}: missing isbn/title/authors",
                    row.id
                );
                continue;
            }
        };

        // AC-35/36/37/38/39: try OpenLibrary, fall back to Google Books
        let ol_result = enrich::enrich_from_openlibrary_isbn(
            &client,
            &isbn,
            enrich::OPENLIBRARY_BASE,
        )
        .await;

        let update = match ol_result {
            Ok(Some(u)) => {
                if let Some(ol_id) = &u.ol_work_id {
                    if let Some(wid) = row.work_id {
                        if let Err(e) = db::update_work_ol_id(pool, wid, ol_id).await {
                            eprintln!("WARNING: work OL-ID update failed for id={}: {e}", row.id);
                        }
                    }
                }
                u
            }
            Ok(None) => {
                // OpenLibrary returned empty — try Google Books
                let gb_result = enrich::enrich_from_google_books_isbn(
                    &client,
                    &isbn,
                    enrich::GOOGLE_BOOKS_BASE,
                )
                .await;
                match gb_result {
                    Ok(Some(u)) => u,
                    Ok(None) => {
                        // Both sources empty — mark attempted
                        let empty_update = bookshelf_core::db::EnrichmentUpdate {
                            enrichment_attempted: 1,
                            ..Default::default()
                        };
                        if let Err(e) = db::apply_enrichment(pool, row.id, &empty_update).await {
                            eprintln!("WARNING: DB update failed for id={}: {e}", row.id);
                        }
                        continue;
                    }
                    Err(e) => {
                        eprintln!(
                            "WARNING: Google Books failed for ISBN {isbn}: {e}"
                        );
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("WARNING: OpenLibrary failed for ISBN {isbn}: {e}");
                continue;
            }
        };

        if let Err(e) = db::apply_enrichment(pool, row.id, &update).await {
            eprintln!("WARNING: DB update failed for id={}: {e}", row.id);
        } else {
            println!(
                "Enriched id={} ({})",
                row.id,
                row.title.as_deref().unwrap_or("unknown")
            );
        }
    }

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
    const VALID_SOURCES: &[&str] = &[
        "goodreads_csv",
        "openlibrary",
        "manual",
        "text_file",
        "author_follow",
        "series_fill",
    ];

    if let Some(ref src) = args.source {
        if !VALID_SOURCES.contains(&src.as_str()) {
            eprintln!(
                "Error: invalid source '{src}'. Valid values: {}",
                VALID_SOURCES.join(", ")
            );
            std::process::exit(1);
        }
    }

    let rows = db::list_want(pool, args.source.as_deref()).await?;
    if rows.is_empty() {
        println!("No entries in want list.");
        return Ok(());
    }

    for row in &rows {
        let author = row.author.as_deref().unwrap_or("(none)");
        println!(
            "{}  {}  [{}]  priority:{}  source:{}",
            row.id, row.title, author, row.priority, row.source
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

async fn cmd_grab(pool: &db::DbPool, args: GrabArgs) -> anyhow::Result<()> {
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
    println!("{}", "\u{2500}".repeat(18));
    println!("Books in library:    {:>6}", s.books_in_library);
    println!("  With ISBN:         {:>6}", s.with_isbn);
    println!("  In a series:       {:>6}", s.in_a_series);
    println!("  Enriched:          {:>6}", s.enriched);
    println!();
    println!("Want List");
    println!("{}", "\u{2500}".repeat(18));
    println!("Total entries:       {:>6}", s.want_total);
    println!("  With ISBN:         {:>6}", s.want_with_isbn);
    println!("  By source:");
    println!("    Goodreads CSV:   {:>6}", s.want_by_goodreads_csv);
    println!("    Manual:          {:>6}", s.want_by_manual);
    println!("    OpenLibrary:     {:>6}", s.want_by_openlibrary);
    println!("    Text file:       {:>6}", s.want_by_text_file);
    println!();
    println!("Grab List (not owned):{:>5}", s.grab_count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Follow command handlers
// ---------------------------------------------------------------------------

async fn cmd_follow(pool: &db::DbPool, args: FollowArgs) -> anyhow::Result<()> {
    match args.command {
        FollowCommands::Add(a) => cmd_follow_add(pool, a).await?,
        FollowCommands::Remove(a) => cmd_follow_remove(pool, a).await?,
        FollowCommands::List => cmd_follow_list(pool).await?,
        FollowCommands::Sync => cmd_follow_sync(pool).await?,
    }
    Ok(())
}

async fn cmd_follow_add(pool: &db::DbPool, args: FollowAddArgs) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match follow::follow_add(pool, &client, &args.author, enrich::OPENLIBRARY_BASE).await {
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

async fn cmd_follow_sync(pool: &db::DbPool) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match follow::follow_sync(pool, &client, enrich::OPENLIBRARY_BASE).await {
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

async fn cmd_series(pool: &db::DbPool, args: SeriesArgs) -> anyhow::Result<()> {
    match args.command {
        None => cmd_series_show(pool).await?,
        Some(SeriesCommands::Fill) => cmd_series_fill(pool).await?,
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

async fn cmd_series_fill(pool: &db::DbPool) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    match series::series_fill(pool, &client, enrich::OPENLIBRARY_BASE).await {
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

// No unit tests in main.rs — integration tests cover CLI logic indirectly.
// Public functions are all in bookshelf-core which has its own tests.
