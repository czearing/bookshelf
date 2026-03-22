use anyhow::Context;
use bookshelf_core::{db, enrich, fuzzy, scan};
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

// No unit tests in main.rs — integration tests cover CLI logic indirectly.
// Public functions are all in bookshelf-core which has its own tests.
