use clap::Parser;

#[tokio::main]
async fn main() {
    let code = match bookshelf_identify::cli::run(bookshelf_identify::cli::Args::parse()).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            2
        }
    };
    std::process::exit(code);
}
