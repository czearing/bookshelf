use anyhow::{Context, Result};

fn main() -> Result<()> {
    let path = std::env::args_os().nth(1).context("Supply an EPUB path")?;
    let (metadata, _) = bookshelf_identify::epub::read(std::path::Path::new(&path))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "title": metadata.title,
            "authors": metadata.authors,
            "publisher": metadata.publisher,
            "published_date": metadata.published_date,
            "language": metadata.content.language,
            "title_author_documents": metadata.content.title_author_documents,
            "identity_conflicts": metadata.content.identity_conflicts,
            "headings": metadata.content.documents.iter().map(|doc| (&doc.path, &doc.headings, &doc.bylines)).collect::<Vec<_>>(),
            "warnings": metadata.warnings
        }))?
    );
    Ok(())
}
