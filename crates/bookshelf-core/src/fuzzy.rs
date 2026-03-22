use strsim::jaro_winkler;

/// Similarity threshold for work deduplication during scan.
/// Two books with no ISBN scoring >= this value share a `work_id`.
pub const DEDUP_THRESHOLD: f64 = 0.85;

/// Similarity threshold for the `bookshelf search` command.
/// Results scoring >= this value are returned.
pub const SEARCH_THRESHOLD: f64 = 0.72;

/// Compute similarity between two book identities. Returns the maximum score
/// from three comparisons: title-only, authors-only (when same author), and
/// the concatenated "title authors" string. This gives the best signal for
/// partial title matches (e.g. "The Hobbit" vs "The Hobbit, or There and Back Again").
pub fn book_similarity(
    title_a: &str,
    authors_a: &str,
    title_b: &str,
    authors_b: &str,
) -> f64 {
    let combined_a = format!("{title_a} {authors_a}");
    let combined_b = format!("{title_b} {authors_b}");
    let s_combined = jaro_winkler(&combined_a, &combined_b);
    let s_title = jaro_winkler(title_a, title_b);
    let s_authors = jaro_winkler(authors_a, authors_b);
    // Boost by combining title score with author identity: if same author,
    // title score is the primary dedup signal.
    let s_title_boosted = if s_authors > 0.9 { s_title } else { 0.0 };
    s_combined.max(s_title_boosted)
}

/// Return `true` if two books are similar enough to be treated as the
/// same work (score >= `DEDUP_THRESHOLD`).
pub fn is_same_work(
    title_a: &str,
    authors_a: &str,
    title_b: &str,
    authors_b: &str,
) -> bool {
    book_similarity(title_a, authors_a, title_b, authors_b) >= DEDUP_THRESHOLD
}

/// Score a book against a search query. Returns the highest score from:
/// title alone, authors alone, the combined "title authors" string, and
/// each individual word in the title (so a single-word typo query like
/// "Hobitt" can match "The Hobbit" via word-level scoring).
pub fn search_score(title: &str, authors: &str, query: &str) -> f64 {
    let combined = format!("{title} {authors}");
    let s1 = jaro_winkler(title, query);
    let s2 = jaro_winkler(authors, query);
    let s3 = jaro_winkler(&combined, query);
    // Also try each word in the title so a single-word query can match.
    let s_word = title
        .split_whitespace()
        .map(|word| jaro_winkler(word, query))
        .fold(0.0_f64, f64::max);
    s1.max(s2).max(s3).max(s_word)
}

/// Return `true` if a book matches the search query above `SEARCH_THRESHOLD`.
pub fn matches_search(title: &str, authors: &str, query: &str) -> bool {
    search_score(title, authors, query) >= SEARCH_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_same_work_identical() {
        assert!(is_same_work("The Hobbit", "Tolkien", "The Hobbit", "Tolkien"));
    }

    #[test]
    fn test_is_same_work_slight_variation() {
        // "The Hobbit, or There and Back Again" vs "The Hobbit" — same authors
        assert!(is_same_work(
            "The Hobbit, or There and Back Again",
            "J.R.R. Tolkien",
            "The Hobbit",
            "J.R.R. Tolkien"
        ));
    }

    #[test]
    fn test_is_same_work_different_books() {
        assert!(!is_same_work(
            "War and Peace",
            "Tolstoy",
            "Crime and Punishment",
            "Dostoevsky"
        ));
    }

    #[test]
    fn test_matches_search_typo() {
        // "Hobitt" vs "The Hobbit" — one transposition, should pass at 0.72
        assert!(matches_search("The Hobbit", "J.R.R. Tolkien", "Hobitt"));
    }

    #[test]
    fn test_matches_search_no_match() {
        assert!(!matches_search(
            "War and Peace",
            "Tolstoy",
            "Neuromancer"
        ));
    }

    #[test]
    fn test_search_score_returns_max() {
        // Score against title should dominate when it is a good match
        let s = search_score("The Hobbit", "J.R.R. Tolkien", "The Hobbit");
        assert!(s > 0.95);
    }

    #[test]
    fn test_dedup_threshold_constant() {
        assert_eq!(DEDUP_THRESHOLD, 0.85);
    }

    #[test]
    fn test_search_threshold_constant() {
        assert_eq!(SEARCH_THRESHOLD, 0.72);
    }
}
