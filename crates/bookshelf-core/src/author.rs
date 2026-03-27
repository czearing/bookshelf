/// Normalize a single raw author name to "Firstname Lastname" form.
///
/// Handles:
/// - "Lastname, Firstname" → "Firstname Lastname"
/// - "Lastname, Firstname, 1832-1898" → "Firstname Lastname"
/// - "Firstname Lastname" → unchanged
/// - "" → ""
pub fn normalize_author(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let parts: Vec<&str> = raw.splitn(3, ", ").collect();
    match parts.as_slice() {
        [single] => single.to_string(),
        [last, first, ..] => {
            // Check if "first" is actually a date/junk token
            if is_date_like(first) {
                last.to_string()
            } else {
                format!("{} {}", first, last)
            }
        }
        _ => raw.to_string(),
    }
}

fn is_date_like(s: &str) -> bool {
    let s = s.trim();
    // Starts with 3+ digits (e.g. "1832", "1832-1898", "1832-98")
    if s.chars().take(3).all(|c| c.is_ascii_digit()) {
        return true;
    }
    // Abbreviated catalog entries: "fl. 1640", "b. 1800", "d. 1900", "active 1500s"
    let lower = s.to_lowercase();
    lower.starts_with("fl.")
        || lower.starts_with("b.")
        || lower.starts_with("d.")
        || lower.starts_with("active")
}

/// Normalize a comma-separated list of author names, each of which may be in
/// "Lastname, Firstname" or "Firstname Lastname" form.
///
/// The tricky part: the separator between names is also ", " — the same as the
/// separator inside an inverted name. We use a heuristic:
///
/// Walk the `, `-split tokens. If the current token has no space in it
/// (could be a bare surname like "Tolkien") AND the next token is not date-like
/// AND the current token is not already the second half of an inverted name,
/// treat (token[i], token[i+1]) as (Lastname, Firstname).
///
/// Examples:
///   "Tolkien, J.R.R."               → "J.R.R. Tolkien"
///   "Pratchett, Terry, Gaiman, Neil" → "Terry Pratchett, Neil Gaiman"
///   "Carroll, Lewis, 1832-1898"      → "Lewis Carroll"
///   "Terry Pratchett, Neil Gaiman"   → "Terry Pratchett, Neil Gaiman"
///   "Frank Herbert"                  → "Frank Herbert"
pub fn normalize_authors(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    let tokens: Vec<&str> = raw.split(", ").collect();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let cur = tokens[i].trim();
        // If the current token contains a space it's already a forward-order name ("Terry Pratchett")
        if cur.contains(' ') {
            out.push(normalize_author(cur));
            i += 1;
            continue;
        }
        // Look ahead
        let next = tokens.get(i + 1).map(|s| s.trim());
        match next {
            Some(n) if is_date_like(n) => {
                // "Lastname, date" or "Lastname, Firstname, date" — consume up to 3 tokens
                let after = tokens.get(i + 2).map(|s| s.trim());
                let reconstructed = if let Some(a) = after {
                    format!("{}, {}, {}", cur, n, a)
                } else {
                    format!("{}, {}", cur, n)
                };
                out.push(normalize_author(&reconstructed));
                i += if after.is_some() { 3 } else { 2 };
            }
            Some(n) => {
                // "Lastname, Firstname" inverted pair — also check if a date follows
                let after = tokens.get(i + 2).map(|s| s.trim());
                if let Some(a) = after {
                    if is_date_like(a) {
                        // "Lastname, Firstname, date" — consume all three, drop date
                        out.push(format!("{} {}", n, cur));
                        i += 3;
                    } else {
                        out.push(format!("{} {}", n, cur));
                        i += 2;
                    }
                } else {
                    out.push(format!("{} {}", n, cur));
                    i += 2;
                }
            }
            None => {
                // Lone surname with no pair — output as-is
                out.push(cur.to_string());
                i += 1;
            }
        }
    }
    out.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_author_forward_order() {
        assert_eq!(normalize_author("Terry Pratchett"), "Terry Pratchett");
    }

    #[test]
    fn test_normalize_author_inverted() {
        assert_eq!(normalize_author("Tolkien, J.R.R."), "J.R.R. Tolkien");
    }

    #[test]
    fn test_normalize_author_inverted_with_date() {
        assert_eq!(normalize_author("Carroll, Lewis, 1832-1898"), "Lewis Carroll");
    }

    #[test]
    fn test_normalize_author_date_only_suffix() {
        assert_eq!(normalize_author("Plato, 428-348 B.C"), "Plato");
    }

    #[test]
    fn test_normalize_author_empty() {
        assert_eq!(normalize_author(""), "");
    }

    #[test]
    fn test_normalize_author_idempotent_inverted() {
        let once = normalize_author("Tolkien, J.R.R.");
        let twice = normalize_author(&once);
        assert_eq!(once, twice);
    }

    #[test]
    fn test_normalize_authors_multiple_inverted() {
        assert_eq!(
            normalize_authors("Pratchett, Terry, Gaiman, Neil"),
            "Terry Pratchett, Neil Gaiman"
        );
    }

    #[test]
    fn test_normalize_authors_forward_order_multi() {
        assert_eq!(
            normalize_authors("Terry Pratchett, Neil Gaiman"),
            "Terry Pratchett, Neil Gaiman"
        );
    }

    #[test]
    fn test_normalize_authors_single_inverted_with_date() {
        assert_eq!(normalize_authors("Carroll, Lewis, 1832-1898"), "Lewis Carroll");
    }

    #[test]
    fn test_normalize_authors_single_forward() {
        assert_eq!(normalize_authors("Frank Herbert"), "Frank Herbert");
    }

    #[test]
    fn test_normalize_authors_already_normalized_is_idempotent() {
        let input = "J.R.R. Tolkien, Neil Gaiman";
        let once = normalize_authors(input);
        let twice = normalize_authors(&once);
        assert_eq!(once, twice);
    }
}
