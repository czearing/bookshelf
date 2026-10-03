use serde::Serialize;
use std::collections::BTreeSet;
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

pub fn normalized(value: &str) -> String {
    value
        .nfkd()
        .filter(|c| !is_combining_mark(*c))
        .filter(|c| !matches!(c, '\'' | '\u{2019}' | '\u{2018}' | '\u{02bc}'))
        .flat_map(char::to_lowercase)
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn author_key(value: &str) -> BTreeSet<String> {
    let without_dates = value
        .split(',')
        .filter(|part| {
            let part = part.trim();
            !(part.chars().filter(|c| c.is_ascii_digit()).count() >= 3
                && part
                    .chars()
                    .all(|c| c.is_ascii_digit() || c.is_whitespace() || "-–?.".contains(c)))
        })
        .collect::<Vec<_>>()
        .join(" ");
    normalized(&without_dates)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

pub fn same_author(left: &str, right: &str) -> bool {
    let a = author_key(left);
    !a.is_empty() && a == author_key(right)
}

/// Leading articles in the languages the library handles. A title with or without
/// one ("The Time Machine" / "Time Machine") is the same title.
const ARTICLES: &[&str] = &[
    "the", "a", "an", "le", "la", "les", "el", "los", "las", "il", "lo", "der", "die", "das", "de",
    "het", "een", "un", "une", "ein", "eine", "o", "os", "as",
];

fn without_leading_article(text: &str) -> &str {
    match text.split_once(' ') {
        Some((first, rest)) if ARTICLES.contains(&first) && !rest.is_empty() => rest,
        _ => text,
    }
}

pub fn title_similarity(left: &str, right: &str) -> f64 {
    let left = normalized(left);
    let right = normalized(right);
    let left = without_leading_article(&left).to_owned();
    let right = without_leading_article(&right).to_owned();
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    if left == right {
        return 1.0;
    }
    let a: BTreeSet<_> = left.split_whitespace().collect();
    let b: BTreeSet<_> = right.split_whitespace().collect();
    if a.symmetric_difference(&b).any(|word| {
        word.chars().any(|c| c.is_numeric())
            || [
                "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
                "first", "second", "third", "fourth", "fifth", "i", "ii", "iii", "iv", "v", "vi",
                "vii", "viii", "ix", "x",
            ]
            .contains(word)
    }) {
        return 0.0;
    }
    let overlap = a.intersection(&b).count() as f64 / a.union(&b).count() as f64;
    let edit = strsim::normalized_levenshtein(&left, &right);
    if edit >= 0.92 && overlap >= 0.6 {
        edit
    } else {
        0.0
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DocumentEvidence {
    pub path: String,
    pub front_matter: bool,
    pub headings: Vec<String>,
    pub lines: Vec<String>,
    pub bylines: Vec<String>,
    /// Short passages from the body text, used to ask a full-text index which
    /// book this is (independent of the file's own tags and title page).
    pub passages: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ContentEvidence {
    pub documents: Vec<DocumentEvidence>,
    pub language: crate::language::Assessment,
    pub title_author_documents: Vec<String>,
    pub identity_conflicts: Vec<String>,
    /// Passages of the body text for a full-text search (best first).
    pub passages: Vec<String>,
}

fn visible(node: roxmltree::Node<'_, '_>) -> bool {
    !node.ancestors().any(|ancestor| {
        if !ancestor.is_element() {
            return false;
        }
        let name = ancestor.tag_name().name();
        let id = ancestor.attribute("id").unwrap_or("").to_lowercase();
        let kind = ancestor
            .attribute(("http://www.idpf.org/2007/ops", "type"))
            .unwrap_or("");
        matches!(name, "script" | "style" | "nav" | "footer" | "head")
            || ancestor.attribute("hidden").is_some()
            || ancestor.attribute("aria-hidden") == Some("true")
            || ["pg-header", "pg-footer", "pglicense"]
                .iter()
                .any(|prefix| id.starts_with(prefix))
            || kind
                .split_whitespace()
                .any(|part| matches!(part, "toc" | "copyright-page" | "colophon"))
    })
}

fn node_text(node: roxmltree::Node<'_, '_>) -> String {
    node.descendants()
        .filter(|n| n.is_text() && visible(*n))
        .filter_map(|n| n.text())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Named character references that HTML (and many XHTML EPUBs) use without
/// declaring them. XML itself defines only five; anything else is an error.
const LATIN1: [&str; 96] = [
    "nbsp", "iexcl", "cent", "pound", "curren", "yen", "brvbar", "sect", "uml", "copy", "ordf",
    "laquo", "not", "shy", "reg", "macr", "deg", "plusmn", "sup2", "sup3", "acute", "micro",
    "para", "middot", "cedil", "sup1", "ordm", "raquo", "frac14", "frac12", "frac34", "iquest",
    "Agrave", "Aacute", "Acirc", "Atilde", "Auml", "Aring", "AElig", "Ccedil", "Egrave", "Eacute",
    "Ecirc", "Euml", "Igrave", "Iacute", "Icirc", "Iuml", "ETH", "Ntilde", "Ograve", "Oacute",
    "Ocirc", "Otilde", "Ouml", "times", "Oslash", "Ugrave", "Uacute", "Ucirc", "Uuml", "Yacute",
    "THORN", "szlig", "agrave", "aacute", "acirc", "atilde", "auml", "aring", "aelig", "ccedil",
    "egrave", "eacute", "ecirc", "euml", "igrave", "iacute", "icirc", "iuml", "eth", "ntilde",
    "ograve", "oacute", "ocirc", "otilde", "ouml", "divide", "oslash", "ugrave", "uacute", "ucirc",
    "uuml", "yacute", "thorn", "yuml",
];

fn named_entity(name: &str) -> Option<char> {
    if let Some(index) = LATIN1.iter().position(|known| *known == name) {
        return char::from_u32(160 + index as u32);
    }
    Some(match name {
        "OElig" => '\u{152}',
        "oelig" => '\u{153}',
        "Scaron" => '\u{160}',
        "scaron" => '\u{161}',
        "Yuml" => '\u{178}',
        "fnof" => '\u{192}',
        "circ" => '\u{2c6}',
        "tilde" => '\u{2dc}',
        "ensp" => '\u{2002}',
        "emsp" => '\u{2003}',
        "thinsp" => '\u{2009}',
        "zwnj" => '\u{200c}',
        "zwj" => '\u{200d}',
        "lrm" => '\u{200e}',
        "rlm" => '\u{200f}',
        "ndash" => '\u{2013}',
        "mdash" => '\u{2014}',
        "lsquo" => '\u{2018}',
        "rsquo" => '\u{2019}',
        "sbquo" => '\u{201a}',
        "ldquo" => '\u{201c}',
        "rdquo" => '\u{201d}',
        "bdquo" => '\u{201e}',
        "dagger" => '\u{2020}',
        "Dagger" => '\u{2021}',
        "bull" => '\u{2022}',
        "hellip" => '\u{2026}',
        "permil" => '\u{2030}',
        "prime" => '\u{2032}',
        "Prime" => '\u{2033}',
        "lsaquo" => '\u{2039}',
        "rsaquo" => '\u{203a}',
        "euro" => '\u{20ac}',
        "trade" => '\u{2122}',
        "larr" => '\u{2190}',
        "rarr" => '\u{2192}',
        "minus" => '\u{2212}',
        _ => return None,
    })
}

/// Replaces known HTML named references with the characters they stand for. The
/// five XML entities, numeric references and any unknown name are left alone, so
/// an undeclared custom entity still fails to parse.
fn expand_named_entities(xml: &str) -> std::borrow::Cow<'_, str> {
    if !xml.contains('&') {
        return std::borrow::Cow::Borrowed(xml);
    }
    let mut out = String::with_capacity(xml.len());
    let mut rest = xml;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let replacement = rest[1..]
            .find(';')
            .filter(|end| *end > 0 && *end <= 8)
            .and_then(|end| named_entity(&rest[1..=end]).map(|c| (c, end + 2)));
        match replacement {
            Some((c, consumed)) => {
                out.push(c);
                rest = &rest[consumed..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

/// Resolves internal-subset entity declarations of the one harmless shape:
/// `<!ENTITY name "&#NNN;">`, a name for a single character. The declarations
/// are removed and the references replaced. Any other declaration (text values,
/// SYSTEM/PUBLIC, parameter entities, nesting) is refused, so entity expansion
/// can never be used to inflate a document.
fn resolve_declared_entities(xml: &str) -> anyhow::Result<std::borrow::Cow<'_, str>> {
    let Some(start) = xml.find("<!ENTITY") else {
        return Ok(std::borrow::Cow::Borrowed(xml));
    };
    let doctype = xml[..start]
        .rfind("<!DOCTYPE")
        .ok_or_else(|| anyhow::anyhow!("Entity declaration outside a DOCTYPE"))?;
    let open = xml[doctype..]
        .find('[')
        .map(|i| doctype + i)
        .ok_or_else(|| anyhow::anyhow!("Unsupported entity declaration"))?;
    let close = xml[open..]
        .find("]>")
        .map(|i| open + i)
        .ok_or_else(|| anyhow::anyhow!("Unterminated DOCTYPE internal subset"))?;
    let mut names: Vec<(String, char)> = Vec::new();
    let mut subset = &xml[open + 1..close];
    while let Some(at) = subset.find("<!ENTITY") {
        let rest = &subset[at + "<!ENTITY".len()..];
        let end = rest
            .find('>')
            .ok_or_else(|| anyhow::anyhow!("Unterminated entity declaration"))?;
        let declaration = rest[..end].trim();
        let (name, value) = declaration
            .split_once(char::is_whitespace)
            .ok_or_else(|| anyhow::anyhow!("Unsupported entity declaration"))?;
        let value = value.trim();
        let quote = value.chars().next();
        let inner = match quote {
            Some(q @ ('"' | '\'')) => value.strip_prefix(q).and_then(|v| v.strip_suffix(q)),
            _ => None,
        };
        let digits = inner
            .and_then(|v| v.strip_prefix("&#"))
            .and_then(|v| v.strip_suffix(';'));
        let code = match digits {
            Some(d) => match d.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok(),
                None => d.parse::<u32>().ok(),
            },
            None => None,
        };
        let character = code.and_then(char::from_u32).filter(|c| !c.is_control());
        let valid_name = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
        match (valid_name, character) {
            (true, Some(c)) if names.len() < 64 => names.push((name.to_owned(), c)),
            _ => anyhow::bail!(
                "Unsupported entity declaration; only single-character references are accepted"
            ),
        }
        subset = &rest[end + 1..];
    }
    let mut out = String::with_capacity(xml.len());
    out.push_str(&xml[..open]);
    out.push_str(&xml[close + 1..]);
    for (name, character) in names {
        out = out.replace(&format!("&{name};"), &character.to_string());
    }
    Ok(std::borrow::Cow::Owned(out))
}

pub fn read_document(path: &str, xml: &str) -> anyhow::Result<(DocumentEvidence, String)> {
    let resolved = resolve_declared_entities(xml)?;
    let expanded = expand_named_entities(&resolved);
    let doc = roxmltree::Document::parse_with_options(
        &expanded,
        roxmltree::ParsingOptions {
            allow_dtd: true,
            nodes_limit: 200_000,
        },
    )?;
    let lower_path = path.to_lowercase();
    let front_matter = ["title", "front", "cover"]
        .iter()
        .any(|part| lower_path.contains(part))
        || doc.descendants().any(|n| {
            n.attribute(("http://www.idpf.org/2007/ops", "type"))
                .is_some_and(|kind| kind.split_whitespace().any(|kind| kind == "titlepage"))
        });
    let mut result = DocumentEvidence {
        path: path.to_owned(),
        front_matter,
        ..Default::default()
    };
    let mut paragraphs = Vec::new();
    let mut visible_letters = 0;
    for node in doc.descendants().filter(|n| n.is_element() && visible(*n)) {
        let name = node.tag_name().name();
        if !matches!(name, "h1" | "h2" | "h3" | "p" | "div") {
            continue;
        }
        if name == "div" && node.children().any(|n| n.is_element()) {
            continue;
        }
        let text = node_text(node);
        if text.is_empty() {
            continue;
        }
        let lower = text.to_lowercase();
        if lower.contains("project gutenberg")
            || lower.contains("gutenberg.org")
            || lower.contains("electronic work")
            || lower.contains("ebook license")
        {
            continue;
        }
        visible_letters += text.len();
        if matches!(name, "h1" | "h2" | "h3")
            && text.chars().count() <= 240
            && result.headings.len() < 24
        {
            result.headings.push(text.clone());
        }
        if (front_matter || visible_letters <= 3000)
            && text.chars().count() <= 240
            && result.lines.len() < 40
        {
            result.lines.push(text.clone());
            for prefix in ["by ", "author: ", "par ", "von ", "por "] {
                if lower.starts_with(prefix) {
                    let byline = text[prefix.len()..].trim().to_owned();
                    if !byline.is_empty() {
                        result.bylines.push(byline);
                    }
                }
            }
        }
        if !front_matter && name == "p" && text.chars().filter(|c| c.is_alphabetic()).count() >= 80
        {
            paragraphs.push(text);
        }
    }
    result.passages = pick_passages(&paragraphs);
    let mut starts = vec![0, paragraphs.len() / 2, paragraphs.len().saturating_sub(1)];
    starts.sort_unstable();
    starts.dedup();
    let mut samples = Vec::new();
    for start in starts {
        let mut sample = String::new();
        let mut remaining = 2000;
        for paragraph in paragraphs.iter().skip(start) {
            let part: String = paragraph.chars().take(remaining).collect();
            remaining -= part.chars().count();
            sample.push_str(&part);
            sample.push('\n');
            if remaining == 0 || sample.chars().filter(|c| c.is_alphabetic()).count() >= 600 {
                break;
            }
        }
        if !sample.trim().is_empty() && !samples.contains(&sample) {
            samples.push(sample);
        }
    }
    let prose = samples.join("\n");
    Ok((result, prose))
}

/// Fourteen-word passages from running prose: from a quarter, half and three quarters
/// of the way through the paragraphs, skipping anything that would not identify a
/// book (digits, quotation marks, web addresses, licence text, very short lines).
fn pick_passages(paragraphs: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for fraction in [2usize, 1, 3] {
        let start = paragraphs.len() * fraction / 4;
        for paragraph in paragraphs.iter().skip(start).take(12) {
            let words: Vec<&str> = paragraph.split_whitespace().collect();
            if words.len() < 30 {
                continue;
            }
            let chunk = words[3..17].join(" ");
            let lower = chunk.to_lowercase();
            let bad = chunk.chars().any(|c| {
                c.is_ascii_digit()
                    || matches!(c, '"' | '\u{201c}' | '\u{201d}' | '<' | '>' | '[' | ']')
            }) || ["http", "gutenberg", "copyright", "www."]
                .iter()
                .any(|marker| lower.contains(marker));
            if !bad && !out.contains(&chunk) {
                out.push(chunk);
                break;
            }
        }
    }
    out
}

/// Words that join or introduce separate credits rather than belonging to a name.
const CREDIT_WORDS: &[&str] = &[
    "and",
    "with",
    "by",
    "et",
    "und",
    "y",
    "translated",
    "translation",
    "edited",
    "illustrated",
    "illustrations",
    "preface",
    "introduction",
    "traduit",
    "traduction",
    "uebersetzt",
    "ubersetzt",
    "traducido",
    "foreword",
    "notes",
];

fn name_tokens(piece: &str) -> Vec<String> {
    normalized(piece)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Splits a credit line into individual credited names: at punctuation that
/// separates credits, and at words that introduce another contributor.
fn credit_segments(credit: &str) -> Vec<Vec<String>> {
    let mut segments = Vec::new();
    for piece in credit.split([',', ';', '&', '/', '|', ':']) {
        let mut current: Vec<String> = Vec::new();
        for token in name_tokens(piece) {
            if CREDIT_WORDS.contains(&token.as_str()) {
                if !current.is_empty() {
                    segments.push(std::mem::take(&mut current));
                }
            } else {
                current.push(token);
            }
        }
        if !current.is_empty() {
            segments.push(current);
        }
    }
    segments
}

/// How two name tokens relate: identical, one an initial of the other, or neither.
#[derive(PartialEq)]
enum Token {
    Same,
    Initial,
    Different,
}

fn compare_tokens(left: &str, right: &str) -> Token {
    if left == right {
        Token::Same
    } else if (left.chars().count() == 1 && right.starts_with(left))
        || (right.chars().count() == 1 && left.starts_with(right))
    {
        Token::Initial
    } else {
        Token::Different
    }
}

/// Every token of `small` has a counterpart in `large`. When any counterpart is
/// only an initial, the surname must also close the credited name, so that
/// "J. E. Austen-Leigh" cannot stand in for "Jane Austen".
fn covered(small: &[String], large: &[String], surname: &str) -> bool {
    let mut initial = false;
    for token in small {
        let best = large.iter().map(|other| compare_tokens(token, other)).fold(
            Token::Different,
            |best, next| match (&best, &next) {
                (Token::Same, _) => best,
                (_, Token::Same) => next,
                (Token::Initial, _) => best,
                _ => next,
            },
        );
        match best {
            Token::Same => {}
            Token::Initial => initial = true,
            Token::Different => return false,
        }
    }
    !initial || large.last().map(String::as_str) == Some(surname)
}

/// Whether a credit line names `author`, tolerating initials, omitted middle
/// names, bracketed expansions and trailing publisher/date text. This compares
/// two pieces of local evidence about the same file; it is never used to match
/// a provider record, which requires the strict `same_author`.
pub fn credit_names_author(author: &str, credit: &str) -> bool {
    if same_author(author, credit) {
        return true;
    }
    let author = strip_brackets(author);
    let credit = strip_brackets(credit);
    let surname = match author.split_once(',') {
        Some((last, _)) => name_tokens(last).pop(),
        None => name_tokens(&author).pop(),
    };
    let Some(surname) = surname else {
        return false;
    };
    let wanted = name_tokens(&author.replace(',', " "));
    credit_segments(&credit).iter().any(|segment| {
        segment.contains(&surname)
            && (covered(&wanted, segment, &surname)
                || (segment.len() >= 2 && covered(segment, &wanted, &surname)))
    })
}

fn strip_brackets(value: &str) -> String {
    let mut depth = 0u32;
    value
        .chars()
        .filter(|c| match c {
            '(' | '[' => {
                depth += 1;
                false
            }
            ')' | ']' => {
                depth = depth.saturating_sub(1);
                false
            }
            _ => depth == 0,
        })
        .collect()
}

fn has_volume_marker(text: &str) -> bool {
    name_tokens(text).iter().any(|word| {
        word.chars().any(|c| c.is_numeric())
            || [
                "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
                "first", "second", "third", "fourth", "fifth", "i", "ii", "iii", "iv", "v", "vi",
                "vii", "viii", "ix", "x", "volume", "vol", "part", "book", "tome",
            ]
            .contains(&word.as_str())
    })
}

/// Joins letter-spaced display titles ("D R A C U L A") back into words.
fn despaced(line: &str) -> String {
    let words: Vec<_> = line.split_whitespace().collect();
    if words.len() >= 3 && words.iter().all(|w| w.chars().count() == 1) {
        words.concat()
    } else {
        line.to_owned()
    }
}

fn subtitle_free(title: &str) -> Option<&str> {
    let cut = title
        .char_indices()
        .find(|(_, c)| matches!(c, ':' | ';' | '/'))
        .map(|(i, _)| i)?;
    let (main, rest) = title.split_at(cut);
    (!main.trim().is_empty() && !has_volume_marker(rest)).then_some(main.trim())
}

/// Whether the page shows the title: as one line, as up to three consecutive
/// lines (title and subtitle set on separate lines), or, when the package title
/// carries a subtitle with no volume or part marker, as its main title.
fn title_on_page(title: &str, lines: &[&String]) -> bool {
    let main = subtitle_free(title);
    for start in 0..lines.len() {
        let mut joined = String::new();
        for line in lines.iter().skip(start).take(3) {
            if !joined.is_empty() {
                joined.push(' ');
            }
            joined.push_str(&despaced(line));
            if title_similarity(title, &joined) >= 0.92
                || main.is_some_and(|m| title_similarity(m, &joined) >= 0.92)
            {
                return true;
            }
        }
    }
    false
}

/// Whether a document's own headings or opening lines show `title`.
pub fn shows_title(title: &str, doc: &DocumentEvidence) -> bool {
    let lines: Vec<&String> = doc.headings.iter().chain(&doc.lines).collect();
    title_on_page(title, &lines)
}

pub fn corroborate(
    content: &mut ContentEvidence,
    title: &str,
    authors: &[String],
    translators: &[String],
) {
    content.title_author_documents.clear();
    content.identity_conflicts.clear();
    for (position, doc) in content.documents.iter().enumerate() {
        let mut lines: Vec<&String> = doc.headings.iter().collect();
        // A title page may be an image; the printed title and author then appear
        // in the next few documents (preface, half-title), so those count too.
        if doc.front_matter {
            lines.extend(doc.lines.iter());
        } else if position < 4 && doc.headings.is_empty() {
            // Only a heading-less document, and only its opening lines: a
            // half-title, not a page that has its own heading such as "References".
            lines.extend(doc.lines.iter().take(8));
        }
        let credits: Vec<&String> = doc
            .bylines
            .iter()
            .filter(|credit| !translators.iter().any(|t| credit_names_author(t, credit)))
            .collect();
        let names_primary = |pool: &[&String]| {
            authors.first().is_some_and(|primary| {
                pool.iter().any(|line| {
                    credit_names_author(primary, line)
                        || credit_names_author(primary, line.strip_prefix("by ").unwrap_or(line))
                })
            })
        };
        let title_matches = title_on_page(title, &lines);
        let everything: Vec<&String> =
            if doc.front_matter || (position < 4 && doc.headings.is_empty()) {
                doc.lines
                    .iter()
                    .take(if doc.front_matter { usize::MAX } else { 8 })
                    .chain(doc.bylines.iter())
                    .collect()
            } else {
                doc.lines.iter().chain(doc.bylines.iter()).collect()
            };
        if title_matches && names_primary(&everything) {
            content.title_author_documents.push(doc.path.clone());
        }
        // Only a page that plays the role of a title page can contradict the
        // package: front matter, or one of the first two documents. An essay or
        // chapter with its own heading and byline says nothing about the book.
        let title_page = doc.front_matter || position < 2;
        let has_identity_heading = doc.headings.iter().any(|line| {
            let key = normalized(line);
            !key.is_empty()
                && ![
                    "chapter",
                    "part",
                    "book",
                    "contents",
                    "introduction",
                    "preface",
                    "foreword",
                    "dedication",
                ]
                .iter()
                .any(|word| key == *word || key.starts_with(&format!("{word} ")))
        });
        if title_page
            && has_identity_heading
            && !credits.is_empty()
            && (doc.front_matter || doc.lines.len() <= 12)
            && (!title_matches || !names_primary(&credits))
        {
            content.identity_conflicts.push(format!(
                "Title/author text in {} conflicts with package metadata",
                doc.path
            ));
        }
    }
}

/// Entity handling shared by every XML document read from a book: HTML named
/// references and single-character declarations are resolved; anything else that
/// would need entity expansion is refused.
pub fn normalize_entities(xml: &str) -> anyhow::Result<String> {
    let resolved = resolve_declared_entities(xml)?;
    Ok(expand_named_entities(&resolved).into_owned())
}

/// Counts a chapter-number label that opens a new numbered run: "Chapter 1",
/// "Chapter I", "I.", "1", "Chapter One". Returns `None` for any other label.
fn starts_numbering(label: &str) -> bool {
    let words: Vec<String> = normalized(label)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let first = ["1", "i", "one", "uno", "un", "une", "eins", "ein", "een"];
    match words.as_slice() {
        [only] => first.contains(&only.as_str()),
        [kind, number, ..] => {
            matches!(
                kind.as_str(),
                "chapter" | "chapitre" | "capitulo" | "kapitel" | "hoofdstuk" | "capitolo"
            ) && first.contains(&number.as_str())
        }
        _ => false,
    }
}

fn is_division(label: &str) -> bool {
    let key = normalized(label);
    // "Book the First", "Part II", "Primera parte", "Second Part", "First Epilogue"
    key.split_whitespace().take(3).any(|word| {
        matches!(
            word,
            "book"
                | "part"
                | "livre"
                | "partie"
                | "teil"
                | "buch"
                | "libro"
                | "parte"
                | "deel"
                | "volume"
                | "epilogue"
                | "epilogo"
                | "epilog"
                | "epiloog"
                | "prologue"
                | "prologo"
        )
    })
}

/// Structural signs that a file bundles several works. Each signal names what was
/// seen; any one is enough to refuse naming a single work.
pub fn collection_signals(title: &str, labels: &[String]) -> Vec<String> {
    let mut signals = Vec::new();
    let key = format!(" {} ", normalized(title));
    for phrase in [
        " omnibus ",
        " box set ",
        " boxed set ",
        " boxset ",
        " bundle ",
        " six pack ",
        " five pack ",
        " ten pack ",
        " complete works ",
        " collected works ",
        " complete novels ",
        " complete stories ",
        " complete novels and stories ",
        " oeuvres completes ",
        " obras completas ",
        " gesammelte werke ",
        " samtliche werke ",
    ] {
        if key.contains(phrase) {
            signals.push(format!("title says{}", phrase.trim_end()));
            break;
        }
    }
    // "The Essential Dickens: A Tale of Two Cities, A Christmas Carol, Great ..."
    let list = title
        .split_once([':', ';', '/'])
        .map(|(_, rest)| rest)
        .unwrap_or(title);
    let items: Vec<&str> = list
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    // Each listed item must itself look like a title: capitalised and short. A
    // subtitle such as "an Epic Poem, Translated by ..." is not a list of works.
    if items.len() >= 3
        && items.iter().all(|part| {
            part.split_whitespace().count() <= 8
                && part
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_uppercase() || c.is_numeric())
        })
        && title.split_once([':', ';', '/']).is_some()
    {
        signals.push("title lists several works".to_owned());
    }
    // One "By <Author>" entry per bundled work.
    let bylines = labels
        .iter()
        .filter(|label| {
            let key = normalized(label);
            let mut words = key.split_whitespace();
            matches!(words.next(), Some("by" | "par" | "von" | "por" | "door"))
                && words.count() >= 1
                && label.split_whitespace().count() <= 8
        })
        .count();
    if bylines >= 2 {
        signals.push(format!("contents repeat a byline {bylines} times"));
    }
    // Chapter numbering that starts over with no Book/Part divider to explain it.
    let restarts = labels.iter().filter(|l| starts_numbering(l)).count();
    let divisions = labels.iter().filter(|l| is_division(l)).count();
    if restarts >= 3 && divisions + 1 < restarts {
        signals.push(format!(
            "chapter numbering restarts {} times without book or part divisions",
            restarts - 1
        ));
    }
    signals
}

/// A title with library decoration removed: a trailing "(Series, #N)" and a generic
/// descriptor such as ": A Novel". Only these two shapes are recognised; a bare
/// "(Volume 2)" or "(Revised Edition)" is part of the title and is kept.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TitleParts {
    pub title: String,
    pub series: Option<String>,
    pub series_index: Option<String>,
}

const DESCRIPTORS: &[&str] = &[
    "a novel",
    "novel",
    "a novella",
    "a story",
    "a short story",
    "a mystery",
    "a thriller",
    "a romance",
    "a memoir",
    "a tale",
    "a play",
    "stories",
    "a roman",
    "roman",
    "un roman",
    "romanzo",
    "un romanzo",
    "een roman",
    "eine novelle",
    "novela",
    "una novela",
    "novelle",
];

/// Words that introduce an installment number inside a series marker.
const NUMBER_WORDS: &[&str] = &[
    "book", "livre", "libro", "buch", "boek", "tome", "volume", "vol",
];

fn series_marker(inner: &str) -> Option<(String, String)> {
    let inner = inner.trim();
    // "Name, #3", "Name #3", "Name, Book 3", "Name Book 3"
    let (name, number) = if let Some(at) = inner.rfind('#') {
        (&inner[..at], inner[at + 1..].trim())
    } else {
        let words: Vec<&str> = inner.split_whitespace().collect();
        let [.., word, number] = words.as_slice() else {
            return None;
        };
        if !NUMBER_WORDS.contains(&word.trim_end_matches('.').to_lowercase().as_str()) {
            return None;
        }
        let end = inner.len() - number.len();
        let cut = inner[..end].trim_end().len() - word.len();
        (&inner[..cut], *number)
    };
    let name = name.trim().trim_end_matches([',', ';', ':']).trim();
    let number_ok = !number.is_empty()
        && number.len() <= 6
        && number.chars().all(|c| c.is_ascii_digit() || c == '.');
    let name_ok = name.chars().any(char::is_alphabetic);
    (number_ok && name_ok).then(|| (name.to_owned(), number.to_owned()))
}

pub fn split_title(title: &str) -> TitleParts {
    let mut rest = title.trim().to_owned();
    let mut series = None;
    let mut series_index = None;
    // Trailing "(Series, #N)".
    if rest.ends_with(')')
        && let Some(open) = rest.rfind('(')
        && open > 0
        && let Some((name, number)) = series_marker(&rest[open + 1..rest.len() - 1])
    {
        series = Some(name);
        series_index = Some(number);
        rest = rest[..open].trim_end().to_owned();
    }
    // Trailing ": A Novel" (or " - A Novel").
    for separator in [": ", " - ", " \u{2013} ", ", "] {
        if let Some((main, tail)) = rest.rsplit_once(separator) {
            let key = normalized(tail);
            if !main.trim().is_empty() && DESCRIPTORS.contains(&key.as_str()) {
                rest = main.trim_end().to_owned();
                break;
            }
        }
    }
    TitleParts {
        title: rest,
        series,
        series_index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typography_and_catalog_author_forms_do_not_change_identity() {
        assert_eq!(
            normalized("Alice’s Adventures"),
            normalized("Alice's Adventures")
        );
        assert!(same_author("Lewis Carroll", "Carroll, Lewis, 1832-1898"));
        assert!(!same_author("Jane Smith", "John Smith"));
        assert_eq!(title_similarity("Book One", "Book Two"), 0.0);
        for (left, right) in [
            (
                "The Very Long Chronicles of an Imaginary Kingdom Volume 1",
                "The Very Long Chronicles of an Imaginary Kingdom Volume 2",
            ),
            (
                "The Very Long Chronicles of an Imaginary Kingdom Part One",
                "The Very Long Chronicles of an Imaginary Kingdom Part Two",
            ),
            (
                "The Very Long Chronicles of an Imaginary Kingdom Volume II",
                "The Very Long Chronicles of an Imaginary Kingdom Volume III",
            ),
        ] {
            assert_eq!(title_similarity(left, right), 0.0);
        }
    }

    #[test]
    fn credit_lines_match_name_variants_but_never_another_person() {
        for (author, credit) in [
            ("Arthur Conan Doyle", "A. Conan Doyle"),
            ("Ivan Sergeevich Turgenev", "Ivan Turgenev"),
            ("Johann Wolfgang von Goethe", "J.W. von Goethe"),
            (
                "Jane Austen",
                "Jane Austen, with a Preface by George Saintsbury",
            ),
            (
                "Bram Stoker",
                "Bram Stoker NEW YORK GROSSET & DUNLAP Publishers",
            ),
            (
                "Mary Wollstonecraft Shelley",
                "Mary Wollstonecraft (Godwin) Shelley",
            ),
            ("Jacob Grimm", "Jacob Grimm and Wilhelm Grimm"),
            ("Ann Ward Radcliffe", "Ann Radcliffe"),
        ] {
            assert!(credit_names_author(author, credit), "{author} / {credit}");
        }
        for (author, credit) in [
            ("John Smith", "Jane Smith"),
            ("John Smith", "Jonathan Smith"),
            ("John Smith", "John Adams and Jane Smith"),
            ("Jane Austen", "Memoir by J. E. Austen-Leigh"),
            ("Jane Austen", "Austen"),
            ("Mary Shelley", "Percy Bysshe Shelley"),
        ] {
            assert!(!credit_names_author(author, credit), "{author} / {credit}");
        }
    }

    #[test]
    fn omnibuses_are_flagged_and_single_works_with_divisions_are_not() {
        let labels = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // Dickens "Six Pack": each novel restarts numbering and repeats a byline.
        let six = labels(&[
            "THE OLD CURIOSITY SHOP",
            "By Charles Dickens",
            "CHAPTER 1",
            "CHAPTER 2",
            "DAVID COPPERFIELD",
            "By Charles Dickens",
            "CHAPTER 1",
            "CHAPTER 2",
            "BLEAK HOUSE",
            "By Charles Dickens",
            "CHAPTER I",
            "CHAPTER II",
            "GREAT EXPECTATIONS",
            "By Charles Dickens",
            "Chapter I",
            "Chapter II",
        ]);
        assert!(
            !collection_signals("Charles Dickens Six Pack - The Old Curiosity Shop", &six)
                .is_empty()
        );
        assert!(!collection_signals("The Essential Dickens: A Tale of Two Cities, A Christmas Carol, Great Expectations", &[]).is_empty());
        assert!(
            !collection_signals("Complete Works of Jane Austen. Emma, Lady Susan", &[]).is_empty()
        );
        assert!(!collection_signals("Gesammelte Werke", &[]).is_empty());
        // A Tale of Two Cities restarts numbering under each Book.
        let tale = labels(&[
            "Book the First",
            "I. The Period",
            "Chapter 1",
            "Chapter 2",
            "Book the Second",
            "Chapter 1",
            "Chapter 2",
            "Book the Third",
            "Chapter 1",
            "Chapter 2",
        ]);
        assert!(collection_signals("A Tale of Two Cities", &tale).is_empty());
        let spanish = labels(&[
            "Primera parte",
            "I",
            "II",
            "Segunda parte",
            "I",
            "II",
            "Tercera parte",
            "I",
            "II",
        ]);
        assert!(collection_signals("El crimen y el castigo", &spanish).is_empty());
        assert!(
            collection_signals(
                "The Lusiad; Or, The Discovery of India, an Epic Poem, Translated by W. J. Mickle",
                &[]
            )
            .is_empty()
        );
        assert!(
            collection_signals(
                "Alice's Adventures in Wonderland",
                &labels(&["Chapter 1", "Chapter 2"])
            )
            .is_empty()
        );
        assert!(
            collection_signals(
                "Frankenstein; or, the Modern Prometheus",
                &labels(&["Letter 1", "Chapter 1", "Chapter 2"])
            )
            .is_empty()
        );
    }

    #[test]
    fn library_decoration_is_removed_but_volume_numbers_and_real_subtitles_are_kept() {
        let parts = |t: &str| split_title(t);
        let p = parts("Children of Dune (Dune, #3)");
        assert_eq!(
            (
                p.title.as_str(),
                p.series.as_deref(),
                p.series_index.as_deref()
            ),
            ("Children of Dune", Some("Dune"), Some("3"))
        );
        let p = parts("The Hunger Games (The Hunger Games, #1)");
        assert_eq!(
            (p.title.as_str(), p.series.as_deref()),
            ("The Hunger Games", Some("The Hunger Games"))
        );
        let p = parts("A Feast for Crows (A Song of Ice and Fire, Book 4)");
        assert_eq!(
            (p.title.as_str(), p.series_index.as_deref()),
            ("A Feast for Crows", Some("4"))
        );
        assert_eq!(parts("Mockingjay: A Novel").title, "Mockingjay");
        assert_eq!(parts("Gone Girl - A Novel").title, "Gone Girl");
        // Never stripped: volumes, editions and real subtitles are part of the title.
        for keep in [
            "My Life (Volume 1)",
            "Les Miserables (Revised Edition)",
            "Frankenstein; or, the Modern Prometheus",
            "The Man Who Was Thursday: A Nightmare",
            "A Novel Idea",
            "(Untitled)",
            "Book One",
            "Dune, #3",
        ] {
            assert_eq!(parts(keep).title, keep, "{keep}");
            assert!(parts(keep).series.is_none(), "{keep}");
        }
    }

    #[test]
    fn a_leading_article_never_makes_two_titles_differ_or_the_same() {
        assert_eq!(title_similarity("The Time Machine", "Time Machine"), 1.0);
        assert_eq!(
            title_similarity("Le Comte de Monte-Cristo", "Comte de Monte-Cristo"),
            1.0
        );
        assert_eq!(
            title_similarity("A Study in Scarlet", "Study in Scarlet"),
            1.0
        );
        assert_eq!(
            title_similarity("The Time Machine", "The Invisible Man"),
            0.0
        );
        assert_eq!(title_similarity("The Odyssey", "The Iliad"), 0.0);
        assert_eq!(title_similarity("The Book One", "Book Two"), 0.0);
    }

    #[test]
    fn titles_are_found_across_split_lines_but_volumes_are_not_confused() {
        let lines = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let on = |title: &str, items: &[&str]| {
            let owned = lines(items);
            title_on_page(title, &owned.iter().collect::<Vec<_>>())
        };
        assert!(on(
            "Frankenstein; or, the modern prometheus",
            &["Frankenstein;", "or, the Modern Prometheus"]
        ));
        assert!(on("Dracula", &["D R A C U L A"]));
        assert!(on(
            "The Man Who Was Thursday: A Nightmare",
            &["The Man Who Was Thursday"]
        ));
        assert!(!on(
            "The Works of Poe - Volume 1",
            &["The Works of Poe", "VOLUME II."]
        ));
        assert!(!on("The Works of Poe - Volume 2", &["The Works of Poe"]));
        assert!(!on("Emma", &["Persuasion"]));
    }

    #[test]
    fn html_named_entities_and_character_declarations_parse_but_text_entities_do_not() {
        let page = |doctype: &str, body: &str| {
            format!(
                r#"{doctype}<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>{body}</h1></body></html>"#
            )
        };
        let (doc, _) = read_document(
            "title.xhtml",
            &page("", "Tom&nbsp;&amp;&nbsp;Jerry &mdash; &eacute;"),
        )
        .unwrap();
        assert_eq!(doc.headings, ["Tom & Jerry \u{2014} \u{e9}"]);
        let declared = page(
            r#"<!DOCTYPE html [<!ENTITY nbsp "&#160;"><!ENTITY dash "&#8212;">]>"#,
            "A&nbsp;B&dash;C",
        );
        let (doc, _) = read_document("title.xhtml", &declared).unwrap();
        assert_eq!(doc.headings, ["A B\u{2014}C"]);
        for hostile in [
            r#"<!DOCTYPE html [<!ENTITY a "&#38;#38;">]>"#,
            r#"<!DOCTYPE html [<!ENTITY a SYSTEM "file:///etc/passwd">]>"#,
            r#"<!DOCTYPE html [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;">]>"#,
        ] {
            assert!(
                read_document("title.xhtml", &page(hostile, "&a;&b;")).is_err(),
                "{hostile}"
            );
        }
        assert!(read_document("title.xhtml", &page("", "&bogus;")).is_err());
    }

    #[test]
    fn standard_xhtml_doctype_is_allowed_but_entity_expansion_is_not() {
        let valid = r#"<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
        <html xmlns="http://www.w3.org/1999/xhtml"><body><h1>A Test Book</h1><p>by Jane Example</p></body></html>"#;
        let (doc, _) = read_document("title.xhtml", valid).unwrap();
        assert_eq!(doc.headings, ["A Test Book"]);
        assert!(
            read_document(
                "title.xhtml",
                r#"<!DOCTYPE html [<!ENTITY x "text">]><html>&x;</html>"#
            )
            .is_err()
        );
    }
}
