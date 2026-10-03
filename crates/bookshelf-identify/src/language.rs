use lingua::{LanguageDetector, LanguageDetectorBuilder};
use serde::Serialize;
use std::{collections::BTreeMap, sync::OnceLock};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Assessment {
    pub language: Option<String>,
    pub declared: Vec<String>,
    pub detected: Option<String>,
    pub status: String,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Sample {
    pub document: String,
    pub language: String,
    pub confidence: f64,
    pub letters: usize,
    /// What the second, independent detector (lingua) made of the same text.
    pub second_opinion: Option<String>,
    pub second_confidence: f64,
}

/// A second, independent detector. Both must agree before a declared language is
/// ever overruled, and it is what allows a correction between closely related
/// languages that either one alone confuses.
fn second_opinion(text: &str) -> Option<(String, f64)> {
    static DETECTOR: OnceLock<LanguageDetector> = OnceLock::new();
    let detector = DETECTOR.get_or_init(|| {
        LanguageDetectorBuilder::from_all_languages()
            .with_preloaded_language_models()
            .build()
    });
    let (language, confidence) = detector
        .compute_language_confidence_values(text)
        .into_iter()
        .next()?;
    let code = normalize(&language.iso_code_639_1().to_string())?;
    Some((code, confidence))
}

pub fn normalize(value: &str) -> Option<String> {
    let base = value.trim().split(['-', '_']).next()?.to_lowercase();
    let lang = if base.len() == 2 {
        isolang::Language::from_639_1(&base)
    } else {
        let canonical = match base.as_str() {
            "fre" => "fra",
            "ger" => "deu",
            "dut" => "nld",
            "chi" => "zho",
            "cze" => "ces",
            "gre" => "ell",
            "rum" => "ron",
            "wel" => "cym",
            "alb" => "sqi",
            "arm" => "hye",
            "baq" => "eus",
            "bur" => "mya",
            "geo" => "kat",
            "ice" => "isl",
            "mac" => "mkd",
            "may" => "msa",
            "per" => "fas",
            "slo" => "slk",
            "tib" => "bod",
            _ => &base,
        };
        isolang::Language::from_639_3(canonical)
    }?;
    Some(lang.to_639_1().unwrap_or(lang.to_639_3()).to_owned())
}

/// Languages a text detector is known to mix up. A disagreement inside one of
/// these groups is never settled automatically.
const CONFUSABLE: &[&[&str]] = &[
    &["da", "nb", "no", "nn", "sv", "is", "fo"],
    &["es", "pt", "ca", "gl", "it", "fr", "ro", "oc"],
    &["hr", "sr", "bs", "sl", "sk", "cs", "pl"],
    &["ru", "uk", "be", "bg", "mk", "sr"],
    &["id", "ms", "tl", "jv", "su"],
    &["hi", "ur", "mr", "ne", "bn", "pa"],
    &["nl", "af", "fy", "de", "lb"],
    &["zh", "ja", "ko"],
    &["en", "sco"],
    &["fa", "ar", "ps", "ku"],
    &["la", "it", "es", "fr", "pt", "ro"],
];

fn confusable(a: &str, b: &str) -> bool {
    CONFUSABLE
        .iter()
        .any(|group| group.contains(&a) && group.contains(&b))
}

pub fn assess(declared: &[String], documents: &[(String, String)]) -> Assessment {
    let mut samples = Vec::new();
    for (document, text) in documents {
        let mut segment = String::new();
        let mut letters = 0;
        for line in text.lines() {
            segment.push_str(line);
            segment.push('\n');
            letters += line.chars().filter(|c| c.is_alphabetic()).count();
            if letters < 300 {
                continue;
            }
            if let Some(info) = whatlang::detect(&segment)
                && info.is_reliable()
                && info.confidence() >= 0.85
                && let Some(code) = normalize(info.lang().code())
            {
                let (second_opinion, second_confidence) = match second_opinion(&segment) {
                    Some((code, confidence)) => (Some(code), confidence),
                    None => (None, 0.0),
                };
                samples.push(Sample {
                    document: document.clone(),
                    language: code,
                    confidence: info.confidence(),
                    letters,
                    second_opinion,
                    second_confidence,
                });
            }
            segment.clear();
            letters = 0;
        }
    }
    let mut detected_counts = BTreeMap::<String, usize>::new();
    for sample in &samples {
        *detected_counts.entry(sample.language.clone()).or_default() += sample.letters;
    }
    let mut detected = detected_counts.iter().collect::<Vec<_>>();
    detected.sort_by(|a, b| b.1.cmp(a.1));
    let top = detected.first().map(|(lang, _)| (*lang).clone());
    let total: usize = detected_counts.values().sum();
    let mixed = detected
        .iter()
        .skip(1)
        .any(|(_, count)| **count >= 600 && **count * 5 >= total);
    // "und" (undetermined) and "zxx" (no linguistic content) declare nothing.
    let meaningful: Vec<&String> = declared
        .iter()
        .filter(|value| !matches!(value.trim().to_lowercase().as_str(), "und" | "zxx" | ""))
        .collect();
    let mut normalized: Vec<_> = meaningful
        .iter()
        .filter_map(|value| normalize(value))
        .collect();
    normalized.sort();
    normalized.dedup();
    let invalid_declaration = meaningful.iter().any(|value| normalize(value).is_none());
    let (language, status) = if invalid_declaration {
        (top.clone(), "invalid_declaration")
    } else if mixed || normalized.len() > 1 {
        (None, "multilingual")
    } else if let Some(detected) = &top {
        match normalized.first() {
            Some(declared) if declared != detected => {
                let letters = detected_counts.get(detected).copied().unwrap_or(0);
                // Samples where BOTH detectors name the same language.
                let both: Vec<_> = samples
                    .iter()
                    .filter(|s| {
                        &s.language == detected && s.second_opinion.as_deref() == Some(detected)
                    })
                    .collect();
                let sure = both.iter().all(|s| s.second_confidence >= 0.9);
                let allowed = !confusable(declared, detected) || sure;
                if letters >= 900 && both.len() >= 3 && allowed {
                    (Some(detected.clone()), "corrected")
                } else {
                    (Some(detected.clone()), "conflict")
                }
            }
            Some(_) => (Some(detected.clone()), "confirmed"),
            None => (Some(detected.clone()), "detected"),
        }
    } else if let Some(declared) = normalized.first() {
        (Some(declared.clone()), "declared_only")
    } else {
        (None, "unknown")
    };
    Assessment {
        language,
        declared: declared.to_vec(),
        detected: top,
        status: status.to_owned(),
        samples,
    }
}

impl Assessment {
    pub fn conflicted(&self) -> bool {
        matches!(
            self.status.as_str(),
            "conflict" | "multilingual" | "invalid_declaration"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn undetermined_declarations_declare_nothing_and_strong_text_corrects_a_wrong_tag() {
        let german = "Die Bibliothek stand neben einem ruhigen Garten, in dem sich die Kinder nach der Schule trafen. Jeden Nachmittag öffnete die Bibliothekarin die Fenster und legte neue Bücher auf den großen Holztisch. Eine junge Leserin wollte verstehen, wie Reisende früher ohne Maschinen die weiten Meere überquert hatten. Ihr Bruder bevorzugte Geschichten über Tiere, die in den Bergen lebten. Die Lehrerin ermutigte die Kinder, verschiedene Berichte miteinander zu vergleichen und bei unklaren Erklärungen nachzufragen. Draußen begann es zu regnen, aber niemand wollte nach Hause gehen. Sie lasen weiter, bis die Straßenlaternen leuchteten und der Abendzug am Bahnhof eintraf.
";
        let docs: Vec<(String, String)> = (0..4)
            .map(|i| (format!("d{i}"), german.to_owned()))
            .collect();
        assert_eq!(assess(&["und".into()], &docs).status, "detected");
        assert_eq!(assess(&["en".into()], &docs).status, "corrected");
        assert_eq!(assess(&["de".into()], &docs).status, "confirmed");
        // Too little text, or a confusable pair, is never corrected.
        assert_eq!(assess(&["en".into()], &docs[..1]).status, "conflict");
        assert!(confusable("es", "pt") && confusable("hr", "sr") && !confusable("en", "de"));
    }

    #[test]
    fn language_codes_and_regions_are_normalized_without_guessing() {
        for (value, expected) in [
            ("en-US", "en"),
            ("eng", "en"),
            ("fre", "fr"),
            ("ger", "de"),
            ("pt-BR", "pt"),
        ] {
            assert_eq!(normalize(value).as_deref(), Some(expected));
        }
        assert!(normalize("unknown").is_none());
        assert_eq!(assess(&[], &[]).status, "unknown");
        assert_eq!(
            assess(&["en".into(), "fr".into()], &[]).status,
            "multilingual"
        );
    }
}
