mod support;
use bookshelf_identify::language::assess;

#[test]
fn five_languages_are_detected_confirmed_and_checked_against_wrong_declarations() {
    for &(language, text) in support::LANGUAGES {
        let documents = vec![("chapter.xhtml".into(), text.into())];
        let detected = assess(&[], &documents);
        assert_eq!(detected.status, "detected", "{language}: {detected:?}");
        assert_eq!(detected.language.as_deref(), Some(language));
        let confirmed = assess(&[language.into()], &documents);
        assert_eq!(confirmed.status, "confirmed", "{language}: {confirmed:?}");
        let wrong = if language == "en" { "fr" } else { "en" };
        let conflict = assess(&[wrong.into()], &documents);
        assert_eq!(conflict.status, "conflict", "{language}: {conflict:?}");
        assert_eq!(conflict.language.as_deref(), Some(language));
        assert!(conflict.conflicted());
    }
}

#[test]
fn bilingual_unknown_and_invalid_declarations_are_not_silently_english() {
    let documents = vec![
        ("english.xhtml".into(), support::ENGLISH.into()),
        ("french.xhtml".into(), support::FRENCH.into()),
    ];
    assert_eq!(assess(&["en".into()], &documents).status, "multilingual");
    assert_eq!(assess(&[], &documents).status, "multilingual");
    let unknown = assess(
        &[],
        &[("short.xhtml".into(), "A small amount of text.".into())],
    );
    assert_eq!(unknown.status, "unknown");
    assert!(unknown.language.is_none());
    assert_eq!(
        assess(&["invalid-language".into()], &documents).status,
        "invalid_declaration"
    );
    let declared = assess(&["fr-CA".into()], &[]);
    assert_eq!(declared.status, "declared_only");
    assert_eq!(declared.language.as_deref(), Some("fr"));
}
