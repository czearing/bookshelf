# Accuracy plan

Goal: a library where every book is identified correctly and its filename, embedded
metadata and cover are written consistently. This file is the single source of
truth for how we get there and how we know.

## What "100%" means here

Literal 100% on every possible book is not provable: embedded tags and provider
records can both be wrong, and no test set covers the world. The enforceable target:

1. **Zero wrong auto-applies.** Any book the tool changes is changed to the correct
   identity. Measured as `WRONG_AUTO == 0` on the gold corpus and on every
   corrupted variant of it; a failure here blocks release.
2. **Everything uncertain goes to review, never a guess.** Recall (books resolved
   without a human) is tracked and improved separately; it may never be bought
   with precision.
3. **Every field written is verified by re-reading it**, and every untouched byte is
   proven untouched.

## Design rules

- No regular expressions in identification, parsing or writing. Structure comes from
  real parsers (ZIP, XML, image decoders) and explicit tokenisation.
- Work and edition are separate questions. A title/author match names the work; an
  ISBN is a hint, not a license.
- Source files are never modified. Tagging writes a verified copy, then swaps.
- Each provider is an independent witness. Disagreement between witnesses is a
  review reason, not something to average away.

## Status

| Piece | State |
|---|---|
| Identify + rename | Built. Google Books (default) and OpenLibrary providers behind `src/provider.rs`. Keyless Google quota is currently 429, so OpenLibrary is what is measured. |
| Tagging (`--tag`, `--undo`) | Built and tested: `src/tagwrite.rs` (metadata + cover writer, round-trip verified), `src/place.rs` (backup, journal, undo). |
| Gold corpus | `corpus/manifest.json` (110 books, tuned against) and `corpus/holdout.json` (104 books, never used for tuning). Project Gutenberg, 13 strata. |
| Scorer (`examples/score.rs`) | Built. Each book is scored as shipped and with four corrupted tag variants. |
| Omnibus / collection detection | Built (`text::collection_signals`): collection wording or a listed set of works in the title, repeated bylines in the contents, chapter numbering that restarts with no Book/Part divisions. Flags are review reasons, never renames. |
| Standard modern books and series | `tools/synth_modern.py` writes 77 synthetic EPUBs (Harry Potter, Lord of the Rings, Hunger Games, Dune, Song of Ice and Fire, Narnia, Wheel of Time and more; titles, authors and series order only, no copyrighted text) in five metadata styles: clean, "Last, First", "(Series, #N)" suffix, Calibre series metadata, ": A Novel". With `--provider both` all 77 get the exact expected name and none is renamed to the wrong volume. Decoration is stripped by `text::split_title` (never a bare volume number), and the series is written back as `calibre:series` plus an EPUB 3 collection. |
| Google as primary (2026-10-02) | Plain-text queries (Google's operators return nothing), field-limited responses, search results reused, 100/min quota respected with a global pause, clean 575 px covers with placeholder rejection and per-candidate retry. `--provider google-first`: Google alone 77 of 77 modern with 0 wrong in the chain; unseen Gutenberg holdout 47 correct (Google alone 46, OpenLibrary alone 38), 0 wrong. Default Google quota is 1,000 queries per day, which caps a run at roughly 700 to 1,000 books per day until raised. |
| Full-text witness (2026-10-02) | Passage search in Google Books full text as independent corroboration; also arbitrates a title page that contradicts the tags; "The Iliad of Homer" == "The Iliad". Unseen holdout 47 -> 75 of 104 correct (72%), 0 wrong; tuning set 70 of 110 (64%, quota-limited, 0 wrong); 160 of 160 corrupted-tag attacks refused. Provider failures now fall through to the next provider. Remaining review is mostly omnibus/collection refusals, text only found inside collections, foreign-language retrieval gaps. |
| Resolved without a human (2026-10-02) | Declared collections named by their own title (the 4 LibGen omnibus files that were review are now named); structural bundle signs demand every passage found; duplicates resolved deterministically (identical copy flagged, different file numbered). Holdout 76 of 104 (73%), tuning 76 of 110 (69%), 0 wrong. Needs a live re-test when Google's daily quota resets: structural-bundle witness, LibGen and Standard Ebooks sets, tagless files identified from their text, foreign-language retrieval. |
| Closing the gap (2026-10-02) | Second text witness (OpenLibrary search-inside), declared collections must be corroborated, self-consistent naming for books no provider has. Attack testing found and closed 3 holes (collection claims; author and title of a bundle not verified). Final: unseen holdout 78 of 104 (75%), tuning 77 of 110 (70%), 0 wrong; 856 of 856 corrupted-tag variants refused. A tag-trusting mode was measured (+14 genuine, 175 of 312 corrupted accepted) and deleted. Remaining review is books no provider or text index has, text found only inside anthologies, and files with no usable tags. |
| Cover quality gate (2026-10-02) | Google serves the true original through `fife=w1200` (up to 1665x2500; never upscaled). Real examples found and rejected: a JPEG "image not available" variant my hash list missed, a 2000x320 title strip, an interior title page, a generated grey template. Gate: portrait 1.2 to 1.9, width at least 500, real content, not a text page, not the placeholder at any scale. Result on 77 modern books: 50 covers, all 1200 px, independent audit 0 flagged, 0 duplicates; 27 books get no cover rather than a poor one. `--upgrade-covers` verified: 200 px covers replaced by 1200 px of the same artwork, a different artwork left alone. |
| Apple Books provider (experimental) | Built (`src/apple.rs`, `--provider apple`, `--provider all`) with a 15/min client and a 45 minute self-imposed cool-down after any 403. Not recommended for bulk: Apple blocked this machine after bursts and again at 15 requests/min. Useful as a cover source only. |
| Lossless compression | Built (`src/shrink.rs`) and verified, but only 2 to 3% on real books at 2 to 3 s each (75% of bytes are JPEG), so opt-in via a probe example, not in the default flow. |
| Provider chain | `--provider both` tries OpenLibrary, then Google (with a plain-text fallback for its broken operators) only for books the first could not settle. Each lookup uses identical acceptance rules. OpenLibrary alone failed 3 of 77 (bad catalogue data); Google alone missed others; together 77 of 77. |
| Length signal | Word count of the whole book versus the provider's page count: far longer suggests a collection, far shorter a sample or excerpt. A veto only, with wide margins. |
| Language handling | `whatlang` + `lingua` (two existing detectors) and `isolang`. `und` declares nothing; a wrong declared language is corrected only when both detectors agree on strong text, and closely related pairs (es/pt, da/no/sv, hr/sr, ...) need lingua at 0.9+. `--tag` rewrites `dc:language`. |
| Real-world reader hardening | Found with real LibGen public-domain files: HTML named entities, single-character DTD entity declarations, missing `mimetype`, OPF 2.0 `dc-metadata` wrappers, creators with several MARC roles. |
| Second independent provider witness | Not started (Google keyed queries with `intitle:` return nothing; a plain-text fallback is needed). |
| EPUB validity check (epubcheck) | Blocked: no Java on this machine. |
| LibGen-sourced files | `libgen` CLI is installed. Used only for public-domain titles (authors who died before 1930) and editions with no modern commercial publisher: `tools/libgen_pd.py`, `tools/libgen_pd_lang.py`, `tools/show_batch.py`. Debug in batches of about 10. |
| PDF / MOBI / AZW3 | Out of scope until EPUB is at gate. |

## Measured results (2026-10-01, OpenLibrary)

| Set | Books | Correct auto-rename | WRONG auto-rename | Sent to review | Corrupted variants wrongly renamed |
|---|---|---|---|---|---|
| Tuning corpus | 110 | 62 (56%) | 0 | 48 | 0 of 440 |
| Holdout (unseen) | 104 | 35 (34%) | 0 | 67 (+2 unreadable) | 0 of 416 |

Reading this honestly:

- **Precision generalized.** No wrong rename on 856 corrupted variants or on unseen books.
- **Recall did not fully generalize** (56% vs 34%): the rules were tuned on the first set.
  The holdout is the number to trust, and it is the number to improve.
- **The evidence is still thin.** 97 correct auto-renames with 0 wrong only bounds the true
  error rate on auto-renames below about 3% at 95% confidence. Reaching 0.3% needs roughly
  1,000 correct auto-renames, which is what Phase 5 is for.
- Remaining review causes are legitimate: many books show no author on any title page and
  have no matching cover, the provider has no record in that language, or the provider
  spells the author differently from the local file.

## Readiness for a library of hundreds of thousands

| Requirement | State |
|---|---|
| Survive interruption, resume | Done: `--state` SQLite file, saved per book. |
| Re-runs only touch new or changed files | Done, tested with an empty cache and offline. |
| Review queue you can work through | Done: `--queue`, `--approve`, `--skip`, remembered. Still CLI only; a side-by-side UI would speed it up. |
| Memory bounded | Done: evidence dropped once saved (full `--json` still available). |
| Filesystem pressure | Provider cache sharded. Google cache still flat (fallback only). |
| Throughput | Network bound: about 1.5 requests per book. At 8 requests/second 300,000 books is about 16 hours, resumable. A local OpenLibrary dump index would remove this limit but needs about 17 GB download and tens of GB free; C: has 55 GB free. Needs a disk decision. |
| Accuracy audit at scale | Not done. Needs a preview of 5,000 of the real books and a human audit of a random sample before any apply. |
| Duplicates (same work, several files) | Reported with every competing file and size; never overwritten or deleted. No automatic keep-the-best policy (deliberate). |
| Backup retention | `--prune-backups` removes a backup only when its tagged file is provably untouched and old enough. |

## Phases and gates

0. **Provider layer.** Done.
1. **Gold corpus.** Done for Gutenberg (110 + 104 holdout). Next: add Standard Ebooks and Internet
   Archive files, and a human-verified LibGen sample once its location is known.
   LibGen's own metadata is not ground truth; every LibGen-derived gold row is
   confirmed against the book's title page by a person.
2. **Scorer.** Done. Run after every change on BOTH sets; `WRONG_AUTO` must stay 0, and
   tune only against the tuning set, then confirm on the holdout.
3. **Adversarial mutations.** Four corruptions per book in the scorer today (wrong
   author, wrong title, swapped identity, unknown author). Add: truncated text,
   copied cover, anthology vs part, same title by different authors, series volumes.
4. **Tagging.** Built (`--tag`, `--undo`) and verified on 8 real Standard Ebooks and
   Gutenberg files with an independent Python check (valid ZIP, mimetype first and stored,
   exact title/author, cover declared and present, no dangling refinements, every other
   member byte-identical to the backup). Known limit: a provider cover is the work's
   cover from some edition (the Dracula test got an abridged Longman reader cover), so
   covers are only ever added to books that have none. Remaining: `epubcheck` once Java
   is available, and opening tagged files in real readers.
5. **Soak.** A 2,000+ book preview run with a human audit of 5% of auto-matches and
   all review items. Release gate: zero wrong auto-applies across all of it.

## Open items needing the owner

- Whether to install a JRE for `epubcheck`.
- Rotate the Google Books test key that was pasted into chat.
- Hardcover: create a free account token (Settings, API) if you want it evaluated as a second source (series data, 5,000/day).
- Request a Google Books quota increase before a large run.
