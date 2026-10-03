# book-manager

A native Rust CLI that identifies EPUBs using Google Books and renames them in
place. **Preview is the default.** Only `--apply` changes book filenames.
EPUB contents and embedded metadata are never modified.

## Build and run (Windows / PowerShell)

```powershell
Set-Location C:\Code\book-manager
cargo build --release --locked

# Preview a directory recursively, or pass one or more individual EPUB files.
.\target\release\book-manager.exe 'A:\Books'

# Rename uniquely matched books, without moving them between directories.
.\target\release\book-manager.exe 'A:\Books' --apply

# Machine-readable report with candidates and matching evidence.
.\target\release\book-manager.exe 'A:\Books' --json

# Reuse fresh cached responses without any network access.
.\target\release\book-manager.exe 'A:\Books' --offline
```

Google Books is the default provider and OpenLibrary (`--provider openlibrary`) is the
keyless alternative. **No API key is required by this application.**
It sends keyless public lookups when `GOOGLE_BOOKS_API_KEY` is absent. You may
optionally set that environment variable to use your own Google project's quota;
do not commit the key or put it in CLI arguments. Keyless access is still subject
to Google's server-side allocation and is not guaranteed.

HTTP 429 is not automatically an authentication failure. The client inspects
Google's structured error: a daily quota rejection, including a reported limit of
zero, stops further live requests for that run instead of repeatedly hitting an
unavailable quota. A zero allocation is not something faster/slower retries can
repair, and there is no guarantee it will be restored the next day.
Transient 429s retain bounded retries. HTTP failures are reported, not treated as missing books.
API keys are never printed in request-error URLs. Book search terms are sent
to Google; book contents are not uploaded.

## Identification rules

ComicTagger's candidate search and corroboration are adapted to distinguish a
**work** (title, authors, language) from an **edition** (ISBN, publisher, date,
ebook format). **An ISBN is a search hint, never permission to rename.**

1. Read the EPUB package, optional cover, and bounded visible XHTML samples.
   Corroborate package title/author against visible title headings and author
   credits. Explicit conflicting title-page evidence requires review.
2. Assess declared language against locally detected prose language. Missing
   language is never assumed to be English. Conflicting, multilingual, invalid,
   and completely unknown language evidence requires review.
3. Search title/author and up to four checksum-valid ISBNs, normalizing ISBN-10
   to ISBN-13. Preserve apostrophes in queries. Retrieve up to 40 records per
   page, with a second page and a broader query when no eligible result is found.
4. Filter title, author, language, collections, and adaptations. Typographic and
   conservative spelling variations are allowed; differing volume numbers are
   not. Fetch full Google records for up to four prioritized candidates and
   recheck their metadata.
5. Identify a work only when Google agrees with local title/author **and** either
   visible title/author text or a strong actual cover comparison corroborates it.
   A different or missing cover does not veto text-backed work identification.
   Multiple Google records for the same work do not require choosing an edition.
6. Assess edition separately. A supported edition requires a matching ISBN,
   a loaded full record explicitly identifying an ebook, and matching publisher
   plus publication year, or matching cover plus publisher/year. Known publisher,
   year, format, and informative cover conflicts prevent edition assignment.
   Multiple distinct supported editions remain ambiguous.
7. Rename using the corroborated work. Leave edition identifiers and publication
   fields unset for unknown, ambiguous, or conflicting editions. An ignored ISBN
   is explained; the EPUB and its embedded metadata are never rewritten.

Cover thresholds are explicit heuristics, not measured accuracy guarantees:
both hash distances must be at most 8/64, normalized mean color error at most
0.12, and relative aspect-ratio error at most 0.15. Blank/low-information artwork
cannot qualify. This is not full-text authentication: copied metadata, a copied
title page, and copied artwork cannot prove that every chapter belongs to a work.
Language detector confidence is also a heuristic, not a probability guarantee.
Passing a test corpus does not establish 100% accuracy on arbitrary books.
No low-confidence or metadata-only auto-rename override is provided.

Capitalization, punctuation, and diacritics are normalized for comparisons;
author name order is normalized. This is deliberately conservative: initials,
pen names, substantially different subtitles, and incorrect embedded tags can
require manual review. This is not a statistical confidence or accuracy claim.
Collection/omnibus/companion-book wording in a provider title or subtitle is
rejected unless the local title also contains that wording.
Known provider language must match the assessed local language. Missing provider
language is not invented and cannot confirm an edition's language. Reports
distinguish `confirmed`, `detected`, `declared_only`, `unknown`, `conflict`,
`multilingual`, and `invalid_declaration`. ISO language codes are normalized for
comparison; original declarations are retained. Text detection needs at least
300 alphabetic characters and a reliable detector score of at least 0.85.
An observed secondary language needs at least 600 letters and 20% of detected
letters to classify the sampled text as multilingual. Short text may remain
`declared_only`; regional dialects and unsampled translations are not inferred.

EPUB 2 creator roles and EPUB 3 role refinements exclude explicitly marked
translators/editors from author matching. Missing roles are treated as authors.
EPUB 3 main-title refinements take precedence over the first title element.
Files with insufficient embedded metadata are reported as errors; filenames
are not guessed into identities. Multi-rendition EPUBs require review.
Cover discovery supports EPUB 3 `cover-image`, EPUB 2 `meta name="cover"`, and
declared guide cover pages (including XHTML/SVG wrappers around raster images).
Raster decoding supports JPEG, PNG, and WebP. Missing covers require text evidence;
invalid, multiple, unsupported, or oversized covers are reported and never
silently replaced with placeholder evidence. Standard XHTML doctypes are accepted;
custom XML entity declarations are rejected. Visible text extraction excludes
navigation, hidden elements, scripts, and marked license/copyright material.
PDF, MOBI, AZW, DRM removal, format conversion, metadata rewriting, cover
replacement, and series inference are outside this version's scope.

## Filenames and safety

Names use `Author - Title.epub` (multiple authors are comma-separated). Windows
reserved filename characters are removed and long names are truncated without
splitting Unicode characters. Dates are deliberately omitted because a book's
original publication date and its ebook edition date are not interchangeable.

The complete batch is identified and checked for destination collisions before
renaming starts. Existing destinations and duplicate planned destinations are
never overwritten or automatically numbered. File size and modification time
are rechecked immediately before applying each rename. Avoid concurrently editing
or moving the input library while running the tool.

Windows uses a native no-replace rename. Other platforms use a no-clobber hard
link followed by removal of the source, requiring filesystem hard-link support;
if removal fails both paths remain and the error is reported. The batch is not
a transaction: successful renames are not rolled back if a later file fails.
Repeated runs normally report already-correct filenames as unchanged.

## Performance and cache

- Bounded concurrent book processing (`--jobs 8`, range 1-64).
- Shared HTTP connection pool and request pacing
  (`--requests-per-second 4`, range 1-20). Higher settings do not increase Google's quota.
- Concurrent identical searches and cover requests share locks. Decoded Google
  cover signatures are reused within a run rather than repeatedly decoding images.
- Successful searches, full volume details, and covers are cached for seven days,
  including empty searches. Search cache keys are versioned separately so old
  ten-result responses are not mistaken for forty-result pages. Detail and cover
  retrieval costs additional requests; text-backed works without matching ISBNs
  do not need cover downloads.
- Default cache: the OS cache directory under `book-manager\google-books-v1`.
  Override with `--cache-dir PATH`; bypass reads with `--refresh`.
- `--offline` requires fresh search entries and cannot be combined with
  `--refresh`. Optional missing detail/cover evidence generates warnings and
  cannot establish an edition; real text evidence may still establish a work.
- Corrupt search caches produce errors. Detail/cover failures produce explicit
  warnings; `--refresh` replaces cached entries with successful live responses.
- Transient rate limits and server errors receive at most three total attempts; daily
  quota exhaustion stops live requests for the run immediately. Permanent
  failures, including API-key/quota errors, are surfaced. Long or dated
  `Retry-After` requests stop processing that lookup rather than retrying early.
- XML and HTTP response sizes are bounded. Covers are limited to 8 MiB compressed
  and 16 megapixels, with decoder allocation/dimension limits. Only approved
  Google Books cover hosts are fetched over HTTPS, without forwarding the API key
  or following redirects. At most ten spine documents (first eight, middle, last)
  are examined, each capped at 4 MiB, retaining at most approximately 6,000 prose
  characters per document from beginning/middle/end paragraph samples.
  No archive extraction, full-book hashing, or subprocess-based ebook tools
  are used in the production CLI. The trial runner hashes files for verification.

Cache files contain bibliographic metadata and cover images; reports include
local paths, candidate rejection reasons, ISBN agreement, independent-search
membership, language samples, numerical cover scores, and separate `work` and
`edition` results. Top-level `google_id` denotes a supported edition only;
`work.supporting_google_ids` lists records corroborating the work.
Google API access does not automatically
grant unrestricted rights to redistribute its data or artwork; review applicable
terms before publishing cached records.

Exit codes: `0` all items planned/renamed/unchanged; `1` one or more items need
review or failed; `2` invalid arguments or a fatal setup/output error. Other
successfully identified files can still be renamed when an item fails.

## Development

```powershell
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

Tests create isolated EPUB fixtures and exercise a local HTTP server and offline
caches. Positive cases are paired with contradictory metadata, language,
edition, and missing-evidence controls. Language fixtures cover English, French,
German, Spanish, and Russian; these are synthetic fixtures, not a measured global
accuracy benchmark. Tests never modify your library or require an API key.
The upstream ComicTagger reference checkout lives in `reference\comictagger`;
it is not compiled or used at runtime.

An opt-in real-book trial downloads Project Gutenberg's public-domain English
EPUB of *Alice's Adventures in Wonderland*, runs preview/apply/repeat against the
release CLI, checks actual work identity, detected English, an uninvented edition,
SHA-256 preservation, and repeat-run idempotency, and saves `trial-report.json`. This is
not a LibGen integration. Supply an explicit isolated directory; an existing
`trial` subfolder is not overwritten. The command fails if live identification
and renaming cannot be demonstrated; it never seeds fake Google results.

```powershell
cargo run --release --example alice_trial -- C:\Code\book-manager\test-data\alice C:\Code\book-manager\target\release\book-manager.exe

# Inspect local title-page/language evidence without contacting Google.
cargo run --release --example inspect_epub -- 'C:\path\book.epub'
```

## Tagging

`--tag` (with `--apply`) writes into each corroborated book: the work's title and
authors; ISBN, publisher and date only when the edition is supported; and a cover
only when the book has none. Other credits (translators, editors) and every other
archive member are left byte-identical. The package document is edited by splicing
the parser's byte ranges, not by regenerating it. An existing declared cover is
overwritten in place so every reference to it stays valid.

A tagged copy is written beside the book, re-read, and compared field by field and
member by member with the original before it replaces anything. The original moves
to `.book-manager-backup` (named by its SHA-256) and each swap is appended to
`journal.jsonl`; `--undo` restores originals exactly. That folder is never scanned
as part of the library. All-capital or all-lower-case local titles and authors take
the provider's spelling when it spells the same words in normal case.

## Measured accuracy

`PLAN.md` defines the target and the phases. The measuring tools:

```powershell
# Gold corpus from Project Gutenberg (gold identity from the Gutenberg catalogue).
cargo run --release --example corpus_fetch -- corpus\manifest.json test-data\corpus
# Score identification on every book, as shipped and with four kinds of corrupted tags.
cargo run --release --example score -- corpus\manifest.json test-data\corpus --provider openlibrary
```

The scorer exits non-zero if any book would be renamed to an identity other than
the gold one (`WRONG_AUTO`); that count must stay zero. See `PLAN.md` for current
results and what remains unmeasured.

## Real-world files

Public-domain files from a LibGen mirror showed what synthetic fixtures never did, and
each fix has a regression test: HTML entities such as `&nbsp;` in XHTML, single-character
DTD entity declarations (anything else is still refused), a missing `mimetype`, the OPF 2.0
`dc-metadata` wrapper, and creators carrying several MARC roles. Omnibuses and collections
are detected structurally and sent to review rather than named as one work. A wrongly
declared language is corrected only when two independent detectors agree on strong text.
There are no regular expressions anywhere in the project, including its tools.

## Large libraries

`--state FILE` keeps every result in one SQLite file, saved the moment it is known.

```powershell
# First pass: preview only. An interrupted run resumes; unchanged files cost nothing.
book-manager.exe 'A:\Books' --provider both --state A:\books.db
book-manager.exe --state A:\books.db --status          # counts by status
book-manager.exe --state A:\books.db --queue 50        # books awaiting a human

# Decide once; the decision is remembered.
book-manager.exe --state A:\books.db --approve 'A:\Books\x.epub' --title 'Dune' --author 'Frank Herbert'
book-manager.exe --state A:\books.db --skip 'A:\Books\y.epub'

# Apply from the stored results (no provider requests for anything already settled).
book-manager.exe 'A:\Books' --provider both --state A:\books.db --apply --tag
```

A stored result is reused only when the file's size and modification time are unchanged
and the matching rules version is current. Measured costs: about 1.0 live request per
clean book and 1.5 to 1.7 for messy real files; local analysis (reading, language,
word count, contents) is 2 to 13 ms per book, so the network is the limit. Provider
responses are cached in sharded folders, and heavy evidence is dropped from memory once
a result is saved (use `--json` for a full report). `--recheck` looks up reviewed books
again after the rules have improved.

Housekeeping for big libraries: tagging keeps each original in `.book-manager-backup`,
which roughly doubles disk use. `--prune-backups DIR --older-than-days 30` deletes a
backup only when its tagged file still has exactly the hash recorded at tagging time
(so it provably has not been touched since); those books can no longer be undone.
Two files that resolve to the same book are reported together with their sizes, never
overwritten or deleted.

## Using Google Books (recommended: `--provider google-first`)

Set `GOOGLE_BOOKS_API_KEY` in the environment (never on the command line or in a file).
`--provider google-first` asks Google for metadata and covers and falls back to OpenLibrary,
which has no quota, only for books Google could not settle.

- **Quota.** The Books API allows 100 queries per minute per user (the client holds 90, and
  on a 429 every worker pauses together) and, by default, 1,000 queries per day per project.
  A daily limit stops live requests and leaves unprocessed books unsaved, so re-running
  with the same `--state` file the next day continues where it stopped. A much larger
  library needs a quota increase requested in the Google Cloud console.
- **Queries.** Google's `intitle:`/`inauthor:`/`isbn:` operators currently return nothing, so
  lookups are plain words, then the title as an exact phrase restricted to the book's own
  language. `isbn:` queries are off by default (`--google-isbn-search` re-enables them).
  Responses are limited to the fields the matcher reads (a quarter of the payload) and the
  search result is reused instead of fetching a record per candidate (`--google-details`
  restores that).
- **Covers: quality over quantity.** A book with no good cover gets none, never a poor one.
  Search results only link a 128 pixel thumbnail with a page-curl effect, so the tool asks for
  the same image at `--cover-width` (default 1200); Google returns the original if that is
  smaller and never an upscaled copy, so a narrow answer means a small original. Every
  candidate must pass a content check before it is used: portrait shape (a 2000x320 strip
  that says "HARRY POTTER" is rejected), at least `--min-cover-width` (default 500), real
  artwork rather than a blank or generated grey template, not a scanned page of text (the
  "front cover" Google holds for some books is an interior title page), and not Google's
  "image not available" picture, which is recognised by how it looks at any scale because it
  comes as both PNG and JPEG and is resized to whatever width is requested. The accepted
  editions are ranked (the edition matching the file's ISBN first, film and TV tie-ins last),
  the widest valid cover wins, and a missing cover never stops the rest of the metadata from
  being written. If the identifying provider has no good cover, the other providers are asked
  for the same work and their images go through the identical gate (OpenLibrary's largest
  covers are about 320x500, so at the default bar only Google qualifies).
  `--upgrade-covers` also replaces an existing cover narrower than `--min-cover-width`, but
  only when the provider's image is provably the same artwork (perceptual match) at higher
  resolution; a different edition's cover never replaces a book's own.

## Lossless compression (`src/shrink.rs`, not in the default flow)

Measured on 290 real EPUBs (600 MB): 75% of the bytes are JPEG, 20% text, 6% PNG.
Recompressing text with zopfli and PNGs with oxipng saves only 2 to 3% and costs 2 to 3
seconds per book, so it is not applied automatically. It is available as a library and
`cargo run --release --example shrink_probe -- DIR [EFFORT] [LIMIT]`, and it is strict:
text is byte-identical after decompression, PNGs decode to identical pixels, JPEG, GIF and
fonts are copied untouched, a change is kept only if smaller, and the copy is verified
member by member. Lossless JPEG optimization (the other 4 to 8%) needs a C encoder and is
not built.

## Providers compared (measured 2026-10-02 unless marked)

| Provider | Key | Limits | Strengths | Weaknesses |
|---|---|---|---|---|
| Google Books | free key | 100/min, 1,000/day by default | ISBN, pages, language, publisher; best overall match rate (47 of 104 unseen Gutenberg, 77 of 77 modern with the chain) | operators broken; daily quota is the cap; some titles drowned by tie-ins |
| OpenLibrary | none | polite use (about 1 to 8/s worked) | no quota, aliases, works | some bad catalogue data (Larsson filed under the Swedish title) |
| Apple Books (`--provider apple`, experimental) | none | documented about 20/min; in practice blocked (HTTP 403) after about 15 to 45 requests, for 35+ minutes | covers up to 2000 px; 26 of 30 modern titles found in a hand test | no ISBN, language or pages; US titles (Sorcerer's Stone); sticky blocks, so it pauses itself for 45 minutes after one 403 |
| Hardcover (not built; from its docs) | free token | 60/min, burst 10, 5,000/day on the free plan | series name and position, editions, ISBNs, covers | not tested here: needs an account token |
| Wikidata, Library of Congress | none | slow (LoC about 3 s per query) | authoritative catalogue data | no covers or page counts |

## Full-text witness (independent evidence from the book's own text)

When a provider record agrees with the file's title and author but nothing in the file
corroborates it (no title page text, no matching cover), the tool searches up to three
14-word passages from the middle of the book in Google Books full text. If the passage is
found in a book with this title and author, that is evidence which does not depend on the
file's tags at all. The same witness settles a title page that merely disagrees with the
tags (a spelling or edition variant); if the tags were wrong, the passage is found under a
different book and the file stays in review. Neither the page text nor the cover may be
used to accept a file whose title page contradicts its tags.

Measured on 104 unseen Gutenberg books: this took automatic matches from 47 to 75 correct
(72%) with 0 wrong. Attack test: 160 files with deliberately corrupted tags (wrong author,
wrong title, swapped identity, unknown author) were all still refused. A passage taken from
a different book wrongly corroborated the claimed book 0 of 30 times. If any provider fails
(daily quota, outage), the next one is tried, so a book only errors when all of them fail.

## Outcomes that need no human

- **Declared collections** ("Complete Works of ...", a title listing several works) are named
  by their own declared title and the first creator, instead of being sent to review: there
  is no single work to look up. Structural signs alone (chapter numbering restarting,
  repeated bylines) no longer block a book, but they forbid accepting it on a title page or
  cover; only the book's own text may, and every passage (at least two) must be found under
  the tagged title.
- **Duplicates** never collide. The first file (by path) takes the plain name; a byte-identical
  copy is reported as `duplicate` and left exactly where it is; a different file of the same
  book takes the next free numbered name ("Author - Title (2).epub"). Nothing is overwritten
  or deleted.
- A provider that fails (daily quota, outage) is skipped for that book and the next one is
  tried; a book errors only when every provider fails.

## What the tool will not do, and why

- **It will not trust tags on their own.** An experiment with a mode that named any book from
  its embedded tags (when nothing contradicted them) gained 14 of 104 genuine books but also
  accepted 175 of 312 files whose tags had been deliberately corrupted. That mode was removed.
- **A file cannot claim to be a collection.** A collection title is accepted only when nothing
  contradicts it and either the book's own title page agrees or its contents really are a
  bundle and its own front text shows both the title and the author. (Corrupted-file attacks
  found two holes here; both are closed and covered by tests.)
- **A book names itself** (`self_consistent`) only when its tags and printed title page agree,
  its language is confirmed, it is not a bundle, no provider lists the same title under another
  author, and every provider answered (a provider that merely failed never counts).
- **OpenLibrary full-text search** (Internet Archive scans, no key, no daily quota) is a second
  text witness behind Google's; alone it confirmed 31% of the hard cases with 0 false confirmations.
