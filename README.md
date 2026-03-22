# bookshelf

A Rust CLI tool that recursively scans a directory for EPUB files, extracts
OPF metadata, stores it in a local SQLite database, and enriches records via
the OpenLibrary and Google Books APIs.

## Prerequisites

- Rust stable toolchain (`rustup install stable`)
- Windows MSVC build tools (Visual Studio Build Tools 2022)
- Internet access for the `bookshelf enrich` command

## Database location

The SQLite database is stored at:

```
%APPDATA%\bookshelf\library.db
```

On Windows this resolves to `C:\Users\<username>\AppData\Roaming\bookshelf\library.db`.

Override with `--db <path>`:

```
bookshelf --db C:\my\custom\path.db list
```

## Build

```
cargo build --release
```

The binary is produced at `target\release\bookshelf.exe`.

## Usage

### Scan a directory

```
bookshelf scan <path>
```

Recursively discovers every `.epub` file under `<path>`, parses OPF metadata,
and inserts new records into the database. Running scan twice on the same
directory does not create duplicate records.

### List all books

```
bookshelf list
```

Prints `id`, `title`, and `authors` for every book in the library.

### Search by title or author

```
bookshelf search "The Hobbit"
bookshelf search "Tolkien"
bookshelf search "Hobitt"    # typo tolerance via fuzzy matching
```

Returns results ordered by descending similarity score. The search threshold
is `0.72` (configurable constant `SEARCH_THRESHOLD` in `bookshelf-core/src/fuzzy.rs`).

### Show full metadata

```
bookshelf info <id>
```

Prints all non-NULL columns for the edition with the given integer `id`.
Exits with a non-zero status if the id does not exist.

### Enrich metadata from external APIs

```
bookshelf enrich
```

For every book where `enrichment_attempted = 0`:
1. Queries the OpenLibrary API by ISBN.
2. Falls back to Google Books if OpenLibrary returns no data.
3. Falls back to an OpenLibrary title+author search if no ISBN is present.

After a successful enrichment, sets `enriched_at` to the current UTC
timestamp and `enrichment_attempted = 1`.

## Running tests

```
cargo test
```

Integration tests generate fixture EPUBs programmatically using the `zip`
crate — no binary blobs are committed. Two reference fixture files
(`tests/fixtures/the_hobbit.epub` and `tests/fixtures/the_hobbit_alt.epub`)
are committed for AC-51 compliance and can be regenerated at any time with:

```
cargo run --bin make_fixtures
```

## Known limitations

- Fuzzy deduplication uses an O(n²) scan of all no-ISBN editions on each new
  insert. For libraries of thousands of books this will be noticeably slow;
  a future phase can replace this with an indexed similarity search.
- DRM-encrypted EPUBs are skipped with a warning.
- Only EPUB format is supported; PDF, MOBI, and other formats are ignored.
