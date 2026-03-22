PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS works (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_title     TEXT    NOT NULL,
    canonical_authors   TEXT    NOT NULL,
    openlibrary_work_id TEXT,
    created_at          TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS editions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    title                TEXT,
    authors              TEXT,
    isbn                 TEXT,
    series_name          TEXT,
    series_position      TEXT,
    publisher            TEXT,
    publish_date         TEXT,
    language             TEXT,
    description          TEXT,
    cover_image_path     TEXT,
    source_path          TEXT    NOT NULL UNIQUE,
    work_id              INTEGER REFERENCES works(id),
    owned                INTEGER NOT NULL DEFAULT 1,
    enriched_at          TEXT,
    enrichment_attempted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_editions_isbn    ON editions(isbn);
CREATE INDEX IF NOT EXISTS idx_editions_work_id ON editions(work_id);
