CREATE TABLE IF NOT EXISTS want_list (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    author      TEXT,
    isbn13      TEXT,
    source      TEXT NOT NULL,
    source_id   TEXT,
    added_at    TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 5,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_want_list_isbn13 ON want_list(isbn13);
CREATE INDEX IF NOT EXISTS idx_want_list_source ON want_list(source);
