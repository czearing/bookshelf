CREATE TABLE IF NOT EXISTS followed_authors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    ol_key      TEXT,
    last_synced TEXT,
    added_at    TEXT NOT NULL
);
