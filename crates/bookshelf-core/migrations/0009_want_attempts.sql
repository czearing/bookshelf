ALTER TABLE want_list ADD COLUMN download_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE want_list ADD COLUMN last_attempted_at TEXT;
