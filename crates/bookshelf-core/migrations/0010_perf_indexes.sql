-- Speed up grab-list computation on large libraries.
-- idx_editions_owned accelerates "SELECT ... WHERE owned = 1" scans.
-- idx_editions_owned_work accelerates the work-level AC-41c JOIN (only rows
-- with a work_id are indexed to keep the index tight).
-- idx_editions_isbn accelerates the per-want ISBN EXISTS sub-query in AC-41a.
-- idx_want_attempts accelerates the backoff check in should_retry.
CREATE INDEX IF NOT EXISTS idx_editions_owned      ON editions(owned);
CREATE INDEX IF NOT EXISTS idx_editions_owned_work ON editions(owned, work_id) WHERE work_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_editions_isbn       ON editions(isbn);
CREATE INDEX IF NOT EXISTS idx_want_attempts       ON want_list(download_attempts, last_attempted_at);
