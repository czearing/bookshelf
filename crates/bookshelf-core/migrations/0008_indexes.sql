-- Speed up enrichment queue and duplicate-detection queries on large libraries.
-- source_path already has an implicit index via UNIQUE constraint.
CREATE INDEX IF NOT EXISTS idx_editions_enrichment_attempted ON editions(enrichment_attempted);
CREATE INDEX IF NOT EXISTS idx_editions_title_authors        ON editions(title, authors);
