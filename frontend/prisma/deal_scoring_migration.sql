-- deal_scoring_migration.sql
-- Adds deal-scoring columns to the Record table.
--
-- Run manually against your database before deploying the updated crawler:
--
--   psql "$DATABASE_URL" -f deal_scoring_migration.sql
--
-- All statements use IF NOT EXISTS / DO NOTHING so the migration is safe
-- to re-run and the crawler's ensure_schema_extras() will also apply these
-- idempotently on each crawl startup.
-- ---------------------------------------------------------------------------

-- deal_score — computed tier: 1 = Good Deal, 2 = Great Deal, 3 = Best Price
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS deal_score SMALLINT;

-- last_flagged_at — UTC timestamp of the most recent deal flag (used for cooldown)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS last_flagged_at TIMESTAMPTZ;

-- last_flagged_price — price at time of last flag (used for early-re-flag detection)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS last_flagged_price DECIMAL(10, 2);

-- avg_30d — adaptive 30-day rolling average (falls back to all-time avg if < 30 days data)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS avg_30d DECIMAL(10, 2);

-- avg_90d — 90-day rolling average (confirmation signal; NULL when < 90 days of history)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS avg_90d DECIMAL(10, 2);

-- low_30d — lowest price recorded in the last 30 days (proximity anchor)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS low_30d DECIMAL(10, 2);

-- low_all_time — lowest price ever recorded for this product
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS low_all_time DECIMAL(10, 2);

-- confidence_level — scoring confidence tier:
--   insufficient_data | low_confidence | moderate_confidence | high_confidence
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS confidence_level VARCHAR(30);

-- history_days — number of days spanned by price history (0 for brand-new products)
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS history_days INTEGER;

-- last_crawled_at — UTC timestamp of the most recent crawler visit (upsert or stale-check).
-- Set on every write. Used by the frontend to suppress stale deal badges (> 4 hours old).
ALTER TABLE "Record"
    ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMPTZ;

-- Index on deal_score for fast active-deal queries (Phase 0 re-validation)
CREATE INDEX IF NOT EXISTS "Record_deal_score_idx"
    ON "Record" (deal_score)
    WHERE deal_score IS NOT NULL;
