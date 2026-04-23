"""
database.py — PostgreSQL persistence layer for the vinyl crawler.

Responsibilities:
  - Connect to Supabase via DATABASE_URL env var
  - Upsert Record rows (insert or update metadata)
  - Insert PriceHistory rows (always append, never update)
  - Clean up PriceHistory rows older than 365 days
"""
import os
import socket
import logging
import urllib.parse

import contextlib

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


@contextlib.contextmanager
def _cursor(conn):
    """
    Opens a cursor and immediately disables the statement timeout for the
    current transaction.  Supabase's PgBouncer (transaction mode) resets
    session-level settings between transactions, so the only reliable way
    to override the role-level statement_timeout is with SET LOCAL inside
    every transaction block.
    """
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = 0")
        yield cur


def get_connection():
    """
    Returns a psycopg2 connection using DATABASE_URL from environment.
    Use the Transaction Pooler URL from Supabase (port 6543).

    Resolves the hostname to an IPv4 address and passes it via libpq's
    ``hostaddr`` parameter so GitHub Actions (which can't reach Supabase
    over IPv6) connects successfully.  The original hostname is kept in
    ``host`` for SSL certificate validation (SNI).
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set.\n"
            "Export it before running:\n"
            "  export DATABASE_URL='postgresql://postgres:[PASSWORD]@...supabase.com:6543/postgres'"
        )

    # Force IPv4 to avoid IPv6 connectivity failures on GitHub Actions.
    # libpq's hostaddr overrides DNS resolution while host= still handles SSL SNI.
    try:
        parsed = urllib.parse.urlparse(database_url)
        hostname = parsed.hostname
        if hostname:
            ipv4_results = socket.getaddrinfo(hostname, None, socket.AF_INET)
            if ipv4_results:
                ipv4 = ipv4_results[0][4][0]
                log.debug("Resolved %s → %s (IPv4)", hostname, ipv4)
                return psycopg2.connect(
                    database_url,
                    hostaddr=ipv4,
                    options="-c statement_timeout=0 -c idle_in_transaction_session_timeout=60000",
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
    except Exception as exc:
        log.warning("IPv4 resolution failed (%s) — falling back to default DNS", exc)

    return psycopg2.connect(
        database_url,
        options="-c statement_timeout=0 -c idle_in_transaction_session_timeout=60000",
        keepalives=1,
        keepalives_idle=60,
        keepalives_interval=10,
        keepalives_count=5,
    )


def upsert_batch(conn, items: list[dict]) -> int:
    """
    Upserts a batch of crawled items into the database.

    For each item:
      - Inserts or updates the Record row (metadata: title, artist, img, url, rating)
      - Always inserts a new PriceHistory row (price history is append-only)

    Uses executemany for performance.
    Returns the number of items processed.
    """
    if not items:
        return 0

    with _cursor(conn) as cur:
        # ── Step 1: upsert Record metadata ────────────────────────────────
        # ON CONFLICT (asin) → update mutable fields only.
        # slug and createdAt are never overwritten once set.
        record_rows = [
            (
                item["asin"],
                item["title"],
                item["artist"],
                item["slug"],
                item.get("style") or None,
                item.get("imgUrl") or None,
                item["url"],
                item.get("rating"),        # float or None
                item.get("reviewCount"),   # int or None
            )
            for item in items
        ]

        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO "Record" (
                id, asin, title, artist, slug, style, "imgUrl", url, rating,
                "reviewCount", "createdAt", "updatedAt", last_crawled_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s,
                NOW(), NOW(), NOW()
            )
            ON CONFLICT (asin) DO UPDATE SET
                title           = EXCLUDED.title,
                artist          = EXCLUDED.artist,
                style           = COALESCE(EXCLUDED.style, "Record".style),
                "imgUrl"        = EXCLUDED."imgUrl",
                url             = EXCLUDED.url,
                rating          = EXCLUDED.rating,
                "reviewCount"   = COALESCE(EXCLUDED."reviewCount", "Record"."reviewCount"),
                "updatedAt"     = NOW(),
                last_crawled_at = NOW()
            """,
            record_rows,
            page_size=500,
        )

        log.debug("Upserted %d Record rows.", len(record_rows))

        # ── Step 2: fetch asin → id map for the items we just upserted ───
        asins = [item["asin"] for item in items]
        cur.execute(
            'SELECT asin, id FROM "Record" WHERE asin = ANY(%s)',
            (asins,)
        )
        asin_to_id = {row[0]: row[1] for row in cur.fetchall()}

        # ── Step 3: write PriceHistory every crawl ────────────────────────────
        # Record every price capture regardless of whether the price changed.
        # Gives 2-hour granularity for the chart and deal scorer.
        #
        # Skip active deals (deal_score IS NOT NULL): their prices come from
        # Phase 0 product-page fetches which are authoritative. Search-result
        # cards can show the CD price for a multi-format vinyl ASIN (Amazon
        # aggregates the cheapest format in listings), which would corrupt the
        # chart and the deal scorer with a false low price.
        price_rows = []
        for item in items:
            record_id = asin_to_id.get(item["asin"])
            if record_id is None:
                continue
            price_rows.append((str(record_id), item["price"], item["capturedAt"], str(record_id)))

        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO "PriceHistory" (id, "recordId", "price", "capturedAt")
            SELECT gen_random_uuid(), %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM "Record" WHERE id = %s AND deal_score IS NOT NULL
            )
            """,
            price_rows,
            page_size=500,
        )

        log.debug("Inserted %d PriceHistory rows.", len(price_rows))

    conn.commit()
    return len(items)


def ensure_schema_extras(conn) -> None:
    """
    Idempotently adds columns that are not part of the Prisma schema
    (managed here via raw DDL so no migration tooling is needed).

    Columns added:
      Record.available BOOLEAN NOT NULL DEFAULT TRUE
        — FALSE when the product page returned 404 or showed out-of-stock.
          Records with available = FALSE are still queried for stale checks
          so they can come back online.

      Record.deal_score SMALLINT
        — Computed deal tier: 1 = Good Deal, 2 = Great Deal,
          3 = Best Price. NULL means no active deal.

      Record.last_flagged_at TIMESTAMPTZ
        — UTC timestamp of the most recent deal flag (used for cooldown).

      Record.last_flagged_price DECIMAL(10,2)
        — Price at time of last flag (used for early-re-flag detection).

      Record.avg_30d / avg_90d / low_30d / low_all_time DECIMAL(10,2)
        — Rolling price benchmarks updated by the deal scorer after each crawl.

      Record.confidence_level VARCHAR(30)
        — Scoring confidence tier: insufficient_data | low_confidence |
          moderate_confidence | high_confidence.

      Record.history_days INTEGER
        — Number of days spanned by the product's price history.

      Record.last_crawled_at TIMESTAMPTZ
        — UTC timestamp of the most recent crawler visit (upsert or stale-check).
          Set on every write, including price-unchanged upserts. Used by the
          frontend to suppress deal badges older than 4 hours.
    """
    with _cursor(conn) as cur:
        # Fast path: check the catalog first. After the first successful run
        # all columns exist and we can skip DDL entirely (no lock needed).
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'Record'
              AND column_name IN (
                'available','deal_score','last_flagged_at','last_flagged_price',
                'avg_30d','avg_90d','low_30d','low_all_time','confidence_level',
                'history_days','last_crawled_at','lastfm_tags'
              )
            """
        )
        existing = cur.fetchone()[0]
        if existing == 12:
            log.debug("ensure_schema_extras: schema already complete, skipping DDL.")
            return

        # Columns are missing — run DDL.
        # lock_timeout prevents hanging when Vercel/other clients hold a read lock.
        # statement_timeout=0 is already set at the connection level via options.
        cur.execute("SET LOCAL lock_timeout = '10s'")
        cur.execute(
            """
            ALTER TABLE "Record"
                ADD COLUMN IF NOT EXISTS available          BOOLEAN      NOT NULL DEFAULT TRUE,
                ADD COLUMN IF NOT EXISTS deal_score         SMALLINT,
                ADD COLUMN IF NOT EXISTS last_flagged_at    TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS last_flagged_price DECIMAL(10, 2),
                ADD COLUMN IF NOT EXISTS avg_30d            DECIMAL(10, 2),
                ADD COLUMN IF NOT EXISTS avg_90d            DECIMAL(10, 2),
                ADD COLUMN IF NOT EXISTS low_30d            DECIMAL(10, 2),
                ADD COLUMN IF NOT EXISTS low_all_time       DECIMAL(10, 2),
                ADD COLUMN IF NOT EXISTS confidence_level   VARCHAR(30),
                ADD COLUMN IF NOT EXISTS history_days       INTEGER,
                ADD COLUMN IF NOT EXISTS last_crawled_at    TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS lastfm_tags        TEXT
            """
        )
        # Partial index for fast active-deal lookups (Phase 0 re-validation)
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS "Record_deal_score_idx"
                ON "Record" (deal_score)
                WHERE deal_score IS NOT NULL
            """
        )
    conn.commit()
    log.info("ensure_schema_extras: schema migration applied.")


def fetch_active_deals(conn) -> list[dict]:
    """
    Returns all Record rows that are currently on an active deal.

    A deal is active when deal_score IS NOT NULL — i.e. the deal scorer
    (deal_scorer.score_deals) has evaluated the product and assigned a tier.
    All active deals are returned on every call (no recency filter) because
    deal prices can change within minutes. Results are ordered by deal_score
    DESC so the highest-quality deals are re-validated first in Phase 0.

    Each returned dict has: asin, id, title.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            SELECT asin, id, COALESCE(title, '') AS title
            FROM "Record"
            WHERE deal_score IS NOT NULL
            ORDER BY deal_score DESC, "updatedAt" ASC
            """,
        )
        return [
            {"asin": row[0], "id": str(row[1]), "title": row[2]}
            for row in cur.fetchall()
        ]


def fetch_stale_records(
    conn,
    seen_asins: set[str],
    limit: int = 500,
) -> list[dict]:
    """
    Returns Record rows whose ASINs were NOT encountered during this crawl run.

    Priority order within the limit:
      1. Records with an active deal score — may have drifted off search results
         while still carrying a deal badge.
      2. Records flagged as a deal in the past 14 days — ensures recently-promoted
         records are re-checked even after dropping from search results.
      3. All others by last_crawled_at ASC NULLS FIRST — most neglected first.

    Each returned dict has: asin, id, title.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            SELECT asin, id, COALESCE(title, '') AS title
            FROM "Record"
            WHERE asin != ALL(%s)
            ORDER BY
                CASE
                    WHEN deal_score IS NOT NULL                        THEN 0
                    WHEN last_flagged_at > NOW() - INTERVAL '14 days' THEN 1
                    ELSE                                                    2
                END,
                last_crawled_at ASC NULLS FIRST
            LIMIT %s
            """,
            (list(seen_asins) if seen_asins else ["__none__"], limit),
        )
        return [
            {"asin": row[0], "id": row[1], "title": row[2]}
            for row in cur.fetchall()
        ]


def mark_stale_price(
    conn,
    record_id: str,
    price: float,
    captured_at,
    review_count: int | None = None,
) -> None:
    """
    Inserts a new PriceHistory entry for a stale record whose product page
    confirmed it is still available, and resets available to TRUE (in case it
    had previously been marked unavailable).

    Always inserts — no dedup window. Matches upsert_batch behaviour so Phase 0
    and Phase 3 records appear in the chart on every crawl cycle.

    If review_count is provided it overwrites the stored value; otherwise the
    existing count is preserved via COALESCE.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO "PriceHistory" (id, "recordId", "price", "capturedAt")
            VALUES (gen_random_uuid(), %s, %s, %s)
            """,
            (record_id, price, captured_at),
        )
        cur.execute(
            """
            UPDATE "Record"
            SET available       = TRUE,
                "reviewCount"   = COALESCE(%s, "reviewCount"),
                "updatedAt"     = NOW(),
                last_crawled_at = NOW()
            WHERE id = %s
            """,
            (review_count, record_id),
        )
    conn.commit()


def clear_deal_score(conn, record_id: str) -> None:
    """
    Clears deal_score so the product stops appearing as a deal, without
    marking it unavailable. Used when the scraper cannot confirm the vinyl
    price (e.g. multi-format page served with a non-vinyl format selected)
    but the product is still listed as in-stock.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            UPDATE "Record"
            SET deal_score      = NULL,
                "updatedAt"     = NOW(),
                last_crawled_at = NOW()
            WHERE id = %s
            """,
            (record_id,),
        )
    conn.commit()


def mark_unavailable(conn, record_id: str) -> None:
    """
    Marks a Record as unavailable (product page 404 or out-of-stock).
    Also clears deal_score so the product stops appearing as a deal — we
    cannot confirm the price is still valid when the page is unreachable.
    Does NOT insert a PriceHistory entry.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            UPDATE "Record"
            SET available       = FALSE,
                deal_score      = NULL,
                "updatedAt"     = NOW(),
                last_crawled_at = NOW()
            WHERE id = %s
            """,
            (record_id,),
        )
    conn.commit()


def ensure_category_tables(conn, category_seed: list[tuple[str, str]]) -> None:
    """
    Idempotently creates the Category and RecordCategories tables and seeds
    Category with the known genre URLs.

    category_seed: list of (url, name) pairs, one per entry in CATEGORY_URLS.
    Safe to call on every startup — CREATE TABLE IF NOT EXISTS and
    INSERT ... ON CONFLICT DO NOTHING make it a no-op after the first run.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "Category" (
                id         SERIAL        PRIMARY KEY,
                name       TEXT          NOT NULL,
                url        TEXT          NOT NULL UNIQUE,
                created_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "RecordCategories" (
                record_id     TEXT        NOT NULL REFERENCES "Record"(id)   ON DELETE CASCADE,
                category_id   INTEGER     NOT NULL REFERENCES "Category"(id) ON DELETE CASCADE,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (record_id, category_id)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS "RecordCategories_category_id_idx"
                ON "RecordCategories" (category_id)
            """
        )
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO "Category" (url, name)
            VALUES (%s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
            category_seed,
            page_size=200,
        )
    conn.commit()
    log.debug("ensure_category_tables: tables ready.")


def upsert_category_associations(
    conn,
    asin_categories: dict[str, set[str]],
) -> int:
    """
    Records which categories each product was found in during this crawl run.

    asin_categories: maps ASIN → set of category URLs where it appeared.
    Uses upsert so first_seen_at is preserved on repeat visits and
    last_seen_at is always bumped to NOW().
    Returns the number of (record, category) rows written.
    """
    if not asin_categories:
        return 0

    with _cursor(conn) as cur:
        cur.execute('SELECT url, id FROM "Category"')
        url_to_cat_id: dict[str, int] = {row[0]: row[1] for row in cur.fetchall()}

        asins = list(asin_categories.keys())
        cur.execute(
            'SELECT asin, id FROM "Record" WHERE asin = ANY(%s)',
            (asins,),
        )
        asin_to_id = {row[0]: row[1] for row in cur.fetchall()}

        rows = []
        for asin, cat_urls in asin_categories.items():
            record_id = asin_to_id.get(asin)
            if record_id is None:
                continue
            for cat_url in cat_urls:
                cat_id = url_to_cat_id.get(cat_url)
                if cat_id is None:
                    log.warning("Category URL not in DB (skipping): %.80s", cat_url)
                    continue
                rows.append((str(record_id), cat_id))

        if not rows:
            return 0

        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO "RecordCategories" (record_id, category_id, first_seen_at, last_seen_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (record_id, category_id) DO UPDATE
                SET last_seen_at = EXCLUDED.last_seen_at
            """,
            rows,
            page_size=500,
        )
        log.debug("Upserted %d RecordCategories rows.", len(rows))

    conn.commit()
    return len(rows)


def fetch_untagged_artists(conn, artists: list[str] | None = None) -> list[str]:
    """
    Returns distinct artist values whose lastfm_tags column is NULL.

    If `artists` is provided, only those names are checked (incremental mode).
    If None, returns all untagged artists in the table (backfill mode).
    """
    with _cursor(conn) as cur:
        if artists:
            cur.execute(
                """
                SELECT DISTINCT artist FROM "Record"
                WHERE lastfm_tags IS NULL
                  AND artist = ANY(%s)
                """,
                (artists,),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT artist FROM "Record"
                WHERE lastfm_tags IS NULL
                ORDER BY artist
                """
            )
        return [row[0] for row in cur.fetchall()]


def bulk_update_tags(conn, artist_to_tags: dict[str, str]) -> int:
    """
    Sets lastfm_tags for every artist key in artist_to_tags.
    An empty-string value marks the artist as "fetched but no genre tags found"
    so it is not re-fetched on future runs.
    Returns the number of rows updated.
    """
    if not artist_to_tags:
        return 0

    rows = [(tags, artist) for artist, tags in artist_to_tags.items()]
    with _cursor(conn) as cur:
        psycopg2.extras.execute_batch(
            cur,
            'UPDATE "Record" SET lastfm_tags = %s WHERE artist = %s',
            rows,
            page_size=500,
        )
        updated = cur.rowcount
    conn.commit()
    log.debug("bulk_update_tags: updated tags for %d artist values.", len(rows))
    return updated


def delete_old_price_history(conn, days: int = 365) -> int:
    """
    Deletes PriceHistory records older than `days` days.
    Called at the end of each crawl run to keep the DB tidy.

    With 5,000 records × 2 crawls/day, keeping 365 days stores ~365MB.
    Stay within the free tier (500MB) or upgrade to Supabase Pro for more.
    Returns the number of rows deleted.
    """
    with _cursor(conn) as cur:
        cur.execute(
            """
            DELETE FROM "PriceHistory"
            WHERE "capturedAt" < NOW() - (%s * INTERVAL '1 day')
            """,
            (days,)
        )
        deleted = cur.rowcount
    conn.commit()
    if deleted > 0:
        log.info("Cleaned up %d PriceHistory rows older than %d days.", deleted, days)
    return deleted
