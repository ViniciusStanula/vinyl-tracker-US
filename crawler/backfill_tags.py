"""
backfill_tags.py — One-time script to tag all existing artists with Last.fm genres.

Fetches tags for every Disco row where lastfm_tags IS NULL, respecting Last.fm's
~5 req/s rate limit.  Safe to interrupt and re-run: already-tagged artists
(including those with an empty string) are skipped.

Usage:
    python backfill_tags.py
    python backfill_tags.py --dry-run        # print what would be fetched, no writes
    python backfill_tags.py --delay 0.3      # slower rate (default: 0.21 s)

Requires:
    LASTFM_API_KEY and DATABASE_URL in environment (or .env file).
"""
import os
import sys
import time
import argparse
import logging

# Load .env if python-dotenv is available (dev convenience).
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from database import get_connection, ensure_schema_extras, fetch_untagged_artists, bulk_update_tags
from lastfm import fetch_artist_tags

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Backfill Last.fm genre tags for all artists")
    p.add_argument("--dry-run",  action="store_true", help="Fetch tags but do not write to DB")
    p.add_argument("--delay",    type=float, default=0.21, metavar="S",
                   help="Seconds between Last.fm requests (default: 0.21)")
    p.add_argument("--batch",    type=int, default=100, metavar="N",
                   help="DB commit batch size (default: 100)")
    p.add_argument("--verbose",  action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    api_key = os.environ.get("LASTFM_API_KEY", "")
    if not api_key:
        log.error("LASTFM_API_KEY is not set. Aborting.")
        sys.exit(1)

    log.info("Connecting to database...")
    conn = get_connection()
    ensure_schema_extras(conn)   # adds lastfm_tags column if it doesn't exist yet

    untagged = fetch_untagged_artists(conn)   # all artists with lastfm_tags IS NULL
    total = len(untagged)
    log.info("Artists to tag: %d%s", total, "  (dry-run — no writes)" if args.dry_run else "")

    if total == 0:
        log.info("Nothing to do. All artists already have tags.")
        conn.close()
        return

    t_start = time.monotonic()
    batch: dict[str, str] = {}
    done = 0

    for i, artist_name in enumerate(untagged, 1):
        tags = fetch_artist_tags(artist_name, api_key)
        tag_str = ", ".join(tags)
        batch[artist_name] = tag_str

        if args.verbose or i % 50 == 0:
            log.info("[%d/%d] %r → %s", i, total, artist_name, repr(tag_str) if tag_str else "(none)")

        # Flush batch to DB
        if not args.dry_run and len(batch) >= args.batch:
            written = bulk_update_tags(conn, batch)
            done += written
            log.info("  Committed %d rows (%d/%d total).", written, done, total)
            batch = {}

        if i < total:
            time.sleep(args.delay)

    # Final flush
    if not args.dry_run and batch:
        written = bulk_update_tags(conn, batch)
        done += written

    conn.close()
    elapsed = time.monotonic() - t_start
    log.info(
        "Done. %d/%d artists %s in %.0fs.",
        done if not args.dry_run else total,
        total,
        "tagged" if not args.dry_run else "inspected (dry-run)",
        elapsed,
    )


if __name__ == "__main__":
    main()
