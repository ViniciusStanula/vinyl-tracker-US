"""
lastfm.py — Last.fm tag enrichment for the vinyl crawler.

Fetches top genre tags from Last.fm's artist.getTopTags endpoint and writes
them to Disco.lastfm_tags.  Called once per new artist; tags are never
overwritten once stored (NULL → empty-string or comma-separated list).

Requires LASTFM_API_KEY environment variable.
Rate limit: Last.fm free API allows ~5 req/s.  Use a >=0.2 s delay between calls.
"""
import re
import json
import time
import logging
import urllib.parse
import urllib.request

from database import fetch_untagged_artists, bulk_update_tags

log = logging.getLogger(__name__)

LASTFM_BASE = "https://ws.audioscrobbler.com/2.0/"

# Tags that carry no genre information — personal labels, nationality markers,
# subjective adjectives, era labels, and instrument mentions.
NON_GENRE_TAGS: frozenset[str] = frozenset({
    # Personal / subjective
    "seen live", "beautiful", "favourite", "my favourite", "my favorites",
    "favorite", "favorites", "love", "awesome", "best", "amazing", "great",
    "perfect", "legend", "legends", "excellent", "loved", "buy", "under review",
    "spotify", "youtube", "all", "albums i own", "artists i've seen live",
    # Instruments / vocals (not genres)
    "guitar", "bass", "drums", "vocals", "instrumental", "piano", "keyboards",
    "male vocalists", "female vocalists",
    # Nationality (not genre)
    "american", "british", "german", "french", "swedish", "english", "scottish",
    "canadian", "australian", "norwegian", "brazilian", "japanese", "korean",
    "italian", "spanish", "portuguese", "irish", "danish", "finnish",
    # Generic descriptors
    "classic", "classics", "oldies", "retro", "vintage",
})

# Artist placeholder used when Amazon doesn't provide a name.
_UNKNOWN_RE = re.compile(r"artista\s+n[ãa]o\s+identificad[oa]", re.IGNORECASE)


def _uninvert(name: str) -> str:
    """Convert 'LAST, FIRST' to 'FIRST LAST' to match Last.fm entries."""
    if "," not in name:
        return name
    last, _, first = name.partition(",")
    first = first.strip()
    last = last.strip()
    return f"{first} {last}" if first else name


def fetch_artist_tags(artist_name: str, api_key: str, max_tags: int = 3) -> list[str]:
    """
    Returns up to max_tags genre tags for artist_name from Last.fm.
    Returns [] for placeholder artists, on API errors, or if no genre tags exist.
    """
    if _UNKNOWN_RE.search(artist_name):
        return []

    name = _uninvert(artist_name)
    params = urllib.parse.urlencode({
        "method": "artist.getTopTags",
        "artist": name,
        "api_key": api_key,
        "format": "json",
    })

    try:
        with urllib.request.urlopen(f"{LASTFM_BASE}?{params}", timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        log.debug("Last.fm request failed for %r: %s", artist_name, exc)
        return []

    if "error" in data:
        log.debug("Last.fm API error for %r (%s): %s",
                  artist_name, data.get("error"), data.get("message"))
        return []

    raw_tags: list[dict] = data.get("toptags", {}).get("tag", [])
    tags: list[str] = []
    for tag in raw_tags:
        label = tag.get("name", "").lower().strip()
        if not label or len(label) < 2 or len(label) > 40:
            continue
        if label in NON_GENRE_TAGS:
            continue
        tags.append(label)
        if len(tags) >= max_tags:
            break

    return tags


def enrich_new_artists(
    conn,
    artistas: set[str],
    api_key: str | None,
    delay: float = 0.21,
    deadline: float | None = None,
) -> int:
    """
    Fetches Last.fm tags for artists in `artistas` that have lastfm_tags IS NULL.
    Stores the result (even if empty) so they are not re-fetched next run.
    Returns the number of artists updated.

    `delay` controls the inter-request pause (≥0.2 s keeps us under 5 req/s).
    `deadline` is a monotonic timestamp; the loop stops early when reached so
    the caller's hard time limit is respected. Unprocessed artists remain NULL
    and will be picked up on the next run.
    """
    if not api_key:
        log.debug("LASTFM_API_KEY not set — skipping tag enrichment.")
        return 0

    # Only process artistas from this batch that still have NULL tags in the DB.
    needs_tags = fetch_untagged_artists(conn, list(artistas))
    if not needs_tags:
        log.debug("Tag enrichment: all artists in this batch already have tags.")
        return 0

    log.info("Tag enrichment: fetching tags for %d new artists.", len(needs_tags))
    updates: dict[str, str] = {}

    for i, artista in enumerate(needs_tags, 1):
        if deadline is not None and time.monotonic() >= deadline:
            log.info("Tag enrichment: deadline reached — saved %d/%d, remaining picked up next run.", i - 1, len(needs_tags))
            break
        tags = fetch_artist_tags(artista, api_key)
        updates[artista] = ", ".join(tags)   # "" if no tags — marks as fetched
        log.debug("[%d/%d] %r → %r", i, len(needs_tags), artista, updates[artista])
        if i < len(needs_tags):
            time.sleep(delay)

    return bulk_update_tags(conn, updates)
