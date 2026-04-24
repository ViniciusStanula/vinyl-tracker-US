import os
import logging
import re
import unicodedata

log = logging.getLogger(__name__)

# INDEXNOW_PLACEHOLDER — set INDEXNOW_KEY to your new IndexNow API key
_KEY = os.environ.get("INDEXNOW_KEY", "")
# INDEXNOW_PLACEHOLDER — set INDEXNOW_HOST to your production domain (e.g. "www.example.com")
_HOST = os.environ.get("INDEXNOW_HOST", "")
_BASE = f"https://{_HOST}" if _HOST else ""
_KEY_LOCATION = f"{_BASE}/{_KEY}.txt" if (_KEY and _HOST) else ""
_ENDPOINT = "https://api.indexnow.org/IndexNow"
_BATCH_SIZE = 10_000


def _slugify_artist(name: str) -> str:
    """Port of frontend/lib/slugify.ts slugifyArtist."""
    if "," in name:
        last, *rest = name.split(",")
        first = ",".join(rest).strip()
        name = f"{first} {last.strip()}" if first else name
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    name = name.strip("-")
    return name[:60]


def _slugify_style(tag: str) -> str:
    """Port of frontend/lib/styleUtils.ts slugifyStyle."""
    tag = unicodedata.normalize("NFD", tag)
    tag = "".join(c for c in tag if unicodedata.category(c) != "Mn")
    tag = tag.lower()
    tag = re.sub(r"[^a-z0-9]+", "-", tag)
    return tag.strip("-")


def build_urls(items: list[dict], db_styles: list[str] | None = None) -> list[str]:
    """Build all frontend URLs from crawled items: record + artist + style pages.

    `db_styles` is a list of raw lastfm tag strings from the DB
    (each is a comma-separated tag list like "Rock, Classic Rock").
    Required because `style` is not present in crawled item dicts.
    """
    urls: list[str] = []
    seen_artists: set[str] = set()
    seen_styles: set[str] = set()

    for item in items:
        slug = item.get("slug")
        if slug:
            urls.append(f"{_BASE}/record/{slug}")

        artist = item.get("artist") or ""
        if artist:
            artist_slug = _slugify_artist(artist)
            if artist_slug and artist_slug not in seen_artists:
                seen_artists.add(artist_slug)
                urls.append(f"{_BASE}/artist/{artist_slug}")

    for raw in (db_styles or []):
        for tag in raw.split(", "):
            tag = tag.strip()
            if not tag:
                continue
            style_slug = _slugify_style(tag)
            if style_slug and style_slug not in seen_styles:
                seen_styles.add(style_slug)
                urls.append(f"{_BASE}/genre/{style_slug}")

    log.info(
        "IndexNow: built %d URLs (%d record, %d artist, %d genre).",
        len(urls),
        len(urls) - len(seen_artists) - len(seen_styles),
        len(seen_artists),
        len(seen_styles),
    )
    return urls


def submit_crawl_results(items: list[dict], db_styles: list[str] | None = None) -> None:
    """Build and submit all frontend URLs derived from crawled items.

    Pass `db_styles` (raw lastfm tag strings from the Record table) to also
    submit style pages — they are not available on crawled item dicts.
    """
    if not items:
        log.info("IndexNow: no items — skipping submission.")
        return
    if not _KEY or not _HOST:
        log.warning("IndexNow: INDEXNOW_KEY or INDEXNOW_HOST not set — skipping submission.")
        return
    urls = build_urls(items, db_styles)
    submit_urls(urls)


def submit_urls(urls: list[str]) -> None:
    if not urls:
        log.info("IndexNow: no URLs to submit.")
        return
    if not _KEY or not _HOST:
        log.warning("IndexNow: INDEXNOW_KEY or INDEXNOW_HOST not set — skipping submission.")
        return

    import requests as _requests

    total = len(urls)
    log.info("IndexNow: submitting %d URL(s) in batch(es) of %d...", total, _BATCH_SIZE)

    submitted = 0
    for i in range(0, total, _BATCH_SIZE):
        batch = urls[i : i + _BATCH_SIZE]
        payload = {
            "host": _HOST,
            "key": _KEY,
            "keyLocation": _KEY_LOCATION,
            "urlList": batch,
        }
        try:
            resp = _requests.post(
                _ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=30,
            )
            if _handle_response(resp, len(batch)):
                submitted += len(batch)
        except Exception as exc:
            log.warning("IndexNow: request error (non-fatal): %s", exc)

    log.info("IndexNow: done — %d/%d URLs accepted.", submitted, total)


def _handle_response(resp, batch_size: int) -> bool:
    """Log the response and return True if the batch was accepted."""
    code = resp.status_code
    if code == 200:
        log.info("IndexNow: HTTP 200 OK — %d URLs accepted.", batch_size)
        return True
    elif code == 202:
        log.info("IndexNow: HTTP 202 — %d URLs queued for processing.", batch_size)
        return True
    elif code == 400:
        log.error("IndexNow: HTTP 400 — bad request. Malformed payload.")
    elif code == 403:
        log.error(
            "IndexNow: HTTP 403 — key invalid or key file unreachable at %s.",
            _KEY_LOCATION,
        )
    elif code == 422:
        log.error(
            "IndexNow: HTTP 422 — URLs don't match declared host '%s'.", _HOST
        )
    elif code == 429:
        log.warning("IndexNow: HTTP 429 — rate limited. Reduce submission frequency.")
    else:
        log.warning(
            "IndexNow: HTTP %d — unexpected response: %s",
            code,
            resp.text[:300],
        )
    return False
