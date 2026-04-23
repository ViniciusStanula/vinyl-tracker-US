"""
main.py — Daily Amazon Brazil vinyl crawler → PostgreSQL (Supabase)
────────────────────────────────────────────────────────────────────
Crawls Amazon.com.br for vinyl records from the main popularity-ranked
vinyl page, appends price data to PostgreSQL for historical tracking.

Usage:
    python main.py                        # crawl all pages
    python main.py --max-pages 3          # limit pages
    python main.py --dry-run              # crawl but don't write to DB

Schedule (GitHub Actions):
    cron: '0 9,21 * * *'   # 9h and 21h UTC (6h and 18h BRT)

Dependencies:
    pip install requests beautifulsoup4 lxml curl_cffi psycopg2-binary python-slugify
"""
import os
import re
import time
import random
import logging
import argparse
import threading as _threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from database import (
    upsert_batch,
    delete_old_price_history,
    get_connection,
    ensure_schema_extras,
    ensure_category_tables,
    upsert_category_associations,
    fetch_active_deals,
    fetch_stale_records,
    mark_stale_price,
    mark_unavailable,
)
from bs4 import BeautifulSoup
from deal_scorer import score_deals
from utils import generate_slug

LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY", "")

# ─────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────
ASSOCIATE_TAG      = os.environ.get("ASSOCIATE_TAG", "")
MAX_PAGES_DEFAULT    = 400     # main popularity URL — high ceiling; early-exit (5 consecutive empty) handles real termination
MAX_PAGES_CATEGORY   = 500     # per genre URL — effectively unlimited; consecutive-empty logic stops at true end of results
MAX_PAGES_EXTRA      = 100     # per extra sort URL — same reasoning
DELAY_SECONDS        = 1.5     # seconds between requests; safe with curl_cffi browser impersonation
MAX_CATEGORY_WORKERS = int(os.environ.get("CATEGORY_WORKERS", "6"))  # parallel threads for genre category crawling
MIN_PRICE          = 30.0

# Stale-records session hygiene: rotate after a random number of product-page
# hits in this range.  Amazon degrades sessions to skeleton pages after ~1-2
# hits; jittering the rotation count removes the mechanical every-N pattern.
_STALE_MAX_HITS_RANGE = (1, 4)

# ─────────────────────────────────────────────────────────────
#  Proxy configuration
# ─────────────────────────────────────────────────────────────
# Set PROXY_LIST (comma-separated proxy URLs) or PROXY_FILE (one URL per line).
# Proxy URL format: http://user:pass@host:port
# Leave both empty to crawl without proxies (uses the runner's IP directly).
PROXY_LIST_ENV   = os.environ.get("PROXY_LIST", "")
PROXY_FILE_ENV   = os.environ.get("PROXY_FILE", "")
PROXY_COOLDOWN_S = int(os.environ.get("PROXY_COOLDOWN", "300"))  # seconds before retired proxy re-enters pool
PROXY_MAX_BLOCKS = int(os.environ.get("PROXY_MAX_BLOCKS", "3"))  # consecutive blocks before retiring a proxy

# URL principal — todos os vinis ordenados por popularidade
VINYL_URL_PATH = (
    "/s?i=popular&srs=14772275011"
    "&rh=n%3A14772275011"
    "&s=popularity-rank"
    "&fs=true"
    "&ref=lp_14772275011_sar"
)

BASE_URL = "https://www.amazon.com"

# Browse node landing page — editorial carousels (Best Sellers, New Releases, etc.)
# Scraped for embedded ASINs; products not yet in the DB get individual product-page fetches.
BROWSE_NODE_URL = BASE_URL + "/b?ie=UTF8&node=14772275011"

# Additional search sort URLs — each crawled for MAX_PAGES_EXTRA pages to surface
# records that rank poorly by popularity but appear in other sort orders.
EXTRA_SORT_URLS = [
    BASE_URL + "/s?i=popular&srs=14772275011&rh=n%3A14772275011&s=date-desc-rank&fs=true",   # New releases
    BASE_URL + "/s?i=popular&srs=14772275011&rh=n%3A14772275011&s=review-rank&fs=true",       # Most reviewed
    BASE_URL + "/s?i=popular&srs=14772275011&rh=n%3A14772275011&s=featured-rank&fs=true",     # Featured / editorial
    BASE_URL + "/s?i=popular&srs=14772275011&rh=n%3A14772275011&s=price-asc-rank&fs=true",    # Price low→high (surfaces cheap/niche records missed by popularity sort)
    BASE_URL + "/s?i=popular&srs=14772275011&rh=n%3A14772275011&s=price-desc-rank&fs=true",   # Price high→low (surfaces expensive/rare records)
]

# Genre category URLs — each paginated separately
CATEGORY_URLS = [
    # ── Blues (31) ───────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A31&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Broadway & Vocalists (265640) ────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A265640&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Children's Music (173425) ────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A173425&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Christian & Gospel (173429) ──────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A173429&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Classic Rock (67204) ─────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A67204&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Classical (85) ───────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A85&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Comedy & Spoken Word (63936) ─────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A63936&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Country (16) ─────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A16&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Dance & Electronic (7) ───────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A7&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Folk (32) ────────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A32&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Holiday & Wedding (292572) ───────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A292572&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Indie & Alternative (21165438011) ────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A21165438011&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── International Music (33) ─────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A33&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Jazz (34) ────────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A34&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Latin Music (289122) ─────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A289122&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Metal (67207) ────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A67207&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── New Age (36) ─────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A36&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Opera & Classical Vocal (84) ─────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A84&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Pop (37) ─────────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A37&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── R&B (39) ─────────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A39&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Rap & Hip-Hop (38) ───────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A38&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Reggae (63885) ───────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A63885&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Rock (40) ────────────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A40&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Soundtracks (42) ─────────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A42&s=popularity-rank&dc&fs=true&rnid=14772275011",
    # ── Special Interest (35) ────────────────────────────────────────────────
    "https://www.amazon.com/s?i=popular&srs=14772275011&rh=n%3A5174%2Cn%3A14772275011%2Cn%3A35&s=popularity-rank&dc&fs=true&rnid=14772275011",
]

# Human-readable names for each URL in CATEGORY_URLS (same order).
# Used to seed the Category table on first run.
CATEGORY_NAMES: list[str] = [
    "Blues",
    "Broadway & Vocalists",
    "Children's Music",
    "Christian & Gospel",
    "Classic Rock",
    "Classical",
    "Comedy & Spoken Word",
    "Country",
    "Dance & Electronic",
    "Folk",
    "Holiday & Wedding",
    "Indie & Alternative",
    "International Music",
    "Jazz",
    "Latin Music",
    "Metal",
    "New Age",
    "Opera & Classical Vocal",
    "Pop",
    "R&B",
    "Rap & Hip-Hop",
    "Reggae",
    "Rock",
    "Soundtracks",
    "Special Interest",
]

BROWSER_IDENTITIES = [
    "chrome136", "chrome133a", "chrome131", "chrome124", "chrome120",
    "edge101", "firefox144", "firefox135", "firefox133",
]

# Rotate Accept-Language per session so every session doesn't share an identical
# header fingerprint. All strings reflect realistic Brazilian-user browser configs.
_ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8",
    "en-US,en;q=0.9,en-GB;q=0.8",
    "en-US,en;q=1.0",
    "en-US,en;q=0.9,fr;q=0.5",
    "en-US,en;q=0.8,es;q=0.5",
]

# ─────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("vinyl_crawler.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  Thread-local session pool (stale-records workers)
# ─────────────────────────────────────────────────────────────
_tl = _threading.local()


def _get_worker_session() -> tuple:
    """Return (session, proxy) for this thread, creating and warming them if needed."""
    if not getattr(_tl, "session", None):
        proxy = get_proxy_pool().acquire()
        session, _ = make_session(proxy=proxy)
        warm_up(session)
        _tl.session  = session
        _tl.proxy    = proxy
        _tl.hit_count = 0
        _tl.max_hits  = random.randint(*_STALE_MAX_HITS_RANGE)
    return _tl.session, getattr(_tl, "proxy", None)


def _invalidate_worker_session() -> None:
    """Discard the current thread's session so the next call rebuilds it."""
    _tl.session  = None
    _tl.proxy    = None
    _tl.hit_count = 0

# ─────────────────────────────────────────────────────────────
#  Compiled regexes
# ─────────────────────────────────────────────────────────────
_RATING_TEXT_RE = re.compile(
    r"^\d[\d,.]* de \d"
    r"|^\d[\d.]* out of \d"
    r"|estrelas?$",
    re.IGNORECASE,
)
_PRICE_START_RE = re.compile(r"^R\$|^\$|^\d+[.,]")
_VINYL_LABEL_RE = re.compile(r"vinil|vinyl", re.IGNORECASE)

_CD_RE = re.compile(
    r"\bcd\b|\[cd\]|\(cd\)|compact disc|\bcd\s*\d",
    re.IGNORECASE,
)
_VINYL_TITLE_RE = re.compile(
    r"vinil|vinyl|\blp\b"
    r'|\b7["\']\b'
    r'|\b10["\']?\b\s*(?:inch|polegadas)'
    r'|\b12["\']?\b\s*(?:inch|polegadas)'
    r"|33\s?rpm|45\s?rpm"
    r"|180\s?g(?:r(?:am)?)?"
    r"|picture\s+(?:disc|vinyl)|gatefold"
    r"|disco\s+(?:de\s+)?vinil|single\s+de\s+vinil"
    r"|\b7\s*polegadas\b|\b12\s*polegadas\b",
    re.IGNORECASE,
)
_VINYL_CARD_RE = re.compile(
    r"vinil|vinyl|\blp\b|180\s?g(?:r(?:am)?)?|gatefold|picture\s+disc"
    r"|disco\s+(?:de\s+)?vinil|formato:\s*vinil|format:\s*vinyl|33\s+rpm|45\s+rpm",
    re.IGNORECASE,
)
_BOT_SIGNAL_RE = re.compile(
    r"Robot Check"
    r"|Verificação de robô"
    r"|Digite os caracteres"
    r"|just need to make sure you.re not a robot"
    r"|automated access to Amazon"
    r"|Access Denied"
    r"|Enter the characters you see"
    r"|validateCaptcha"
    r"|Prove you.re not a robot",
    re.IGNORECASE,
)
_PRICE_CLEAN_RE = re.compile(r"R\$\s*|\xa0|\s")
_PRICE_NUM_RE   = re.compile(r"\d+\.?\d*")

# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────
def _human_delay(base: float) -> float:
    """
    Returns a delay that mimics human reading-time variance.

    80% of requests get the standard 0.5–1.5 s jitter on top of base.
    5% get a medium pause (2–4 s extra) — like mousing over a product.
    15% get a long pause (4–9 s extra) — like actually reading a listing.
    A pure uniform(0.5, 1.5) range is easy to fingerprint; adding the long
    tail removes the hard upper bound that automated tools exhibit.
    """
    r = random.random()
    if r < 0.80:
        return base + random.uniform(0.5, 1.5)
    if r < 0.85:
        return base + random.uniform(2.0, 4.0)
    return base + random.uniform(4.0, 9.0)


def affiliate_link(asin: str) -> str:
    return f"https://www.amazon.com/dp/{asin}?tag={ASSOCIATE_TAG}"


def parse_price_br(text: str) -> float | None:
    if not text:
        return None
    cleaned = _PRICE_CLEAN_RE.sub("", text)
    cleaned = cleaned.replace(".", "").replace(",", ".")
    m = _PRICE_NUM_RE.search(cleaned)
    if m is None:
        log.debug("parse_price_br: no numeric value found in %r (cleaned: %r)", text, cleaned)
        return None
    return float(m.group())


def is_vinyl(title: str, card=None) -> bool:
    if _CD_RE.search(title):
        return False

    if _VINYL_TITLE_RE.search(title):
        return True

    if card is not None:
        card_text = card.get_text(" ", strip=True)
        if _VINYL_CARD_RE.search(card_text):
            if not (_CD_RE.search(card_text) and not _VINYL_TITLE_RE.search(title)):
                return True

    return True


def _to_title_case(name: str) -> str:
    """Title-cases a name, keeping small connector words lowercase."""
    SMALL = {"of", "the", "and", "or", "in", "on", "at", "to", "a", "an",
             "de", "da", "do", "e", "y", "los", "las", "el", "la"}
    words = name.split()
    result = []
    for i, word in enumerate(words):
        lower = word.lower()
        result.append(lower if (i > 0 and lower in SMALL) else word.capitalize())
    return " ".join(result)


def normalize_artist(name: str) -> str:
    """
    Normalizes an artist name coming from Amazon to a clean human-readable form.

    Handles two common formats:
      1. Inverted "LAST,FIRST" or "LAST, FIRST" → "First Last"
         e.g. "SWIFT,TAYLOR" → "Taylor Swift"
      2. ALL CAPS names (more than 4 alpha chars) → Title Case
         e.g. "LED ZEPPELIN" → "Led Zeppelin"
         (Short all-caps like "ABBA" or "AC/DC" are left alone.)
    """
    if not name or name == _UNKNOWN_ARTIST:
        return name

    # Case 1: inverted "LAST,FIRST" format
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2 and all(parts):
            candidate = f"{parts[1]} {parts[0]}"
            return _to_title_case(candidate)

    # Case 2: ALL CAPS (more than 4 alpha chars — preserves ABBA, AC/DC etc.)
    letters = [c for c in name if c.isalpha()]
    if len(letters) > 4 and all(c.isupper() for c in letters):
        return _to_title_case(name)

    return name


_ARTIST_REJECT_PHRASES = (
    "ouça com amazon music", "ouça com music unlimited", "listen with amazon music",
    "adicionar ao carrinho", "add to cart", "comprar agora", "buy now",
    "prime", "frete grátis", "em estoque", "disponível",
    "vendido por", "sold by", "patrocinado", "sponsored",
    "em até", "in up to", "x de r$", "x r$", "sem juros",
    # Amazon social proof badges
    "compras no mês", "compras nos últimos", "bought in past", "bought last month",
    # Amazon promotional noise picked up by fallback selectors
    "amazon music",           # "90 dias de Amazon Music grátis incluso"
    "oferta",                 # "30(6 Ofertas de Novos) Mais Opções de Comprar$ 278"
    "mais opções de comprar", # same
    "opções de comprar",      # same
    "dias de",                # "90 dias de ..."
    # Page-chrome and price labels leaked by broad CSS selectors
    "página",                 # "Página do Produtor$ 0,00r$0,00 Preço"
    "preço",                  # same
    "r$",                     # embedded Brazilian real sign e.g. "r$0,00"
    "outro formato",          # "Outro formato:" — Amazon format-switcher label
    "other format",           # same in English
)
_UNKNOWN_ARTIST = "Artista não identificado"


def is_fake_artist(artist: str) -> bool:
    if not artist:
        return False
    low = artist.lower()
    return any(phrase in low for phrase in _ARTIST_REJECT_PHRASES)


_EMBEDDED_PRICE_RE = re.compile(r"\d+[,\.]\d{2}")  # catches "0,00", "29.90" etc.


def _is_plausible_artist(text: str) -> bool:
    if not text or len(text) > 120:
        return False
    if not re.search(r"[a-zA-ZÀ-ÿ]", text):  # must contain at least one letter
        return False
    if _PRICE_START_RE.match(text):
        return False
    if is_fake_artist(text):
        return False
    if re.fullmatch(r"[\d.,\s/\\-]+", text):
        return False
    if _EMBEDDED_PRICE_RE.search(text):
        return False
    return True


def build_page_url(page: int) -> str:
    url = BASE_URL + VINYL_URL_PATH
    url = re.sub(r"[&?]page=\d+", "", url)
    url = re.sub(r"[&?]qid=\d+", "", url)
    # Small jitter so qid is not an exact wall-clock second on every call —
    # exact-integer qids are a bot pattern that real browsers don't produce.
    qid = int(time.time()) + random.randint(-3, 3)
    if page == 1:
        # Real Amazon page-1 URLs don't include &page=1; omitting it matches
        # the URL a browser produces when landing on the first results page.
        return url + f"&qid={qid}&ref=sr_pg_1"
    return url + f"&qid={qid}&page={page}&ref=sr_pg_{page}"


def build_category_page_url(base_url: str, page: int) -> str:
    url = re.sub(r"[&?]page=\d+", "", base_url)
    url = re.sub(r"[&?]qid=\d+", "", url)
    qid = int(time.time()) + random.randint(-3, 3)
    if page == 1:
        return url + f"&qid={qid}"
    return url + f"&qid={qid}&page={page}"


# ─────────────────────────────────────────────────────────────
#  Proxy pool
# ─────────────────────────────────────────────────────────────
def _mask_proxy(proxy: str) -> str:
    return re.sub(r"(https?://)[^:]+:[^@]+@", r"\1***:***@", proxy)


def _load_proxy_list() -> list[str]:
    proxies: list[str] = []
    for raw in re.split(r"[,\n]", PROXY_LIST_ENV):
        p = raw.strip()
        if p:
            proxies.append(p)
    if PROXY_FILE_ENV:
        try:
            with open(PROXY_FILE_ENV, encoding="utf-8") as fh:
                for line in fh:
                    p = line.strip()
                    if p and not p.startswith("#"):
                        proxies.append(p)
        except OSError as exc:
            log.warning("[proxy] Cannot read PROXY_FILE %s: %s", PROXY_FILE_ENV, exc)
    return proxies


class ProxyPool:
    """Thread-safe residential proxy pool with per-proxy block tracking."""

    def __init__(self, proxies: list[str]) -> None:
        self._lock    = _threading.Lock()
        self._active  = list(proxies)
        self._retired: dict[str, float] = {}
        self._blocks:  dict[str, int]   = defaultdict(int)
        self._reqs:    dict[str, int]   = defaultdict(int)

    # ── Public API ─────────────────────────────────────────────────────────

    def acquire(self) -> str | None:
        with self._lock:
            self._reactivate()
            return random.choice(self._active) if self._active else None

    def report_ok(self, proxy: str | None) -> None:
        if not proxy:
            return
        with self._lock:
            self._reqs[proxy] += 1

    def report_block(self, proxy: str | None) -> None:
        if not proxy:
            return
        with self._lock:
            self._blocks[proxy] += 1
            self._reqs[proxy]   += 1
            if self._blocks[proxy] >= PROXY_MAX_BLOCKS:
                self._retire(proxy)

    def log_stats(self) -> None:
        with self._lock:
            block_summary = {
                _mask_proxy(p): c
                for p, c in self._blocks.items() if c
            }
            log.info(
                "[proxy] Pool: %d active, %d retired | blocks: %s",
                len(self._active), len(self._retired), block_summary,
            )

    @property
    def has_proxies(self) -> bool:
        with self._lock:
            self._reactivate()
            return bool(self._active)

    # ── Internal ────────────────────────────────────────────────────────────

    def _retire(self, proxy: str) -> None:
        if proxy in self._active:
            self._active.remove(proxy)
        self._retired[proxy] = time.monotonic()
        log.warning(
            "[proxy] Retired %s — %d blocks / %d requests",
            _mask_proxy(proxy), self._blocks[proxy], self._reqs[proxy],
        )

    def _reactivate(self) -> None:
        now = time.monotonic()
        for proxy in [p for p, t in self._retired.items() if now - t >= PROXY_COOLDOWN_S]:
            del self._retired[proxy]
            self._blocks[proxy] = 0
            self._active.append(proxy)
            log.info("[proxy] Reactivated %s after cooldown.", _mask_proxy(proxy))


_proxy_pool: ProxyPool | None = None


def get_proxy_pool() -> ProxyPool:
    global _proxy_pool
    if _proxy_pool is None:
        proxies = _load_proxy_list()
        _proxy_pool = ProxyPool(proxies)
        if proxies:
            log.info("[proxy] Pool initialised with %d proxies.", len(proxies))
        else:
            log.info("[proxy] No proxies configured — using runner IP directly.")
    return _proxy_pool


# ─────────────────────────────────────────────────────────────
#  Session factory
# ─────────────────────────────────────────────────────────────
def make_session(proxy: str | None = None):
    try:
        from curl_cffi import requests as cffi_requests
        kwargs: dict = {"impersonate": random.choice(BROWSER_IDENTITIES)}
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        s = cffi_requests.Session(**kwargs)
        s.headers.update({
            # Rotate Accept-Language so sessions don't share a single fixed string.
            "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
            "Referer": "https://www.amazon.com/",
        })
        return s, "curl_cffi"
    except ImportError:
        import requests as req_lib
        s = req_lib.Session()
        if proxy:
            s.proxies = {"http": proxy, "https": proxy}
        s.headers.update({
            "User-Agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/136.0.0.0 Safari/537.36",
            ]),
            # Accept header is mandatory — its absence is a clear non-browser signal.
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
            "DNT": "1",
            "Connection": "keep-alive",
            "Referer": "https://www.amazon.com/",
        })
        return s, "requests"


def warm_up(session) -> None:
    # Three-step warm-up: homepage → vinyl category → vinyl search results.
    # Visiting search results sets the same cookies the main crawl accumulates
    # so subsequent product-page requests look like organic search-then-click.
    try:
        session.get("https://www.amazon.com/", timeout=15)
        time.sleep(random.uniform(0.8, 1.8))
        session.get("https://www.amazon.com/b?ie=UTF8&node=14772275011", timeout=15)
        time.sleep(random.uniform(0.5, 1.2))
        session.get(BASE_URL + VINYL_URL_PATH, timeout=15)
        time.sleep(random.uniform(0.5, 1.0))
    except Exception:
        pass


def _quick_warmup(session) -> None:
    """Mid-run re-warm after session rotation: homepage only, keeps overhead low."""
    try:
        session.get("https://www.amazon.com/", timeout=12)
        time.sleep(random.uniform(0.5, 1.2))
        session.get("https://www.amazon.com/b?ie=UTF8&node=14772275011", timeout=12)
        time.sleep(random.uniform(0.3, 0.8))
    except Exception:
        pass


def safe_get(session, url: str, retries: int = 3, proxy: str | None = None,
             referer: str | None = None):
    """
    Fetch a search-results page. Returns (soup_or_none, session, proxy).

    On CAPTCHA/rate-limit the session and proxy are rotated so the caller
    always gets the current live session back regardless of what happened
    during retries.
    """
    pool = get_proxy_pool()
    req_headers = {}
    if referer:
        req_headers["Referer"] = referer

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=25, headers=req_headers or None)
            size = len(resp.content)
            # 403 is Amazon's soft-block alongside 429/503 — treat identically.
            if resp.status_code in (403, 503, 429):
                log.warning(
                    "[safe_get] Rate-limited %s proxy=%s size=%d — backing off",
                    resp.status_code, _mask_proxy(proxy) if proxy else "none", size,
                )
                pool.report_block(proxy)
                time.sleep(random.uniform(6, 12))
                proxy = pool.acquire()
                session, _ = make_session(proxy=proxy)
                _quick_warmup(session)
                continue
            resp.raise_for_status()
        except Exception as exc:
            log.warning("[safe_get] Request error (attempt %d/%d): %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(random.uniform(4, 8))
                proxy = pool.acquire()
                session, _ = make_session(proxy=proxy)
                continue
            return None, session, proxy

        verdict = "ok"
        # Keep CAPTCHA signals in sync with fetch_product_page — both functions
        # must detect the same bot-challenge pages.
        if _BOT_SIGNAL_RE.search(resp.text):
            verdict = "captcha"
            log.warning(
                "[safe_get] CAPTCHA detected proxy=%s size=%d — rotating session",
                _mask_proxy(proxy) if proxy else "none", size,
            )
            pool.report_block(proxy)
            proxy = pool.acquire()
            session, _ = make_session(proxy=proxy)
            _quick_warmup(session)
            return None, session, proxy

        log.debug(
            "[safe_get] %s status=%d size=%d proxy=%s verdict=%s",
            url[:80], resp.status_code, size,
            _mask_proxy(proxy) if proxy else "none", verdict,
        )
        pool.report_ok(proxy)
        return BeautifulSoup(resp.content, "lxml"), session, proxy

    return None, session, proxy


def mine_browse_node(session, proxy: str | None = None) -> set[str]:
    """
    Fetches the Amazon Brazil vinyl browse node page and extracts ASINs from:
      1. data-asin attributes on any HTML element (initial carousel state)
      2. JSON blobs embedded in <script> tags (encoded product data)

    Returns a deduplicated set of valid 10-char ASINs.  Failures are logged
    and return an empty set so the calling crawl is not disrupted.
    """
    log.info("[browse-node] Mining %s", BROWSE_NODE_URL)
    soup, session, proxy = safe_get(
        session, BROWSE_NODE_URL, proxy=proxy,
        referer=BASE_URL + "/b?ie=UTF8&node=14772275011",
    )
    if soup is None:
        log.warning("[browse-node] Failed to fetch browse node page — skipping.")
        return set()

    asins: set[str] = set()
    _ASIN_JSON_RE = re.compile(r'"[Aa][Ss][Ii][Nn]"\s*:\s*"([A-Za-z0-9]{10})"')

    for el in soup.find_all(attrs={"data-asin": True}):
        val = el.get("data-asin", "").strip()
        if len(val) == 10 and val.isalnum():
            asins.add(val.upper())

    for script in soup.find_all("script"):
        text = script.string or ""
        if not text or ('"asin"' not in text and '"ASIN"' not in text):
            continue
        for m in _ASIN_JSON_RE.finditer(text):
            asins.add(m.group(1).upper())

    asins.discard("")
    log.info("[browse-node] Extracted %d ASINs.", len(asins))
    return asins


# ─────────────────────────────────────────────────────────────
#  Product-page fetch + parse (stale-records check)
# ─────────────────────────────────────────────────────────────
_VINYL_SEARCH_REFERER = BASE_URL + VINYL_URL_PATH


def fetch_product_page(session, url: str, retries: int = 3, referer: str | None = None,
                       proxy: str | None = None):
    """
    Fetches a single Amazon product detail page.

    Returns (soup_or_none, http_status_or_none, session, proxy).

    Callers must inspect http_status:
      404          → product definitively gone; mark unavailable
      None         → transient error (rate-limit, network, CAPTCHA); skip this run
      2xx / other  → soup is populated; parse normally
    """
    pool = get_proxy_pool()
    req_headers = {"Referer": referer or _VINYL_SEARCH_REFERER}

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=25, headers=req_headers)
            size = len(resp.content)
            if resp.status_code == 404:
                log.debug(
                    "[fetch_product] 404 %s proxy=%s",
                    url[:80], _mask_proxy(proxy) if proxy else "none",
                )
                pool.report_ok(proxy)
                return None, 404, session, proxy
            # 403 is Amazon's soft-block signal — treat as rate-limit alongside 429/503.
            if resp.status_code in (403, 503, 429):
                log.warning(
                    "[fetch_product] Rate-limited %s proxy=%s size=%d — backing off",
                    resp.status_code, _mask_proxy(proxy) if proxy else "none", size,
                )
                pool.report_block(proxy)
                time.sleep(random.uniform(6, 12))
                proxy = pool.acquire()
                session, _ = make_session(proxy=proxy)
                _quick_warmup(session)
                continue
            resp.raise_for_status()
        except Exception as exc:
            log.warning(
                "[fetch_product] Request error (attempt %d/%d): %s",
                attempt, retries, exc,
            )
            if attempt < retries:
                time.sleep(random.uniform(4, 8))
                proxy = pool.acquire()
                session, _ = make_session(proxy=proxy)
                continue
            return None, None, session, proxy

        if _BOT_SIGNAL_RE.search(resp.text):
            log.warning(
                "[BOT-DETECTED] CAPTCHA proxy=%s size=%d — %s (attempt %d)",
                _mask_proxy(proxy) if proxy else "none", size, url[:80], attempt,
            )
            pool.report_block(proxy)
            return None, None, session, proxy

        soup = BeautifulSoup(resp.content, "lxml")

        # Amazon silently serves ~290 KB skeleton pages to suspected bots: correct
        # title + nav chrome, but #dp-container is empty — no #ppd, no buy-box, no
        # price.  These pass every CAPTCHA string check above.  Without this guard
        # parse_product_page() returns (None, True, None) → deal_cleared, wiping the
        # deal score even though the product is live and correctly priced.
        if not soup.select_one("#ppd"):
            log.warning(
                "[BOT-DETECTED] Skeleton page proxy=%s size=%d — %s. "
                "Session will be rotated; deal score preserved.",
                _mask_proxy(proxy) if proxy else "none", size, url[:80],
            )
            pool.report_block(proxy)
            return None, None, session, proxy

        log.debug(
            "[fetch_product] ok status=%d size=%d proxy=%s %s",
            resp.status_code, size,
            _mask_proxy(proxy) if proxy else "none", url[:80],
        )
        pool.report_ok(proxy)
        return soup, resp.status_code, session, proxy

    return None, None, session, proxy


# In-stock keywords for Amazon Brazil product pages (span.a-color-success / #availability)
_INSTOCK_KW = ("em estoque", "in stock", "disponível", "disponivel")
_OUTOFSTOCK_KW = (
    "atualmente indisponível", "currently unavailable",
    "fora de estoque", "out of stock",
    "não disponível", "not available",
)


def parse_product_page(soup) -> tuple[float | None, bool, int | None]:
    """
    Extracts price, availability, and review count from an Amazon product page.

    Returns (price_brl, in_stock, review_count).

    price_brl is None when the price widget is absent (e.g. "sold by third
    party only" pages where the add-to-cart block isn't rendered).
    in_stock defaults to True (conservative) and is only set to False when
    explicit out-of-stock signals are found. Pages with no availability
    signal (e.g. bot-detection pages that slipped the CAPTCHA guard) are
    treated as available rather than incorrectly marked unavailable.
    review_count is None when the review widget is absent.
    """
    # ── Availability ──────────────────────────────────────────────────────
    in_stock = True  # conservative default — only override on explicit OOS signals

    avail_el = soup.select_one("#availability")
    if avail_el:
        avail_text = avail_el.get_text(" ", strip=True).lower()
        if any(kw in avail_text for kw in _INSTOCK_KW):
            in_stock = True
        elif any(kw in avail_text for kw in _OUTOFSTOCK_KW):
            in_stock = False
        else:
            # Ambiguous availability text — treat as in-stock so we don't
            # incorrectly mark records unavailable.
            in_stock = True
    else:
        # Fallback: green badge anywhere on the page
        for el in soup.select("span.a-color-success"):
            text = el.get_text(" ", strip=True).lower()
            if any(kw in text for kw in _INSTOCK_KW):
                in_stock = True
                break

    # Qualified buy box pin: if Amazon renders #qualifiedBuybox the product has
    # an active offer and is definitively in stock.  Set in_stock=True now so
    # that no downstream check (hard-override selectors, unqualified-buybox
    # detection, etc.) can flip it back to False on a page that is clearly
    # purchasable.
    if soup.select_one("#qualifiedBuybox"):
        in_stock = True

    # Hard out-of-stock override: explicit widget IDs Amazon uses.
    # Only applied when #qualifiedBuybox is absent — if the qualified buy box
    # is present these selectors are stale template shells, not real OOS signals.
    if not soup.select_one("#qualifiedBuybox"):
        for sel in ("#outOfStock", "#soldByThirdParty"):
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                text = el.get_text(" ", strip=True).lower()
                if any(kw in text for kw in _OUTOFSTOCK_KW):
                    in_stock = False

    # Unqualified buy box: product is listed but sold only by third-party
    # sellers — no price is rendered in the page HTML (only a "Ver todas as
    # opções de compra" button).  Treat as unavailable so the record is marked
    # accordingly and removed from the deals page.
    #
    # NOTE: check #unqualifiedBuyBox (inner widget), NOT #unqualifiedBuyBox_feature_div
    # (outer wrapper). Amazon renders the _feature_div shell on every page even when
    # empty; the inner #unqualifiedBuyBox div only appears when the page genuinely
    # has no qualified seller.  Also skip this check when #qualifiedBuybox is
    # present — the two are mutually exclusive and the qualified box wins.
    if soup.select_one("#unqualifiedBuyBox") and not soup.select_one("#qualifiedBuybox"):
        log.debug(
            "parse_product_page: unqualified buy box detected "
            "(third-party sellers only) — clearing deal, preserving availability"
        )
        return None, in_stock, None

    # ── Review count ──────────────────────────────────────────────────────
    # Extracted before price so it's available in early OOS returns below.
    review_count: int | None = None

    for sel in (
        "#acrCustomerReviewText",
        '[data-hook="total-review-count"]',
        '[aria-label*="classificações"]',
        '[aria-label*="avaliações de clientes"]',
        '[aria-label*="ratings"]',
        '[aria-label*="customer reviews"]',
    ):
        el = soup.select_one(sel)
        if not el:
            continue
        text = el.get("aria-label", "") or el.get_text(strip=True)
        m = re.search(r"([\d.,]+)", text)
        if m:
            count_str = m.group(1).replace(".", "").replace(",", "")
            try:
                val = int(count_str)
                if val > 0:
                    review_count = val
                    break
            except ValueError:
                pass

    # ── Format detection (multi-format pages: Vinyl + CD + MP3) ──────────
    # On pages that offer multiple formats, the buy-box reflects whichever
    # format is currently selected — which may be CD, not vinyl.  We must
    # anchor price extraction to the vinyl format explicitly.
    #
    # Strategy:
    #   1. Check #twister .top-level rows for a row labelled "vinil/vinyl"
    #      and extract its price directly (most reliable anchor).
    #   2. Check #tmmSwatches .swatchElement.selected to see which format
    #      is active.  If vinyl is selected, the buy-box price is the vinyl
    #      price and we can fall through to normal buy-box extraction.
    #   3. If #outOfStockBuyBox_feature_div is present and no vinyl table
    #      price was found, vinyl is OOS — return null, never a sibling price.
    #   4. If another format (CD/MP3) is selected and no vinyl table price
    #      was found, we cannot trust the buy-box — return null.
    #   5. Single-format pages (no #tmmSwatches) are unaffected.

    has_format_switcher = bool(soup.select_one("#tmmSwatches"))

    # Step 1: scan MediaMatrix format table for a vinyl-specific price.
    tmm_vinyl_price: float | None = None
    if has_format_switcher:
        for row in soup.select("#twister .top-level"):
            if _VINYL_LABEL_RE.search(row.get_text(" ", strip=True)):
                offscreen = row.select_one(".a-offscreen")
                if offscreen:
                    p = parse_price_br(offscreen.get_text(strip=True).replace("\xa0", ""))
                    if p and p >= MIN_PRICE_BRL:
                        tmm_vinyl_price = p
                        log.debug(
                            "parse_product_page: vinyl price from format table: %.2f", p
                        )
                        break

        # Fallback: some pages only render #tmmSwatches (no #twister rows).
        # The vinyl swatch may be hidden (display:none) when a different format's
        # ASIN was fetched, but its price is still present in aria-label attributes.
        if tmm_vinyl_price is None:
            for swatch in soup.select("#tmmSwatches .swatchElement"):
                if _VINYL_LABEL_RE.search(swatch.get_text(" ", strip=True)):
                    for price_el in swatch.select("[aria-label]"):
                        p = parse_price_br(price_el.get("aria-label", "").replace("\xa0", ""))
                        if p and p >= MIN_PRICE_BRL:
                            tmm_vinyl_price = p
                            log.debug(
                                "parse_product_page: vinyl price from tmmSwatches swatch: %.2f", p
                            )
                            break
                    if tmm_vinyl_price is None:
                        offscreen = swatch.select_one(".a-offscreen")
                        if offscreen:
                            p = parse_price_br(offscreen.get_text(strip=True).replace("\xa0", ""))
                            if p and p >= MIN_PRICE_BRL:
                                tmm_vinyl_price = p
                                log.debug(
                                    "parse_product_page: vinyl price from tmmSwatches offscreen: %.2f", p
                                )
                    if tmm_vinyl_price is not None:
                        break

    # Step 2: which format is currently selected?
    selected_swatch = soup.select_one("#tmmSwatches .swatchElement.selected")
    if selected_swatch is not None:
        selected_is_vinyl = bool(
            _VINYL_LABEL_RE.search(selected_swatch.get_text(" ", strip=True))
        )
    elif not has_format_switcher:
        # No swatch widget at all → single-format page; treat as vinyl.
        selected_is_vinyl = True
    else:
        # Multi-format page but no swatch marked selected (page loaded for a
        # non-vinyl ASIN, e.g. the CD ASIN on a CD+Vinyl listing).
        # Vinyl is only "selected" if no other vinyl swatch exists to contradict it.
        any_vinyl_swatch = any(
            _VINYL_LABEL_RE.search(s.get_text(" ", strip=True))
            for s in soup.select("#tmmSwatches .swatchElement")
        )
        selected_is_vinyl = not any_vinyl_swatch

    # Step 3 & 4: OOS / wrong format guard.
    if has_format_switcher and tmm_vinyl_price is None:
        vinyl_oos = bool(soup.select_one("#outOfStockBuyBox"))
        if vinyl_oos:
            log.debug(
                "parse_product_page: vinyl OOS (#outOfStockBuyBox inner widget) "
                "— clearing deal but preserving availability status"
            )
            # Return in_stock as-is (not hardcoded False): if vinyl is OOS but another
            # format is in stock, the product still exists. price=None triggers the
            # deal_cleared outcome → mark_unavailable(), hiding the record until the
            # category crawl re-discovers it with a confirmed vinyl price.
            return None, in_stock, review_count
        if not selected_is_vinyl:
            log.debug(
                "parse_product_page: multi-format page, selected swatch is not "
                "vinyl and no vinyl row in format table — returning null price"
            )
            return None, in_stock, review_count

    # ── Price ─────────────────────────────────────────────────────────────
    price: float | None = None

    # Priority 0: vinyl-specific price from the MediaMatrix format table.
    if tmm_vinyl_price is not None:
        price = tmm_vinyl_price

    # Priority 1: priceToPay / apex-pricetopay-value buy-box containers.
    # Safe to use here because either: (a) single-format page, or (b) the
    # vinyl swatch is selected so the buy-box already shows the vinyl price.
    if price is None:
        for container_sel in (".priceToPay", ".apex-pricetopay-value"):
            container = soup.select_one(container_sel)
            if not container:
                continue

            offscreen = container.select_one(".a-offscreen")
            if offscreen:
                p = parse_price_br(offscreen.get_text(strip=True).replace("\xa0", ""))
                if p and p >= MIN_PRICE_BRL:
                    price = p
                    break

            whole_el = container.select_one(".a-price-whole")
            frac_el  = container.select_one(".a-price-fraction")
            if whole_el:
                whole_text = "".join(
                    t for t in whole_el.strings
                    if t.strip() and t.strip() not in (",", ".")
                ).strip().replace(".", "")
                frac_text = frac_el.get_text(strip=True) if frac_el else "00"
                p = parse_price_br(f"{whole_text},{frac_text}")
                if p and p >= MIN_PRICE_BRL:
                    price = p
                    break

    # Priority 2: generic .a-offscreen fallback — only on single-format pages.
    # On multi-format pages this selector would capture a sibling format's price.
    if price is None and not has_format_switcher:
        for el in soup.select(".a-offscreen"):
            text = el.get_text(strip=True).replace("\xa0", "")
            if text.startswith("R$") or re.match(r"^\d+[,.]", text):
                p = parse_price_br(text)
                if p and p >= MIN_PRICE_BRL:
                    price = p
                    break

    return price, in_stock, review_count


def parse_product_page_discovery(soup, asin: str) -> dict | None:
    """
    Extracts a full record from a product detail page for ASINs newly discovered
    via the browse node that are not yet in the database.

    Returns a record dict compatible with upsert_batch, or None when:
    - No title found (not a product page / bot detection)
    - Product is out of stock or has no vinyl price
    - Product is not vinyl
    """
    title_el = soup.select_one("#productTitle")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    if not is_vinyl(title, soup):
        log.debug("[discovery] ASIN %s: non-vinyl — skipping.", asin)
        return None

    price, in_stock, review_count = parse_product_page(soup)
    if not in_stock or price is None:
        return None

    artist = _UNKNOWN_ARTIST
    for sel in (
        "#bylineInfo .author a.a-link-normal",
        "#bylineInfo a.a-link-normal",
        ".author.notFaded a",
    ):
        el = soup.select_one(sel)
        if el:
            text = re.sub(r"^(por|by|de)\s+", "", el.get_text(strip=True), flags=re.IGNORECASE).strip()
            text = text.lstrip(":·•–—,;").rstrip(":·•–—,;").strip()
            if _is_plausible_artist(text):
                artist = normalize_artist(text)
                break

    img_url = ""
    for sel in ("#landingImage", "#imgBlkFront", "#main-image"):
        img_el = soup.select_one(sel)
        if img_el:
            src = img_el.get("src", "").strip() or img_el.get("data-old-hires", "").strip()
            if src and not src.startswith("data:"):
                img_url = re.sub(r"\._[A-Z0-9_,]+_\.", "._AC_SX300_.", src)
                break

    rating = None
    for sel in ('[aria-label*="de 5 estrelas"]', '[aria-label*="out of 5 stars"]'):
        el = soup.select_one(sel)
        if el:
            m = re.search(
                r"([\d,]+)\s*de\s*5|([\d.]+)\s*out\s*of\s*5",
                el.get("aria-label", ""), re.IGNORECASE,
            )
            if m:
                raw = (m.group(1) or m.group(2) or "").replace(",", ".")
                try:
                    rating = round(float(raw), 1)
                except ValueError:
                    pass
            break

    return {
        "asin":        asin,
        "title":       title,
        "artist":      artist,
        "slug":        generate_slug(title, asin),
        "imgUrl":      img_url,
        "url":         affiliate_link(asin),
        "rating":      rating,
        "reviewCount": review_count,
        "price":       price,
        "capturedAt":  datetime.now(timezone.utc),
    }


def fetch_catalog_discovery(
    session,
    asin: str,
    proxy: str | None = None,
) -> tuple:
    """
    Fetches a product detail page for a brand-new ASIN (not yet in the DB)
    discovered via the browse node and builds a full upsert_batch-compatible record.

    Returns (record_or_none, session, proxy).
    """
    url = affiliate_link(asin)
    soup, status, session, proxy = fetch_product_page(session, url, proxy=proxy)
    if status == 404:
        log.debug("[catalog-discovery] ASIN %s: 404 — skipping.", asin)
        return None, session, proxy
    if soup is None:
        return None, session, proxy
    record = parse_product_page_discovery(soup, asin)
    return record, session, proxy


# ─────────────────────────────────────────────────────────────
#  Extraction
# ─────────────────────────────────────────────────────────────
def extract_title(card) -> str:
    PROMO_PHRASES = (
        "ouça com amazon music", "ouça com music unlimited", "listen with amazon music",
        "adicionar ao carrinho", "add to cart", "comprar agora", "buy now",
        "patrocinado", "sponsored",
    )
    candidates = []
    for sel in [
        "h2 a.a-link-normal span.a-text-normal",
        "h2 span.a-text-normal",
        "h2 a span",
        "h2 span",
        "[data-cy='title-recipe'] h2 span",
        "[data-cy='title-recipe'] span.a-text-normal",
        ".a-size-medium.a-color-base.a-text-normal",
        ".a-size-base-plus.a-color-base.a-text-normal",
        ".s-title-instructions-style span",
        ".a-size-medium.a-color-base",
        ".a-size-base-plus.a-color-base",
    ]:
        el = card.select_one(sel)
        if not el:
            continue
        t = el.get_text(strip=True)
        if not t or len(t) <= 3:
            continue
        if _RATING_TEXT_RE.search(t):
            continue
        if re.fullmatch(r"[\d.,\s%R$]+", t):
            continue
        if any(phrase in t.lower() for phrase in PROMO_PHRASES):
            continue
        candidates.append(t)

    if not candidates:
        return ""
    return max((c for c in candidates if len(c) <= 300), key=len, default=candidates[0])


def extract_artist(card) -> str:
    for sel in [
        # Priority 0: structured byline — most reliable when present
        "span.author.notFaded a.a-link-normal",
        "span.author a.a-link-normal",
        # Legacy / fallback selectors
        "h2 ~ .a-row .a-color-secondary .a-size-base",
        "h2 ~ .a-row .a-color-secondary",
        "[data-cy='title-recipe'] ~ .a-row .a-color-secondary",
        ".s-title-instructions-style + div .a-color-secondary",
        ".a-row .a-size-base+ .a-size-base",
        "[data-cy='secondary-offer-recipe'] .a-color-secondary",
        ".a-section .a-color-secondary.a-size-base",
        ".a-size-small .a-color-secondary",
        ".s-line-clamp-2 + .a-row .a-size-base",
        ".a-size-base.a-color-secondary",
    ]:
        el = card.select_one(sel)
        if not el:
            continue
        text = el.get_text(strip=True)
        # strip "por/by/de" prefix even when not followed by a space
        # e.g. "por$uicideboy$" → "$uicideboy$"
        text = re.sub(r"^(por|by|de)(?=\s|[^a-zA-ZÀ-ÿ])", "", text, flags=re.IGNORECASE).strip()
        # strip leading/trailing punctuation left behind by label elements e.g. ": Artist" or "Label:"
        text = text.lstrip(":·•–—,;").rstrip(":·•–—,;").strip()
        # strip trailing year/format suffix e.g. "|2022" or "| 2022 (Deluxe Edition)"
        text = re.sub(r"\s*\|\s*\d{4}\b.*$", "", text).strip()
        if _is_plausible_artist(text):
            return text
    log.debug("extract_artist: no plausible artist found; returning fallback")
    return _UNKNOWN_ARTIST


def _is_in_secondary_section(el) -> bool:
    """
    Returns True if el is nested inside a secondary/alternative-format section.
    Amazon uses these to show CD/Streaming alternatives at the bottom of a card;
    their prices must not be confused with the main vinyl price.
    """
    for ancestor in el.parents:
        if not hasattr(ancestor, "get"):
            break
        # data-cy attribute used by Amazon for secondary offer sections
        if ancestor.get("data-cy") in (
            "secondary-offer-recipe",
            "format-list-recipe",
            "secondary-price-recipe",
        ):
            return True
        cls = " ".join(ancestor.get("class", []))
        if "s-secondary" in cls or "secondary-offer" in cls:
            return True
    return False


def _price_block_is_instalment(block) -> bool:
    block_classes = " ".join(block.get("class", []))
    if any(c in block_classes for c in ("a-text-price", "s-installment")):
        return True
    parent = block.parent
    for _ in range(4):
        if parent is None:
            break
        parent_text = parent.get_text(" ", strip=True).lower()
        if any(kw in parent_text for kw in (
            "parcela", "parcel", "sem juros", "installment",
            "em até", "in up to", "x r$", "x de r$",
        )):
            return True
        parent = parent.parent
    return False


def _read_price_block(block) -> float | None:
    parent = block.parent
    if parent:
        a11y = parent.select_one(
            "#apex-pricetopay-accessibility-label, "
            "[id$='-pricetopay-accessibility-label'], "
            "[id$='-accessibility-label'].aok-offscreen"
        )
        if a11y:
            text = a11y.get_text(strip=True).replace("\xa0", "").strip()
            if text:
                p = parse_price_br(text)
                if p and p > 0:
                    return p

    offscreen = block.select_one(".a-offscreen")
    if offscreen:
        text = offscreen.get_text(strip=True).replace("\xa0", "").strip()
        if text:
            p = parse_price_br(text)
            if p and p > 0:
                return p

    whole_el = block.select_one(".a-price-whole")
    frac_el  = block.select_one(".a-price-fraction")
    if whole_el:
        whole_text = "".join(
            t for t in whole_el.strings
            if t.strip() and t.strip() not in (",", ".")
        ).strip().replace(".", "")
        frac_text = frac_el.get_text(strip=True) if frac_el else "00"
        p = parse_price_br(f"{whole_text},{frac_text}")
        if p and p > 0:
            return p

    return None


def extract_price(card) -> float | None:
    """
    Extrai o preço de compra do card.

    Prioridade:
      0. [data-cy="price-recipe"] → span.a-price[xl][base] → .a-offscreen (seletor confirmado)
      1. Container apex-core-price-identifier → accessibility label (estrutura real)
      2. .s-price-instructions-style — container principal do preço nos resultados de busca
      3. Accessibility labels soltos no card (fallback)
      4. Seletores explícitos do buy-box (excluindo seções secundárias)
      5. Primeiro bloco .a-price não parcelado e fora de seções secundárias
      6. Regex no texto completo do card (último recurso)

    Seções secundárias (data-cy="secondary-offer-recipe" etc.) exibem preços de
    formatos alternativos (ex: CD) — esses nunca devem ser capturados como preço principal.
    """
    # ── Prioridade 0: data-cy="price-recipe" (seletor confirmado pela análise do HTML) ──
    price_recipe = card.select_one('[data-cy="price-recipe"]')
    if price_recipe:
        offscreen = price_recipe.select_one(
            '.a-price[data-a-size="xl"][data-a-color="base"] .a-offscreen'
        )
        if offscreen:
            text = offscreen.get_text(strip=True).replace("\xa0", "").strip()
            p = parse_price_br(text)
            if p and p >= MIN_PRICE_BRL:
                log.debug("Price via price-recipe a-offscreen: %.2f", p)
                return p

    # ── Prioridade 1: apex-core-price-identifier (estrutura real confirmada) ──
    apex = card.select_one(".apex-core-price-identifier")
    if apex:
        a11y = apex.select_one("#apex-pricetopay-accessibility-label, [id$='-pricetopay-accessibility-label']")
        if a11y:
            text = a11y.get_text(strip=True).replace("\xa0", "").strip()
            p = parse_price_br(text)
            if p and p >= MIN_PRICE_BRL:
                log.debug("Price via apex-core-price-identifier a11y: %.2f", p)
                return p
        price_span = apex.select_one(".priceToPay, .apex-pricetopay-value")
        if price_span and not _price_block_is_instalment(price_span):
            p = _read_price_block(price_span)
            if p and p >= MIN_PRICE_BRL:
                log.debug("Price via apex-core-price-identifier priceToPay: %.2f", p)
                return p

    # ── Prioridade 2: price-instructions-style container (principal nos resultados) ──
    # Amazon changed the class prefix from "s-" to "puis-"; match both.
    price_section = card.select_one(
        ".s-price-instructions-style, .puis-price-instructions-style"
    )
    if price_section:
        for block in price_section.select(".a-price"):
            if not _price_block_is_instalment(block):
                p = _read_price_block(block)
                if p and p >= MIN_PRICE_BRL:
                    log.debug("Price via price-instructions-style: %.2f", p)
                    return p

    # ── Prioridade 3: accessibility labels soltos ──────────────────────────
    for a11y_sel in (
        "#apex-pricetopay-accessibility-label",
        "[id$='-pricetopay-accessibility-label']",
        "[id$='-accessibility-label'].aok-offscreen",
    ):
        el = card.select_one(a11y_sel)
        if el and not _is_in_secondary_section(el):
            text = el.get_text(strip=True).replace("\xa0", "").strip()
            p = parse_price_br(text)
            if p and p >= MIN_PRICE_BRL:
                log.debug("Price via a11y label '%s': %.2f", a11y_sel, p)
                return p

    # ── Prioridade 4: seletores explícitos do buy-box (fora de seções secundárias) ──
    for sel in (
        ".priceToPay",
        ".apex-pricetopay-value",
        ".a-price[data-a-color='base']",
    ):
        for block in card.select(sel):
            if _is_in_secondary_section(block):
                continue
            if not _price_block_is_instalment(block):
                p = _read_price_block(block)
                if p and p >= MIN_PRICE_BRL:
                    log.debug("Price via selector '%s': %.2f", sel, p)
                    return p

    # ── Prioridade 5: primeiro bloco .a-price fora de seções secundárias ─────
    for block in card.select(".a-price"):
        if _is_in_secondary_section(block):
            continue
        if _price_block_is_instalment(block):
            continue
        p = _read_price_block(block)
        if p and p >= MIN_PRICE_BRL:
            log.debug("Price via first-valid block: %.2f", p)
            return p

    # ── Prioridade 6: regex no texto completo (último recurso) ────────────
    card_text = card.get_text(" ", strip=True)
    for m in re.finditer(r"R\$\s*[\d.,]+", card_text):
        p = parse_price_br(m.group())
        if p and p >= MIN_PRICE_BRL:
            log.debug("Price via card-text regex: %.2f", p)
            return p

    log.debug("No plausible price found on card.")
    return None


def extract_rating(card) -> float | None:
    for sel in [
        '[aria-label*="de 5 estrelas"]',
        '[aria-label*="out of 5 stars"]',
        '[aria-label*="estrelas"]',
        ".a-icon-star-small",
        ".a-icon-star",
    ]:
        el = card.select_one(sel)
        if el:
            label = el.get("aria-label", "") or el.get_text(strip=True)
            m = re.search(
                r"([\d,]+)\s*de\s*5|([\d.]+)\s*out\s*of\s*5|([\d,]+)\s*estrelas",
                label,
                re.IGNORECASE,
            )
            if m:
                raw = (m.group(1) or m.group(2) or m.group(3) or "").replace(",", ".")
                try:
                    value = float(raw)
                    if 0.0 <= value <= 5.0:
                        return round(value, 1)
                except ValueError:
                    log.debug("extract_rating: failed to parse %r as float", raw)
    return None  # None em vez de "" — o banco aceita NULL


def extract_review_count(card) -> int | None:
    """
    Extracts the number of customer reviews from a search-result card.

    Amazon US renders the count in a span whose aria-label reads e.g.
    "1,235 ratings" (comma = thousands separator).
    Falls back to the plain visible text inside the same span.
    """
    for sel in (
        '[aria-label*="ratings"]',
        '[aria-label*="customer reviews"]',
    ):
        el = card.select_one(sel)
        if not el:
            continue
        # Prefer the aria-label; fall back to visible text (both carry the count)
        text = el.get("aria-label", "") or el.get_text(strip=True)
        m = re.search(r"([\d.,]+)", text)
        if m:
            # Remove thousands separators (US uses "," as thousands sep)
            count_str = m.group(1).replace(",", "").replace(".", "")
            try:
                val = int(count_str)
                if val > 0:
                    return val
            except ValueError:
                pass
    return None


def extract_image(card) -> str:
    for sel in ["img.s-image", "img[data-image-index]", ".s-product-image-container img"]:
        el = card.select_one(sel)
        if not el:
            continue
        url = el.get("src", "").strip() or el.get("data-src", "").strip()
        if not url or url.startswith("data:"):
            srcset = el.get("srcset", "") or el.get("data-srcset", "")
            if srcset:
                entries = [part.strip().split() for part in srcset.split(",") if part.strip()]
                best = max(
                    (e for e in entries if len(e) == 2),
                    key=lambda e: int(re.sub(r"\D", "", e[1]) or "0"),
                    default=None,
                )
                if best:
                    url = best[0]
                else:
                    log.debug("extract_image: srcset present but no valid 2-part entries found")
        if url and not url.startswith("data:"):
            url = re.sub(r"\._[A-Z0-9_,]+_\.", "._AC_SX300_.", url)
            return url
    return ""


# ─────────────────────────────────────────────────────────────
#  Page parsing
# ─────────────────────────────────────────────────────────────
def parse_page(soup) -> list[dict]:
    """Extrai todos os vinis de uma página de resultados."""
    cards = soup.select('[data-component-type="s-search-result"]')
    results = []
    now = datetime.now(timezone.utc)
    skipped = {"no_asin": 0, "no_title": 0, "not_vinyl": 0, "no_price": 0}

    for card in cards:
        asin = card.get("data-asin", "").strip()
        if not asin:
            skipped["no_asin"] += 1
            continue

        title = extract_title(card)
        if not title:
            skipped["no_title"] += 1
            continue

        if not is_vinyl(title, card):
            skipped["not_vinyl"] += 1
            log.debug("Non-vinyl filtered: %s", title[:60])
            continue

        price = extract_price(card)
        if price is None:
            skipped["no_price"] += 1
            log.debug("No price for ASIN %s (%s)", asin, title[:50])
            continue

        results.append({
            "asin":        asin,
            "title":       title,
            "artist":      normalize_artist(extract_artist(card)),
            "slug":        generate_slug(title, asin),
            "imgUrl":      extract_image(card),
            "url":         affiliate_link(asin),
            "rating":      extract_rating(card),
            "reviewCount": extract_review_count(card),
            "price":       price,
            "capturedAt":  now,
        })

    log.debug(
        "parse_page: %d found | skipped → %s",
        len(results), skipped,
    )
    return results


def has_next_page(soup) -> bool:
    """
    Returns True while more pages should be fetched.

    Amazon's pagination UI sometimes omits the next-page link deep in
    paginated results even though more pages exist.  We keep going as long
    as the current page contained product cards, stopping only when a page
    comes back empty (genuine end-of-catalogue).
    """
    if soup.select_one("a.s-pagination-next") is not None:
        return True
    # No explicit next-link — stop only if this page was also empty.
    return bool(soup.select('[data-component-type="s-search-result"]'))


# ─────────────────────────────────────────────────────────────
#  Main crawl loop
# ─────────────────────────────────────────────────────────────
def crawl_single_url(
    session,
    url_builder,
    label: str,
    max_pages: int,
    delay: float,
    seen_asins: set,
    max_consecutive_empty: int = 5,
    proxy: str | None = None,
    deadline: float | None = None,
):
    """
    Crawls a single paginated URL until exhausted or max_pages reached.

    url_builder(page) → URL string
    seen_asins is mutated in-place — ASINs collected here are added so the
    caller can share it across multiple crawl_single_url calls to deduplicate
    across sources within the same run.

    Returns (new_items, session, proxy).
    """
    items: list[dict] = []
    consecutive_empty = 0
    prev_url: str | None = None

    for page in range(1, max_pages + 1):
        if deadline is not None and time.monotonic() >= deadline:
            log.info("[%s] Time limit reached — stopping at page %d.", label, page)
            break

        url = url_builder(page)
        log.info("[%s] Page %d", label, page)
        soup, session, proxy = safe_get(session, url, proxy=proxy, referer=prev_url)
        prev_url = url

        if soup is None:
            log.warning("[%s] Page %d failed, skipping.", label, page)
            continue

        page_items = parse_page(soup)
        new_on_page = 0
        for item in page_items:
            if item["asin"] not in seen_asins:
                seen_asins.add(item["asin"])
                items.append(item)
                new_on_page += 1

        # Only count a page as "empty" when it has zero product cards at all —
        # that is the true end of Amazon's catalogue for this URL.  Pages that
        # contain cards but all ASINs are already seen are duplicates, not the
        # end of results; keep paginating through them.
        if len(page_items) == 0:
            consecutive_empty += 1
            log.info(
                "[%s] Truly empty page %d (%d/%d consecutive).",
                label, page, consecutive_empty, max_consecutive_empty,
            )
            if consecutive_empty >= max_consecutive_empty:
                log.info("[%s] End of results — stopping at page %d.", label, page)
                break
        else:
            consecutive_empty = 0
            if new_on_page == 0:
                log.info("[%s] Page %d: all %d cards already seen, continuing.", label, page, len(page_items))

        if not has_next_page(soup):
            log.info("[%s] No next page — stopping at page %d.", label, page)
            break

        if page < max_pages:
            sleep_time = _human_delay(delay)
            log.info("[%s] Waiting %.1fs...", label, sleep_time)
            time.sleep(sleep_time)

    return items, session, proxy


def _crawl_one_category(cat_url: str, label: str, delay: float, deadline: float | None = None) -> list[dict]:
    """
    Thread worker: crawl a single genre category URL end-to-end.

    Each worker gets its own session (different browser identity) and its own
    local seen-ASINs set.  Global deduplication against the main URL results
    happens on the calling thread after all futures complete.
    """
    proxy = get_proxy_pool().acquire()
    session, _ = make_session(proxy=proxy)
    # Stagger worker starts so they don't all hit Amazon simultaneously.
    time.sleep(random.uniform(0.5, 3.0))
    _quick_warmup(session)
    local_seen: set[str] = set()
    items, _, _ = crawl_single_url(
        session,
        lambda page, base=cat_url: build_category_page_url(base, page),
        label,
        MAX_PAGES_CATEGORY,
        delay,
        local_seen,
        max_consecutive_empty=3,
        proxy=proxy,
        deadline=deadline,
    )
    for item in items:
        item["source_category_url"] = cat_url
    return items


def _crawl_one_extra(extra_url: str, label: str, delay: float, deadline: float | None = None) -> list[dict]:
    """
    Thread worker: crawl a single extra sort URL end-to-end.

    Runs inside the same ThreadPoolExecutor as category workers so extra sorts
    overlap with the category crawl instead of running sequentially after it.
    Each worker gets its own session (different browser identity).
    """
    proxy = get_proxy_pool().acquire()
    session, _ = make_session(proxy=proxy)
    # Stagger slightly more than category workers since these share the pool.
    time.sleep(random.uniform(1.5, 5.0))
    _quick_warmup(session)
    local_seen: set[str] = set()
    items, _, _ = crawl_single_url(
        session,
        lambda page, base=extra_url: build_category_page_url(base, page),
        label,
        MAX_PAGES_EXTRA,
        delay,
        local_seen,
        max_consecutive_empty=3,
        proxy=proxy,
        deadline=deadline,
    )
    return items


def crawl(max_pages: int, delay: float, deadline: float | None = None) -> list[dict]:
    """
    Orchestrates the full crawl:
      1. Main popularity-ranked URL (up to max_pages pages), sequential.
      2. All genre category URLs + extra sort URLs in the same
         ThreadPoolExecutor (MAX_CATEGORY_WORKERS concurrent threads),
         each worker with its own session.
      3. Browse node ASIN mining (sequential, after the pool closes).

    Final deduplication is done in-memory after merging results: ASINs
    already seen in earlier steps are skipped to prevent duplicate
    HistoricoPreco rows within the same run.
    """
    proxy = get_proxy_pool().acquire()
    session, backend = make_session(proxy=proxy)
    log.info("Starting — backend: %s | max_pages (main): %d", backend, max_pages)
    warm_up(session)

    seen_asins: set[str] = set()
    all_items: list[dict] = []

    # ── 1. Main popularity URL ─────────────────────────────────────────────
    log.info("═" * 50)
    log.info("Crawling main popularity URL...")
    items, session, proxy = crawl_single_url(
        session,
        build_page_url,
        "main",
        max_pages,
        delay,
        seen_asins,
        max_consecutive_empty=5,
        proxy=proxy,
        deadline=deadline,
    )
    all_items.extend(items)
    log.info("Main URL complete — %d products.", len(items))

    # ── 2. Genre category URLs + extra sort URLs (parallel) ───────────────
    # Extra sort URLs run in the same executor as categories so they overlap
    # with the category crawl rather than running sequentially after it.
    log.info("═" * 50)
    log.info(
        "Crawling %d category URLs + %d extra sort URLs with %d parallel workers...",
        len(CATEGORY_URLS), len(EXTRA_SORT_URLS), MAX_CATEGORY_WORKERS,
    )
    cat_items_all: list[dict] = []
    extra_items_all: list[dict] = []
    # futures value: ("cat", index) or ("extra", index) to tell results apart.
    with ThreadPoolExecutor(max_workers=MAX_CATEGORY_WORKERS) as pool:
        futures: dict = {
            pool.submit(_crawl_one_category, cat_url, f"cat-{i}", delay, deadline): ("cat", i)
            for i, cat_url in enumerate(CATEGORY_URLS, 1)
        }
        for i, extra_url in enumerate(EXTRA_SORT_URLS, 1):
            fut = pool.submit(_crawl_one_extra, extra_url, f"extra-{i}", delay, deadline)
            futures[fut] = ("extra", i)

        for future in as_completed(futures):
            kind, idx = futures[future]

            if deadline is not None and time.monotonic() >= deadline:
                pending = sum(1 for f in futures if not f.done())
                log.warning(
                    "Time limit reached during category/extra crawl — cancelling %d pending futures.",
                    pending,
                )
                for f in futures:
                    f.cancel()
                break

            try:
                result_items = future.result()
                if kind == "cat":
                    log.info("Category %d complete — %d products.", idx, len(result_items))
                    cat_items_all.extend(result_items)
                else:
                    log.info("Extra sort %d complete — %d products.", idx, len(result_items))
                    extra_items_all.extend(result_items)
            except Exception as exc:
                log.warning("%s %d worker raised: %s", kind, idx, exc)

    # Build category associations before dedup — an ASIN seen in the main URL
    # crawl can still appear in a category and should have that association recorded.
    asin_categories: dict[str, set[str]] = {}
    for item in cat_items_all:
        cat_url = item.get("source_category_url")
        if cat_url:
            asin_categories.setdefault(item["asin"], set()).add(cat_url)

    # Merge category results, deduplicating against main-URL seen_asins.
    new_from_categories = 0
    for item in cat_items_all:
        if item["asin"] not in seen_asins:
            seen_asins.add(item["asin"])
            all_items.append(item)
            new_from_categories += 1
    log.info(
        "Categories done — %d new products (after dedup against main), "
        "%d unique ASINs with category tags.",
        new_from_categories, len(asin_categories),
    )

    # Merge extra sort results, deduplicating against all seen ASINs so far.
    new_from_extras = 0
    for item in extra_items_all:
        if item["asin"] not in seen_asins:
            seen_asins.add(item["asin"])
            all_items.append(item)
            new_from_extras += 1
    log.info(
        "Extra sorts done — %d new products (after dedup).", new_from_extras,
    )

    # ── 4. Browse node ASIN mining ─────────────────────────────────────────
    log.info("═" * 50)
    browse_asins: set[str] = set()
    if deadline is None or time.monotonic() < deadline:
        browse_asins = mine_browse_node(session, proxy=proxy)
    else:
        log.warning("Time limit reached — skipping browse node mining.")

    log.info("═" * 50)
    log.info("Full crawl done — %d unique products total.", len(all_items))
    get_proxy_pool().log_stats()
    return all_items, asin_categories, browse_asins


# ─────────────────────────────────────────────────────────────
#  Stale-records check
# ─────────────────────────────────────────────────────────────
def _fetch_one_stale(record: dict, delay: float, worker_idx: int,
                     deadline: float | None = None) -> dict:
    """
    Worker function: fetches a single product page using a persistent per-thread
    session. Reusing the session across tasks lets cookies accumulate so requests
    look like organic browsing rather than isolated bot hits.

    Called from a ThreadPoolExecutor — must be stateless w.r.t. the DB
    connection (all DB writes happen back on the main thread).

    Returns a result dict with keys: record, outcome, price, review_count.
    outcome is one of: "updated", "unavailable", "deal_cleared", "error", "skipped".
    """
    # Bail immediately if the wall-clock budget has already been exhausted.
    # Tasks submitted before the deadline but not yet started will self-cancel
    # here rather than running a full network round-trip past the time limit.
    if deadline is not None and time.monotonic() >= deadline:
        return {"record": record, "outcome": "skipped", "price": None, "review_count": None}

    # Stagger worker starts so they don't all fire simultaneously.
    time.sleep(worker_idx * random.uniform(1.0, 2.0))

    url = affiliate_link(record["asin"])

    # Proactively rotate session before Amazon's silent bot-detection triggers.
    # Sessions serve real pages for the first N product-page hits, then silently
    # degrade to ~290 KB skeleton pages that contain no buy-box content.
    # Rotating proactively (with jitter) resets that counter and removes the
    # mechanical every-N pattern from the request stream.
    hit_count = getattr(_tl, "hit_count", 0)
    max_hits  = getattr(_tl, "max_hits", random.randint(*_STALE_MAX_HITS_RANGE))
    if hit_count >= max_hits:
        log.debug(
            "[session] Worker %d: proactive rotation after %d hits — rebuilding session",
            worker_idx, hit_count,
        )
        _invalidate_worker_session()

    session, proxy = _get_worker_session()
    soup, status, session, proxy = fetch_product_page(session, url, proxy=proxy)
    # Propagate any session/proxy the retry logic created back into thread-local storage.
    _tl.session = session
    _tl.proxy   = proxy

    if soup is not None:
        # Successful fetch — increment hit counter.
        _tl.hit_count = getattr(_tl, "hit_count", 0) + 1
    elif status is None:
        # Bot-detection (CAPTCHA or skeleton page): discard session so the next
        # task gets a fresh warmed one.
        _invalidate_worker_session()

    result = {"record": record, "outcome": "error", "price": None, "review_count": None}

    if status == 404:
        result["outcome"] = "unavailable"
    elif soup is None:
        result["outcome"] = "error"
    else:
        price, in_stock, review_count = parse_product_page(soup)
        if not in_stock:
            result["outcome"] = "unavailable"
        elif price is None:
            # Full page received (passed #ppd check) but price extraction still
            # returned None — genuine parse failure: unqualified buy-box, vinyl OOS
            # on a multi-format page, or wrong swatch selected with no TMM price.
            # The scraped price in our DB is likely for a different format (e.g. CD),
            # so mark unavailable to hide stale/wrong data until the category crawl
            # re-discovers this product with a confirmed vinyl price.
            log.warning(
                "[DEAL-CLEARED] ASIN %s — full page received but vinyl price "
                "could not be confirmed (unqualified buy-box / vinyl OOS / "
                "wrong swatch). Marking unavailable.",
                record["asin"],
            )
            result["outcome"] = "deal_cleared"
        else:
            result["outcome"] = "updated"
            result["price"] = price
            result["review_count"] = review_count

    # Per-worker delay so the combined request rate stays at ≤ max_workers/delay req/s.
    time.sleep(_human_delay(delay))
    return result


def crawl_stale_records(
    stale: list[dict],
    delay: float,
    conn,
    dry_run: bool,
    max_workers: int = 2,
    deadline: float | None = None,
) -> tuple[int, int, int]:
    """
    Fetches individual product pages for records absent from the category crawl
    and updates the database accordingly.

    Uses ThreadPoolExecutor(max_workers) to overlap I/O waits.  DB writes are
    performed sequentially on the calling thread so no connection locking is
    needed (psycopg2 connections are not thread-safe).

    For each stale record:
      - HTTP 404            → mark_unavailable()
      - Out-of-stock page         → mark_unavailable()
      - In-stock + price          → mark_stale_price()
      - In-stock, no vinyl price  → mark_unavailable() (price unconfirmable = stale data)
      - Transient error           → warning; DB not touched (will retry next run)

    Returns (updated, unavailable, errors).
    """
    now = datetime.now(timezone.utc)
    updated = unavailable = deals_cleared = errors = 0
    total = len(stale)

    log.info("Stale-records: %d records, %d parallel workers", total, max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for idx, record in enumerate(stale):
            if deadline is not None and time.monotonic() >= deadline:
                log.warning(
                    "Time limit reached — stopping stale submission after %d/%d records.",
                    idx, total,
                )
                break
            futures[pool.submit(_fetch_one_stale, record, delay, idx % max_workers, deadline)] = record

        completed = 0
        skipped = 0
        for future in as_completed(futures):
            completed += 1

            # Hard deadline: cancel every future that hasn't started yet and stop
            # harvesting results.  pool.submit() is non-blocking, so all tasks are
            # queued instantly — without this check the pool works through all of
            # them even after the wall-clock budget is exhausted.
            if deadline is not None and time.monotonic() >= deadline:
                pending = sum(1 for f in futures if not f.done())
                log.warning(
                    "Time limit reached during stale harvest — cancelling %d pending futures.",
                    pending,
                )
                for f in futures:
                    f.cancel()
                break

            try:
                res = future.result()
            except Exception as exc:
                log.warning("[stale %d/%d] Worker raised: %s", completed, total, exc)
                errors += 1
                continue

            record   = res["record"]
            outcome  = res["outcome"]
            asin     = record["asin"]
            disco_id = record["id"]
            label    = record.get("title", "")[:50]

            log.info(
                "[stale %d/%d] ASIN %s — %s → %s",
                completed, total, asin, label, outcome,
            )

            if outcome == "unavailable":
                if not dry_run:
                    mark_unavailable(conn, disco_id)
                unavailable += 1
            elif outcome == "updated":
                log.info("  R$ %.2f  reviews=%s", res["price"], res["review_count"])
                if not dry_run:
                    mark_stale_price(conn, disco_id, res["price"], now, res["review_count"])
                updated += 1
            elif outcome == "deal_cleared":
                if not dry_run:
                    mark_unavailable(conn, disco_id)
                deals_cleared += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                errors += 1

    log.info(
        "Stale-records check done — %d updated | %d unavailable | %d deals_cleared"
        " | %d skipped | %d errors",
        updated, unavailable, deals_cleared, skipped, errors,
    )
    return updated, unavailable, errors


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Amazon vinyl crawler → PostgreSQL")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_DEFAULT, metavar="N")
    parser.add_argument("--delay", type=float, default=DELAY_SECONDS, metavar="S")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--stale-max", type=int, default=200, metavar="N",
        help="Max stale records to re-fetch per run (0 = unlimited, default: 200)",
    )
    parser.add_argument(
        "--stale-workers", type=int, default=2, metavar="N",
        help="Parallel workers for stale-records fetching (default: 2)",
    )
    parser.add_argument(
        "--skip-stale", action="store_true",
        help="Skip the stale-records check entirely",
    )
    parser.add_argument(
        "--skip-deal-revalidation", action="store_true",
        help="Skip the pre-crawl deal re-validation phase",
    )
    parser.add_argument(
        "--time-limit", type=int, default=50, metavar="MIN",
        help="Wall-clock budget in minutes; stale submission stops when exceeded (default: 50)",
    )
    return parser.parse_args()


def _notify_revalidate() -> None:
    """POST to the Next.js on-demand revalidation endpoint after a successful crawl."""
    import requests as _requests

    url = os.environ.get("REVALIDATE_URL")
    secret = os.environ.get("REVALIDATE_SECRET")
    if not url or not secret:
        return
    try:
        resp = _requests.post(url, json={"secret": secret}, timeout=10)
        log.info("Revalidation triggered: HTTP %s", resp.status_code)
    except Exception as exc:
        log.warning("Revalidation request failed (non-fatal): %s", exc)


def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 60)
    log.info("Vinyl Crawler — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Max pages: %d  |  Delay: %.1fs  |  Dry run: %s",
             args.max_pages, args.delay, args.dry_run)
    log.info("═" * 60)

    t_start = time.monotonic()
    deadline = t_start + args.time_limit * 60

    if args.dry_run:
        log.info("DRY RUN — skipping DB phases; running crawl only.")
        t0 = time.monotonic()
        all_items, *_ = crawl(args.max_pages, args.delay)
        log.info("Phase crawl: %.0fs", time.monotonic() - t0)
        log.info("DRY RUN — Sample of first 3 items:")
        for item in all_items[:3]:
            log.info("  ASIN: %s | %s | $ %.2f", item["asin"], item["title"][:50], item["price"])
        return

    log.info("Connecting to database...")
    conn = get_connection()
    log.info("Connected. Running schema check...")
    category_tables_ready = False
    try:
        ensure_schema_extras(conn)
        ensure_category_tables(conn, list(zip(CATEGORY_URLS, CATEGORY_NAMES)))
        category_tables_ready = True
        log.info("Schema OK.")
    except Exception as exc:
        log.warning("Schema check failed (will retry next run): %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    try:

        # ── Phase 0: Re-validate active deals (highest priority) ──────────
        # Query for records currently flagged as deals and re-crawl them
        # immediately so that the DB reflects the freshest prices before we
        # spend time discovering new ones.
        phase0_asins: set[str] = set()  # tracked so Phase 3 doesn't re-fetch them
        if args.skip_deal_revalidation:
            log.info("Deal re-validation skipped (--skip-deal-revalidation).")
        else:
            log.info("═" * 60)
            t0 = time.monotonic()
            active_deals = fetch_active_deals(conn)
            log.info(
                "Phase 0 — Deal re-validation: %d active deals to re-check.",
                len(active_deals),
            )
            if active_deals:
                phase0_asins = {d["asin"] for d in active_deals}
                crawl_stale_records(
                    active_deals, args.delay, conn,
                    dry_run=False, max_workers=args.stale_workers,
                    deadline=deadline,
                )
                # Re-score immediately after re-validation so that deals whose
                # prices just went up (or products that became unavailable) are
                # cleared before Phase 1 runs.  Without this, a deal that Phase 0
                # invalidated would stay visible if Phase 1 returns no results and
                # Phase 2.5 never executes.
                score_deals(conn)
                log.info("Phase 0 done: %.0fs", time.monotonic() - t0)
            else:
                log.info("No active deals found — skipping re-validation.")

        # ── Phase 1: Regular crawl ─────────────────────────────────────────
        log.info("═" * 60)
        t0 = time.monotonic()
        all_items, asin_categories, browse_asins = crawl(args.max_pages, args.delay, deadline=deadline)
        log.info("Phase 1 crawl: %.0fs", time.monotonic() - t0)

        if not all_items:
            log.warning("No products found. Nothing to write.")
        else:
            # ── Phase 2: Upsert crawl results ──────────────────────────────
            # The crawl can take 15+ minutes; the DB connection may have been
            # dropped by Supabase during that idle period.  Ping first and
            # reconnect if needed so upsert_batch doesn't fail with SSL EOF.
            try:
                conn.cursor().execute("SELECT 1")
            except Exception:
                log.warning("DB connection lost during crawl — reconnecting...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_connection()

            t0 = time.monotonic()
            written = upsert_batch(conn, all_items)
            log.info("Phase 2 upsert: %.0fs — %d records written.", time.monotonic() - t0, written)

            if category_tables_ready:
                t0 = time.monotonic()
                assoc_written = upsert_category_associations(conn, asin_categories)
                log.info("Phase 2 categories: %.0fs — %d associations written.", time.monotonic() - t0, assoc_written)
            else:
                log.warning("Skipping category associations — schema setup failed at startup.")



            # ── Phase 2.5: Deal scoring ─────────────────────────────────────
            # Runs after every upsert so deal_score reflects the freshest prices.
            # score_deals computes multi-window benchmarks (avg_30d, avg_90d,
            # low_30d, low_all_time) in a single SQL query, applies tiered scoring
            # rules with cooldown logic in Python, then batch-updates Disco.
            log.info("═" * 60)
            t0 = time.monotonic()
            scoring_summary = score_deals(conn)
            log.info(
                "Phase 2.5 scoring: %.0fs — flagged=%d | maintained=%d"
                " | cleared=%d | cooldown_skipped=%d",
                time.monotonic() - t0,
                scoring_summary["flagged"],
                scoring_summary["scored"],
                scoring_summary["cleared"],
                scoring_summary["skipped"],
            )

            # ── Phase 2.7: Browse node catalog discovery ────────────────
            # Process ASINs found on the browse node page that weren't seen
            # in the Phase 1 search crawl. Records already in the DB but not
            # seen this run will be caught by Phase 3; truly new ASINs get
            # individual product-page fetches so they enter the catalog now.
            if browse_asins and not (deadline is not None and time.monotonic() >= deadline):
                log.info("═" * 60)
                t0 = time.monotonic()
                phase1_asins = {item["asin"] for item in all_items}
                new_browse = browse_asins - phase1_asins
                log.info(
                    "Phase 2.7 browse discovery — %d browse ASINs, %d not in Phase 1.",
                    len(browse_asins), len(new_browse),
                )
                if new_browse:
                    with conn.cursor() as _cur:
                        _cur.execute(
                            'SELECT asin FROM "Disco" WHERE asin = ANY(%s)',
                            (list(new_browse),),
                        )
                        already_in_db = {row[0] for row in _cur.fetchall()}
                    truly_new = new_browse - already_in_db
                    log.info(
                        "  %d already in DB (Phase 3 will cover them), %d brand new.",
                        len(already_in_db), len(truly_new),
                    )
                    if truly_new and not (deadline is not None and time.monotonic() >= deadline):
                        disc_proxy = get_proxy_pool().acquire()
                        disc_session, _ = make_session(proxy=disc_proxy)
                        _quick_warmup(disc_session)
                        browse_records: list[dict] = []
                        cap = list(truly_new)[:100]
                        for idx, asin in enumerate(cap):
                            if deadline is not None and time.monotonic() >= deadline:
                                log.warning(
                                    "Phase 2.7: time limit at %d/%d new ASINs.", idx, len(cap)
                                )
                                break
                            record, disc_session, disc_proxy = fetch_catalog_discovery(
                                disc_session, asin, proxy=disc_proxy,
                            )
                            if record:
                                browse_records.append(record)
                                log.debug("  [new] %s — %s", asin, record["title"][:50])
                            time.sleep(DELAY_SECONDS + random.uniform(0.5, 1.5))
                        if browse_records:
                            upsert_batch(conn, browse_records)
                            log.info(
                                "Phase 2.7: upserted %d newly discovered records. (%.0fs)",
                                len(browse_records), time.monotonic() - t0,
                            )
                        else:
                            log.info("Phase 2.7: no new records built from %d new ASINs.", len(truly_new))
                log.info("Phase 2.7 done: %.0fs", time.monotonic() - t0)

            # ── Phase 3: Stale-records check ───────────────────────────────
            if args.skip_stale:
                log.info("Stale-records check skipped (--skip-stale).")
            else:
                # Exclude both Phase 1 discoveries and Phase 0 deal re-checks so
                # we don't fetch the same product pages a second time this run.
                seen_asins = {item["asin"] for item in all_items} | phase0_asins
                stale_limit = args.stale_max if args.stale_max > 0 else 10_000
                stale = fetch_stale_records(conn, seen_asins, limit=stale_limit)

                log.info("═" * 60)
                log.info(
                    "Phase 3 stale-records — %d records not seen in this run (limit %d).",
                    len(stale), stale_limit,
                )

                if stale:
                    # Reserve 10 min before the hard deadline for Phase 3.5 scoring
                    # and Phase 4 cleanup so they aren't starved when Phase 3 runs long.
                    phase3_deadline = (deadline - 10 * 60) if deadline is not None else None
                    t0 = time.monotonic()
                    crawl_stale_records(
                        stale, args.delay, conn,
                        dry_run=False, max_workers=args.stale_workers,
                        deadline=phase3_deadline,
                    )
                    log.info("Phase 3 stale: %.0fs", time.monotonic() - t0)

                    # Re-score after Phase 3: stale-records can change prices and
                    # availability, so deal scores may have changed.  Without this,
                    # products that came back in-stock (or dropped in price) during
                    # Phase 3 won't receive deal badges until the next full run.
                    if deadline is not None and time.monotonic() >= deadline:
                        log.warning("Time limit reached — skipping Phase 3.5 re-scoring.")
                    else:
                        log.info("═" * 60)
                        t0 = time.monotonic()
                        scoring_summary = score_deals(conn)
                        log.info(
                            "Phase 3.5 scoring: %.0fs — flagged=%d | maintained=%d"
                            " | cleared=%d | cooldown_skipped=%d",
                            time.monotonic() - t0,
                            scoring_summary["flagged"],
                            scoring_summary["scored"],
                            scoring_summary["cleared"],
                            scoring_summary["skipped"],
                        )
                else:
                    log.info("No stale records — all known records appeared in this crawl.")

        # ── Phase 4: History cleanup ───────────────────────────────────────
        if deadline is not None and time.monotonic() >= deadline:
            log.warning("Time limit reached — skipping Phase 4 history cleanup.")
        else:
            t0 = time.monotonic()
            delete_old_price_history(conn)
            log.info("Phase 4 cleanup: %.0fs", time.monotonic() - t0)

        # ── IndexNow: notify Bing of updated URLs ─────────────────────────
        if all_items:
            from indexnow import submit_crawl_results as _indexnow_submit
            with conn.cursor() as _cur:
                _cur.execute(
                    'SELECT style FROM "Record" WHERE style IS NOT NULL AND available = true'
                )
                _db_styles = [row[0] for row in _cur.fetchall()]
            _indexnow_submit(all_items, db_styles=_db_styles)
    finally:
        conn.close()
        _notify_revalidate()

    log.info("Total runtime: %.0fs", time.monotonic() - t_start)
    log.info("Done. ✓")


if __name__ == "__main__":
    main()
