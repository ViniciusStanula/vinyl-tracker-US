"""
debug_search_card.py — Fetch a search results page, find a specific ASIN's card,
and dump its HTML + run extract_title / extract_price against it.

Usage:
    cd crawler
    python debug_search_card.py B0DKVJJFPX
"""
import sys
import os
import re
import time
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

TARGET_ASIN = sys.argv[1] if len(sys.argv) > 1 else "B0DKVJJFPX"

SEARCH_URL = (
    "https://www.amazon.com.br/s?k=vinil&i=music&rh=n%3A7791937011&s=price-asc-rank"
)

DEBUG_DIR = Path(__file__).parent.parent / "debug"
DEBUG_DIR.mkdir(exist_ok=True)

BROWSER_IDENTITIES = [
    "chrome136", "chrome133a", "chrome131", "chrome124",
    "edge101", "firefox144", "firefox135",
]


def make_session():
    try:
        from curl_cffi import requests as cffi_requests
        s = cffi_requests.Session(impersonate=random.choice(BROWSER_IDENTITIES))
    except ImportError:
        import requests as req_lib
        s = req_lib.Session()
    s.headers.update({
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://www.amazon.com.br/",
    })
    return s


def fetch_search_page(session, url: str) -> str:
    print(f"[fetch] {url}")
    resp = session.get(url, timeout=30)
    print(f"  HTTP {resp.status_code} — {len(resp.text):,} chars")
    return resp.text


def find_card_by_asin(html: str, asin: str):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    card = soup.find(attrs={"data-asin": asin})
    return card


def search_across_pages(session, asin: str, max_pages: int = 5) -> object | None:
    from bs4 import BeautifulSoup
    base = "https://www.amazon.com.br/s?k=vinil&i=music&rh=n%3A7791937011"
    for page in range(1, max_pages + 1):
        url = f"{base}&page={page}" if page > 1 else base
        html = fetch_search_page(session, url)
        # check for bot wall
        if "Robot Check" in html or "validateCaptcha" in html:
            print("  [bot] CAPTCHA page — aborting")
            break
        card = find_card_by_asin(html, asin)
        if card:
            print(f"  [found] ASIN {asin} on page {page}")
            return card
        print(f"  [page {page}] ASIN not found — trying next page")
        time.sleep(random.uniform(2.0, 3.5))
    return None


def diagnose_card(card):
    from main import extract_title, extract_price, extract_artist, normalize_artist

    print("\n" + "=" * 60)
    print(f"ASIN card found — outer HTML length: {len(str(card)):,}")
    print("=" * 60)

    title = extract_title(card)
    artist = normalize_artist(extract_artist(card))
    price = extract_price(card)

    print(f"  extract_title  → {repr(title)}")
    print(f"  extract_artist → {repr(artist)}")
    print(f"  extract_price  → {price}")

    # Dump all .a-price blocks with their ancestor data-cy values
    print("\n-- .a-price blocks in card --")
    for i, block in enumerate(card.select(".a-price")):
        offscreen = block.select_one(".a-offscreen")
        offscreen_text = offscreen.get_text(strip=True) if offscreen else "(no offscreen)"
        ancestors_cy = [
            a.get("data-cy") for a in block.parents
            if hasattr(a, "get") and a.get("data-cy")
        ]
        ancestors_cls = [
            " ".join(a.get("class", []))[:60] for a in list(block.parents)[:4]
            if hasattr(a, "get")
        ]
        print(f"  [{i}] price_text={offscreen_text!r}  data-cy ancestry={ancestors_cy}")
        print(f"       class ancestry={ancestors_cls}")

    # Dump format-list / secondary sections
    print("\n-- secondary/format sections --")
    for sel in [
        '[data-cy="secondary-offer-recipe"]',
        '[data-cy="format-list-recipe"]',
        '[data-cy="secondary-price-recipe"]',
        ".s-secondary-offer-recipe",
        ".puis-secondary",
    ]:
        els = card.select(sel)
        if els:
            print(f"  {sel}: {len(els)} element(s) found")
            for el in els:
                print(f"    text: {el.get_text(' ', strip=True)[:120]!r}")

    # Save card HTML
    out = DEBUG_DIR / f"card_{TARGET_ASIN}.html"
    out.write_text(str(card), encoding="utf-8")
    print(f"\n[saved] card HTML → {out}")


def main():
    print(f"Searching for ASIN: {TARGET_ASIN}")
    session = make_session()

    # Warm up
    print("[warm-up] hitting homepage...")
    try:
        session.get("https://www.amazon.com.br/", timeout=15)
        time.sleep(random.uniform(1.0, 2.0))
    except Exception as e:
        print(f"  warm-up error (non-fatal): {e}")

    card = search_across_pages(session, TARGET_ASIN)
    if card is None:
        print(f"\n[fail] ASIN {TARGET_ASIN} not found in first 5 pages of search results.")
        print("Try a more targeted search URL or check if the product is sponsored-only.")
        return

    diagnose_card(card)


if __name__ == "__main__":
    main()
