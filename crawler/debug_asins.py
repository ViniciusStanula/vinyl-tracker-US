"""
debug_asins.py — Fetch failing ASINs and diagnose why parse_product_page returns price=None.

Usage:
    cd crawler
    python debug_asins.py

Saves raw HTML to ../debug/{ASIN}.html and prints a diagnosis table.
"""
import os
import re
import sys
import time
import random
import json
from pathlib import Path

# ── Same browser identities as main.py ───────────────────────────────────────
BROWSER_IDENTITIES = [
    "chrome136", "chrome133a", "chrome131", "chrome124", "chrome120",
    "edge101", "firefox144", "firefox135", "firefox133",
]

BASE_URL = "https://www.amazon.com"

FAILING_ASINS = {
    "B0DHJ5JDTM": "multi-format test ASIN",
}
REFERENCE_ASIN = {"B09WVYGXFW": "single-format reference"}

DEBUG_DIR = Path(__file__).parent.parent / "debug"
DEBUG_DIR.mkdir(exist_ok=True)

# ── Session ───────────────────────────────────────────────────────────────────
def make_session():
    try:
        from curl_cffi import requests as cffi_requests
        s = cffi_requests.Session(impersonate=random.choice(BROWSER_IDENTITIES))
        s.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.amazon.com/",
        })
        print("[session] curl_cffi with browser impersonation")
        return s
    except ImportError:
        import requests as req_lib
        s = req_lib.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.amazon.com/",
        })
        print("[session] fallback: requests (no browser impersonation)")
        return s


def warm_up(session):
    try:
        print("[warm-up] fetching amazon.com homepage...")
        session.get("https://www.amazon.com/", timeout=15)
        time.sleep(random.uniform(1.0, 2.0))
        print("[warm-up] fetching vinyl category page...")
        session.get("https://www.amazon.com/s?i=popular&rh=n%3A5174&s=review-rank", timeout=15)
        time.sleep(random.uniform(0.5, 1.2))
        print("[warm-up] done.")
    except Exception as e:
        print(f"[warm-up] error (non-fatal): {e}")


# ── Fetch + save ──────────────────────────────────────────────────────────────
def fetch_and_save(session, asin: str) -> tuple[str | None, int | None]:
    """Returns (html, http_status). Saves raw HTML to debug/{asin}.html."""
    url = f"{BASE_URL}/dp/{asin}"
    out_path = DEBUG_DIR / f"{asin}.html"

    # Use cached file if recent enough (< 1 hour) to avoid re-hammering Amazon
    if out_path.exists() and (time.time() - out_path.stat().st_mtime) < 3600:
        print(f"  [cache] using existing {out_path.name}")
        return out_path.read_text(encoding="utf-8"), 200

    print(f"  [fetch] {url}")
    try:
        resp = session.get(url, timeout=25)
        status = resp.status_code
        html = resp.text
    except Exception as e:
        print(f"  [error] {e}")
        return None, None

    out_path.write_text(html, encoding="utf-8")
    print(f"  [saved] {out_path} ({len(html):,} bytes, HTTP {status})")
    time.sleep(random.uniform(2.0, 3.5))
    return html, status


# ── Diagnosis ─────────────────────────────────────────────────────────────────
BOT_SIGNALS = [
    "Robot Check", "Verificação de robô", "Digite os caracteres",
    "Sorry, we just need to make sure you're not a robot",
    "To discuss automated access to Amazon data",
    "Access Denied", "Enter the characters you see below",
    "amazon.com.br/errors/validateCaptcha", "Prove you're not a robot",
]

_VINYL_LABEL_RE = re.compile(
    r"vinil|vinyl|\blp\b|lp\s+record|lp\s+vinyl|\d+[\"']?\s*(?:inch|in\.?)\s+vinyl",
    re.IGNORECASE,
)
_PRICE_CLEAN_US_RE = re.compile(r"[$,\xa0\s]")
_INSTOCK_KW = ("in stock", "available")
_OUTOFSTOCK_KW = ("currently unavailable", "out of stock", "not available")
MIN_PRICE = 5.0


def parse_price_us(text: str) -> float | None:
    if not text:
        return None
    cleaned = _PRICE_CLEAN_US_RE.sub("", text)
    m = re.search(r"\d+\.?\d*", cleaned)
    return float(m.group()) if m else None


def diagnose(asin: str, html: str, http_status: int) -> dict:
    from bs4 import BeautifulSoup

    result = {
        "asin": asin,
        "http_status": http_status,
        "is_bot_page": False,
        "has_format_switcher": False,
        "format_rows": [],            # list of {"label": str, "price": float|None}
        "selected_swatch": None,
        "selected_is_vinyl": False,
        "tmm_vinyl_price": None,
        "has_outOfStockBuyBox": False,
        "has_qualifiedBuybox": False,
        "has_unqualifiedBuyBox": False,
        "has_outOfStock": False,
        "availability_text": None,
        "in_stock": True,
        "price_priceToPay": None,
        "price_offscreen_fallback": None,
        "price_corePriceDisplay": None,
        "price_priceblock": None,
        "predicted_outcome": None,
        "root_cause": None,
        "evidence": [],
    }

    # Bot check
    for sig in BOT_SIGNALS:
        if sig in html:
            result["is_bot_page"] = True
            result["root_cause"] = "BOT_DETECTION / CAPTCHA page"
            result["predicted_outcome"] = "deal_cleared (price=None, in_stock=True default)"
            return result

    soup = BeautifulSoup(html, "lxml")
    title_el = soup.select_one("#productTitle")
    result["page_title"] = title_el.get_text(strip=True) if title_el else "(no #productTitle)"

    # ── Availability ──────────────────────────────────────────────────────────
    avail_el = soup.select_one("#availability")
    if avail_el:
        avail_text = avail_el.get_text(" ", strip=True).lower()
        result["availability_text"] = avail_text
        if any(kw in avail_text for kw in _INSTOCK_KW):
            result["in_stock"] = True
        elif any(kw in avail_text for kw in _OUTOFSTOCK_KW):
            result["in_stock"] = False
    else:
        result["availability_text"] = "(#availability absent)"

    if soup.select_one("#qualifiedBuybox"):
        result["has_qualifiedBuybox"] = True
        result["in_stock"] = True

    # ── Buy box type ──────────────────────────────────────────────────────────
    result["has_unqualifiedBuyBox"] = bool(
        soup.select_one("#unqualifiedBuyBox") and not soup.select_one("#qualifiedBuybox")
    )
    result["has_outOfStock"] = bool(soup.select_one("#outOfStock"))

    # ── Format switcher ───────────────────────────────────────────────────────
    result["has_format_switcher"] = bool(soup.select_one("#tmmSwatches"))

    if result["has_format_switcher"]:
        # Scan #twister .top-level rows
        for row in soup.select("#twister .top-level"):
            label = row.get_text(" ", strip=True)
            offscreen = row.select_one(".a-offscreen")
            price = None
            if offscreen:
                price = parse_price_us(offscreen.get_text(strip=True).replace("\xa0", ""))
                if price and price < MIN_PRICE:
                    price = None
            result["format_rows"].append({"label": label[:120], "price": price})
            if _VINYL_LABEL_RE.search(label) and price:
                result["tmm_vinyl_price"] = price

        # Also scan tmmSwatches for a broader view of format labels/prices
        if not result["format_rows"]:
            for swatch in soup.select("#tmmSwatches .swatchElement"):
                label = swatch.get_text(" ", strip=True)
                offscreen = swatch.select_one(".a-offscreen")
                price = parse_price_us(offscreen.get_text(strip=True).replace("\xa0", "")) if offscreen else None
                result["format_rows"].append({"label": label[:120], "price": price})
                if _VINYL_LABEL_RE.search(label) and price:
                    result["tmm_vinyl_price"] = price

        # Which swatch is selected?
        selected = soup.select_one("#tmmSwatches .swatchElement.selected")
        if selected:
            result["selected_swatch"] = selected.get_text(" ", strip=True)[:120]
            result["selected_is_vinyl"] = bool(_VINYL_LABEL_RE.search(result["selected_swatch"]))
        else:
            result["selected_swatch"] = "(none found)"
            result["selected_is_vinyl"] = True  # no swatch = single-format, treated as vinyl

        result["has_outOfStockBuyBox"] = bool(soup.select_one("#outOfStockBuyBox"))
    else:
        result["selected_is_vinyl"] = True  # no switcher = single-format

    # ── Price selectors ───────────────────────────────────────────────────────
    # priceToPay / apex-pricetopay-value
    for sel in (".priceToPay", ".apex-pricetopay-value"):
        el = soup.select_one(sel)
        if el:
            offscreen = el.select_one(".a-offscreen")
            if offscreen:
                result["price_priceToPay"] = offscreen.get_text(strip=True)
                break
            whole = el.select_one(".a-price-whole")
            frac  = el.select_one(".a-price-fraction")
            if whole:
                result["price_priceToPay"] = whole.get_text(strip=True) + "." + (frac.get_text(strip=True) if frac else "00")
                break

    # corePriceDisplay
    core = soup.select_one("#corePriceDisplay_desktop_feature_div")
    if core:
        offscreen = core.select_one(".a-offscreen")
        result["price_corePriceDisplay"] = offscreen.get_text(strip=True) if offscreen else core.get_text(" ", strip=True)[:100]

    # priceblock_ourprice (legacy)
    el = soup.select_one("#priceblock_ourprice")
    if el:
        result["price_priceblock"] = el.get_text(strip=True)

    # Generic .a-offscreen fallback (first matching price)
    for el in soup.select(".a-offscreen"):
        text = el.get_text(strip=True).replace("\xa0", "")
        if text.startswith("$") or re.match(r"^\d+[,.]", text):
            p = parse_price_us(text)
            if p and p >= MIN_PRICE:
                result["price_offscreen_fallback"] = text
                break

    # ── Predict crawler outcome ───────────────────────────────────────────────
    # Replicate parse_product_page() logic
    price: float | None = None
    in_stock = result["in_stock"]

    if result["has_unqualifiedBuyBox"]:
        result["predicted_outcome"] = "deal_cleared (unqualifiedBuyBox → price=None, in_stock=True)"
        result["root_cause"] = "THIRD_PARTY_ONLY: #unqualifiedBuyBox present, no qualified offer → price=None"
        return result

    if result["has_format_switcher"] and result["tmm_vinyl_price"] is None:
        if result["has_outOfStockBuyBox"]:
            result["predicted_outcome"] = "deal_cleared (vinyl OOS via #outOfStockBuyBox)"
            result["root_cause"] = "VINYL_OOS: #outOfStockBuyBox on multi-format page, vinyl not available"
            return result
        if not result["selected_is_vinyl"]:
            result["predicted_outcome"] = "deal_cleared (wrong swatch selected, no vinyl row price)"
            result["root_cause"] = "WRONG_FORMAT_SELECTED: multi-format page served with non-vinyl swatch active, format table has no vinyl price"
            return result

    # Try to extract price as the crawler would
    if result["tmm_vinyl_price"]:
        price = result["tmm_vinyl_price"]
    elif result["price_priceToPay"]:
        price = parse_price_us(result["price_priceToPay"])
    elif not result["has_format_switcher"] and result["price_offscreen_fallback"]:
        price = parse_price_us(result["price_offscreen_fallback"])

    if price and price >= MIN_PRICE:
        result["predicted_outcome"] = f"updated (price extracted: $ {price:.2f})"
        result["root_cause"] = "SHOULD_WORK — price reachable"
    else:
        result["predicted_outcome"] = "deal_cleared (price=None after all selectors)"
        result["root_cause"] = "NO_PRICE_ELEMENT: none of the expected price selectors yielded a value"

    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    from bs4 import BeautifulSoup  # noqa: F401 — verify import early

    session = make_session()
    warm_up(session)

    all_asins = {**FAILING_ASINS, **REFERENCE_ASIN}
    results = {}

    for asin, title in all_asins.items():
        print(f"\n{'-'*60}")
        print(f"ASIN: {asin}  —  {title}")
        html, status = fetch_and_save(session, asin)
        if html is None:
            results[asin] = {"asin": asin, "root_cause": f"FETCH_FAILED (HTTP {status})", "predicted_outcome": "skip"}
            continue
        results[asin] = diagnose(asin, html, status)

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n\n" + "="*80)
    print("DIAGNOSIS SUMMARY")
    print("="*80)

    header = f"{'ASIN':<14} {'HTTP':>4}  {'Format?':>7}  {'VinylSel?':>9}  {'TMM$':>8}  {'ptpToPay':>12}  OUTCOME"
    print(header)
    print("-"*80)

    for asin, r in results.items():
        label = (FAILING_ASINS | REFERENCE_ASIN).get(asin, "")[:30]
        fmt_sw = "YES" if r.get("has_format_switcher") else "no"
        vsel   = "YES" if r.get("selected_is_vinyl") else "NO"
        tmm    = f"${r['tmm_vinyl_price']:.2f}" if r.get("tmm_vinyl_price") else "—"
        ptp    = r.get("price_priceToPay") or "—"
        outcome = r.get("predicted_outcome", "?")
        print(f"{asin:<14} {r.get('http_status','?'):>4}  {fmt_sw:>7}  {vsel:>9}  {tmm:>8}  {ptp:>12}  {outcome}")

    print("\n\nDETAILED ROOT CAUSES")
    print("-"*80)
    for asin, r in results.items():
        label = (FAILING_ASINS | REFERENCE_ASIN).get(asin, "")
        print(f"\n{asin} — {label}")
        print(f"  Page title    : {r.get('page_title', '?')}")
        print(f"  HTTP status   : {r.get('http_status')}")
        print(f"  Bot page      : {r.get('is_bot_page')}")
        print(f"  Availability  : {r.get('availability_text')}")
        print(f"  in_stock      : {r.get('in_stock')}")
        print(f"  Format switch : {r.get('has_format_switcher')}")
        if r.get("format_rows"):
            print(f"  Format rows   :")
            for row in r["format_rows"]:
                marker = " ← selected" if r.get("selected_swatch") and row["label"][:40] in r.get("selected_swatch","") else ""
                print(f"    [{row['label'][:60]}]  price={row['price']}{marker}")
        print(f"  Selected swatch: {r.get('selected_swatch')}")
        print(f"  selectedIsVinyl: {r.get('selected_is_vinyl')}")
        print(f"  TMM vinyl $   : {r.get('tmm_vinyl_price')}")
        print(f"  #outOfStockBB : {r.get('has_outOfStockBuyBox')}")
        print(f"  #qualifiedBB  : {r.get('has_qualifiedBuybox')}")
        print(f"  #unqualifiedBB: {r.get('has_unqualifiedBuyBox')}")
        print(f"  price.priceToPay     : {r.get('price_priceToPay')}")
        print(f"  price.corePricDisp   : {r.get('price_corePriceDisplay')}")
        print(f"  price.priceblock     : {r.get('price_priceblock')}")
        print(f"  price.offscreen_fall : {r.get('price_offscreen_fallback')}")
        print(f"  ROOT CAUSE    : {r.get('root_cause')}")
        print(f"  OUTCOME       : {r.get('predicted_outcome')}")

    # Save JSON for further analysis
    out_json = DEBUG_DIR / "diagnosis.json"
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n\nFull JSON saved to: {out_json}")


if __name__ == "__main__":
    main()
