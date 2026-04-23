"""
utils.py — Shared utilities for the vinyl crawler.
"""
import re
from slugify import slugify


def generate_slug(title: str, asin: str) -> str:
    """
    Generates a URL-friendly slug from the title, with the last 6 chars
    of the ASIN appended as a suffix to guarantee uniqueness.

    Examples:
      "The Essential Stevie Ray Vaughan [Disco de Vinil]", "B01IB6Q1M0"
      → "the-essential-stevie-ray-vaughan-disco-de-vinil-q1m0"
    """
    base = slugify(title[:80], separator="-", lowercase=True)
    suffix = asin[-6:].lower()
    return f"{base}-{suffix}"
