"""
marketplace.tf client.

HONEST ARCHITECTURE NOTE: marketplace.tf's *formal* developer API
(marketplace.tf/apidocs) is seller-only (API key generation is "limited
to sellers only" per their own help article) and doesn't expose a
"browse everything for sale" endpoint - it's for managing your own
deposited items. There is, however, a public, unauthenticated page at
https://marketplace.tf/deals that lists their current Unusual/cosmetic
discounts directly, with each item linking to
https://marketplace.tf/items/tf2/{defindex};{quality};u{particle} - the
same SKU convention used across the whole TF2 trading ecosystem
(tf2-automatic, tf2-sku, etc). That page is what this module reads.

Because this is a real webpage rather than a documented JSON API, the
parser here is inherently more fragile than the other three sources: if
marketplace.tf changes their page layout, this can stop finding results
(it fails safe - zero deals found, not a crash or garbage data). It's
built defensively (matching on the stable /items/tf2/ URL pattern rather
than any particular CSS class) and against a realistic reconstruction of
the page's structure, not a raw HTML capture (no network access from
where this was written to save one) - if it turns out not to match after
a real run, the fix is almost always just this file.

marketplace.tf shows its own "X% Off" per item - that number is only
used for display, never as the actual pass/fail decision. Every deal
found here still goes through the same backpack.tf comparison as every
other source (see matcher.evaluate_listing), so the bar is consistent
across all four platforms.
"""

import logging
import re

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("marketplacetf")

DEALS_URL = "https://marketplace.tf/deals"
ITEM_URL_PATTERN = re.compile(r"/items/tf2/([^/?#\"']+)")
PRICE_PATTERN = re.compile(r"\$([\d,]+\.\d{2})")
DISCOUNT_PATTERN = re.compile(r"(\d+)\s*%\s*Off", re.IGNORECASE)

QUALITY_ID_TO_NAME = {
    0: "Normal", 1: "Genuine", 3: "Vintage", 5: "Unusual", 6: "Unique",
    7: "Community", 8: "Valve", 9: "Self-Made", 11: "Strange",
    13: "Haunted", 14: "Collector's", 15: "Decorated Weapon",
}


def parse_sku(sku: str):
    """
    Parses a tf2-automatic-style SKU ("31194;5;u188") into
    (defindex, quality_name, particle_id, craftable). Any part that
    can't be read comes back None (quality_name) - the caller should
    skip items it can't classify rather than guess.
    """
    parts = sku.split(";")
    defindex = int(parts[0]) if parts and parts[0].isdigit() else None
    quality_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    quality_name = QUALITY_ID_TO_NAME.get(quality_id) if quality_id is not None else None
    particle_id = None
    craftable = True
    for token in parts[2:]:
        if token.startswith("u") and token[1:].isdigit():
            particle_id = int(token[1:])
        elif token == "uncraftable":
            craftable = False
    return defindex, quality_name, particle_id, craftable


def parse_deals_page(html: str):
    """
    Returns a list of {"sku", "name", "price_usd", "site_discount_percent"}
    dicts, one per deal found. Best-effort: an item missing a usable price
    is skipped rather than included with a guessed value.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen_skus = set()

    for a in soup.find_all("a", href=True):
        m = ITEM_URL_PATTERN.search(a["href"])
        if not m:
            continue
        sku = m.group(1)
        if sku in seen_skus:
            continue

        name = a.get_text(strip=True)
        if not name:
            continue  # very likely the thumbnail-image link for the same
            # item, not the text/name link - the real one comes later in
            # the loop and will have text.

        # Price and discount % live somewhere in the same visual card as
        # the link, not necessarily as siblings of the <a> itself - walk
        # up a few ancestor levels looking for both.
        price = None
        discount = None
        node = a
        for _ in range(6):
            if node.parent is None:
                break
            node = node.parent
            text = node.get_text(" ", strip=True)
            if price is None:
                pm = PRICE_PATTERN.search(text)
                if pm:
                    price = float(pm.group(1).replace(",", ""))
            if discount is None:
                dm = DISCOUNT_PATTERN.search(text)
                if dm:
                    discount = float(dm.group(1))
            if price is not None and discount is not None:
                break

        if price is None:
            continue

        seen_skus.add(sku)
        results.append({
            "sku": sku,
            "name": name,
            "price_usd": price,
            "site_discount_percent": discount,
        })

    return results


class MarketplaceTFClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "tf2-deal-watcher/1.0"})

    def fetch_deals(self):
        """Returns parse_deals_page's result list, or [] on any failure -
        callers should treat a fetch problem as "nothing new this round",
        not as an error to crash on (this source polls repeatedly, so a
        transient failure is recovered on the next round)."""
        try:
            resp = self.session.get(DEALS_URL, timeout=20)
            resp.raise_for_status()
        except Exception:
            log.warning("marketplace.tf deals page request failed.")
            return []

        try:
            deals = parse_deals_page(resp.text)
        except Exception:
            log.exception("Failed to parse marketplace.tf deals page (site layout may have changed).")
            return []

        if not deals:
            log.warning(
                "Parsed 0 deals from marketplace.tf - either there are genuinely none "
                "right now, or the page layout changed and the parser needs a look."
            )
        return deals
