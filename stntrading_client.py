"""
stntrading.eu client.

HONEST ARCHITECTURE NOTE, please read before relying on this: unlike
backpack.tf, mannco.store and marketplace.tf, I could not find any
public way to browse "everything currently for sale" on stntrading.eu -
their real, official API (confirmed via their own GitHub org,
github.com/STNTrading/public-api, and the real `stntrading` PyPI
package) is built around checking ONE named item at a time
(get_item_details("The Team Captain") -> current price + stock), not a
feed or catalog of what's newly listed. So this source works
differently from the other three: instead of discovering new cheap
items on its own, it periodically re-checks a list of items YOU tell it
to watch (see /watchstn in Telegram) against backpack.tf. If that list
is empty, this source simply does nothing - it's opt-in.

IMPORTANT GATE, confirmed directly from STN's own docs: item-level
pricing (the exact call this module makes) is listed as a BETA endpoint
that requires STN Premium (a paid subscription) - a plain/free API key
gets a request that comes back unsuccessful every time, which this
module already handles as a normal "no price available" result (no
crash, no special-cased error) rather than something that looks broken.
Concretely: without STN Premium, /watchstn accepts items onto the list
but nothing will ever actually alert from this source - worth knowing
before assuming it's silently failing due to a bug.

Also worth knowing before turning this on: stntrading.eu's reputation in
the TF2 trading community is genuinely mixed, not just a stray old
complaint - re-checked this directly rather than taking the original
finding on faith. The same Dec 2020 Steam discussion ("scammed numerous
people" vs. "I use it all the time, it's safe") still turns up, and
independent site-trust scanners rate it in the middle (60-68/100, not
flagged as malicious but not clean either), plus recent Trustpilot
feedback about sellers using the platform to offload stolen items
specifically. None of this is a verdict - just what a real check
surfaced - decide for yourself whether you want prices from here
influencing what you buy.

Base URL and the get_item_details shape are confirmed against the real
`stntrading` PyPI package's documented usage. The exact currency unit
convention for `get_key_prices()` was NOT independently confirmed (their
official docs are behind a Swagger UI that needs a JS-capable fetch to
read, which wasn't available while writing this) - get_item_details's
own pricing sub-object is used directly instead, since real examples
show it's plainly refined-metal/keys (e.g. {"metal": 10.0}), consistent
with the rest of this project's currency handling.
"""

import logging

import requests

log = logging.getLogger("stntrading")

API_BASE = "https://api.stntrading.eu"


class STNTradingClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def get_item_price_keys(self, item_name: str, key_price_metal: float):
        """
        Returns (sell_price_keys, stock_level) for the named item, or
        (None, None) if unavailable. `key_price_metal` converts a
        metal-denominated price into keys (reuse backpack.tf's own live
        key-in-metal rate, for consistency with the rest of the project -
        stntrading.eu is not itself used as a keys/metal exchange rate
        source).
        """
        if not self.api_key:
            return None, None
        try:
            resp = self.session.get(
                f"{API_BASE}/items/{item_name}",
                params={"apiKey": self.api_key},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.warning("stntrading.eu item lookup failed for %r", item_name)
            return None, None

        if not data.get("success"):
            return None, None

        item = data.get("item") or data.get("result", {}).get("item") or {}
        pricing = item.get("pricing", {})
        sell = pricing.get("sell", {})
        stock = (item.get("stock") or {}).get("level")

        keys = sell.get("keys", 0) or 0
        metal = sell.get("metal", 0) or 0
        if not keys and not metal:
            return None, stock

        price_keys = keys + (metal / key_price_metal if key_price_metal else 0)
        return price_keys, stock
