"""
Checks whether a Steam user's TF2 inventory is public, so the watcher can
skip alerting on backpack.tf listings from sellers whose inventory is
private (or otherwise unreachable) - a "trade with the seller" link is
useless if the buyer can't even see what's being offered / the trade
window can't load their items.

Uses Steam's public inventory endpoint directly - no API key needed:
    https://steamcommunity.com/inventory/{steamid}/440/2

Results are cached per steamid for a while, since the same active seller
can trigger many listing events in a row and this endpoint is easy to get
rate-limited on if hit too often.
"""

import logging
import threading
import time

import requests

log = logging.getLogger("steam_inventory")

INVENTORY_URL_TEMPLATE = "https://steamcommunity.com/inventory/{steamid}/440/2"

# Same reasoning as bptf_client's request throttle - a real production log
# showed this endpoint's own 429s recurring alongside backpack.tf's, and
# this endpoint has no separate concurrency cap the way backpack.tf's
# requests do (each inventory check runs in its own asyncio.to_thread
# call), so a burst of sellers being checked in quick succession could
# hit Steam's limit with no smoothing at all. A conservative minimum gap
# between request starts, shared across every SteamInventoryChecker
# instance (there's only ever one in practice, but module-level state
# keeps this correct even if that changes).
_MIN_REQUEST_INTERVAL_SECONDS = 0.5
_last_request_started_at = 0.0
_throttle_lock = threading.Lock()


def _throttle():
    global _last_request_started_at
    with _throttle_lock:
        now = time.time()
        wait = _last_request_started_at + _MIN_REQUEST_INTERVAL_SECONDS - now
        if wait > 0:
            time.sleep(wait)
        _last_request_started_at = time.time()


class SteamInventoryChecker:
    def __init__(self, cache_ttl_seconds: int = 900):
        self.cache_ttl_seconds = cache_ttl_seconds
        self.session = requests.Session()
        self._cache = {}  # steamid -> (timestamp, is_public: bool)

    def is_public(self, steamid: str):
        """
        Returns True (public), False (confirmed private), or None (could
        not determine - request failed, timed out, or got rate-limited).
        Callers should treat None as "unknown, don't block on it" - this
        check exists to filter out confirmed-private sellers, not to
        require positive confirmation for every alert, since Steam's
        inventory endpoint is easy to rate-limit and a transient failure
        shouldn't cost the user a real deal.
        """
        cached = self._cache.get(steamid)
        now = time.time()
        if cached and (now - cached[0]) < self.cache_ttl_seconds:
            return cached[1]

        url = INVENTORY_URL_TEMPLATE.format(steamid=steamid)
        try:
            _throttle()
            resp = self.session.get(url, params={"l": "english", "count": 1}, timeout=10)
        except requests.RequestException:
            log.warning("Inventory check request failed for %s (network error).", steamid)
            return None

        if resp.status_code == 403:
            # This is Steam's standard response for a private profile /
            # private inventory on this endpoint.
            self._cache[steamid] = (now, False)
            return False

        if resp.status_code == 429:
            log.warning("Rate-limited by Steam's inventory endpoint - skipping check this time.")
            return None

        if resp.status_code != 200:
            log.warning("Unexpected status %s checking inventory for %s.", resp.status_code, steamid)
            return None

        try:
            data = resp.json()
        except ValueError:
            return None

        # Steam returns {"success": false, ...} (sometimes with no body at
        # all) for private inventories that don't 403 outright.
        success = data.get("success")
        is_public = bool(success) and success not in (False, 0)

        self._cache[steamid] = (now, is_public)
        return is_public
