"""
Mannco.store REST API client.

Docs: https://docs.mannco.store/
Base URL: https://api.mannco.store

Auth flow: POST /user/login with {"apiKey": "..."} -> {"content": {"jwt": "..."}}
Then send `Authorization: Bearer <jwt>` on every other call.
The JWT is IP-bound and lasts ~31 days - refresh_if_needed() re-logs-in well
before that, and also whenever a call comes back 403 (in case the JWT
became invalid early for some reason).
"""

import logging
import re
import threading
import time

import requests

log = logging.getLogger("mannco")

# Same reasoning as bptf_client's request throttle and steam_inventory's -
# a real production log showed 4854 mannco.store events received in 5
# minutes with ZERO successfully evaluated, while backpack.tf and Steam's
# inventory endpoint already had this same protection and mannco.store's
# own get_item_details call - hit once per received event, no cap at all
# - did not. At that volume (~16/sec) with no pacing, hitting mannco's
# own rate limit on nearly every request is exactly what "received high,
# evaluated near-zero, with no specific rejection reason accounting for
# the gap" looks like.
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


_RATE_LIMIT_WAIT_PATTERN = re.compile(r"try again in (\d+)\s*seconds?", re.IGNORECASE)
# Cap however long the server claims to need, but generously - a real
# production log showed mannco.store asking for 152 seconds, well above
# the original 60s cap here, which meant the retry below waited only 61s
# (min(152, 60) + 1) and was then guaranteed to fail, since it hadn't
# actually waited out the real cooldown. Raised to match the same
# philosophy as backpack.tf's own adaptive-backoff ceiling (also 300s,
# see bptf_client.py) - generous enough for real observed values, still
# bounded against something truly pathological.
_RATE_LIMIT_MAX_WAIT_SECONDS = 300

BASE_URL = "https://api.mannco.store"
TF2_GAME_ID = 440
KEY_SLUG = "440-mann-co-supply-crate-key"


class ManncoClient:
    def __init__(self, api_key: str, jwt_refresh_seconds: int = 3600):
        self.api_key = api_key
        self.jwt = None
        self.jwt_obtained_at = 0
        self.jwt_refresh_seconds = jwt_refresh_seconds
        self.session = requests.Session()

        # itemId -> item details dict, cached for the life of the process.
        # Catalog items (name+quality+effect combo) essentially never
        # change, so this cache never needs to expire.
        self._item_details_cache = {}

    # -- auth -----------------------------------------------------------

    def login(self):
        """
        Raises RuntimeError only for a genuinely unexpected/permanent
        failure. A RATE LIMIT specifically is treated as transient: this
        waits out the exact cooldown mannco.store itself reports (capped,
        defensively, at _RATE_LIMIT_MAX_WAIT_SECONDS) and retries once
        before giving up. A real crash-loop was traced to this method
        raising unconditionally on ANY login failure, including a rate
        limit hit during the very first startup call - which happens
        outside the try/except that protects every *later* scheduled
        refresh (see price_refresh_loop in main.py), so it took down the
        whole process instead of just logging a warning and trying again
        shortly after, the way a transient hiccup should be handled.
        """
        log.info("Logging in to mannco.store...")
        resp = self.session.post(
            f"{BASE_URL}/user/login",
            json={"apiKey": self.api_key},
            timeout=30,
        )
        data = resp.json()

        if not data.get("success"):
            content = str(data.get("content", ""))
            match = _RATE_LIMIT_WAIT_PATTERN.search(content)
            if match:
                wait_seconds = min(int(match.group(1)), _RATE_LIMIT_MAX_WAIT_SECONDS) + 1
                log.warning(
                    "mannco.store login rate-limited (%s) - waiting %ds and retrying once...",
                    content, wait_seconds,
                )
                time.sleep(wait_seconds)
                resp = self.session.post(
                    f"{BASE_URL}/user/login",
                    json={"apiKey": self.api_key},
                    timeout=30,
                )
                data = resp.json()

        if not data.get("success"):
            raise RuntimeError(f"mannco.store login failed: {data}")

        self.jwt = data["content"]["jwt"]
        self.jwt_obtained_at = time.time()
        log.info("mannco.store login OK.")

    def ensure_logged_in(self):
        if self.jwt is None or (time.time() - self.jwt_obtained_at) > self.jwt_refresh_seconds:
            self.login()

    def _headers(self):
        return {"Authorization": f"Bearer {self.jwt}"}

    def _get(self, path, params=None, retry_on_auth_error=True):
        self.ensure_logged_in()
        _throttle()
        resp = self.session.get(f"{BASE_URL}{path}", headers=self._headers(), params=params, timeout=30)
        if resp.status_code == 403 and retry_on_auth_error:
            log.warning("Got 403 from mannco.store, re-logging in and retrying once.")
            self.login()
            _throttle()
            resp = self.session.get(f"{BASE_URL}{path}", headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # -- items ------------------------------------------------------------

    def get_item_details(self, item_id):
        """
        Returns the cached catalog details for an item id (from the
        market_stream 'itemId' field), fetching + caching on first use.
        Returns None if the item can't be resolved (e.g. it belongs to a
        game we don't care about, or the API returned an error).
        """
        if item_id in self._item_details_cache:
            return self._item_details_cache[item_id]

        try:
            data = self._get(f"/item/details/{item_id}")
        except requests.HTTPError as e:
            log.warning("Failed to fetch item details for %s: %s", item_id, e)
            return None

        if not data.get("success"):
            self._item_details_cache[item_id] = None
            return None

        details = data["content"]["informations"]
        self._item_details_cache[item_id] = details
        return details

    def get_item_pricing(self, item_identifier):
        """
        item_identifier can be a numeric item id or a URL slug.
        Returns the 'pricing' dict, or None on failure.
        """
        try:
            data = self._get(f"/item/pricing/{item_identifier}")
        except Exception as e:
            # Was requests.HTTPError only - broadened to catch everything
            # (including a RuntimeError from login() failing outright, or
            # any other unexpected failure) after a real production log
            # showed this failing silently and *consistently* for hours,
            # with none of the expected log messages appearing - meaning
            # whatever went wrong wasn't even reaching the code below to
            # be diagnosed. Catching broadly here at least guarantees SOME
            # log line for every failure mode, not just HTTP-level ones.
            log.warning("Failed to fetch pricing for %s: %s", item_identifier, e)
            return None
        if not isinstance(data, dict) or not data.get("success"):
            log.warning(
                "mannco.store pricing for %s came back unsuccessful - raw response: %r",
                item_identifier, data,
            )
            return None
        pricing = (data.get("content") or {}).get("pricing")
        if not pricing:
            log.warning(
                "mannco.store pricing for %s had no 'pricing' field - raw content: %r",
                item_identifier, data.get("content"),
            )
        return pricing

    def get_key_price_usd_cents(self):
        """
        Live USD price (in cents) of a Mann Co. Supply Crate Key on
        mannco.store, used as the USD<->keys exchange rate for every other
        comparison. Falls back to the lowest_sale_price -> steam_price if
        needed.

        Resolves KEY_SLUG to a numeric item id via get_item_details()
        first, then prices THAT id - a real production log showed
        get_item_pricing(KEY_SLUG) itself failing outright with "Invalid
        item ID - must be numeric", contradicting the pricing endpoint's
        own docs (which say it accepts "a numeric item id or a URL
        slug"). get_item_details() is a separate endpoint that DOES
        document (and, per that same log, does) accept a slug - so this
        goes through it to get a real numeric id, then prices that,
        rather than trusting the pricing endpoint to accept the same
        slug format its own error message says it won't.
        """
        details = self.get_item_details(KEY_SLUG)
        if not details or not details.get("id"):
            log.warning("Could not resolve mannco.store key slug %r to a numeric item id.", KEY_SLUG)
            return None
        pricing = self.get_item_pricing(details["id"])
        if not pricing:
            return None
        for field in ("lowest_sale_price", "suggested_price", "steam_price"):
            value = pricing.get(field)
            if value:
                return value
        log.warning(
            "mannco.store key pricing had none of the expected fields (lowest_sale_price/"
            "suggested_price/steam_price) - raw pricing dict: %r",
            pricing,
        )
        return None
