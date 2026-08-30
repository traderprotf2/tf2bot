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
import time

import requests

log = logging.getLogger("mannco")

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
        log.info("Logging in to mannco.store...")
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
        resp = self.session.get(f"{BASE_URL}{path}", headers=self._headers(), params=params, timeout=30)
        if resp.status_code == 403 and retry_on_auth_error:
            log.warning("Got 403 from mannco.store, re-logging in and retrying once.")
            self.login()
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
        except requests.HTTPError as e:
            log.warning("Failed to fetch pricing for %s: %s", item_identifier, e)
            return None
        if not data.get("success"):
            return None
        return data["content"].get("pricing")

    def get_key_price_usd_cents(self):
        """
        Live USD price (in cents) of a Mann Co. Supply Crate Key on
        mannco.store, used as the USD<->keys exchange rate for every other
        comparison. Falls back to the lowest_sale_price -> steam_price if
        needed.
        """
        pricing = self.get_item_pricing(KEY_SLUG)
        if not pricing:
            return None
        for field in ("lowest_sale_price", "suggested_price", "steam_price"):
            value = pricing.get(field)
            if value:
                return value
        return None
