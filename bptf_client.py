"""
backpack.tf price list client.

Wraps GET https://backpack.tf/api/IGetPrices/v4 and turns the (fairly deeply
nested) response into a flat lookup table:

    (item_name, quality_name, particle_id_or_None) -> price_in_keys

TF2 quality name <-> id mapping is fixed by Valve and hasn't changed in
over a decade, so it's safe to hard-code.

Why "keys" as the common unit: backpack.tf prices things in either refined
metal or keys. mannco.store prices things in real money (USD cents). To
compare the two apples-to-apples we convert everything into "keys", using
mannco.store's own live key price as the USD<->key exchange rate (see
mannco_client.get_key_price_usd_cents). backpack.tf's own listed price for
the key (in refined metal) is used to convert its metal-denominated prices
into keys.
"""

import logging
import threading
import time

import requests

log = logging.getLogger("bptf")

# Caps how many backpack.tf requests (snapshot + price-history endpoints -
# the two that scale with how many items are being evaluated, not the
# once-every-15-minutes bulk price refresh) can be in flight at once,
# across every concurrent evaluate_listing() call. Without this, a burst
# of many qualifying items at once (more likely now that pricier filters
# are looser - more watched qualities, lower discount/price bars) could
# fire a large, unbounded number of simultaneous requests, risking
# backpack.tf's own rate limiting kicking in and every one of those
# requests failing together, right when there's the most going on.
# threading.Semaphore (not asyncio.Semaphore) because these calls run in
# worker threads via asyncio.to_thread, not on the event loop itself.
MAX_CONCURRENT_REQUESTS = 8
_request_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)

QUALITY_NAME_TO_ID = {
    "Normal": 0,
    "Genuine": 1,
    "Vintage": 3,
    "Unusual": 5,
    "Unique": 6,
    "Community": 7,
    "Valve": 8,
    "Self-Made": 9,
    "Strange": 11,
    "Haunted": 13,
    "Collector's": 14,
    "Decorated Weapon": 15,
}

# Confirmed directly from Valve's own official TF2 wiki templates
# (wiki.teamfortress.com/wiki/Template:Killstreakers and .../Template:Sheens)
# - the complete, official lists, not reconstructed from trading-community
# sources. Killstreaker only exists on Professional Killstreak (tier 3) -
# it's the "eyes burning" particle effect. Sheen exists on Specialized
# (tier 2) and Professional (tier 3) - the colour flash on kills.
VALID_KILLSTREAKERS = [
    "Cerebral Discharge", "Fire Horns", "Flames", "Hypno-Beam",
    "Incinerator", "Singularity", "Tornado",
]
VALID_SHEENS = [
    "Agonizing Emerald", "Deadly Daffodil", "Hot Rod", "Manndarin",
    "Mean Green", "Team Shine", "Villainous Violet",
]

# RGB values cross-checked against THREE independent, mutually-agreeing
# Steam Community reference guides (not just one - an earlier version of
# this table used a single source and turned out to have several wrong
# entries, caught by cross-checking: Radigan Conagher Brown, Noble
# Hatter's Violet, Peculiarly Drab Tincture, The Bitter Taste of Defeat
# and Lime, The Color of a Gentlemann's Business Pants, Ye Olde Rustic
# Colour, and Zepheniah's Greed were all wrong before this fix - a wrong
# decimal here would make the search link look precise while actually
# filtering to the wrong colour, worse than not filtering at all, so
# this was worth re-verifying properly rather than trusting one source.
# Team-coloured paints (different RGB per RED/BLU) are left out - which
# team a listing is on isn't something this project tracks, and picking
# the wrong one would be worse than not filtering by paint at all.
PAINT_NAME_TO_RGB = {
    "A Color Similar to Slate": (47, 79, 79),
    "A Deep Commitment to Purple": (125, 64, 113),
    "A Distinctive Lack of Hue": (20, 20, 20),
    "A Mann's Mint": (188, 221, 179),
    "After Eight": (45, 45, 36),
    "Aged Moustache Grey": (126, 126, 126),
    "An Extraordinary Abundance of Tinge": (230, 230, 230),
    "Australium Gold": (231, 181, 59),
    "Color No. 216-190-216": (216, 190, 216),
    "Dark Salmon Injustice": (233, 150, 122),
    "Drably Olive": (128, 128, 0),
    "Indubitably Green": (114, 158, 66),
    "Mann Co. Orange": (207, 115, 54),
    "Muskelmannbraun": (165, 117, 69),
    "Noble Hatter's Violet": (81, 56, 74),
    "Peculiarly Drab Tincture": (197, 175, 145),
    "Pink as Hell": (255, 105, 180),
    "Radigan Conagher Brown": (105, 77, 58),
    "The Bitter Taste of Defeat and Lime": (50, 205, 50),
    "The Color of a Gentlemann's Business Pants": (240, 230, 140),
    "Ye Olde Rustic Colour": (124, 108, 87),
    "Zepheniah's Greed": (66, 79, 59),
}


def paint_rgb_decimal(paint_name: str):
    """
    Packs a known paint's RGB into the single decimal integer format the
    classifieds `paint` search param expects (see build_classifieds_url).
    Returns None for anything not in PAINT_NAME_TO_RGB above - never
    guesses at an unmapped colour's value.

    HONEST LIMITATION: the RGB *source values* are solid (real published
    reference), but the R*65536+G*256+B packing formula used here is the
    universal standard for encoding an RGB triple as one integer, not a
    backpack.tf-specific confirmation - a single real example of this
    exact param wasn't independently reproduced. Worst case if this
    formula is wrong: the paint filter doesn't narrow the search
    correctly for painted items, same as before this was added, not a
    regression.
    """
    rgb = PAINT_NAME_TO_RGB.get(paint_name)
    if rgb is None:
        return None
    r, g, b = rgb
    return r * 65536 + g * 256 + b

PRICES_URL = "https://backpack.tf/api/IGetPrices/v4"
SNAPSHOT_URL = "https://backpack.tf/api/classifieds/listings/snapshot"
HISTORY_URL = "https://backpack.tf/api/IGetPriceHistory/v1"
# NOTE: the three API endpoints above are on the stable `backpack.tf/api`
# base - confirmed current via backpack.tf's own live OpenAPI/Swagger spec
# (server url "https://backpack.tf/api") plus a real forum post from
# Nov 2025 actively using the snapshot endpoint at this exact path. The
# *website* (human-facing pages) moved to next.backpack.tf - a separate
# concern from these API calls, see CLASSIFIEDS_URL below.
CLASSIFIEDS_URL = "https://backpack.tf/classifieds"
KEY_ITEM_NAME = "Mann Co. Supply Crate Key"


def strip_quality_prefix(full_name: str, quality_name: str) -> str:
    """
    backpack.tf displays a quality's name as a prefix on the item name for
    every quality except Unique (e.g. "Strange Rocket Launcher", but just
    "Rocket Launcher" for Unique). backpack.tf's own price list is indexed
    by that de-prefixed name (quality is a separate dimension), so this is
    needed to turn a listing's display name into a price-list lookup key.
    """
    if quality_name and quality_name != "Unique" and full_name.startswith(quality_name + " "):
        return full_name[len(quality_name) + 1:]
    return full_name


# Killstreak tier prefixes, longest-first so "Professional Killstreak "
# doesn't get accidentally left half-stripped by matching "Killstreak "
# inside it first. Also doubles as the tier->prefix map (index 0 = no
# killstreak = no prefix) used to reconstruct another tier's name for
# the price-boost sanity check (see matcher.py).
KILLSTREAK_TIER_NAMES = {0: "", 1: "Killstreak ", 2: "Specialized Killstreak ", 3: "Professional Killstreak "}
_KILLSTREAK_PREFIXES = ["Professional Killstreak ", "Specialized Killstreak ", "Killstreak "]


def strip_killstreak_prefix(name: str) -> str:
    """Strips only the Killstreak-tier prefix, leaving Australium (and
    everything else) intact - e.g. "Professional Killstreak Australium
    Rocket Launcher" -> "Australium Rocket Launcher". Used to recover the
    tier-independent base name so another tier's name can be
    reconstructed (see name_for_killstreak_tier)."""
    for prefix in _KILLSTREAK_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def name_for_killstreak_tier(name_without_killstreak: str, tier: int) -> str:
    """The inverse of strip_killstreak_prefix() - rebuilds the display
    name for a specific killstreak tier (0-3) of the same base item."""
    return f"{KILLSTREAK_TIER_NAMES.get(tier, '')}{name_without_killstreak}"


def strip_variant_prefixes(name: str) -> str:
    """
    Strips Killstreak-tier and Australium prefixes on top of
    strip_quality_prefix(), e.g. "Professional Killstreak Australium
    Rocket Launcher" -> "Rocket Launcher". Needed for the classifieds
    *search* family (the live snapshot API and the webpage link) - a
    real search the user built by hand and confirmed working uses just
    the bare weapon name ("Ambassador"), with killstreak_tier/australium
    as their own separate filter params, NOT baked into the name text;
    a name like "Professional Killstreak Ambassador" or "Australium
    Rocket Launcher" doesn't match anything there.

    Deliberately NOT used for IGetPrices / IGetPriceHistory (see
    get_price_keys / _fetch_price_history) - those are a different,
    older API family, independently confirmed to index Australium items
    by their full name ("Australium Rocket Launcher" is genuinely its
    own top-level entry, not "Rocket Launcher" + a flag, there).
    """
    name = strip_killstreak_prefix(name)
    if name.startswith("Australium "):
        name = name[len("Australium "):]
    return name



def build_classifieds_url(name: str, quality_name: str, particle_id=None,
                           steamid=None, killstreak_tier=None, australium: bool = False,
                           spell=None, paint=None, craftable: bool = True,
                           killstreaker=None, sheen=None) -> str:
    """
    Link to backpack.tf's classifieds search, filtered down to this exact
    item/quality/effect/spell - i.e. "the offer, as it appears on
    backpack.tf". Built to include every attribute that distinguishes
    this specific item (inclusion/exclusion by the traits that do or
    don't apply), so the search actually narrows down to it instead of
    returning every variant of the base item.

    HONEST LIMITATION: backpack.tf has no permalink page for a single
    listing - confirmed both by their API (a "get listing by id" call
    404s per their own forum: forums.backpack.tf/topic/69932) and by
    their own users being told, when asking for exactly this, that a
    tightly-filtered search link is the closest thing that exists. So
    this is genuinely the best available link, not a shortcut.

    When `steamid` is supplied (the seller's, e.g. from a live backpack.tf
    listing event), the search is filtered to that one seller too - in
    practice this narrows it down to just their listing(s) of this item.

    Domain: the plain backpack.tf domain (not next.backpack.tf) - the
    redesigned next.backpack.tf classifieds page filters via an in-app
    modal rather than URL query params (confirmed by their own
    "Returning User Guide": "this replaces the item filter modal"), so a
    query-string link there doesn't filter anything.

    Params confirmed directly from a real, working search the user built
    by hand and verified finds the exact right item:
    "backpack.tf/classifieds?item=Ambassador&quality=11&spell=Exorcism
    &australium=-1&killstreak_tier=0" - notably `australium` and
    `killstreak_tier` are present even when NOT applicable (-1 and 0
    respectively), and `tradable`/`craftable` are absent entirely at
    their default (craftable). This replaces an earlier, incorrect
    version of this function that always included tradable/craftable and
    never included spell at all - the missing `spell` param in
    particular meant a spelled item's search link wasn't actually
    narrowed down to it. `craftable` IS added now, but only when the
    item is uncraftable (mirroring the SKU convention where ";uncraftable"
    is only appended when true, never omitted-but-implied) - an
    uncraftable item is real and distinguishing, worth filtering by, not
    an oversight to always skip. `paint` narrows to a specific painted
    colour when recognised (see paint_rgb_decimal - confirmed real
    colour values, best-effort packing format). `killstreaker`/`sheen`
    (only meaningful on Professional/Specialized Killstreak items - see
    VALID_KILLSTREAKERS/VALID_SHEENS, both confirmed against Valve's own
    wiki) are added the same defensive way as spell: real listing
    attributes per multiple third-party backpack.tf listing-data tools,
    but not independently confirmed as *search* filter param names on
    this specific page - unrecognised params are normally just ignored,
    so this can't make a search less precise, only more if honoured.

    `name` can be passed in with or without Killstreak/Australium
    prefixes still attached (e.g. "Australium Rocket Launcher") -
    strip_variant_prefixes() is applied internally, so callers don't
    need to remember to do it themselves.
    """
    from urllib.parse import urlencode

    name = strip_variant_prefixes(name)
    quality_id = QUALITY_NAME_TO_ID.get(quality_name)
    params = {"item": name}
    if quality_id is not None:
        params["quality"] = quality_id
    if particle_id is not None:
        params["particle"] = particle_id
    if spell:
        params["spell"] = spell
    if killstreaker:
        params["killstreaker"] = killstreaker
    if sheen:
        params["sheen"] = sheen
    if not craftable:
        params["craftable"] = 0
    paint_value = paint_rgb_decimal(paint) if paint else None
    if paint_value is not None:
        params["paint"] = paint_value
    # Always present, even when not applicable - the confirmed working
    # example includes both at their "not applicable" values (-1, 0)
    # rather than omitting them.
    params["australium"] = 1 if australium else -1
    params["killstreak_tier"] = killstreak_tier if killstreak_tier else 0
    # Every listing this project evaluates is, by definition, actively
    # for sale/trade - so always tradable, unconditionally (unlike
    # craftable, there's no meaningful "untradeable listing" case to
    # distinguish). Confirmed as part of the community-standard link
    # pattern by multiple independent real examples
    # ("...&tradable=1&craftable=1" turns up repeatedly across backpack.tf
    # forum posts sharing search links for specific items).
    params["tradable"] = 1
    if steamid:
        params["steamid"] = steamid
    return f"{CLASSIFIEDS_URL}?{urlencode(params)}"


def _filter_price_outliers(prices, floor_fraction: float = 0.3, ceiling_fraction=None):
    """
    Drops prices priced suspiciously far below (and, if `ceiling_fraction`
    is given, above) the rest of the pack before a reference price is
    computed from them - the same principle the community's
    bptf-autopricer project uses ("Filters Outliers... removes listings
    with prices that deviate too much from the average"). Used for sell
    listings (floor only, guards the discount calculation against a
    troll/mistake listing dragging the reference down) and for buy orders
    (floor + ceiling, since a single implausibly-high buy order would
    otherwise make a flip look more profitable than it really is).

    Needs at least 3 prices to bother - with fewer there's no reliable
    "pack" to judge outliers against, so nothing is dropped. Never
    returns an empty list: if everything gets filtered out (extremely
    wide spread), the original unfiltered list is returned instead of
    leaving the caller with nothing to work with.
    """
    if len(prices) < 3:
        return list(prices)
    sorted_prices = sorted(prices)
    mid = len(sorted_prices) // 2
    if len(sorted_prices) % 2 == 0:
        median = (sorted_prices[mid - 1] + sorted_prices[mid]) / 2
    else:
        median = sorted_prices[mid]
    if median <= 0:
        return list(prices)
    floor = median * floor_fraction
    ceiling = median * ceiling_fraction if ceiling_fraction else None
    filtered = [p for p in prices if p >= floor and (ceiling is None or p <= ceiling)]
    return filtered if filtered else list(prices)


class BackpackTFPriceList:
    def __init__(self, api_key: str, token: str = "", snapshot_cache_seconds: int = 20):
        self.api_key = api_key
        self.token = token
        self.snapshot_cache_seconds = snapshot_cache_seconds
        self.by_name_quality = {}   # (name, quality_id) -> list of price entries
        self.key_price_metal = None
        self.last_refreshed = 0
        self.session = requests.Session()
        self._snapshot_cache = {}  # (name, quality_id, particle_id, intent) -> (timestamp, [(listing_id, price_keys)])
        self._history_cache = {}  # (name, quality_id, particle_id) -> (timestamp, history_list)

    def refresh(self):
        log.info("Refreshing backpack.tf price list...")
        resp = requests.get(PRICES_URL, params={"key": self.api_key}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        response = payload.get("response", {})
        if response.get("success") != 1:
            raise RuntimeError(f"backpack.tf IGetPrices error: {payload}")

        items = response.get("items", {})
        table = {}

        for name, item_data in items.items():
            prices = item_data.get("prices", {})
            for quality_id_str, quality_block in prices.items():
                try:
                    quality_id = int(quality_id_str)
                except ValueError:
                    continue
                tradable = quality_block.get("Tradable", {})
                craftable = tradable.get("Craftable", tradable.get("Non-Craftable", {}))
                if not isinstance(craftable, dict):
                    continue
                for particle_key, entries in craftable.items():
                    if not isinstance(entries, list):
                        entries = [entries]
                    particle_id = None
                    if particle_key not in ("0", 0):
                        try:
                            particle_id = int(particle_key)
                        except ValueError:
                            particle_id = None
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        table.setdefault((name, quality_id, particle_id), []).append(entry)

        self.by_name_quality = table

        # Pin down the live key price in refined metal, so we can convert
        # any metal-denominated price into keys.
        key_entries = table.get((KEY_ITEM_NAME, QUALITY_NAME_TO_ID["Unique"], None), [])
        for entry in key_entries:
            if entry.get("currency") == "metal":
                self.key_price_metal = float(entry["value"])
                break

        self.last_refreshed = time.time()
        log.info(
            "backpack.tf price list refreshed: %d items, key = %s metal",
            len(table),
            self.key_price_metal,
        )

    def _entry_to_keys(self, entry):
        currency = entry.get("currency")
        value = entry.get("value")
        if value is None:
            return None
        value = float(value)
        if currency == "keys":
            return value
        if currency == "metal":
            if not self.key_price_metal:
                return None
            return value / self.key_price_metal
        # Unknown currency (rare, e.g. "usd" on special items) - skip.
        return None

    def get_price_keys(self, name: str, quality_name: str, particle_id=None):
        """
        Returns the backpack.tf *community suggested* price for an item, in
        keys, or None if we don't have pricing data for it. This is the
        fallback reference used when a live snapshot isn't available (see
        get_snapshot_min_other_keys below, which is preferred).
        """
        quality_id = QUALITY_NAME_TO_ID.get(quality_name)
        if quality_id is None:
            return None
        entries = self.by_name_quality.get((name, quality_id, particle_id))
        if not entries:
            return None
        keys_values = [v for v in (self._entry_to_keys(e) for e in entries) if v is not None]
        if not keys_values:
            return None
        # Use the lowest reported value (conservative reference price).
        return min(keys_values)

    # -- live listings (classifieds snapshot) ----------------------------

    def currencies_to_keys(self, currencies: dict):
        """
        Converts a listing's {"keys": x, "metal": y, "usd": z} price object
        into a single total in keys. A listing can be priced in keys+metal
        (the normal case) or occasionally in raw usd (some third-party
        bots); usd is converted using this same key-in-metal rate is not
        possible (usd isn't metal), so for usd we fall back to whatever
        USD-per-key rate the caller supplies (mannco.store's live key
        price is used for this elsewhere in the project).
        """
        if not currencies:
            return None
        total = 0.0
        got_any = False
        if currencies.get("keys"):
            total += float(currencies["keys"])
            got_any = True
        if currencies.get("metal"):
            if not self.key_price_metal:
                return None
            total += float(currencies["metal"]) / self.key_price_metal
            got_any = True
        if not got_any and currencies.get("usd"):
            return None  # caller must handle pure-usd listings separately
        return total if got_any else None

    def _fetch_snapshot_prices(self, name: str, quality_name: str, particle_id, craftable, intent: str,
                                spell=None, australium: bool = False, killstreak_tier=None, paint=None,
                                killstreaker=None, sheen=None):
        """
        Shared fetch for backpack.tf's classifieds snapshot, used for both
        sell listings (get_snapshot_min_other_keys) and buy orders
        (get_best_buy_order_keys). Returns a list of (listing_id,
        price_in_keys) tuples, cached briefly per (item, intent, spell,
        australium, killstreak_tier, paint) so a burst of updates for the
        same item doesn't hammer the endpoint. Returns None (not a list)
        if unavailable (no token, or the request failed).

        `name` can be passed in with or without Killstreak/Australium
        prefixes - strip_variant_prefixes() is applied internally (see
        that function's docstring for why: a direct report confirmed
        baking them into the name breaks the matching search on the
        classifieds webpage, and this endpoint shares the same
        name+separate-filter convention rather than IGetPrices' fuller-
        name-as-one-string convention).

        `spell`/`australium`/`killstreak_tier`/`paint` are passed through
        defensively - confirmed as real params on the classifieds
        *webpage* search (see build_classifieds_url), not independently
        confirmed for this specific API endpoint. If any aren't actually
        supported here, the normal REST behaviour is to just ignore an
        unrecognised param, and the existing try/except below already
        falls back gracefully on any real request failure - so this
        can't make a spelled/killstreak/australium/painted item's
        reference price worse, only better if they're honoured. Filtering
        by paint here specifically matters for accuracy: without it, a
        painted cosmetic's reference price would be computed against a
        mix of painted and unpainted listings, which trade at different
        premiums.
        """
        if not self.token:
            return None

        name = strip_variant_prefixes(name)
        paint_value = paint_rgb_decimal(paint) if paint else None
        cache_key = (name, quality_name, particle_id, intent, spell, australium, killstreak_tier, paint_value,
                     killstreaker, sheen)
        cached = self._snapshot_cache.get(cache_key)
        now = time.time()
        if cached and (now - cached[0]) < self.snapshot_cache_seconds:
            return cached[1]

        quality_id = QUALITY_NAME_TO_ID.get(quality_name)
        params = {
            "appid": 440,
            "token": self.token,
            "key": self.api_key,
            "sku": name,
            "intent": intent,
            "craftable": 1 if craftable else 0,
            "tradable": 1,
            "australium": 1 if australium else -1,
            "killstreak_tier": killstreak_tier if killstreak_tier else 0,
        }
        if quality_id is not None:
            params["quality"] = quality_id
        if particle_id is not None:
            params["particle"] = particle_id
        if spell:
            params["spell"] = spell
        if paint_value is not None:
            params["paint"] = paint_value
        if killstreaker:
            params["killstreaker"] = killstreaker
        if sheen:
            params["sheen"] = sheen

        try:
            with _request_semaphore:
                resp = self.session.get(SNAPSHOT_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.exception("backpack.tf snapshot request failed for %s (%s, intent=%s)", name, quality_name, intent)
            return None

        listings = data.get("listings", data.get(intent, []))
        if not isinstance(listings, list):
            return None

        prices = []
        for listing in listings:
            if not isinstance(listing, dict):
                continue
            listing_id = listing.get("id") or listing.get("listingId")
            currencies = listing.get("currencies", {})
            price_keys = self.currencies_to_keys(currencies)
            if price_keys is None:
                continue
            prices.append((listing_id, price_keys))

        self._snapshot_cache[cache_key] = (now, prices)
        return prices

    def get_snapshot_min_other_keys(self, name: str, quality_name: str, exclude_listing_id: str,
                                     particle_id=None, craftable=True, spell=None,
                                     australium: bool = False, killstreak_tier=None, paint=None,
                                     killstreaker=None, sheen=None):
        """
        Queries backpack.tf's live classifieds snapshot for this exact item
        (same name/quality/effect/spell/australium/killstreak/paint) and
        returns
        (min_price_in_keys_among_OTHER_active_sell_listings, count_of_other_listings)
        or (None, 0) if unavailable (no token configured, request failed,
        or no other listings exist).

        This is the "real market price" signal: what are people *actually*
        asking for this exact item right now, excluding the listing we're
        currently evaluating. Before taking the minimum, listings priced
        suspiciously far below the rest of the pack are dropped (see
        _filter_price_outliers) - the same principle the community's
        bptf-autopricer project uses ("Filters Outliers... removes
        listings with prices that deviate too much from the average") to
        stop a single troll/mistake/scam-bait listing from corrupting the
        reference price for everyone else. The returned count reflects
        the listings actually used (post-filter), since that's what
        min_other_listings should be trusting.

        Requires a backpack.tf user token (separate from the API key - see
        README). Results are cached briefly per item to avoid hammering the
        endpoint when several listings for the same item update in a row.
        """
        prices = self._fetch_snapshot_prices(name, quality_name, particle_id, craftable, intent="sell",
                                              spell=spell, australium=australium, killstreak_tier=killstreak_tier,
                                              paint=paint, killstreaker=killstreaker, sheen=sheen)
        if prices is None:
            return None, 0

        others = [p for (lid, p) in prices if lid != exclude_listing_id]
        if not others:
            return None, 0
        trustworthy = _filter_price_outliers(others)
        return min(trustworthy), len(trustworthy)

    def get_best_buy_order_keys(self, name: str, quality_name: str, particle_id=None, craftable=True,
                                 spell=None, australium: bool = False, killstreak_tier=None, paint=None,
                                 killstreaker=None, sheen=None):
        """
        Highest current backpack.tf BUY order for this exact item, in
        keys - i.e. the best price someone is right now offering to pay
        for it. Returns (best_buy_price_keys, count_of_buy_orders), or
        (None, 0) if unavailable (no token, request failed, or nobody's
        currently buying).

        This is what tells you whether a cheap listing can be flipped for
        an immediate, guaranteed profit rather than just held hoping to
        resell later. Filtered both floor AND ceiling (see
        _filter_price_outliers) - a single implausibly-high fake buy
        order would otherwise make a flip look more profitable than it
        really is, which matters more here than for sell listings since
        this number directly implies "you could get X for it right now".
        Not excluding any particular order (unlike the sell-side
        reference) since we're not evaluating a specific buy order, just
        asking "what's on offer".
        """
        prices = self._fetch_snapshot_prices(name, quality_name, particle_id, craftable, intent="buy",
                                              spell=spell, australium=australium, killstreak_tier=killstreak_tier,
                                              paint=paint, killstreaker=killstreaker, sheen=sheen)
        if not prices:
            return None, 0

        values = [p for (_lid, p) in prices]
        trustworthy = _filter_price_outliers(values, floor_fraction=0.3, ceiling_fraction=3.0)
        if not trustworthy:
            return None, 0
        return max(trustworthy), len(trustworthy)

    # -- liquidity (best-effort, via price-suggestion recency) ------------

    def get_liquidity_days_since_update(self, name: str, quality_name: str, particle_id=None,
                                         craftable=True):
        """
        Returns how many days ago this item's price was last revised by
        the community (via IGetPriceHistory - the same fetch used for the
        average price, but only the single most recent entry matters
        here), or None if unavailable.

        HONEST LIMITATION: backpack.tf's actual confirmed-sale history is
        a paid Premium feature, not exposed by the free API (confirmed:
        their own forum repeatedly tells free users "you need Premium to
        search sale history"). This uses price-suggestion recency as the
        closest available free proxy for "is this item actively being
        traded" - not verified sales. A long-untouched suggestion isn't
        proof nobody's buying, just a signal worth weighing.
        """
        history = self._fetch_price_history(name, quality_name, particle_id, craftable)
        if not history:
            return None
        timestamps = [h.get("timestamp") for h in history if isinstance(h, dict) and h.get("timestamp")]
        if not timestamps:
            return None
        most_recent = max(timestamps)
        return (time.time() - most_recent) / 86400

    # -- price history (average) -----------------------------------------

    def _fetch_price_history(self, name: str, quality_name: str, particle_id=None, craftable=True):
        """
        Shared fetch for IGetPriceHistory/v1, used by both
        get_average_price_keys() and get_liquidity_days_since_update().
        Cached briefly (same window as the snapshot cache) so a single
        qualifying deal - which wants both the average price and the
        liquidity signal - triggers one history request, not two.
        Returns the raw `history` list, or None on failure/empty.

        HONESTY NOTE: this is the least-documented endpoint used in this
        project. The item/craftable/tradable params match the rest of the
        API family and are solid; the parameter name for filtering by
        Unusual particle effect on THIS SPECIFIC endpoint is not confirmed
        the way it is for the snapshot endpoint (see get_snapshot_min_other_keys) -
        "priceindex" is the best available guess based on this API
        family's older naming convention. If it's wrong, the request
        simply comes back empty and callers skip the feature - it won't
        silently show a wrong number for the wrong effect.
        """
        cache_key = (name, quality_name, particle_id)
        cached = self._history_cache.get(cache_key)
        now = time.time()
        if cached and (now - cached[0]) < self.snapshot_cache_seconds:
            return cached[1]

        params = {
            "key": self.api_key,
            "item": name,
            "quality": quality_name,
            "craftable": 1 if craftable else 0,
            "tradable": 1,
        }
        if particle_id is not None:
            params["priceindex"] = particle_id

        try:
            with _request_semaphore:
                resp = self.session.get(HISTORY_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.warning("backpack.tf price history request failed for %s (%s)", name, quality_name)
            return None

        response = data.get("response", {})
        if response.get("success") not in (1, "1"):
            self._history_cache[cache_key] = (now, None)
            return None

        history = response.get("history", [])
        if not isinstance(history, list) or not history:
            self._history_cache[cache_key] = (now, None)
            return None

        self._history_cache[cache_key] = (now, history)
        return history

    def get_average_price_keys(self, name: str, quality_name: str, particle_id=None,
                                craftable=True, days: int = 30):
        """
        Average price in keys over the trailing `days` days, from
        backpack.tf's price-suggestion history (IGetPriceHistory/v1).
        Returns None if unavailable - callers should omit the average from
        a notification rather than show a guessed number.
        """
        history = self._fetch_price_history(name, quality_name, particle_id, craftable)
        if not history:
            return None

        cutoff = time.time() - days * 86400
        recent = [h for h in history if isinstance(h, dict) and h.get("timestamp", 0) >= cutoff]

        if not recent:
            # Nothing in the primary window - try a wider one (still
            # bounded) for thinly-traded items whose suggested price
            # hasn't needed revising in a while. Never falls back to
            # "whatever's there" regardless of age: if there's nothing
            # even in the wider window, we simply don't have a trustworthy
            # recent average, and the caller should omit it rather than
            # show a stale number without saying so.
            wider_cutoff = time.time() - min(days * 3, 180) * 86400
            recent = [h for h in history if isinstance(h, dict) and h.get("timestamp", 0) >= wider_cutoff]

        if not recent:
            return None

        values_keys = [v for v in (self._entry_to_keys(h) for h in recent) if v is not None]
        if not values_keys:
            return None
        return sum(values_keys) / len(values_keys)
