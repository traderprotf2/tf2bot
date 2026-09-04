"""
backpack.tf price list client.

Wraps GET https://backpack.tf/api/IGetPrices/v4 into a flat lookup table:

    (item_name, quality_name, particle_id_or_None) -> price_in_keys

TF2 quality name<->id mapping is fixed by Valve, safe to hard-code.

Common unit is "keys": backpack.tf prices in refined metal or keys;
comparing anything to a USD price uses mannco.store's own live key price
as the USD<->key exchange rate (see mannco_client.get_key_price_usd_cents).
backpack.tf's own listed key price (in refined metal) converts its
metal-denominated prices into keys.
"""

import collections
import json
import logging
import os
import threading
import time
import uuid

import requests

import unusual_effects

log = logging.getLogger("bptf")

# Caps how many backpack.tf requests (snapshot + price-history - the two
# that scale with items being evaluated, not the 15-min bulk price
# refresh) can be in flight at once. Prevents a burst of qualifying
# items from firing an unbounded number of simultaneous requests and
# tripping backpack.tf's rate limit. threading.Semaphore (not asyncio) -
# these calls run in worker threads via asyncio.to_thread. Reconfigured
# to scale with the account pool below - see configure_request_pacing.
MAX_CONCURRENT_REQUESTS = 4
_request_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)

# Per-account 429 cooldown, adaptive: doubles on a hit shortly after the
# last one (capped at 5 min), resets to base after a quiet while - see
# _AccountPool.note_rate_limited below, which uses these per account,
# not globally.
_RATE_LIMIT_BASE_COOLDOWN_SECONDS = 10
_RATE_LIMIT_MAX_COOLDOWN_SECONDS = 300
_RATE_LIMIT_RESET_AFTER_SECONDS = 300


class _AccountPool:
    """
    Distributes backpack.tf requests across one or more accounts (each
    its own api_key + user token), round-robin, so the AGGREGATE
    throughput scales with the number of accounts configured - added
    per explicit confirmation from backpack.tf that running several
    accounts' worth of requests in parallel is fine (the higher premium
    rate limit is a convenience perk on top of their other paid
    features, not a rule against this). Each account still fully
    respects backpack.tf's own per-key rate limit on its OWN clock - two
    accounts each independently pacing themselves to one request every
    MIN_REQUEST_INTERVAL_SECONDS means the pool as a whole can sustain
    roughly twice that rate, N accounts roughly N times, simply because
    no single account is ever asked to go faster than its own real
    limit allows.

    Picks whichever account has waited longest since its own last use,
    not strict round-robin by position - this self-corrects if one
    account's request happens to take longer than another's (a slow
    response doesn't throw off the whole rotation), and naturally
    degenerates into the exact same behavior as the old single-account
    throttle when there's only one account configured.
    """

    def __init__(self, accounts, min_interval_seconds):
        self._accounts = list(accounts)
        self._min_interval = min_interval_seconds
        self._last_used_at = [0.0] * len(self._accounts)
        # Per-account cooldown-until timestamp and current cooldown
        # duration - a real, confirmed architectural bug: rate-limit
        # cooldown used to be ONE global flag shared by every account,
        # so a single account getting 429'd paused ALL of them at once,
        # completely defeating the purpose of having several - the
        # aggregate throughput a multi-account pool exists to provide
        # never actually materialized, since any one account tripping
        # its own limit (likely, with several workers hammering
        # concurrently) stalled the whole pool regardless of how much
        # headroom the other accounts still had.
        self._cooldown_until = [0.0] * len(self._accounts)
        self._cooldown_seconds = [_RATE_LIMIT_BASE_COOLDOWN_SECONDS] * len(self._accounts)
        self._last_hit_at = [0.0] * len(self._accounts)
        self._lock = threading.Lock()

    def reconfigure(self, accounts, min_interval_seconds):
        with self._lock:
            self._accounts = list(accounts)
            self._min_interval = min_interval_seconds
            self._last_used_at = [0.0] * len(self._accounts)
            self._cooldown_until = [0.0] * len(self._accounts)
            self._cooldown_seconds = [_RATE_LIMIT_BASE_COOLDOWN_SECONDS] * len(self._accounts)
            self._last_hit_at = [0.0] * len(self._accounts)

    def acquire(self):
        """
        Blocks (in the calling thread) until some account's turn comes
        up, then returns (index, api_key, token) - the index is needed
        so a later 429 for this specific request can be reported back
        against the right account only (see note_rate_limited below),
        not the whole pool.
        """
        while True:
            with self._lock:
                now = time.time()
                eligible = [i for i in range(len(self._accounts)) if self._cooldown_until[i] <= now]
                # If literally every account is on cooldown, fall back to
                # picking among all of them anyway (rather than blocking
                # forever) - whichever clears first will actually be
                # usable the moment its own wait below elapses.
                pool = eligible or list(range(len(self._accounts)))
                best_idx = min(pool, key=lambda i: self._last_used_at[i])
                wait = max(
                    self._last_used_at[best_idx] + self._min_interval - now,
                    self._cooldown_until[best_idx] - now,
                )
                if wait <= 0:
                    self._last_used_at[best_idx] = now
                    account = self._accounts[best_idx]
                    return best_idx, account["api_key"], account["token"]
            time.sleep(max(wait, 0.05))

    def note_rate_limited(self, index):
        """Per-account version of the old global _note_rate_limited -
        same adaptive backoff (doubles on a hit shortly after the last
        one, resets to base after a quiet while), scoped to just this
        one account's own cooldown clock."""
        with self._lock:
            now = time.time()
            if now - self._last_hit_at[index] > _RATE_LIMIT_RESET_AFTER_SECONDS:
                self._cooldown_seconds[index] = _RATE_LIMIT_BASE_COOLDOWN_SECONDS
            else:
                self._cooldown_seconds[index] = min(
                    self._cooldown_seconds[index] * 2, _RATE_LIMIT_MAX_COOLDOWN_SECONDS
                )
            self._last_hit_at[index] = now
            self._cooldown_until[index] = now + self._cooldown_seconds[index]

    def any_account_available(self):
        """Whether at least one account is NOT currently on cooldown -
        used for /stats' "currently rate limited" display, which now
        means "every account is cooling down" rather than the old
        single global flag."""
        with self._lock:
            now = time.time()
            return any(self._cooldown_until[i] <= now for i in range(len(self._accounts)))


# Populated by configure_request_pacing at startup (see main.py) with
# whatever account(s) are in config.json - defaults to a single empty
# placeholder account so the module still imports cleanly before that
# call happens (e.g. under a bare unit test).
_account_pool = _AccountPool([{"api_key": "", "token": ""}], 11.0)


def configure_request_pacing(accounts, max_concurrent: int, min_interval_seconds: float):
    """
    Called once at startup (see main.py) with the account list built
    from config.json (backpacktf_accounts if present, otherwise a
    single-item list from backpacktf_api_key/backpacktf_token) plus
    cfg["bptf_max_concurrent_requests"] / cfg["bptf_min_request_interval_
    seconds"] - all tunable from config.json without editing this file.
    Safe to call before any real request has been made (which is when
    main.py calls it) - rebuilds the semaphore and account pool fresh,
    not just relabels the constants.
    """
    global MAX_CONCURRENT_REQUESTS, _request_semaphore
    MAX_CONCURRENT_REQUESTS = max_concurrent
    _request_semaphore = threading.Semaphore(max_concurrent)
    _account_pool.reconfigure(accounts, min_interval_seconds)


def is_rate_limited() -> bool:
    """Whether EVERY configured account is currently in its post-429
    cooldown (see _AccountPool.note_rate_limited) - used for /stats'
    "currently throttled" display. With multiple accounts, one account
    cooling down no longer counts as "rate limited" here, since the
    others can still serve requests."""
    return not _account_pool.any_account_available()


def _get_with_retry(session, url, params, timeout=20):
    """
    Shared GET for the two backpack.tf endpoints that scale with
    evaluation volume (snapshot + price-history). Tracks a 429 against
    the SPECIFIC account that got it (see _AccountPool.note_rate_limited)
    - not every other account in the pool. Retries once, after a short
    pause, on a 5xx (backpack.tf's own infra briefly overloaded).

    `params` should NOT include "key"/"token" - _AccountPool.acquire()
    picks which account's credentials to use for this specific request,
    so a retry after a 5xx goes through the pool fresh too.
    """
    idx, api_key, token = _account_pool.acquire()
    request_params = dict(params, key=api_key, token=token)
    with _request_semaphore:
        resp = session.get(url, params=request_params, timeout=timeout)
    if resp.status_code == 429:
        _account_pool.note_rate_limited(idx)
    elif resp.status_code >= 500:
        time.sleep(1.5)
        idx, api_key, token = _account_pool.acquire()
        request_params = dict(params, key=api_key, token=token)
        with _request_semaphore:
            resp = session.get(url, params=request_params, timeout=timeout)
        if resp.status_code == 429:
            _account_pool.note_rate_limited(idx)
    return resp

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

# RGB values cross-checked against three independent Steam Community
# reference guides (a single-source version had several wrong entries).
# Team-coloured paints (7 of them) are deliberately excluded - a single
# RGB doesn't exist for them (colour depends on which team the CURRENT
# wearer is on, not a fixed value) - not a gap: Valve's wiki confirms 29
# total Paint Cans, and this table's 22 entries + those 7 account for
# all 29 (evaluate_listing skips rather than guesses for the 7 - see
# paint_rgb_decimal()'s own check).
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

# The 7 team-coloured paints, as (RED, BLU) RGB pairs. A single PAINTED
# ITEM has ONE fixed colour, not one that changes with whichever team
# currently has it equipped (confirmed by a real forum post: a user's
# own item "shows as only the red team spirit and not both") - so
# trying both RED and BLU as separate searches is safe: querying with
# the wrong one just returns an empty, honest result, never wrong data.
TEAM_COLOR_PAINT_RGB = {
    "An Air of Debonair": {"RED": (101, 71, 64), "BLU": (40, 57, 77)},
    "Balaclavas Are Forever": {"RED": (59, 31, 35), "BLU": (24, 35, 61)},
    "Cream Spirit": {"RED": (195, 108, 45), "BLU": (184, 128, 53)},
    "Operator's Overalls": {"RED": (72, 56, 56), "BLU": (56, 66, 72)},
    "Team Spirit": {"RED": (184, 56, 59), "BLU": (88, 133, 162)},
    "The Value of Teamwork": {"RED": (128, 48, 32), "BLU": (37, 109, 141)},
    "Waterlogged Lab Coat": {"RED": (168, 154, 140), "BLU": (131, 159, 163)},
}


def paint_rgb_decimal(paint_name: str):
    """
    Packs a known paint's RGB into the decimal integer the classifieds
    `paint` search param expects (see build_classifieds_url). Returns
    None for anything not in PAINT_NAME_TO_RGB - never guesses.
    """
    rgb = PAINT_NAME_TO_RGB.get(paint_name)
    if rgb is None:
        return None
    r, g, b = rgb
    return r * 65536 + g * 256 + b


def team_color_paint_decimals(paint_name: str):
    """
    Returns [RED_decimal, BLU_decimal] for one of the 7 team-coloured
    paints (see TEAM_COLOR_PAINT_RGB above), or None if paint_name isn't
    one of them. Two candidate values, not one, since a single painted
    item has one fixed colour but which of the two isn't knowable from
    the name alone - the caller tries both as separate searches (safe:
    the wrong one just returns no matches, not wrong data - see
    TEAM_COLOR_PAINT_RGB's own comment).
    """
    variants = TEAM_COLOR_PAINT_RGB.get(paint_name)
    if variants is None:
        return None
    return [
        variants["RED"][0] * 65536 + variants["RED"][1] * 256 + variants["RED"][2],
        variants["BLU"][0] * 65536 + variants["BLU"][1] * 256 + variants["BLU"][2],
    ]

PRICES_URL = "https://backpack.tf/api/IGetPrices/v4"
SNAPSHOT_URL = "https://backpack.tf/api/classifieds/listings/snapshot"

# Same directory-relative convention as runtime_settings.py's own
# STATE_PATH - where LocalListingStore's save_to_disk/load_from_disk
# persist across restarts (see main.py's local_store_snapshot_loop and
# Watcher.run()).
LOCAL_LISTINGS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_listings_state.json")
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

    Checks for the quality prefix both at the very start AND right after
    a leading "Non-Craftable " - a real, confirmed bug: some items'
    raw names put "Non-Craftable " BEFORE the quality ("Non-Craftable
    Unusual Taunt: Luxury Lounge Unusualifier"), which the start-of-
    string-only check never matched, so nothing got stripped - and this
    function's own caller then added the quality prefix a SECOND time
    on top of the already-present one ("Unusual Non-Craftable Unusual
    Taunt: ..."), corrupting both the display name and, more seriously,
    the classifieds search link built from it.
    """
    if quality_name and quality_name != "Unique":
        prefix = quality_name + " "
        if full_name.startswith(prefix):
            return full_name[len(prefix):]
        if full_name.startswith("Non-Craftable " + prefix):
            return "Non-Craftable " + full_name[len("Non-Craftable " + prefix):]
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
    everything else) intact - used to recover the tier-independent base
    name so another tier's name can be reconstructed (see
    name_for_killstreak_tier).

    Does NOT strip on a Killstreak Kit/Kit Fabricator itself - unlike a
    weapon, a Kit/Fabricator has no tier-independent base item: each
    tier's Kit is a separate, independently-priced item, not one item
    with a stripped-off modifier. Stripping there searched for a name
    that doesn't exist, producing wrong reference prices (a real, fixed
    bug).
    """
    if name.endswith("Kit") or name.endswith("Fabricator"):
        return name
    for prefix in _KILLSTREAK_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def name_for_killstreak_tier(name_without_killstreak: str, tier: int) -> str:
    """The inverse of strip_killstreak_prefix() - rebuilds the display
    name for a specific killstreak tier (0-3) of the same base item."""
    return f"{KILLSTREAK_TIER_NAMES.get(tier, '')}{name_without_killstreak}"


def strip_effect_prefix(name: str, particle_name: str) -> str:
    """
    Strips an Unusual particle effect name from the FRONT of the item
    name, e.g. ("Circling Heart Hot Dogger", "Circling Heart") ->
    "Hot Dogger". backpack.tf's own item.name already includes the
    effect as a display prefix - without this, the effect ends up
    duplicated in an alert ("Unusual Circling Heart Hot Dogger (Circling
    Heart)") and the classifieds search link ends up searching for a
    name that doesn't match anything real once particle=<id> is also
    passed separately.

    Strips the prefix case-INSENSITIVELY (only the returned slice keeps
    `name`'s own original casing for whatever remains after the prefix) -
    a real, hard-to-spot bug: raw effect-name casing/phrasing from
    backpack.tf's own payload isn't guaranteed identical across every
    event for the same effect, and an exact, case-sensitive match here
    meant the SAME effect could strip successfully on one event and
    silently fail on another - leaving that one item's `name` still
    carrying the effect prefix, which never matches the identity key of
    every OTHER, correctly-stripped event for the exact same effect.
    """
    if not particle_name:
        return name
    prefix = particle_name + " "
    if name.lower().startswith(prefix.lower()):
        return name[len(prefix):]
    return name


def find_effect_prefix(name: str):
    """
    Given a raw item name that MAY start with a known Unusual effect
    name as a literal text prefix (e.g. "Orbiting Planets Merc's
    Mohawk"), returns (effect_name, particle_id, stripped_name) for the
    LONGEST matching known effect in unusual_effects.NAME_TO_ID, or
    (None, None, name) if nothing matches. Longest match wins so a
    short effect name never wins over a longer, more specific one that
    also matches as a prefix of the same text.

    A real, confirmed gap this covers: backpack.tf's real-time
    websocket stream doesn't always include a usable particle.id (or
    any of main.py's other structured-field fallbacks) for every
    Unusual event, even for long-established, well-known effects - but
    the item's own name text still reliably carries the effect as a
    prefix regardless, since that's baked into backpack.tf's own
    item.name convention. When every structured field comes up empty,
    this text is the last, most reliable signal actually available -
    and it's the SAME text the bug this fixes was already leaving
    un-stripped, so it's guaranteed to be present exactly when needed.
    """
    name_lower = name.lower()
    best_name, best_id, best_len = None, None, 0
    for effect_name, effect_id in unusual_effects.NAME_TO_ID.items():
        prefix_len = len(effect_name) + 1
        if prefix_len > best_len and name_lower.startswith(effect_name.lower() + " "):
            best_name, best_id, best_len = effect_name, effect_id, prefix_len
    if best_name is None:
        return None, None, name
    return best_name, best_id, name[best_len:]


def safe_dict(value):
    """
    Returns value if it's a dict, else {} - the safe replacement for the
    "value or {}" pattern used all over this project (and main.py) to
    defensively unpack a nested field that's normally an object.
    "or {}" only substitutes when value is FALSY - a truthy NON-dict
    (a string, a list, a number) sails straight through unchanged, and
    the very next .get() on it throws. A real, confirmed case: a
    scam/fake listing bot's data for one specific item had some nested
    field in this shape, and the SAME "X or {}" pattern was repeated
    across this file AND main.py's own real-time event handler (the
    highest-volume, most exposed path in the whole project) - all
    fixed here at once, the same safe way.
    """
    return value if isinstance(value, dict) else {}


def strip_variant_prefixes(name: str) -> str:
    """
    Strips "Non-Craftable", Killstreak-tier, and Australium prefixes on
    top of strip_quality_prefix() - e.g. "Non-Craftable Professional
    Killstreak Australium Rocket Launcher" -> "Rocket Launcher". Needed
    for the classifieds search family (snapshot API + webpage link),
    which filters killstreak_tier/australium/craftable as separate
    params, NOT baked into the name text - a name still carrying these
    prefixes matches nothing there.

    "Non-Craftable " is checked first (best understanding of Valve's
    naming order - if wrong for some combination, reorder the checks
    here). NOT used for IGetPrices/IGetPriceHistory (see get_price_keys/
    _fetch_price_history) - that older API family indexes Australium
    items by full name, and reads craftable status from which JSON
    branch an entry came from (see _iter_price_entries), never from name
    text either.
    """
    if name.startswith("Non-Craftable "):
        name = name[len("Non-Craftable "):]
    name = strip_killstreak_prefix(name)
    if name.startswith("Australium "):
        name = name[len("Australium "):]
    if name.startswith("The "):
        # Confirmed via a direct, real working search URL: backpack.tf's
        # classifieds search itself indexes items WITHOUT the "The"
        # article ("Beast from Below", not "The Beast from Below") even
        # though "The" is the item's own canonical, in-game display name -
        # a name still carrying it matches nothing there, the same
        # reasoning as the other prefixes above.
        name = name[len("The "):]
    return name



def build_classifieds_url(name: str, quality_name: str, particle_id=None,
                           steamid=None, killstreak_tier=None, australium: bool = False,
                           spell=None, paint=None, craftable: bool = True,
                           killstreaker=None, sheen=None) -> str:
    """
    Link to backpack.tf's classifieds search, filtered to this exact
    item/quality/effect/spell - the closest thing to a permalink that
    exists (backpack.tf has no single-listing permalink - confirmed by
    their API 404ing on that).

    Domain is plain backpack.tf, not next.backpack.tf - the redesigned
    site filters via an in-app modal, not URL params.

    Confirmed from two real, working search URLs (direct report,
    verified against the item's OWN classifieds page rather than any
    documentation): "...?item=Beast+from+Below&quality=6&tradable=1
    &craftable=1&australium=-1&killstreak_tier=0" - note what's
    genuinely absent here versus an EARLIER, wrong assumption this
    project made: no `spell` param at all when there's no spell to
    filter by (an explicit "spell=None" sentinel, sent unconditionally
    before this fix, was never actually confirmed and turned out to
    break real searches - omitted now unless there's an actual spell
    value to filter by), and `craftable` sent explicitly (1, not just
    omitted) even for a plain craftable item, not only when uncraftable
    as this project assumed before. `australium`/`killstreak_tier` are
    still present at their "not applicable" values (-1, 0) exactly as
    already confirmed. `killstreaker`/`sheen` deliberately NOT sent -
    see below.

    `name` can include Killstreak/Australium/"The" prefixes - stripped
    internally (see strip_variant_prefixes).
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
    # killstreaker/sheen deliberately NOT included as search params - a
    # real, confirmed case showed the resulting search page displaying
    # "unknown" for both filters (the name-string format this project
    # was sending clearly isn't what backpack.tf's search expects, and
    # the correct format - likely a numeric id, similar to particle -
    # isn't confirmed anywhere reliable). "unknown" risks actively
    # filtering to the WRONG subset rather than just being ignored, so
    # omitting these entirely is safer than sending a guessed, wrong
    # value - the search is still correctly filtered by every other
    # confirmed dimension (item/quality/spell/paint/killstreak_tier/etc).
    params["craftable"] = 1 if craftable else 0
    paint_value = paint_rgb_decimal(paint) if paint else None
    if paint_value is not None:
        params["paint"] = paint_value
    # Present at their "not applicable" values (-1, 0) rather than omitted.
    params["australium"] = 1 if australium else -1
    params["killstreak_tier"] = killstreak_tier if killstreak_tier else 0
    # Every listing this project evaluates is, by definition, for sale -
    # always tradable, unconditionally.
    params["tradable"] = 1
    if steamid:
        params["steamid"] = steamid
    return f"{CLASSIFIEDS_URL}?{urlencode(params)}"


def _filter_price_outliers(prices, floor_fraction: float = 0.3, ceiling_fraction=None):
    """
    Drops prices far below (and, if `ceiling_fraction` given, above) the
    rest of the pack before computing a reference price - same principle
    the community's bptf-autopricer project uses. Sell listings use
    floor only (guards against a troll/mistake listing dragging the
    reference down); buy orders use floor + ceiling (guards against one
    implausibly-high buy order overstating flip profit).

    Needs 3+ prices to bother (no reliable "pack" with fewer). Never
    returns empty - if everything gets filtered (extremely wide spread),
    the original unfiltered list is returned instead.
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


def _iter_price_entries(craft_block):
    """
    Yields (particle_id, entry) pairs from one Craftable/Non-Craftable
    block of backpack.tf's IGetPrices response. This block has two
    shapes depending on the item (confirmed via a real, working third-
    party script reading this API): items that can't be Unusual (Key,
    Refined Metal, ...) get a plain LIST of price entries; items that
    can be Unusual get a DICT keyed by particle id, one entry per effect.

    An earlier version only handled the dict shape, silently dropping
    the list shape entirely (including the Key's own entry) - serious
    since every metal-denominated backpack.tf listing converts to keys
    using that exact number.
    """
    if isinstance(craft_block, list):
        for entry in craft_block:
            if isinstance(entry, dict):
                yield None, entry
    elif isinstance(craft_block, dict):
        for particle_key, entries in craft_block.items():
            if not isinstance(entries, list):
                entries = [entries]
            particle_id = None
            if particle_key not in ("0", 0):
                try:
                    particle_id = int(particle_key)
                except ValueError:
                    particle_id = None
            for entry in entries:
                if isinstance(entry, dict):
                    yield particle_id, entry


class LocalListingStore:
    """
    Self-collected market data, built from the SAME websocket stream
    already being consumed for new-listing detection - NOT a call to
    backpack.tf's own API. This exists because the entire prior
    reference-price/buy-order mechanism (get_snapshot_min_other_keys,
    get_best_buy_order_keys below) was built on /classifieds/listings/
    snapshot, which backpack.tf's OWN Developer Centre changelog states
    plainly: "v1 listing APIs have been deprecated and rate limited, as
    they are slated for removal. Please use v2." - and as of the most
    recent community discussion found (June 2026), there is still no v2
    endpoint for general market search at all: "still no v2 endpoint for
    classifieds or listings, just relying on websocket events and
    collected data." This class IS that "collected data" approach - the
    only currently-viable one.

    Every incoming listing this project sees (regardless of whether it
    itself qualifies as a discount) is recorded here, keyed by an EXACT
    identity tuple built from this project's own already-validated
    parsing (see listing_identity_key below) - not from trusting an
    external endpoint's filtering. This means the exact contamination
    bugs found and fixed earlier this session (spell/paint/craftable/
    killstreak-tier listings bleeding into each other via an unreliable
    external filter) cannot happen here by construction: a listing only
    ever lands in the bucket matching precisely what was parsed for it,
    and a query only ever reads from that same exact bucket.

    Thread-safe (a plain lock) since listings are recorded from the
    asyncio event loop thread but read from worker threads via
    asyncio.to_thread(evaluate_listing, ...).
    """

    def __init__(self, max_age_seconds=3600, buy_max_age_seconds=None, max_entries_per_key=300,
                 max_total_buckets=50000):
        # OrderedDict, not a plain dict - move_to_end() in record() below
        # keeps buckets ordered LEAST-recently-updated first, so eviction
        # (when max_total_buckets is hit) always drops the coldest bucket
        # first, cheaply (O(1) both to reorder and to pop the front).
        #
        # max_total_buckets is a real, confirmed fix for a real incident:
        # max_entries_per_key only ever bounded entries WITHIN one bucket
        # - nothing bounded the number of DISTINCT buckets (identity
        # keys) this store could hold at once, and buy orders are kept
        # for up to buy_max_age_seconds (24h by default) specifically so
        # a missed delete event doesn't lose real data - meaning NOTHING
        # aged out at all for the first 24h of any fresh process start.
        # At real, confirmed volume (181,113 buy orders recorded in just
        # 80 minutes, per a live /stats reading), an unbounded number of
        # distinct buckets accumulating for hours with nothing pruning
        # them yet is exactly what took the whole process down via the
        # OOM killer. 50,000 buckets is generous for what this project
        # actually needs (every distinct item/quality/effect/attribute
        # combination genuinely worth tracking) while still bounding
        # worst-case memory to a fixed, known ceiling regardless of how
        # long the process has been running or how much of TF2's
        # marketplace it happens to see.
        self._entries = collections.OrderedDict()  # identity_key -> {listing_id: {listing_id, seller_id, price_keys, ts, intent}}
        self._max_age_seconds = max_age_seconds
        # Matches get_max_buy_price's own BUY_ORDER_SAFETY_NET_SECONDS -
        # retaining a buy order no longer than the window that could ever
        # actually use it. See that constant's own comment for the full,
        # corrected reasoning: a recorded buy order IS the live, current
        # value (kept accurate by record()/remove_listing() as real
        # update/delete events arrive), not something that goes stale on
        # its own just from time passing - this window is only a safety
        # net for a missed delete event, not the correctness mechanism.
        self._buy_max_age_seconds = buy_max_age_seconds if buy_max_age_seconds is not None else 24 * 3600
        self._max_entries_per_key = max_entries_per_key
        self._max_total_buckets = max_total_buckets
        self._lock = threading.Lock()

    def record(self, identity_key, listing_id, seller_id, price_keys, intent, timestamp=None):
        """
        Each bucket is a dict keyed by listing_id (not a list) - O(1)
        duplicate replacement instead of scanning the whole bucket on
        every call, the hottest path in this project.
        """
        if price_keys is None or price_keys <= 0:
            return
        ts = timestamp if timestamp is not None else time.time()
        with self._lock:
            is_new_bucket = identity_key not in self._entries
            if is_new_bucket:
                self._entries[identity_key] = {}
            else:
                # Touch: moves this bucket to the "most recently used"
                # end, so eviction below always drops the coldest one.
                self._entries.move_to_end(identity_key)
            bucket = self._entries[identity_key]
            bucket[listing_id] = {
                "listing_id": listing_id, "seller_id": seller_id,
                "price_keys": price_keys, "ts": ts, "intent": intent,
            }
            if len(bucket) > self._max_entries_per_key:
                # Rare (most items never see 300+ distinct fresh
                # listings) - O(n log n) trim only paid when it happens.
                oldest_first = sorted(bucket.values(), key=lambda e: e["ts"])
                for stale in oldest_first[:len(bucket) - self._max_entries_per_key]:
                    del bucket[stale["listing_id"]]
            if is_new_bucket and len(self._entries) > self._max_total_buckets:
                # LRU eviction of a whole bucket, not just entries within
                # one - see __init__'s own comment for the real incident
                # this bounds against. next(iter(...)) is the coldest
                # bucket, since move_to_end() above keeps this dict
                # ordered least-recently-touched first.
                self._entries.popitem(last=False)

    def remove_listing(self, listing_id):
        """Removes a listing from every bucket it might be in, called on
        backpack.tf's "listing-delete" event. Scans every bucket (no
        identity key to jump straight to one), but the removal itself is
        O(1) per bucket (dict pop)."""
        with self._lock:
            for bucket in self._entries.values():
                bucket.pop(listing_id, None)

    def _max_age_for(self, intent):
        return self._buy_max_age_seconds if intent == "buy" else self._max_age_seconds

    def _fresh_entries(self, identity_key, intent, exclude_listing_id=None, max_age=None):
        now = time.time()
        if max_age is None:
            max_age = self._max_age_for(intent)
        with self._lock:
            bucket = list(self._entries.get(identity_key, {}).values())
        return [
            e for e in bucket
            if e["intent"] == intent
            and e["listing_id"] != exclude_listing_id
            and (now - e["ts"]) <= max_age
        ]

    def _fresh_values(self, identity_key, intent, exclude_listing_id=None, max_age=None):
        return [e["price_keys"] for e in self._fresh_entries(identity_key, intent, exclude_listing_id, max_age)]

    def get_all_entries(self, identity_key):
        """
        Every entry (buy and sell, fresh or stale) currently in one
        bucket - the proper, locked way to inspect a bucket's raw
        contents (used by /checkitem's diagnostic display, which wants
        to show age info even for stale entries, not just fresh ones).
        A real, confirmed gap this replaces: main.py used to read
        store._entries directly for this, bypassing the lock every
        other read path in this class goes through.
        """
        with self._lock:
            return list(self._entries.get(identity_key, {}).values())

    def get_min_sell_price(self, identity_key, exclude_listing_id=None):
        """Mirrors get_snapshot_min_other_keys's old return shape:
        (min_price_in_keys, count), or (None, 0) if not enough fresh,
        trustworthy data has been collected yet for this exact item."""
        values = self._fresh_values(identity_key, "sell", exclude_listing_id)
        trustworthy = _filter_price_outliers(values, floor_fraction=0.3, ceiling_fraction=3.0)
        if not trustworthy:
            return None, 0
        return min(trustworthy), len(trustworthy)

    # How recent a buy order needs to be to count as usable for the
    # discount DECISION - deliberately short, and with NO fallback to
    # older data (see get_max_buy_price below): per direct correction,
    # a buy order can genuinely go stale within minutes (fulfilled,
    # cancelled, or repriced without this project necessarily seeing a
    # matching event in time), so using anything older risks acting on
    # a number that's no longer real. There's no live query available
    # to verify a specific buy order at decision time (the only endpoint
    # that could - backpack.tf's own classifieds snapshot - is the
    # THE actual bug behind a real, severe complaint: backpack.tf's own
    # websocket only fires "listing-update" on an actual create or CHANGE
    # (confirmed via their docs earlier this session) - there is no
    # periodic per-listing heartbeat. A buy order a bot posts once and
    # never touches again generates exactly ONE event, then nothing, for
    # as long as it stays posted (often days). Requiring one to have been
    # RE-SEEN within the last few minutes - with no fallback - meant
    # almost every real, still-active buy order aged out and became
    # unusable within minutes of being recorded, long before any genuinely
    # discounted sell listing had a chance to appear against it. That's
    # not a tuning problem to fix with a bigger number - it's the wrong
    # model: this project doesn't need "was this RE-CONFIRMED recently",
    # it needs "is this the CURRENT value, as far as the websocket stream
    # has told me" - which is a fundamentally different, event-driven
    # question already answered correctly elsewhere: record() replaces a
    # listing's price the moment an update event changes it, and
    # remove_listing() deletes it the moment a delete event arrives (see
    # both in this same class). A recorded buy order IS the live, current
    # value - continuously kept correct by that same event stream, not by
    # how recently it happened to be reconfirmed - right up until one of
    # those two events says otherwise.
    #
    # This window is therefore a SAFETY NET, not the correctness
    # mechanism: only relevant if a delete event was somehow missed
    # entirely (e.g. during a brief reconnect gap - bptf_ws.py reconnects
    # within seconds, so this gap is normally tiny). Set generously long
    # so it essentially never fires in normal operation, while still
    # eventually clearing a truly abandoned entry.
    BUY_ORDER_SAFETY_NET_SECONDS = 24 * 3600
    # A shorter window used ONLY to prefer more-recently-reconfirmed data
    # when it happens to exist, for a slightly higher-confidence read -
    # never a requirement, since falling back past it is exactly what
    # fixes the bug above.
    BUY_ORDER_HIGH_CONFIDENCE_SECONDS = 6 * 3600

    def get_max_buy_price(self, identity_key):
        """
        Mirrors get_best_buy_order_keys's old return shape. Prefers a
        value reconfirmed within BUY_ORDER_HIGH_CONFIDENCE_SECONDS if one
        exists, but - unlike an earlier, broken version of this - ALWAYS
        falls back to anything within BUY_ORDER_SAFETY_NET_SECONDS rather
        than returning "no data" just because nothing has re-triggered an
        event recently. See BUY_ORDER_SAFETY_NET_SECONDS above for why
        that distinction is the actual fix, not a tuning tweak.
        """
        recent = self._fresh_values(identity_key, "buy", max_age=self.BUY_ORDER_HIGH_CONFIDENCE_SECONDS)
        recent_trustworthy = _filter_price_outliers(recent, floor_fraction=0.3, ceiling_fraction=3.0)
        if recent_trustworthy:
            return max(recent_trustworthy), len(recent_trustworthy)

        values = self._fresh_values(identity_key, "buy", max_age=self.BUY_ORDER_SAFETY_NET_SECONDS)
        trustworthy = _filter_price_outliers(values, floor_fraction=0.3, ceiling_fraction=3.0)
        if not trustworthy:
            return None, 0
        return max(trustworthy), len(trustworthy)

    def prune_expired(self):
        """Called periodically (see main.py) to bound memory - the
        per-read freshness filtering above already ignores stale entries
        on its own, this just actually removes them so the store doesn't
        grow without bound over a long-running process. Each entry is
        checked against ITS OWN intent's freshness window (buy orders
        live much longer than sell listings - see __init__)."""
        now = time.time()
        with self._lock:
            for key in list(self._entries.keys()):
                bucket = self._entries[key]
                expired_ids = [
                    lid for lid, e in bucket.items()
                    if (now - e["ts"]) > self._max_age_for(e["intent"])
                ]
                for lid in expired_ids:
                    del bucket[lid]
                if not bucket:
                    del self._entries[key]

    def entry_count(self):
        with self._lock:
            return sum(len(b) for b in self._entries.values())

    def save_to_disk(self, path):
        """
        Snapshots the store to a JSON file (see load_from_disk and
        main.py's local_store_snapshot_loop) - so a restart doesn't wipe
        out everything the store has learned. Called periodically, not
        on every record() - a snapshot every minute or two loses at most
        that much of the newest data on a restart. Identity-key tuples
        become JSON lists here - see load_from_disk for the reverse.
        """
        try:
            with self._lock:
                serializable = [
                    {"key": list(key), "bucket": bucket}
                    for key, bucket in self._entries.items()
                ]
            # Unique per call (pid + random suffix), not a fixed
            # "path + .tmp" - two saves close together (periodic +
            # shutdown) sharing one temp filename could collide, with
            # the second's os.replace finding nothing left to rename.
            tmp_path = f"{path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(serializable, f)
            os.replace(tmp_path, path)
        except Exception:
            log.exception("Could not save local listing store to disk.")

    def load_from_disk(self, path):
        """
        Restores a previous save_to_disk snapshot, if one exists -
        called once at startup so evaluations can use recent data
        immediately instead of starting cold. Entries go through the
        normal freshness check on read, same as anything else - no
        special-casing for age here.

        Migrates old-format buckets on the fly (each bucket used to be a
        plain list, now a dict keyed by listing_id for O(1) record() -
        see record()'s own docstring) so a file saved by an older
        version still loads correctly instead of crashing every record()
        call that touches it.
        """
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                serializable = json.load(f)
            with self._lock:
                for item in serializable:
                    key = tuple(item["key"])
                    bucket = item["bucket"]
                    if isinstance(bucket, list):
                        bucket = {e["listing_id"]: e for e in bucket if "listing_id" in e}
                    self._entries[key] = bucket
                # Trims down to max_total_buckets right after loading too,
                # not just in record() going forward - a real, confirmed
                # gap: a file saved BEFORE this cap existed could hold far
                # more buckets than the cap allows (from the exact
                # unbounded-growth period that caused a real OOM kill),
                # and loading it back in would restore that same memory
                # footprint immediately, bypassing the cap entirely until
                # enough NEW buckets happened to trigger record()'s own
                # eviction. Keeps the most recently-updated buckets (by
                # each bucket's own newest entry) - a perfect LRU replay
                # of pre-restart order isn't needed for a one-time
                # startup trim, just bounding the size is.
                if len(self._entries) > self._max_total_buckets:
                    overflow = len(self._entries) - self._max_total_buckets
                    by_recency = sorted(
                        self._entries.items(),
                        key=lambda kv: max((e.get("ts", 0) for e in kv[1].values()), default=0),
                    )
                    for stale_key, _ in by_recency[:overflow]:
                        del self._entries[stale_key]
                    log.warning(
                        "Loaded local listing store had %d buckets, over the %d cap - "
                        "trimmed the %d oldest.",
                        len(self._entries) + overflow, self._max_total_buckets, overflow,
                    )
            log.info("Loaded %d local listing store entries from disk (%s).",
                      self.entry_count(), path)
        except Exception:
            log.exception("Could not load local listing store from disk - starting fresh.")


def listing_identity_key(name, quality_name, particle_id, paint_decimal, craftable,
                          spell, killstreak_tier, australium, texture=None, defindex=None,
                          killstreaker=None, sheen=None):
    """
    The exact tuple LocalListingStore keys entries by - every field is
    already extracted/validated from the raw websocket payload, so two
    listings only share a bucket when genuinely identical in every
    tracked dimension.

    texture is a cosmetic/weapon "grade" (Civilian..Elite) - a separate
    sub-quality with real value differences. Can NOT be read from the
    item's display name (some real items' names contain a different
    grade's word than their true one) - must come from the field.

    defindex (numeric schema id) is preferred over name-text matching
    when available - sidesteps name-text fragility (prefix-stripping,
    casing) that caused two confirmed matching bugs. Falls back to the
    name-based key otherwise.

    killstreaker/sheen: without these, every combo of the "same" weapon
    pooled into one bucket, letting a rare, valuable killstreaker's buy
    order match a cheap combo's sell listing - a real, confirmed bug.
    """
    name_component = defindex if defindex is not None else strip_variant_prefixes(name)
    return (
        name_component, quality_name, particle_id, paint_decimal,
        bool(craftable), spell or None, killstreak_tier or 0, bool(australium),
        texture or None, killstreaker or None, sheen or None,
    )


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
        self.local_listings = LocalListingStore()
        # Logs at most ONE raw sample of a bulk-scan entry that has no
        # resolvable particle_id, across this process's whole lifetime -
        # see fetch_and_record_all_buy_orders' own diagnostic
        # comment for why. One real sample is enough to check a field-
        # name assumption against; a warning on every one of potentially
        # thousands of scans would just spam the log for no extra value.
        self._bulk_scan_sample_logged = False

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
                craft_block = tradable.get("Craftable", tradable.get("Non-Craftable", {}))
                for particle_id, entry in _iter_price_entries(craft_block):
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

        Defensive against a malformed value (a real, confirmed case: this
        function crashed uncaught somewhere in real, high-volume traffic,
        taking down whatever loop called it - main.py's own primary
        event handler is ONE of this function's callers, not just the
        proactive scanner) - float()ing something backpack.tf sent that
        isn't actually string/number-shaped returns None (this listing's
        price genuinely can't be read) rather than raising.
        """
        if not currencies or not isinstance(currencies, dict):
            return None
        total = 0.0
        got_any = False
        try:
            if currencies.get("keys"):
                total += float(currencies["keys"])
                got_any = True
            if currencies.get("metal"):
                if not self.key_price_metal:
                    return None
                total += float(currencies["metal"]) / self.key_price_metal
                got_any = True
        except (TypeError, ValueError):
            return None
        if not got_any and currencies.get("usd"):
            return None  # caller must handle pure-usd listings separately
        return total if got_any else None

    def get_snapshot_min_other_keys(self, name: str, quality_name: str, exclude_listing_id: str,
                                     particle_id=None, craftable=True, spell=None,
                                     australium: bool = False, killstreak_tier=None, paint=None,
                                     killstreaker=None, sheen=None, paint_decimal_override=None,
                                     texture=None, defindex=None):
        """
        Returns (min_price_in_keys_among_OTHER_sell_listings,
        count_of_other_listings), or (None, 0) if not enough fresh,
        trustworthy data has been self-collected yet for this exact item.

        Reads from self.local_listings - replaced a direct call to
        backpack.tf's now-deprecated v1 listings API (confirmed via
        their Developer Centre changelog, no v2 replacement exists).
        "Live" now means "seen recently via the websocket stream" rather
        than "queried on demand" - same accuracy principle either way
        (skip rather than guess when data is thin), different source.
        """
        paint_value = paint_decimal_override if paint_decimal_override is not None else (
            paint_rgb_decimal(paint) if paint else None
        )
        key = listing_identity_key(name, quality_name, particle_id, paint_value, craftable,
                                    spell, killstreak_tier, australium, texture=texture, defindex=defindex,
                                    killstreaker=killstreaker, sheen=sheen)
        return self.local_listings.get_min_sell_price(key, exclude_listing_id=exclude_listing_id)

    def get_best_buy_order_keys(self, name: str, quality_name: str, particle_id=None, craftable=True,
                                 spell=None, australium: bool = False, killstreak_tier=None, paint=None,
                                 killstreaker=None, sheen=None, paint_decimal_override=None,
                                 texture=None, defindex=None):
        """
        Highest current self-collected BUY-intent price for this exact
        item, in keys, plus how many fresh buy-intent listings that
        reflects. Returns (None, 0) if nothing fresh has been collected.
        Not excluding any listing_id here (unlike the sell-side
        reference) since this asks "what's on offer right now", not
        about one specific listing.
        """
        paint_value = paint_decimal_override if paint_decimal_override is not None else (
            paint_rgb_decimal(paint) if paint else None
        )
        key = listing_identity_key(name, quality_name, particle_id, paint_value, craftable,
                                    spell, killstreak_tier, australium, texture=texture, defindex=defindex,
                                    killstreaker=killstreaker, sheen=sheen)
        return self.local_listings.get_max_buy_price(key)

    def fetch_and_record_all_buy_orders(self, name: str, quality_name: str) -> int:
        """
        Bulk proactive scan: ONE snapshot API request for this item at
        this quality (buy intent) - for Unusual, covers EVERY particle
        effect at once; for any other quality, just that one quality's
        buy orders (no particle dimension to split by). Not scoped to a
        single effect the way fetch_live_buy_order_keys is. Records
        every listing directly into LocalListingStore, so a real sell
        listing later likely finds a fresh buy order already waiting -
        no live-query wait needed. Returns how many listings recorded.

        Covers every watched quality, not just Unusual - a real, direct
        point: with fewer known items than worker accounts, idle workers
        were sleeping instead of doing useful work, when they could
        equally well keep other qualities' buy orders fresh too.

        Queries by "sku" - confirmed directly on backpack.tf's own
        forums (a trusted community member correcting another user's
        exact same "'sku' param is required" / bare-acknowledgement-
        only-response confusion) that despite the name, THIS endpoint's
        "sku" is just the item's plain display name ("The Head Prize"),
        NOT the defindex;quality format the wider tf2-sku community
        standard uses - a real, confirmed correction to two of this
        project's own previous, wrong attempts at this same endpoint.

        Missing texture (unlike handle_bptf_event's fuller extraction) -
        a supplementary cache-warming pass, not the final decision path.
        craftable/killstreaker/sheen ARE derived the same careful way as
        the main path, since buy orders here can sit unrefreshed for
        hours before their own next real event.
        """
        try:
            params = {
                "sku": strip_variant_prefixes(name), "appid": 440,
                "quality": QUALITY_NAME_TO_ID.get(quality_name), "intent": "buy",
            }
            resp = _get_with_retry(self.session, SNAPSHOT_URL, params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.warning("Bulk buy-order scan failed for %s (%s).", name, quality_name)
            return 0

        listings = data.get("listings") if isinstance(data, dict) else None
        if not isinstance(listings, list):
            log.warning(
                "Bulk scan response for %s (%s) had an unexpected shape - raw (truncated): %r",
                name, quality_name, str(data)[:500],
            )
            return 0

        recorded = 0
        sample_logged = False
        for i, entry in enumerate(listings):
            if not isinstance(entry, dict) or entry.get("intent") != "buy":
                continue
            try:
                item = entry.get("item")
                if not isinstance(item, dict):
                    item = {}
                particle_obj = item.get("particle")
                if not isinstance(particle_obj, dict):
                    particle_obj = {}
                particle_id = particle_obj.get("id")
                # particle_id is only EXPECTED for Unusual - for every
                # other quality, no particle is the normal case, not a
                # reason to skip (a real, confirmed bug fixed while
                # generalizing this past Unusual-only: the old
                # unconditional "skip if no particle" check would have
                # silently recorded nothing at all for every non-Unusual
                # quality).
                if quality_name == "Unusual" and particle_id is None:
                    if not sample_logged and not self._bulk_scan_sample_logged:
                        # Diagnostic sample - a real, confirmed case: the
                        # response CAN be a valid list (no "unexpected
                        # shape" warning fires) while every entry still
                        # fails to record anything, if this project's
                        # field-name assumptions about EACH entry's own
                        # structure (not the outer response) are wrong.
                        # One real, raw sample makes that visible instead
                        # of silently recording zero forever with no
                        # error anywhere.
                        log.warning(
                            "DIAGNOSTIC SAMPLE (bulk scan entry with no resolvable particle_id) "
                            "for %s - raw entry: %r", name, entry,
                        )
                        sample_logged = True
                        self._bulk_scan_sample_logged = True
                    continue
                price_keys = self.currencies_to_keys(entry.get("currencies") or {})
                if price_keys is None or price_keys <= 0:
                    continue
                # Falls back to an index-based id (never just
                # seller+particle, which - for any quality other than
                # Unusual - would ALWAYS be seller+None, colliding
                # between different real listings from the same seller
                # within one response) only if the response entry itself
                # has no id at all.
                listing_id = entry.get("id") or entry.get("listing_id") or f"bulk-{i}"
                user_obj = entry.get("user")
                if not isinstance(user_obj, dict):
                    user_obj = {}
                seller = user_obj.get("id") or (entry.get("steamid")) or "unknown"
                # Derived from name text, NOT the raw item.craftable
                # field - same confirmed-unreliable field, same fix, as
                # main.py's own handle_bptf_event. Matters MORE here than
                # it first looks: this populates the buy-order side of
                # the store, which can sit unrefreshed for hours until
                # that specific buy order's own next websocket event -
                # "a later real event corrects it" doesn't hold for a
                # long time if that later event may not come for hours.
                entry_name = item.get("name") or name
                craftable = not entry_name.startswith("Non-Craftable ")
                killstreaker_obj = item.get("killstreaker")
                if not isinstance(killstreaker_obj, dict):
                    killstreaker_obj = {}
                sheen_obj = item.get("sheen")
                if not isinstance(sheen_obj, dict):
                    sheen_obj = {}
                # Actual quality from THIS entry, defaulting to what was
                # QUERIED (never hardcoded "Unusual") - a real, confirmed
                # case: a "Strange Unusual" item (quality Strange, but
                # still carrying a particle effect - a real TF2
                # combination this project never accounted for before)
                # was coming back from a quality-filtered query anyway,
                # and blindly labeling every result with the queried
                # quality pooled it with a genuinely different item
                # sharing the same particle_id - wrong buy order matched
                # to the wrong item.
                entry_quality_obj = item.get("quality")
                if not isinstance(entry_quality_obj, dict):
                    entry_quality_obj = {}
                entry_quality = entry_quality_obj.get("name") or quality_name
                defindex = item.get("defindex")
                # Spell - a real, confirmed gap: this call hardcoded
                # None here regardless of what the entry actually
                # carried, unlike every other per-entry field this same
                # function already extracts correctly (killstreaker,
                # sheen, quality, defindex). That meant EVERY buy order
                # this scanner records - spelled or not - filed into the
                # SAME "no spell" bucket, silently pooling a spelled
                # variant's (often far higher) buy order price into a
                # plain item's comparison. Same extraction pattern as
                # main.py's own handle_bptf_event: first spell only.
                entry_spells = [
                    s.get("name") for s in (item.get("spells") or [])
                    if isinstance(s, dict) and s.get("name")
                ]
                entry_spell = entry_spells[0] if entry_spells else None
                key = listing_identity_key(
                    name, entry_quality, particle_id, None, craftable,
                    entry_spell, item.get("killstreakTier") or 0, name.startswith("Australium "),
                    killstreaker=killstreaker_obj.get("name"), sheen=sheen_obj.get("name"),
                    defindex=defindex,
                )
                self.local_listings.record(key, str(listing_id), str(seller), price_keys, "buy")
                recorded += 1
            except Exception:
                # Per-entry, not per-scan: a real, confirmed case - this
                # loop crashed uncaught (past this point, outside the
                # request-level try/except above) for reasons never
                # pinned down against real backpack.tf data, taking the
                # WHOLE scan down with it on every single occurrence,
                # for many different items, at real volume (1554 errors
                # in 3 hours). One malformed entry (an unexpected type in
                # currencies, a missing nested field some other code path
                # doesn't guard) should cost that ONE entry, never the
                # rest of a real, mostly-good response.
                log.exception("Bulk scan entry failed for %s (%s), entry %d - skipping just this one.",
                               name, quality_name, i)
                continue
        return recorded

    def fetch_live_buy_order_keys(self, name: str, quality_name: str, particle_id=None,
                                   craftable=True, australium: bool = False, killstreak_tier=None,
                                   spell=None):
        """
        LIVE query to the snapshot API for this item's current best buy
        order - a deliberate, narrow exception to this project's "local
        store only" rule (the endpoint is backpack.tf's own confirmed-
        deprecated v1 API, rate-limited to 6 req/60s on a free key).
        Only called for PRIORITY items (see matcher.py's evaluate_
        listing) - matches a real production autopricer's own trade-off
        (jack-richards/bptf-autopricer's config has
        "alwaysQuerySnapshotAPI": true, but only for its own small,
        curated item list, never the whole marketplace).

        Queries by "sku" - confirmed directly on backpack.tf's own
        forums that despite the name, this endpoint's "sku" is just the
        item's plain display name, NOT the defindex;quality format the
        wider tf2-sku community standard uses (see fetch_and_record_all_
        unusual_buy_orders' own docstring for the same correction, and
        the two wrong attempts at this endpoint it replaces). Only
        item/quality are sent - particle/killstreak/craftable/
        australium/spell aren't independently confirmed as filterable
        on this endpoint at all, so those are filtered CLIENT-SIDE from
        the response instead of guessed at in the query string. `spell`
        specifically was missing entirely until a real, confirmed
        report: a spell-less sell listing got "verified" against a
        rare, high-value spell variant's (Voices from Below) buy order
        this way, since every spell variant (plus the un-spelled item)
        was being pooled together with no spell dimension checked at
        all.

        Returns (buy_keys, count) matching get_best_buy_order_keys' own
        shape, or (None, 0) on failure - never worse than not trying,
        the local-store value (if any) remains the fallback either way.
        """
        try:
            params = {
                "sku": strip_variant_prefixes(name), "appid": 440,
                "quality": QUALITY_NAME_TO_ID.get(quality_name), "intent": "buy",
            }
            # timeout kept short (5s, not the usual 15-20s elsewhere in
            # this file) - a real, confirmed case: this call runs inside
            # evaluate_listing, itself dispatched via asyncio.to_thread's
            # small, SHARED default thread pool - a slow/hanging request
            # here ties up one of that pool's few threads for the full
            # duration, and enough of these piling up (Unusual items are
            # unconditionally "priority", so this can fire often) was
            # observed starving everything else sharing that same pool,
            # including Telegram responsiveness. A live buy order that
            # takes this long to answer isn't worth blocking a thread
            # for anyway - the local-store value remains the fallback.
            resp = _get_with_retry(self.session, SNAPSHOT_URL, params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.warning("Live snapshot buy-order query failed for %s - falling back to local store only.", name)
            return None, 0

        listings = data.get("listings") if isinstance(data, dict) else None
        if not isinstance(listings, list):
            # Diagnostic sample, not a guess acted on silently - this
            # endpoint's exact response shape for a LIVE, current call
            # isn't independently re-confirmed in this codebase (the
            # function that USED to call it was removed as dead code
            # earlier this session, before this narrower use existed) -
            # if this shape assumption is wrong, this makes it visible
            # in the logs immediately rather than silently mispricing.
            log.warning(
                "Live snapshot response for %s had an unexpected shape - raw (truncated): %r",
                name, str(data)[:500],
            )
            return None, 0

        prices = []
        for entry in listings:
            if not isinstance(entry, dict) or entry.get("intent") != "buy":
                continue
            try:
                item = safe_dict(entry.get("item"))
                # Quality must match what was actually requested - a real,
                # confirmed case: a "Strange Unusual" item (Strange quality,
                # but still carrying a particle effect) came back from a
                # quality-filtered query anyway, which would have silently
                # let a different item's price into this max() otherwise.
                entry_quality = safe_dict(item.get("quality")).get("name")
                if entry_quality and entry_quality != quality_name:
                    continue
                # particle/craftable/killstreak_tier/australium are now
                # filtered HERE, client-side, instead of as query params -
                # the query only sends the confirmed sku=defindex;quality
                # part (see this function's own docstring for why).
                if particle_id is not None:
                    entry_particle = safe_dict(item.get("particle")).get("id")
                    if entry_particle != particle_id:
                        continue
                entry_name = item.get("name") or ""
                entry_craftable = not entry_name.startswith("Non-Craftable ")
                if bool(craftable) != entry_craftable:
                    continue
                if bool(australium) != entry_name.startswith("Australium "):
                    continue
                entry_ks_tier = item.get("killstreakTier") or 0
                if (killstreak_tier or 0) != entry_ks_tier:
                    continue
                # Spell - a real, confirmed gap: this function's own
                # signature never accepted a spell parameter at all,
                # meaning it silently pooled EVERY spell variant of an
                # item together (plus the un-spelled one), including
                # rare, high-value spells like Voices from Below - a
                # spell-less sell listing could get "verified" against a
                # spelled item's buy order this way, a real, confirmed
                # case of exactly that. Same convention as the local
                # store's own identity key (see listing_identity_key):
                # entries are compared by their FIRST spell only, since
                # that's the one dimension TF2 items are actually priced
                # by even when a second, cosmetic-only spell is present.
                entry_spells = [
                    s.get("name") for s in (item.get("spells") or [])
                    if isinstance(s, dict) and s.get("name")
                ]
                entry_spell = entry_spells[0] if entry_spells else None
                if (spell or None) != (entry_spell or None):
                    continue
                price_keys = self.currencies_to_keys(entry.get("currencies") or {})
                if price_keys is not None and price_keys > 0:
                    prices.append(price_keys)
            except Exception:
                # Per-entry, not per-query - same reasoning as the bulk
                # scanner's own identical protection: one malformed entry
                # shouldn't cost every other entry in an otherwise-good
                # response, and this function previously had NO such
                # protection at all despite running for every priority
                # item evaluation - a crash here was just as exposed as
                # the bulk scanner's was before that one got this fix.
                log.exception("Live snapshot entry failed for %s - skipping just this one.", name)
                continue
        if not prices:
            return None, 0
        trustworthy = _filter_price_outliers(prices, floor_fraction=0.3, ceiling_fraction=3.0)
        return max(trustworthy), len(trustworthy)

    # -- liquidity (best-effort, via price-suggestion recency) ------------

    def get_liquidity_days_since_update(self, name: str, quality_name: str, particle_id=None,
                                         craftable=True):
        """
        Days since this item's price was last revised by the community
        (via IGetPriceHistory's most recent entry), or None if
        unavailable. backpack.tf's real sale-confirmation history is a
        paid Premium feature, not exposed by the free API - this uses
        price-suggestion recency as the closest free proxy for "is this
        actively traded" (not verified sales; a long-untouched
        suggestion isn't proof nobody's buying, just a signal).
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
        Shared fetch for IGetPriceHistory/v1, used by get_average_price_
        keys() and get_liquidity_days_since_update() - cached briefly so
        one qualifying deal (wanting both) triggers one request, not two.
        Returns the raw `history` list, or None on failure/empty.

        The filter param name for Unusual particle effect on this
        specific endpoint isn't confirmed the way it is on the snapshot
        endpoint - "priceindex" is the best guess from this API family's
        naming convention. If wrong, the request just comes back empty
        and callers skip the feature, never a silently wrong number.
        """
        cache_key = (name, quality_name, particle_id, craftable)
        cached = self._history_cache.get(cache_key)
        now = time.time()
        if cached and (now - cached[0]) < self.snapshot_cache_seconds:
            return cached[1]
        if is_rate_limited():
            return None

        params = {
            # "key" deliberately NOT set here - _get_with_retry injects
            # it per-request from whichever account the pool hands out.
            "item": name,
            "appid": 440,
            "quality": quality_name,
            "craftable": 1 if craftable else 0,
            "tradable": 1,
        }
        if particle_id is not None:
            params["priceindex"] = particle_id

        resp = None
        try:
            resp = _get_with_retry(self.session, HISTORY_URL, params)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            # Same reasoning as the snapshot endpoint's own fix above -
            # explicit status code and body snippet, not left to
            # whatever survives truncation in the /errors display.
            status = resp.status_code if resp is not None else "no response (connection-level failure)"
            body_snippet = resp.text[:200] if resp is not None else ""
            log.warning(
                "backpack.tf price history request failed for %s (%s) - HTTP status: %s, body: %r",
                name, quality_name, status, body_snippet,
            )
            return None

        response = data.get("response", {}) if isinstance(data, dict) else {}
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
