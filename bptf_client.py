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
# Lowered from an earlier 8 after a real production log showed 429s
# happening even at that level - 8 concurrent evidently wasn't
# conservative enough on its own, hence also the cooldown below.
MAX_CONCURRENT_REQUESTS = 4
_request_semaphore = threading.Semaphore(MAX_CONCURRENT_REQUESTS)

# A cap on CONCURRENT requests alone doesn't cap the SUSTAINED rate over
# time - 4 requests in flight, each finishing quickly and immediately
# replaced by the next one queued, can still add up to well more than
# backpack.tf tolerates per second, minute after minute. A real
# production log showed sustained 429s cycling through the entire
# adaptive backoff range (10s doubling up to the 300s ceiling) repeatedly
# over 90+ minutes straight - not occasional bursts, a continuous
# pattern - which is exactly what "concurrency capped but rate
# unthrottled" looks like. This enforces a minimum gap between when each
# request STARTS, smoothing the actual request rate directly instead of
# just hoping a low concurrency cap implies a low enough rate. No
# confirmed exact number for backpack.tf's real limit, so this is a
# deliberately conservative starting point - see _throttle_request_rate.
_MIN_REQUEST_INTERVAL_SECONDS = 0.4
_last_request_started_lock = threading.Lock()
_last_request_started_at = 0.0


def configure_request_pacing(max_concurrent: int, min_interval_seconds: float):
    """
    Called once at startup (see main.py) with cfg["bptf_max_concurrent_
    requests"] / cfg["bptf_min_request_interval_seconds"], so these are
    tunable from config.json without editing this file - both were
    hardcoded here originally; exposed after real, repeated rate-limit
    troubleshooting made clear these are exactly the kind of knob worth
    adjusting without a code change. Safe to call before any real
    request has been made (which is when main.py calls it) - rebuilds
    the semaphore at the new capacity, not just relabels the constant.
    """
    global MAX_CONCURRENT_REQUESTS, _request_semaphore, _MIN_REQUEST_INTERVAL_SECONDS
    MAX_CONCURRENT_REQUESTS = max_concurrent
    _request_semaphore = threading.Semaphore(max_concurrent)
    _MIN_REQUEST_INTERVAL_SECONDS = min_interval_seconds


def _throttle_request_rate():
    global _last_request_started_at
    with _last_request_started_lock:
        now = time.time()
        wait = _last_request_started_at + _MIN_REQUEST_INTERVAL_SECONDS - now
        if wait > 0:
            time.sleep(wait)
        _last_request_started_at = time.time()

# On top of capping concurrency, back off entirely for a bit once
# backpack.tf actually returns 429 - real production logs showed repeated
# 429s in quick succession right as they started, meaning concurrency
# alone doesn't prevent a burst of requests from tripping the limit and
# then immediately tripping it again on the very next one. Once that
# happens, every snapshot/history request just returns "unavailable"
# (evaluate_listing treats that as "can't evaluate this one right now"
# and skips it, never falling back to a less-reliable number) until the
# cooldown clears, instead of continuing to hammer an endpoint that's
# already said no.
#
# The cooldown itself is adaptive, not a flat 10s every time: if we get
# rate-limited AGAIN shortly after a previous cooldown already ended,
# that's evidence the wait wasn't long enough for genuinely sustained
# overload (not just a momentary burst), so it doubles (capped at 5
# minutes) rather than repeating the same too-short wait indefinitely.
# It resets back to the base duration once we go a while without hitting
# a 429 at all - an old bad patch shouldn't keep the backoff escalated
# forever once things have actually settled down.
_RATE_LIMIT_BASE_COOLDOWN_SECONDS = 10
_RATE_LIMIT_MAX_COOLDOWN_SECONDS = 300
_RATE_LIMIT_RESET_AFTER_SECONDS = 300
_rate_limited_until_lock = threading.Lock()
_rate_limited_until = 0.0
_current_cooldown_seconds = _RATE_LIMIT_BASE_COOLDOWN_SECONDS
_last_rate_limit_hit = 0.0


def _note_rate_limited():
    global _rate_limited_until, _current_cooldown_seconds, _last_rate_limit_hit
    with _rate_limited_until_lock:
        now = time.time()
        if now - _last_rate_limit_hit > _RATE_LIMIT_RESET_AFTER_SECONDS:
            _current_cooldown_seconds = _RATE_LIMIT_BASE_COOLDOWN_SECONDS
        else:
            _current_cooldown_seconds = min(
                _current_cooldown_seconds * 2, _RATE_LIMIT_MAX_COOLDOWN_SECONDS
            )
        _last_rate_limit_hit = now
        _rate_limited_until = now + _current_cooldown_seconds
        log.warning(
            "backpack.tf rate limit (429) hit - backing off for %ds this time (adaptive).",
            _current_cooldown_seconds,
        )


def _currently_rate_limited():
    with _rate_limited_until_lock:
        return time.time() < _rate_limited_until


def is_rate_limited() -> bool:
    """
    Public check for whether backpack.tf's snapshot/history endpoints are
    currently in the post-429 cooldown (see _note_rate_limited above).
    Used by matcher.py to distinguish "no live data because there
    genuinely isn't any" from "no live data because we're deliberately
    not asking right now" - the two look identical from inside
    _fetch_snapshot_prices (both just return None), but they call for
    different handling: the first is a legitimate reason to fall back to
    the (possibly stale) community price, the second is not - falling
    back during a cooldown risks trusting a stale community number for
    an item that actually has an abundance of live listings, just not
    ones we asked about right now. A real report showed exactly this:
    a "reference" of 6.90 keys shown for an item with 6+ live listings
    all sitting at 6.55 - the gap was too small for the community-price
    cross-check (which only catches wildly-off numbers) to catch.
    """
    return _currently_rate_limited()


def _get_with_retry(session, url, params, timeout=20):
    """
    Shared GET for the two backpack.tf endpoints that scale with
    evaluation volume (snapshot + price-history). Tracks 429s into the
    shared cooldown above. Retries ONCE, after a short pause, on a 5xx
    response - a real production log showed a 503 from backpack.tf's own
    infrastructure (their side being briefly overloaded/restarting, not
    something caused by our request volume or pattern the way a 429 is)
    - these tend to be momentary, so it's worth one quick retry rather
    than immediately falling back to the less-precise community price
    for what might just be a one-second blip.
    """
    with _request_semaphore:
        _throttle_request_rate()
        resp = session.get(url, params=params, timeout=timeout)
    if resp.status_code == 429:
        _note_rate_limited()
    elif resp.status_code >= 500:
        time.sleep(1.5)
        with _request_semaphore:
            _throttle_request_rate()
            resp = session.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            _note_rate_limited()
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
# Team-coloured paints (different RGB per RED/BLU) are left out - not
# because they're unmapped, but because a SINGLE RGB value genuinely
# doesn't exist for them: the colour shown depends on which team the
# CURRENT wearer is on during gameplay, not a fixed value chosen when
# the paint was applied - there's no "one correct decimal" to encode for
# an item sitting in a listing. Confirmed complete, not partial: Valve's
# own wiki states Paint Cans come in exactly 29 colours total; this
# table's 22 entries plus the 7 team-coloured ones (An Air of Debonair,
# Balaclavas Are Forever, Cream Spirit, Operator's Overalls, Team
# Spirit, The Value of Teamwork, Waterlogged Lab Coat) account for all
# 29 - there is nothing missing here to "add", the 7 are correctly
# excluded on principle (evaluate_listing skips rather than guesses at
# them - see the paint_rgb_decimal() check there), not a gap.
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

# The 7 team-coloured paints, as (RED, BLU) RGB pairs. Confirmed by a
# Steam Community reference guide cross-checked against these same
# names being independently attributed to a named contributor on
# Valve's own wiki (Paint Can page: "colors and names for Operator's
# Overalls, Waterlogged Lab Coat, Balaclavas Are Forever, An Air of
# Debonair, The Value of Teamwork, and Cream Spirit were provided by
# Mnemo"). A single PAINTED ITEM has ONE fixed colour, not a value that
# changes with whichever team currently has it equipped (confirmed by a
# real backpack.tf forum post: a user's own item "shows as only the red
# team spirit and not both") - so trying both RED and BLU as separate
# searches is safe, not a repeat of the earlier team-colour mistake:
# querying with the WRONG one of the two just returns no matching
# listings from backpack.tf's own filter (an empty, honest result), it
# can't return the WRONG item's data the way an unfiltered search did.
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
    reconstructed (see name_for_killstreak_tier).

    Does NOT strip the prefix on a Killstreak Kit/Kit Fabricator itself
    (e.g. "Professional Killstreak Medi Gun Kit Fabricator") - a real
    report showed this producing wrong reference prices, traced to
    exactly this: unlike a weapon, a Kit/Fabricator has no tier-
    independent "base item" underneath it - "Killstreak X Kit",
    "Specialized Killstreak X Kit", and "Professional Killstreak X Kit"
    are three entirely separate, independently-priced items, not one
    item with a stripped-off tier modifier. Stripping the prefix here
    searched backpack.tf for a name that doesn't exist ("Medi Gun Kit
    Fabricator" alone isn't a real item), and whatever price came back
    for that mismatched query was reference-price noise, not the real
    Fabricator's price.
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


def strip_variant_prefixes(name: str) -> str:
    """
    Strips "Non-Craftable", Killstreak-tier, and Australium prefixes on
    top of strip_quality_prefix(), e.g. "Non-Craftable Professional
    Killstreak Australium Rocket Launcher" -> "Rocket Launcher". Needed
    for the classifieds *search* family (the live snapshot API and the
    webpage link) - a real search the user built by hand and confirmed
    working uses just the bare weapon name ("Ambassador"), with
    killstreak_tier/australium/craftable as their own separate filter
    params, NOT baked into the name text; a name like "Non-Craftable
    Spine-Chilling Skull" doesn't match anything there (confirmed by a
    real report: the search came back empty, and the reference/buy-order
    numbers computed from that broken lookup were themselves wrong -
    once the name doesn't match the real item, nothing downstream that
    depends on it can be trusted either).

    "Non-Craftable " is checked first, since (best available
    understanding of Valve's naming order, not independently verified
    the way the killstreak/australium order was) it comes immediately
    after quality and before killstreak-tier/Australium - if that
    ordering assumption is ever wrong for some combination, the fix is
    almost always just reordering the checks here.

    Deliberately NOT used for IGetPrices / IGetPriceHistory (see
    get_price_keys / _fetch_price_history) - those are a different,
    older API family, independently confirmed to index Australium items
    by their full name ("Australium Rocket Launcher" is genuinely its
    own top-level entry, not "Rocket Launcher" + a flag, there). Craftable
    status there is instead read from which JSON branch the entry came
    from (Tradable.Craftable vs Tradable.Non-Craftable) - see
    _iter_price_entries - never from name text, on that endpoint either.
    """
    if name.startswith("Non-Craftable "):
        name = name[len("Non-Craftable "):]
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
    # Always present, not just when there IS a spell - confirmed directly
    # by the user testing backpack.tf's own spell filter and sharing the
    # resulting URL: "spell=None" (literally that string) is the real
    # sentinel for "must have no spell", the same convention as
    # australium=-1/killstreak_tier=0 below. Before this, an item with no
    # spell just omitted the param entirely, which doesn't tell
    # backpack.tf's search to exclude spelled listings - only including
    # spell=None does that.
    params["spell"] = spell if spell else "None"
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


def _iter_price_entries(craft_block):
    """
    Yields (particle_id, entry) pairs from one Craftable/Non-Craftable
    block of backpack.tf's IGetPrices response.

    CONFIRMED (via a real, working third-party script that reads this
    exact API) that this block has two genuinely different shapes
    depending on the item: for anything that CAN'T carry an Unusual
    effect (the Mann Co. Supply Crate Key itself, Refined Metal, and
    presumably others), it's a plain LIST of price entries directly -
    `prices['6']['Tradable']['Craftable'][0]['value']` in that script's
    own indexing. For items that CAN be Unusual, it's a DICT keyed by
    particle id instead, to hold one price entry per effect.

    An earlier version of this parser only handled the dict shape and
    used an isinstance() check to defensively skip anything else -
    which meant the list shape was silently dropped entirely, including
    the Key's own entry. That's a serious bug beyond just missing the
    Key's price for its own sake: this project converts every
    metal-denominated price (mannco.store listings priced in metal,
    backpack.tf listings in scrap/reclaimed/refined) into keys using
    that exact number, so losing it silently breaks that conversion
    project-wide, not just for the Key itself.
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
                                killstreaker=None, sheen=None, paint_decimal_override=None):
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
        if _currently_rate_limited():
            return None
        name = strip_variant_prefixes(name)
        # An explicit override (one of the two RED/BLU decimals for a
        # team-coloured paint - see team_color_paint_decimals) takes
        # priority over resolving from the name, since team-coloured
        # names have no single correct value to resolve to in the first
        # place - the caller has already picked which of the two this
        # particular attempt is trying.
        paint_value = paint_decimal_override if paint_decimal_override is not None else (
            paint_rgb_decimal(paint) if paint else None
        )
        cache_key = (name, quality_name, particle_id, intent, spell, australium, killstreak_tier, paint_value,
                     killstreaker, sheen, craftable)
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
        # Same "always present" reasoning as build_classifieds_url above -
        # spell=None (confirmed real by testing the site's own filter) is
        # the correct way to ask for "no spell", now that this is known.
        # The client-side filter a few lines below this call (checking
        # each returned listing's own spell data) is kept as a defensive
        # backup regardless - cheap insurance if this param is ever
        # ignored or behaves unexpectedly, not a sign it's not trusted.
        params["spell"] = spell if spell else "None"
        if paint_value is not None:
            params["paint"] = paint_value
        if killstreaker:
            params["killstreaker"] = killstreaker
        if sheen:
            params["sheen"] = sheen

        resp = None
        try:
            resp = _get_with_retry(self.session, SNAPSHOT_URL, params)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            # Status code (and a short body snippet) logged EXPLICITLY,
            # up front in the message text itself - not left to whatever
            # of the exception's own text happens to survive truncation
            # in the /errors display. A real report showed a run of
            # snapshot failures with NO accompanying "rate limit (429)"
            # line, meaning these weren't 429s and (given _get_with_retry
            # already retries 5xx once) very possibly weren't a plain 5xx
            # either - something else was going wrong, and there was no
            # way to tell what from the log as it stood. resp stays None
            # if the failure was a connection-level exception (no
            # response ever received) rather than a bad status code.
            status = resp.status_code if resp is not None else "no response (connection-level failure)"
            body_snippet = resp.text[:200] if resp is not None else ""
            log.exception(
                "backpack.tf snapshot request failed for %s (%s, intent=%s) - HTTP status: %s, body: %r",
                name, quality_name, intent, status, body_snippet,
            )
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

            listing_item = listing.get("item") or {}

            # Verify craftable status client-side too, not just via the
            # `craftable` request param above - same reasoning as the
            # spell check below: a real report showed buy orders for
            # CRAFTABLE Flip-Flops being used as if they applied to a
            # NON-Craftable listing, right after the same kind of gap was
            # found and fixed for spells. Rather than assume this one
            # request param is reliably honored by backpack.tf's search
            # (spell's very existence as a param didn't mean "omit it"
            # correctly excluded spelled items either), checking each
            # returned listing's own craftable flag directly is a small,
            # cheap way to be sure rather than hope.
            listing_craftable = listing_item.get("craftable")
            if listing_craftable is not None and bool(listing_craftable) != bool(craftable):
                continue

            # Same defense-in-depth reasoning as craftable/spell above,
            # now extended to killstreak tier, australium, and Unusual
            # particle - a real report showed the EXACT same wrong buy
            # order (70.41 keys) recurring for the item that originally
            # exposed the spell-contamination bug, well after that fix
            # shipped - meaning spell wasn't the only unguarded dimension
            # for that listing. Rather than assume killstreak_tier=0 /
            # australium=-1 (both have a "proper", documented sentinel
            # value, unlike spell) are actually honored by backpack.tf's
            # search just because the convention looks more official,
            # every dimension that can silently misprice an item now gets
            # the same client-side check as craftable and spell already
            # had - trust the request param less, verify what actually
            # came back.
            #
            # Checked at BOTH the listing level and nested under "item" -
            # unlike spell (confirmed at the listing level by a real
            # scraping library's own field list), there's no equally
            # direct confirmation for exactly where backpack.tf puts
            # killstreakTier/particle specifically, so this checks
            # whichever of the two locations actually has a value rather
            # than betting on one guess - cheap insurance against the
            # exact kind of wrong-nesting bug that just caused spell's
            # check to silently do nothing for who knows how long.
            listing_killstreak_tier = listing_item.get("killstreakTier")
            if listing_killstreak_tier is None:
                listing_killstreak_tier = listing.get("killstreakTier")
            wanted_tier = killstreak_tier or 0
            if listing_killstreak_tier is not None and int(listing_killstreak_tier) != wanted_tier:
                continue

            listing_name = listing_item.get("name") or listing.get("name") or ""
            listing_is_australium = listing_name.startswith("Australium ")
            if listing_name and listing_is_australium != bool(australium):
                continue

            if particle_id is not None:
                listing_particle_obj = listing_item.get("particle") or listing.get("particle") or {}
                listing_particle_id = (
                    listing_particle_obj.get("id") if isinstance(listing_particle_obj, dict)
                    else (listing_item.get("particleId") or listing_item.get("particle_id")
                          or listing.get("particleId") or listing.get("particle_id"))
                )
                if listing_particle_id is not None and int(listing_particle_id) != int(particle_id):
                    continue

            if not spell:
                # We asked for "no particular spell" (spell=None omits the
                # filter entirely) - but per backpack.tf's own forum, there
                # is no confirmed way to tell their search "must have NO
                # spell" the way australium=-1/killstreak_tier=0 mean "not
                # applicable" for those - the only documented way to
                # exclude spelled items is the next.backpack.tf interactive
                # filter UI, which (like everything else there) doesn't
                # translate to a URL param. So when evaluating a spell-less
                # item, a returned listing that DOES carry a spell has to be
                # dropped here, client-side - otherwise a spell-less item's
                # reference/buy-order price can get contaminated by a
                # spelled listing's (often much higher) price, exactly a
                # real report: a buy order that was actually for a SPELLED
                # copy got shown as if it applied to a plain one.
                # CRITICAL FIX: was reading listing_item.get("spells")
                # (nested under "item") - direct, empirical user
                # confirmation (checked backpack.tf directly: an 80-key
                # buy order existed ONLY on a spelled copy, never on a
                # plain one) proved this check was never actually
                # matching anything, letting the exact contamination it
                # was meant to prevent through the whole time. A real
                # scraping library (Preport/getBackpackTFListings) that
                # independently confirmed "details" sits at the LISTING
                # level (not nested under "item") shows the SAME thing
                # for spells/parts/sheen/killstreaker - all listing-level
                # fields, siblings of "item", not properties of it.
                listing_spells = listing.get("spells")
                if listing_spells:
                    continue
            prices.append((listing_id, price_keys))

        self._snapshot_cache[cache_key] = (now, prices)
        return prices

    def get_snapshot_min_other_keys(self, name: str, quality_name: str, exclude_listing_id: str,
                                     particle_id=None, craftable=True, spell=None,
                                     australium: bool = False, killstreak_tier=None, paint=None,
                                     killstreaker=None, sheen=None, paint_decimal_override=None):
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
                                              paint=paint, killstreaker=killstreaker, sheen=sheen,
                                              paint_decimal_override=paint_decimal_override)
        if prices is None:
            return None, 0

        others = [p for (lid, p) in prices if lid != exclude_listing_id]
        if not others:
            return None, 0
        trustworthy = _filter_price_outliers(others)
        return min(trustworthy), len(trustworthy)

    def get_best_buy_order_keys(self, name: str, quality_name: str, particle_id=None, craftable=True,
                                 spell=None, australium: bool = False, killstreak_tier=None, paint=None,
                                 killstreaker=None, sheen=None, paint_decimal_override=None):
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
                                              paint=paint, killstreaker=killstreaker, sheen=sheen,
                                              paint_decimal_override=paint_decimal_override)
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
        cache_key = (name, quality_name, particle_id, craftable)
        cached = self._history_cache.get(cache_key)
        now = time.time()
        if cached and (now - cached[0]) < self.snapshot_cache_seconds:
            return cached[1]
        if _currently_rate_limited():
            return None

        params = {
            "key": self.api_key,
            "item": name,
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
