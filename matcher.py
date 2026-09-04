"""
Deal-matching logic for backpack.tf listings.

bptf_ws.py produces a NormalizedListing for each event it sees;
evaluate_listing() below is the single place that decides whether it's a
deal worth alerting on, and assembles everything the notification needs.
Pure functions (no network calls except through the passed-in `bptf`
client) - easy to reason about and unit-test.

The discount decision runs entirely on backpack.tf's live buy order for
the exact item (self-collected via the websocket stream into
LocalListingStore, see bptf_client.py) - never on the community-
suggested price (IGetPrices), which real cases showed disagreeing with
what was actually for sale. Other live sell listings, when they exist,
are shown as informational "Было: X" context only, never required.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from bptf_client import (
    build_classifieds_url,
    name_for_killstreak_tier,
    paint_rgb_decimal,
    strip_killstreak_prefix,
    strip_quality_prefix,
    team_color_paint_decimals,
)

log = logging.getLogger("matcher")


@dataclass
class NormalizedListing:
    source: str                  # "mannco.store" or "backpack.tf"
    listing_id: str              # unique id, used for dedup and for excluding self from snapshots
    name: str                    # full display name (may or may not include quality prefix)
    quality: str
    item_type: Optional[str] = None      # e.g. "War Paint" - for exclusion filtering
    category: str = "other"              # "hat" / "weapon" / "other" - for /addcategory /removecategory
    particle_id: Optional[int] = None
    particle_name: Optional[str] = None
    craftable: bool = True
    price_keys: Optional[float] = None
    price_usd: Optional[float] = None
    link: Optional[str] = None
    extra_excluded_hint: bool = False    # set True for things like War Paint skins detected structurally
    texture: Optional[str] = None        # item "grade" for decorated weapons/certain cosmetics (Civilian..Elite) - see listing_identity_key's own docstring in bptf_client.py
    defindex: Optional[int] = None       # base item type's own numeric schema id - preferred identity anchor over name text when available, see listing_identity_key's own docstring
    spells: List[str] = field(default_factory=list)     # Halloween spell names, e.g. "Voices from Below"
    strange_parts: List[str] = field(default_factory=list)  # e.g. "Strange Part: Domination Kills"
    paint: Optional[str] = None                          # paint-can colour name, if painted
    paint_decimal_hint: Optional[int] = None              # exact RGB decimal, when the source gives one directly (skips RED/BLU guessing for team-coloured paints)
    seller_steamid: Optional[str] = None                 # narrows the backpack.tf search link to just this seller
    killstreak_tier: Optional[int] = None                # narrows the backpack.tf search link further
    killstreaker: Optional[str] = None                    # Professional Killstreak only - the eye-particle effect
    sheen: Optional[str] = None                           # Specialized/Professional Killstreak - the kill-flash colour
    seller_note: Optional[str] = None                     # the seller's own comment on the listing, if any


def clean_display_name(listing: NormalizedListing) -> str:
    """
    In-game name with quality shown exactly once, regardless of whether
    the source's raw `name` already included the quality prefix.
    Australium weapons are always Strange quality, but nobody calls one
    "Strange Australium Rocket Launcher" - left without the prefix.
    """
    base = strip_quality_prefix(listing.name, listing.quality)
    if base.startswith("Australium "):
        return base
    if listing.quality and listing.quality != "Unique":
        return f"{listing.quality} {base}"
    return base


def detect_special_variant(name: str):
    """
    Flags well-known weapon variants (Botkiller, Festive) that carry a
    value premium but aren't their own quality/category - informational
    context only, not a filterable toggle. Confirmed against backpack.tf's
    own pricelist category names. Festive (a built-in holiday skin) is
    distinct from "Festivized" (an added attribute) - only the former is
    detected here, by Valve's consistent name prefix.
    """
    if "Botkiller" in name:
        return "Botkiller"
    if name.startswith("Festive "):
        return "Festive"
    return None


# Each Halloween Spell only ever applies to ONE item category (weapon or
# cosmetic), confirmed against the official wiki + community spell
# guides - filtered here the same way paint/killstreak are filtered by
# slot in main.py.
WEAPON_ONLY_SPELLS = {
    "Exorcism",
    "Halloween Fire", "Spectral Flame",          # same spell, pre/post Tough Break name
    "Pumpkin Bombs", "Squash Rockets", "Squash Rocket", "Gourd Grenades", "Sentry Quad-Pumpkins",
}
COSMETIC_ONLY_SPELLS = {
    # Footprint spells
    "Team Spirit Footprints", "Rotten Orange Footprints", "Headless Horseshoes",
    "Violent Violet Footprints", "Gangreen Footprints", "Corpse Gray Footprints", "Bruised Purple Footprints",
    # Paint (gradient recolour) spells
    "Die Job", "Chromatic Corruption", "Sinister Staining", "Putrescent Pigmentation", "Spectral Spectrum",
    # Voice spells - generic trading name plus the class-specific names items are labelled with
    "Voices from Below",
    "Scout's Spectral Snarl", "Soldier's Booming Bark", "Pyro's Muffled Moan",
    "Demoman's Cadaverous Croak", "Heavy's Bottomless Bass", "Engineer's Gravelly Growl",
    "Medic's Blood-Curdling Bellow", "Sniper's Deep Down Under Drawl", "Spy's Creepy Croon",
}


def filter_spells_for_category(spells, category: str):
    """Drops any spell that's confirmed impossible for this item's
    category (see WEAPON_ONLY_SPELLS / COSMETIC_ONLY_SPELLS above).
    Unrecognised spell names are kept as-is - only known-incompatible
    combinations are removed, never guessed at."""
    if not spells:
        return spells
    return [
        s for s in spells
        if not (s in WEAPON_ONLY_SPELLS and category != "weapon")
        and not (s in COSMETIC_ONLY_SPELLS and category != "cosmetic")
    ]


def _get_reference_price_keys(bptf, name, quality_name, particle_id, craftable, spell, australium,
                               killstreak_tier, min_other_listings, exclude_listing_id="",
                               paint=None, killstreaker=None, sheen=None, paint_decimal_override=None,
                               texture=None, defindex=None):
    """
    Live-buy-order-ONLY reference price lookup. Used both for the item
    being evaluated, and (in check_killstreak_tier_pricing below) for its
    OTHER killstreak tiers. Returns a float in keys, or None if not
    enough live data exists.

    Never falls back to the community-suggested price (IGetPrices) - real
    cases showed that number disagreeing with what was actually for sale.
    If there isn't enough live data, the honest answer is "skip this one
    right now", not "use a number that isn't from an actual listing".
    Only ever backpack.tf data - no other marketplace factors in here.
    """
    ref_keys, other_count = bptf.get_snapshot_min_other_keys(
        name, quality_name, exclude_listing_id=exclude_listing_id,
        particle_id=particle_id, craftable=craftable, spell=spell,
        australium=australium, killstreak_tier=killstreak_tier,
        paint=paint, killstreaker=killstreaker, sheen=sheen,
        paint_decimal_override=paint_decimal_override, texture=texture, defindex=defindex,
    )

    if ref_keys is not None and other_count >= min_other_listings:
        log.info(
            "Reference for %s: %.2f keys from self-collected live data (%d other listing(s)).",
            name, ref_keys, other_count,
        )
        return ref_keys

    log.info(
        "Skipping %s - only %s listing(s) self-collected so far (needed >= %d) for "
        "this exact item. Not a stale/community price - genuinely no comparable live "
        "data collected yet.",
        name, other_count if ref_keys is not None else 0, min_other_listings,
    )
    return None



def check_killstreak_tier_pricing(bptf, listing: "NormalizedListing", lookup_name: str,
                                   ref_keys: float, cfg: dict) -> bool:
    """
    "Price boost" sanity check: a higher killstreak tier (Killstreak <
    Specialized < Professional) should never be priced BELOW a lower one
    for the same weapon - if it is, that's a bugged/unreliable number for
    the whole item's pricing, not a genuine bargain (and typically harder
    to resell at the implied price, since the market has settled lower).

    Checks the full ladder both directions, including plain (tier 0)
    weapons against tiers 1-3, not just the tier actually being
    evaluated - a one-directional, self-only check missed the case where
    a HIGHER tier being cheap meant the item's whole pricing was
    unreliable, not just that one listing.

    Only applies to weapons. Returns True if every checked pair is
    consistent (or couldn't be checked - missing data isn't an anomaly),
    False if any pair is confirmed backwards.
    """
    if listing.category != "weapon":
        return True

    tier = listing.killstreak_tier or 0

    # Checks ONLY against tier 0 (plain) - the comparison that catches
    # the main real-world bug ("Professional priced below plain"), at 1
    # extra request instead of 3 for the full ladder. Tier-0 listings
    # skip this check entirely (nothing lower to compare against) -
    # they're also the most common weapon listings, so this keeps the
    # cost off the majority case too.
    if tier == 0:
        return True

    base_name = strip_killstreak_prefix(lookup_name)
    is_australium = base_name.startswith("Australium ")
    tier0_name = name_for_killstreak_tier(base_name, 0)
    tier0_ref = _get_reference_price_keys(
        bptf, tier0_name, listing.quality, listing.particle_id, listing.craftable,
        spell=None, australium=is_australium, killstreak_tier=0,
        min_other_listings=cfg["min_other_listings"],
        texture=listing.texture, defindex=listing.defindex,
    )
    if tier0_ref is not None and tier0_ref > ref_keys:
        log.info(
            "Suppressing %s (evaluated at tier %d, %.2f keys) - plain (tier 0) costs "
            "%.2f keys, more than this strictly-more-featured tier; the pricing for this "
            "item looks unreliable right now, not just whichever tier triggered the alert.",
            lookup_name, tier, ref_keys, tier0_ref,
        )
        return False
    return True


def is_watched(listing: NormalizedListing, cfg: dict) -> bool:
    # "Australium only" is ADDITIVE, not exclusive - a real, direct
    # correction after an earlier version got this backwards (it made
    # australium_only require watched_qualities/watched_categories to
    # ALSO separately include Strange/weapon, and reject everything else
    # once on). The actual intent: pressing this button should surface
    # Australium weapons WITHOUT needing to separately enable Strange
    # quality or the weapon category - and everything else already being
    # watched via watched_qualities/watched_categories must keep working
    # completely unaffected, never suppressed by this toggle being on.
    is_australium_weapon = listing.category == "weapon" and (listing.name or "").startswith("Australium ")
    covered_by_australium_toggle = cfg.get("australium_only") and is_australium_weapon

    if not covered_by_australium_toggle:
        # Unusual is unconditionally watched, same as main.py's own
        # early-reject check - never gated by watched_qualities.
        if listing.quality != "Unusual" and listing.quality not in cfg["watched_qualities"]:
            return False
        if listing.category not in cfg.get("watched_categories", ["weapon", "cosmetic", "taunt", "killstreak_kit", "other"]):
            return False
    if listing.extra_excluded_hint:
        return False
    item_type = (listing.item_type or "")
    name = listing.name or ""
    for excluded in cfg["excluded_types"]:
        if excluded.lower() in item_type.lower() or excluded.lower() in name.lower():
            return False
    return True


def evaluate_listing(listing: NormalizedListing, bptf, cfg: dict, stats=None):
    """
    Returns a deal dict if this listing qualifies, otherwise None.
    `stats` (a collections.Counter, typically main.py's self.stats) is
    optional and purely diagnostic - when given, each rejection reason
    gets its own counter (via reject() below) instead of collapsing into
    one generic "rejected" bucket, so /stats can distinguish "genuinely
    not a deal" from "no comparable data yet" and similar.
    """
    _STATS_SOURCE_PREFIX = {
        "backpack.tf": "bptf",
    }

    def reject(reason):
        if stats is not None:
            prefix = _STATS_SOURCE_PREFIX.get(listing.source, listing.source)
            stats[f"{prefix}_rejected_{reason}"] += 1
        return None

    if not is_watched(listing, cfg):
        return None
    if listing.price_keys is None or listing.price_keys < cfg["min_price_keys"]:
        return reject("min_price")
    max_price = cfg.get("max_price_keys")
    if max_price is not None and listing.price_keys > max_price:
        return reject("max_price")

    # Killstreak Kits/Fabricators are skipped entirely, before any API
    # calls - backpack.tf needs a special "find recipes" flow for these,
    # not a normal item= search, and snapshot requests for them
    # repeatedly failed. Skipped outright until there's a working way to
    # price Kits specifically.
    if listing.category == "killstreak_kit":
        return reject("kit_category")

    # Unusuals need a resolved particle id to compare like-for-like -
    # EXCEPT "Unusualifier" tools, which GRANT an effect when used
    # rather than wearing one, so they legitimately have no particle
    # data. Compared by name+quality alone, like any non-Unusual item.
    is_unusualifier = "Unusualifier" in listing.name
    if listing.quality == "Unusual" and listing.particle_id is None and not is_unusualifier:
        log.warning(
            "Skipping Unusual %s (%s) - could not resolve a particle id for effect %r. "
            "If this keeps happening, the effect-name lookup for this source may need a fix.",
            listing.name, listing.source, listing.particle_name,
        )
        return reject("no_particle_id")

    lookup_name = strip_quality_prefix(listing.name, listing.quality)

    # A painted item whose exact colour can't be resolved to RGB must
    # NOT proceed with an unfiltered comparison - the paint param would
    # silently be omitted, comparing against an undifferentiated pool
    # (every colour, plus unpainted) instead of the same colour
    # specifically, which once flooded alerts with false "discounts"
    # while also burning enough extra API calls to push backpack.tf's
    # rate limit to its ceiling. Team-coloured paints (Team Spirit and 6
    # others, see PAINT_NAME_TO_RGB) get a separate path below since
    # they're common enough to be worth handling; anything else unmapped
    # is still skipped.
    team_color_decimals = team_color_paint_decimals(listing.paint) if listing.paint else None
    if listing.paint and team_color_decimals is None and paint_rgb_decimal(listing.paint) is None:
        log.warning(
            "Skipping %s (%s) - paint %r has no known RGB value, so it can't be filtered "
            "for in comparisons; proceeding without the filter risks comparing against the "
            "wrong (unfiltered) pool.",
            listing.name, listing.source, listing.paint,
        )
        return reject("unmapped_paint")

    spells = filter_spells_for_category(listing.spells, listing.category)
    # backpack.tf's search only takes one spell value - a real, confirmed
    # search the user built by hand includes `spell=<name>` and it's this
    # param specifically that was missing before, so an item's search/
    # reference lookups are only as precise as the first spell it has
    # (the overwhelming majority of spelled items have exactly one).
    primary_spell = spells[0] if spells else None
    australium = lookup_name.startswith("Australium ")

    exclude_id = listing.listing_id if listing.source == "backpack.tf" else ""

    # A single painted item has ONE fixed colour, but which of RED/BLU
    # isn't knowable from the paint name alone - try RED first, then BLU
    # only if RED found nothing. Trying the wrong one first just costs
    # one extra request (backpack.tf's paint= filter returns no matches
    # for the wrong colour, never wrong data), never a bad comparison.
    # Whichever decimal succeeds is reused for the buy-order lookup
    # below too, so both describe the same colour.
    winning_paint_decimal = None
    if listing.paint_decimal_hint is not None:
        # The source told us exactly which colour this listing is
        # (confirmed real for mannco.store - see mannco_paint_decimal_
        # hint) - go straight to it, no need to guess-and-try RED then
        # BLU the way team_color_decimals below does for sources that
        # don't give this directly.
        ref_keys = _get_reference_price_keys(
            bptf, lookup_name, listing.quality, listing.particle_id, listing.craftable,
            spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
            min_other_listings=cfg["min_other_listings"], exclude_listing_id=exclude_id,
            paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
            texture=listing.texture, defindex=listing.defindex,
            paint_decimal_override=listing.paint_decimal_hint,
        )
        if ref_keys is not None:
            winning_paint_decimal = listing.paint_decimal_hint
    elif team_color_decimals:
        for candidate in team_color_decimals:
            ref_keys = _get_reference_price_keys(
                bptf, lookup_name, listing.quality, listing.particle_id, listing.craftable,
                spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
                min_other_listings=cfg["min_other_listings"], exclude_listing_id=exclude_id,
                paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
                texture=listing.texture, defindex=listing.defindex,
                paint_decimal_override=candidate,
            )
            if ref_keys is not None:
                winning_paint_decimal = candidate
                break
    else:
        ref_keys = _get_reference_price_keys(
            bptf, lookup_name, listing.quality, listing.particle_id, listing.craftable,
            spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
            min_other_listings=cfg["min_other_listings"], exclude_listing_id=exclude_id,
            paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
            texture=listing.texture, defindex=listing.defindex,
        )

    if ref_keys is None or ref_keys <= 0:
        # No longer a rejection - the sell-side reference is now purely
        # informational ("Было: X") when available, not required. The
        # actual discount decision now runs entirely on the buy order
        # fetched below, per direct correction: comparing against OTHER
        # active sell listings needed at least one other exact-match
        # listing to exist at the same time, which for anything not
        # extremely popular could take a long time to happen even once -
        # a buy order is a single standing signal that, once posted,
        # doesn't need a SECOND coincidental listing to compare against.
        ref_keys = None

    buy_order_keys, buy_order_count = bptf.get_best_buy_order_keys(
        lookup_name, listing.quality, listing.particle_id, craftable=listing.craftable,
        spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
        paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
        texture=listing.texture, defindex=listing.defindex,
        paint_decimal_override=winning_paint_decimal,
    )
    if buy_order_keys is None or buy_order_keys <= 0:
        # Live-query supplement, ONLY for priority items - see
        # fetch_live_buy_order_keys' own docstring. Same priority rule
        # as the alert's own "⭐ ПРИОРИТЕТ" marker further down. Skipped
        # when killstreaker/sheen are set - this endpoint can't reliably
        # scope its query to one specific combo, which risks pooling a
        # rare, valuable combo's buy order into a cheaper one's listing.
        name_lower = listing.name.lower()
        is_priority_for_live_query = listing.quality == "Unusual" or any(
            hype_name.lower() in name_lower for hype_name in cfg.get("priority_item_names", [])
        )
        if is_priority_for_live_query and not listing.killstreaker and not listing.sheen:
            buy_order_keys, buy_order_count = bptf.fetch_live_buy_order_keys(
                lookup_name, listing.quality, listing.particle_id,
                craftable=listing.craftable, australium=australium,
                killstreak_tier=listing.killstreak_tier, spell=primary_spell,
            )
    if buy_order_keys is None or buy_order_keys <= 0:
        # No live buy order in the local store, and (for priority items)
        # the live-query supplement above also came back empty. Not
        # "this item isn't discounted" - "nothing to compare against".
        return reject("no_live_buy_order")

    # Per explicit request: compare STRICTLY against the live buy order,
    # nothing else - no sanity-check against backpack.tf's own community-
    # suggested price. An earlier version of this function DID reject a
    # buy order more than 8x the suggested price as "implausible" (a
    # real, confirmed scam-listing pattern motivated that), but the
    # suggested price itself can be stale/wrong for exactly the volatile,
    # high-demand items this project cares most about catching - and per
    # direct correction, a real, legitimate deal was rejected by this
    # same check. Removed entirely rather than tuned, per explicit
    # instruction: the buy order is the only reference that matters here.

    discount_percent = (buy_order_keys - listing.price_keys) / buy_order_keys * 100
    if discount_percent < cfg["discount_threshold_percent"]:
        return reject("discount_too_small")

    flip_profit_keys = buy_order_keys - listing.price_keys

    # Price-boost sanity check across the whole killstreak tier ladder
    # (0=plain through 3=Professional) - applies even to a plain item
    # being evaluated, not just killstreak-tiered ones: if any tier of
    # this weapon is priced backwards relative to any other, the pricing
    # data for the whole item looks unreliable right now, whichever tier
    # triggered the alert - see check_killstreak_tier_pricing(). Only
    # runs when a sell-side reference actually exists (ref_keys) - the
    # discount decision above no longer depends on it, so there's
    # nothing for this specific sanity check to validate without it.
    if ref_keys is not None and not check_killstreak_tier_pricing(bptf, listing, lookup_name, ref_keys, cfg):
        return reject("tier_inconsistency")

    # Liquidity check: skip items whose price hasn't been revised by the
    # community in a long time - a big discount on something nobody's
    # actively trading is more likely a forgotten/stale price than a real
    # OFF by default now - both this liquidity check and the average
    # price below go through backpack.tf's /IGetPriceHistory/v1, a real,
    # separate HTTP call (throttled the same as every other backpack.tf
    # request - see bptf_client.py's account pool) that was still adding
    # ~11+ seconds to an otherwise near-instant evaluation once the
    # reference-price/buy-order lookups moved to the self-collected local
    # store. Direct feedback that competing bots reply in 1-3 seconds
    # made clear this was no longer an acceptable cost for what these two
    # values actually provide: the liquidity check's own underlying
    # concern (don't trust stale comparison data) is now already covered
    # by LocalListingStore's own freshness window (entries older than
    # max_age_seconds, default 1 hour, are never used - see
    # bptf_client.py), and the average price is purely informational
    # display text, never used in the discount decision itself. Set
    # fetch_price_history_data to true in config.json to bring both
    # back, trading speed for this extra (now largely redundant) context.
    days_since_update = None
    avg_keys = None
    if cfg.get("fetch_price_history_data"):
        # find. Uses the same price-history fetch as the average below (one
        # HTTP call covers both, see bptf_client's history cache). Unknown
        # (None) fails OPEN - an item with literally no price history yet
        # isn't necessarily illiquid, just unpriced, and a fetch hiccup
        # shouldn't cost a real deal.
        days_since_update = bptf.get_liquidity_days_since_update(
            lookup_name, listing.quality, listing.particle_id, craftable=listing.craftable
        )
        if days_since_update is not None and days_since_update > cfg["max_days_since_price_update"]:
            return None

        # Average price is a nice-to-have on top of an already-qualifying
        # deal - only fetched now, not for every listing scanned.
        avg_keys = bptf.get_average_price_keys(lookup_name, listing.quality, listing.particle_id,
                                                craftable=listing.craftable)

    # Best current buy order - "could I flip this for an instant,
    # guaranteed profit". Also only fetched now, same reasoning as the
    # average price above.
    #
    # This same link doubles as a manual double-check against the live
    # market, and (after buying) a ready-made search to list/sell it
    # again. Not filtered to the seller's steamid - backpack.tf sorts by
    # price ascending, so the exact listing that triggered the alert
    # already sorts first, and dropping the filter shows the wider
    # market too.
    backpacktf_search_link = build_classifieds_url(
        lookup_name, listing.quality, listing.particle_id,
        killstreak_tier=listing.killstreak_tier,
        australium=australium, spell=primary_spell,
        paint=listing.paint, craftable=listing.craftable,
        killstreaker=listing.killstreaker, sheen=listing.sheen,
    )

    # Real item picture for the Telegram alert, straight from Valve's own
    # schema (confirmed OK to hotlink directly).
    # Priority flag - Unusuals are the most liquid and highest-margin
    # category, worth calling out so they're not lost in a busy chat;
    # cfg["priority_item_names"] adds specific well-known "hype" items on
    # top of that, by name substring (case-insensitive) - not an
    # authoritative list, just a configurable starting point.
    is_priority = listing.quality == "Unusual"
    if not is_priority:
        name_lower = listing.name.lower()
        is_priority = any(
            hype_name.lower() in name_lower for hype_name in cfg.get("priority_item_names", [])
        )

    return {
        "source": listing.source,
        "is_priority": is_priority,
        "display_name": clean_display_name(listing),
        "variant_label": detect_special_variant(listing.name),
        "particle_name": listing.particle_name,
        "killstreaker": listing.killstreaker,
        "sheen": listing.sheen,
        "spells": spells,
        "strange_parts": listing.strange_parts,
        "paint": listing.paint,
        "price_keys": listing.price_keys,
        "price_usd": listing.price_usd,
        "previous_low_keys": ref_keys,
        "average_keys": avg_keys,
        "suggested_updated_days_ago": days_since_update,
        "buy_order_keys": buy_order_keys,
        "buy_order_count": buy_order_count,
        "flip_profit_keys": flip_profit_keys,
        "days_since_price_update": days_since_update,
        "discount_percent": discount_percent,
        "link": listing.link,
        "seller_id": listing.seller_steamid or listing.listing_id,
        "seller_note": listing.seller_note,
        "backpacktf_search_link": backpacktf_search_link,
    }
