"""
Deal-matching logic, unified across both listening sources
(mannco.store and backpack.tf itself).

Both bptf_ws.py and main.py's mannco handler produce a NormalizedListing
for whatever they see; evaluate_listing() below is the single place that
decides whether it's a deal worth alerting on, and assembles everything
the notification needs. Keeping this as pure functions (no network calls
except through the passed-in `bptf` client) makes it easy to reason about
and unit-test.

Reference price priority:
  1. backpack.tf LIVE listings for this exact item (via the classifieds
     snapshot), excluding the listing being evaluated - the actual going
     rate right now, i.e. "the lowest price before this notification".
     Requires cfg["min_other_listings"] other active listings to trust
     the number.
  2. backpack.tf's community-suggested price list (IGetPrices) - used
     only when (1) isn't available (no backpacktf_token configured, or
     just not enough live listings for that item yet).

The average price (IGetPriceHistory) is only fetched for listings that
already pass every other check, to keep API usage down.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from bptf_client import (
    build_classifieds_url,
    name_for_killstreak_tier,
    strip_killstreak_prefix,
    strip_quality_prefix,
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
    spells: List[str] = field(default_factory=list)     # Halloween spell names, e.g. "Voices from Below"
    strange_parts: List[str] = field(default_factory=list)  # e.g. "Strange Part: Domination Kills"
    paint: Optional[str] = None                          # paint-can colour name, if painted
    seller_steamid: Optional[str] = None                 # narrows the backpack.tf search link to just this seller
    killstreak_tier: Optional[int] = None                # narrows the backpack.tf search link further
    killstreaker: Optional[str] = None                    # Professional Killstreak only - the eye-particle effect
    sheen: Optional[str] = None                           # Specialized/Professional Killstreak - the kill-flash colour


def clean_display_name(listing: NormalizedListing) -> str:
    """
    The exact in-game name with quality shown exactly once (Unusual,
    Strange, ...), regardless of whether the source's raw `name` field
    happened to already include the quality prefix or not.

    Australium weapons are always Strange quality, but nobody (in-game,
    on backpack.tf, or in trader slang) calls one "Strange Australium
    Rocket Launcher" - the name is just "Australium Rocket Launcher".
    So that one case is left without a re-added prefix.
    """
    base = strip_quality_prefix(listing.name, listing.quality)
    if base.startswith("Australium "):
        return base
    if listing.quality and listing.quality != "Unique":
        return f"{listing.quality} {base}"
    return base


def detect_special_variant(name: str):
    """
    Flags well-known weapon variants that carry a real value premium but
    aren't their own quality/category - just informational context added
    to the alert, not a filterable toggle (they're still just "weapon"
    for /addcategory purposes). Both names are confirmed straight from
    backpack.tf's own pricelist category names
    (backpack.tf/pricelist/c/botkillers lists "Botkillers" and "Festive
    Weapons" as first-party categories).

    Festive vs "Festivized" are two different things in TF2 (a built-in
    holiday skin vs. an attribute added by a Festivizer tool) - this only
    detects the former, by the name prefix Valve uses consistently.
    """
    if "Botkiller" in name:
        return "Botkiller"
    if name.startswith("Festive "):
        return "Festive"
    return None


# Confirmed against the official TF2 wiki + multiple community spell
# guides: each Halloween Spell only ever applies to ONE item category,
# never both. A spell showing up on the wrong category is not possible
# in-game, so it's filtered out here the same way paint/killstreak are
# filtered by slot in main.py - upstream data saying otherwise is either
# a naming mismatch on our end or bad data, and either way should not be
# shown as fact.
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
                               paint=None, killstreaker=None, sheen=None):
    """
    Shared snapshot-first / community-pricelist-fallback reference price
    lookup. Used both for the item actually being evaluated, and (in
    check_killstreak_tier_pricing below) for its OTHER killstreak tiers,
    so both use the exact same logic to decide what "the going rate" is.
    Returns a single float in keys, or None if unavailable from either
    source.
    """
    ref_keys, other_count = bptf.get_snapshot_min_other_keys(
        name, quality_name, exclude_listing_id=exclude_listing_id,
        particle_id=particle_id, craftable=craftable, spell=spell,
        australium=australium, killstreak_tier=killstreak_tier,
        paint=paint, killstreaker=killstreaker, sheen=sheen,
    )
    if ref_keys is not None and other_count >= min_other_listings:
        return ref_keys
    return bptf.get_price_keys(name, quality_name, particle_id)


def check_killstreak_tier_pricing(bptf, listing: "NormalizedListing", lookup_name: str,
                                   ref_keys: float, cfg: dict) -> bool:
    """
    "Price boost" sanity check, e.g. for a Strange Festive Ambassador:
    mannco.store/backpack.tf list it in 4 killstreak tiers (none, Killstreak,
    Specialized, Professional). In TF2, a higher tier is never LESS
    desirable than a lower one - same weapon, plus extra kill-effects -
    so its reference price should never be lower than a lesser tier's.

    Checks the FULL ladder (all 4 tiers) against each other, not just
    "is a lower tier priced higher than the one being evaluated" - a
    one-directional version of this check only ever suppressed the
    higher, anomalously-cheap tier, but a real report pointed out the
    gap: if e.g. Professional Killstreak is priced BELOW plain for the
    same weapon, that's not evidence the Professional listing alone is
    bugged - it's evidence the market data for THIS ITEM, across every
    tier, is currently unreliable (thin trading, stale suggestions,
    whatever the cause). The plain version isn't a genuine bargain
    either just because it happens to be the tier someone's evaluating -
    it's sitting in the same unreliable pricing pool. So this now also
    runs for plain (tier 0) items, checking them against tiers 1-3, not
    only for items that themselves have a killstreak tier.

    A "cheap" tier that's actually priced below a lesser tier isn't an
    underpriced grail, it's a bugged number - and it also tends to be
    harder to resell at the price the alert implies, since the market
    has typically already settled at (or near) the lower tier's price
    instead.

    Only applies to weapons (killstreak tiers don't exist elsewhere).
    Returns True if every tier pair checked out consistent (or couldn't
    be checked - missing data isn't treated as an anomaly), False if any
    pair is confirmed priced backwards - the caller should suppress the
    alert regardless of which tier triggered it.
    """
    if listing.category != "weapon":
        return True

    tier = listing.killstreak_tier or 0
    base_name = strip_killstreak_prefix(lookup_name)
    is_australium = base_name.startswith("Australium ")

    tier_refs = {tier: ref_keys}
    for t in range(4):
        if t == tier:
            continue
        other_name = name_for_killstreak_tier(base_name, t)
        other_ref = _get_reference_price_keys(
            bptf, other_name, listing.quality, listing.particle_id, listing.craftable,
            spell=None, australium=is_australium, killstreak_tier=t,
            min_other_listings=cfg["min_other_listings"],
        )
        if other_ref is not None:
            tier_refs[t] = other_ref

    for t1, p1 in tier_refs.items():
        for t2, p2 in tier_refs.items():
            if t1 < t2 and p1 > p2:
                log.info(
                    "Suppressing %s (evaluated at tier %d, %.2f keys) - tier %d costs "
                    "%.2f keys but tier %d (strictly more features) costs less (%.2f keys); "
                    "the tier ladder for this item looks unreliable right now, not just "
                    "whichever tier happened to trigger the alert.",
                    lookup_name, tier, ref_keys, t1, p1, t2, p2,
                )
                return False
    return True


def is_watched(listing: NormalizedListing, cfg: dict) -> bool:
    if listing.quality not in cfg["watched_qualities"]:
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


def evaluate_listing(listing: NormalizedListing, bptf, cfg: dict):
    """Returns a deal dict if this listing qualifies, otherwise None."""
    if not is_watched(listing, cfg):
        return None
    if listing.price_keys is None or listing.price_keys < cfg["min_price_keys"]:
        return None

    # Unusuals need a resolved particle id to compare like-for-like. If we
    # don't have one (e.g. a mannco.store Unusual whose effect name we
    # couldn't resolve without a Steam API key), we can't safely compare -
    # skip rather than risk comparing against the wrong effect's price.
    if listing.quality == "Unusual" and listing.particle_id is None:
        return None

    lookup_name = strip_quality_prefix(listing.name, listing.quality)
    spells = filter_spells_for_category(listing.spells, listing.category)
    # backpack.tf's search only takes one spell value - a real, confirmed
    # search the user built by hand includes `spell=<name>` and it's this
    # param specifically that was missing before, so an item's search/
    # reference lookups are only as precise as the first spell it has
    # (the overwhelming majority of spelled items have exactly one).
    primary_spell = spells[0] if spells else None
    australium = lookup_name.startswith("Australium ")

    exclude_id = listing.listing_id if listing.source == "backpack.tf" else ""
    ref_keys = _get_reference_price_keys(
        bptf, lookup_name, listing.quality, listing.particle_id, listing.craftable,
        spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
        min_other_listings=cfg["min_other_listings"], exclude_listing_id=exclude_id,
        paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
    )

    if ref_keys is None or ref_keys <= 0:
        return None

    discount_percent = (ref_keys - listing.price_keys) / ref_keys * 100
    if discount_percent < cfg["discount_threshold_percent"]:
        return None

    # Price-boost sanity check across the whole killstreak tier ladder
    # (0=plain through 3=Professional) - applies even to a plain item
    # being evaluated, not just killstreak-tiered ones: if any tier of
    # this weapon is priced backwards relative to any other, the pricing
    # data for the whole item looks unreliable right now, whichever tier
    # triggered the alert - see check_killstreak_tier_pricing().
    if not check_killstreak_tier_pricing(bptf, listing, lookup_name, ref_keys, cfg):
        return None

    # Liquidity check: skip items whose price hasn't been revised by the
    # community in a long time - a big discount on something nobody's
    # actively trading is more likely a forgotten/stale price than a real
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

    # Average price is a nice-to-have on top of an already-qualifying deal
    # - only fetched now, not for every listing scanned.
    avg_keys = bptf.get_average_price_keys(lookup_name, listing.quality, listing.particle_id,
                                            craftable=listing.craftable)

    # Best current buy order - "could I flip this for an instant, guaranteed
    # profit". Also only fetched now, same reasoning as the average price.
    buy_order_keys, buy_order_count = bptf.get_best_buy_order_keys(
        lookup_name, listing.quality, listing.particle_id, craftable=listing.craftable,
        spell=primary_spell, australium=australium, killstreak_tier=listing.killstreak_tier,
        paint=listing.paint, killstreaker=listing.killstreaker, sheen=listing.sheen,
    )
    flip_profit_keys = None
    if buy_order_keys is not None:
        flip_profit_keys = buy_order_keys - listing.price_keys

    # This same link doubles as: (a) a way to manually double-check the
    # alert against the live market, and (b) after buying, a ready-made
    # search to jump straight into listing/selling it - no re-searching
    # by hand. Every attribute that does or doesn't apply to this exact
    # item is included, the same way the user's own hand-built reference
    # search does it. Not filtered to the seller's steamid - backpack.tf
    # sorts classifieds by price ascending, so a correctly-filtered search
    # already puts the exact listing that triggered the alert first, and
    # dropping the steamid filter shows the surrounding market too.
    #
    # Killstreak Kits/Fabricators are the one category this link can't
    # serve: backpack.tf doesn't allow a normal item= search for them at
    # all - the community's own guidance is to use the classifieds filter
    # modal's "Find recipes where the output item is used on this item"
    # flow instead, which has no URL-parameter equivalent. Showing a
    # normal-looking link that quietly returns nothing would be worse
    # than no link, so this case is skipped rather than guessed at.
    if listing.category == "killstreak_kit":
        backpacktf_search_link = None
    else:
        backpacktf_search_link = build_classifieds_url(
            lookup_name, listing.quality, listing.particle_id,
            killstreak_tier=listing.killstreak_tier,
            australium=australium, spell=primary_spell,
            paint=listing.paint, craftable=listing.craftable,
            killstreaker=listing.killstreaker, sheen=listing.sheen,
        )

    return {
        "source": listing.source,
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
        "buy_order_keys": buy_order_keys,
        "buy_order_count": buy_order_count,
        "flip_profit_keys": flip_profit_keys,
        "days_since_price_update": days_since_update,
        "discount_percent": discount_percent,
        "link": listing.link,
        "backpacktf_search_link": backpacktf_search_link,
    }
