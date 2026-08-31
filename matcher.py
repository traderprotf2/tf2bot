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
    is_rate_limited,
    name_for_killstreak_tier,
    paint_rgb_decimal,
    strip_killstreak_prefix,
    strip_quality_prefix,
    strip_variant_prefixes,
)

log = logging.getLogger("matcher")

# Set once by evaluate_listing the first time it finds no image mapping at
# all loaded - see the image_url block below. Module-level (not per-Watcher-
# instance) since matcher.py's functions are plain functions, not methods.
_logged_no_image_mapping = False


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
    seller_note: Optional[str] = None                     # the seller's own comment on the listing, if any


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
    Live-snapshot-ONLY reference price lookup. Used both for the item
    actually being evaluated, and (in check_killstreak_tier_pricing
    below) for its OTHER killstreak tiers, so both use the exact same
    logic to decide what "the going rate" is. Returns a single float in
    keys, or None if not enough live data is available.

    Deliberately does NOT fall back to, or cross-check against, the
    community-suggested price (backpack.tf's voted/aggregated IGetPrices
    number) - per explicit correction: the entire point of comparing
    against LIVE listings is to catch real, current underpricing, and a
    community-suggested number doesn't reflect what's actually for sale
    right now. Two real, concrete cases showed a "suggested" price being
    used instead of - or overriding - real, currently-live listings that
    told a different (truer) story, which is the opposite of what this
    project is supposed to do. If there isn't enough live data to trust,
    the honest answer is "can't evaluate this one right now", not "use a
    number that isn't from an actual current listing".
    """
    ref_keys, other_count = bptf.get_snapshot_min_other_keys(
        name, quality_name, exclude_listing_id=exclude_listing_id,
        particle_id=particle_id, craftable=craftable, spell=spell,
        australium=australium, killstreak_tier=killstreak_tier,
        paint=paint, killstreaker=killstreaker, sheen=sheen,
    )

    if ref_keys is not None and other_count >= min_other_listings:
        log.info(
            "Reference for %s: %.2f keys from LIVE snapshot (%d other listing(s)).",
            name, ref_keys, other_count,
        )
        return ref_keys

    if is_rate_limited():
        log.info(
            "Skipping %s - backpack.tf rate-limit cooldown is active, so 'not enough live "
            "listings' can't be trusted as genuine right now.",
            name,
        )
    else:
        log.info(
            "Skipping %s - only %s live listing(s) found, needed >= %d. Not falling back to "
            "the community-suggested price - only live listings count here.",
            name, other_count if ref_keys is not None else 0, min_other_listings,
        )
    return None



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
    if cfg.get("australium_only") and not (listing.name or "").startswith("Australium "):
        return False
    if listing.extra_excluded_hint:
        return False
    item_type = (listing.item_type or "")
    name = listing.name or ""
    for excluded in cfg["excluded_types"]:
        if excluded.lower() in item_type.lower() or excluded.lower() in name.lower():
            return False
    return True


def evaluate_listing(listing: NormalizedListing, bptf, cfg: dict, name_to_image_url=None, stn_client=None):
    """Returns a deal dict if this listing qualifies, otherwise None."""
    if not is_watched(listing, cfg):
        return None
    if listing.price_keys is None or listing.price_keys < cfg["min_price_keys"]:
        return None

    # Killstreak Kits/Fabricators are skipped entirely, before any API
    # calls - the same limitation that already meant no search LINK could
    # be generated for them (backpack.tf's own community confirms these
    # need the special "Find recipes where the output item is used on
    # this item" flow, not a normal item= search) very plausibly applies
    # to the price-lookup snapshot API too, not just the webpage - and a
    # real production log showed repeated failed snapshot requests
    # specifically for Kit items. Rather than keep spending request
    # budget (and contributing to rate-limit pressure for everything
    # else) on a category whose pricing can't be trusted to begin with,
    # this is skipped outright until there's real evidence of a working
    # way to price Kits specifically.
    if listing.category == "killstreak_kit":
        return None

    # Unusuals need a resolved particle id to compare like-for-like - EXCEPT
    # "Unusualifier" tools, which are a genuinely different case: they GRANT
    # an effect when used, rather than wearing one themselves, so the raw
    # payload legitimately has no particle data for them (confirmed via a
    # real production log: these consistently had no particle field at all,
    # while a real worn Unusual in the same log DID - not a parsing miss,
    # a real structural difference for this item type). Comparing them by
    # name+quality alone (no particle requirement) is correct here, same
    # as any other non-Unusual item.
    is_unusualifier = "Unusualifier" in listing.name
    if listing.quality == "Unusual" and listing.particle_id is None and not is_unusualifier:
        log.warning(
            "Skipping Unusual %s (%s) - could not resolve a particle id for effect %r. "
            "If this keeps happening, the effect-name lookup for this source may need a fix.",
            listing.name, listing.source, listing.particle_name,
        )
        return None

    lookup_name = strip_quality_prefix(listing.name, listing.quality)

    # A painted item whose exact colour we can't resolve to RGB must NOT
    # proceed with an unfiltered comparison - the paint param would
    # silently get omitted from every downstream query, comparing this
    # specific painted item's price against an undifferentiated pool
    # (every OTHER colour, plus unpainted) instead of the same colour
    # specifically. A real production incident showed exactly this
    # cascade: several differently-painted copies of the same cosmetic
    # (one of them "Team Spirit", a team-coloured paint with no single
    # universal RGB value - team-coloured paints are excluded from the
    # table on purpose, see PAINT_NAME_TO_RGB) all landed on nearly
    # identical, wrong reference/buy-order numbers because none of them
    # were actually being paint-filtered - flooding alerts with false
    # "discounts" and, because every one of those still burned a full
    # round of API calls, contributing to backpack.tf's rate limit
    # escalating all the way to its 300s ceiling, which then starved
    # everything ELSE of a fair chance to be evaluated too. Skipping here
    # - the same "can't safely compare, so don't guess" principle as the
    # Unusual-particle-id check above - costs missing this one item, but
    # protects both this item's own accuracy and the whole system's
    # request budget from a cascade like that repeating.
    if listing.paint and paint_rgb_decimal(listing.paint) is None:
        log.warning(
            "Skipping %s (%s) - paint %r has no known RGB value, so it can't be filtered "
            "for in comparisons; proceeding without the filter risks comparing against the "
            "wrong (unfiltered) pool.",
            listing.name, listing.source, listing.paint,
        )
        return None

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

    # Community-suggested price, shown as PURELY INFORMATIONAL context
    # alongside the live comparison - never used to gate or compute the
    # discount itself (that's live-listings-only, see _get_reference_
    # price_keys above; a real, repeated correction made clear a
    # suggested price must never drive the actual decision). This is
    # free - get_price_keys reads the already-loaded bulk price list, no
    # extra network call - and gives the user the same side-by-side view
    # a real report showed being genuinely useful on a similar bot
    # elsewhere: live data AND the community number together, clearly
    # labeled, so a big gap between them is visible rather than hidden.
    suggested_keys = bptf.get_price_keys(lookup_name, listing.quality, listing.particle_id)

    # STN.Trading's own buy order, shown alongside backpack.tf's as
    # another independent data point - same "informational only, never
    # decision-relevant" treatment as suggested_keys above. Only
    # attempted when a client was actually supplied (main.py only passes
    # one when stntrading_api_key is configured) and wrapped defensively
    # - this is a nice-to-have on an already-qualifying deal, so any
    # failure here must never cost the alert itself.
    stn_buy_keys = None
    if stn_client is not None:
        try:
            stn_buy_keys = stn_client.get_item_buy_price_keys(lookup_name, bptf.key_price_metal)
        except Exception:
            log.warning("STN buy-order lookup failed for %s - showing the alert without it.", lookup_name)

    # Best current buy order - "could I flip this for an instant, guaranteed
    # profit". Also only fetched now, same reasoning as the average price.
    #
    # A buy order priced ABOVE the sell reference is NOT itself a red flag
    # (an earlier version of this treated it as one and discarded the buy
    # order whenever this happened - wrong, per direct correction: TF2
    # marketplaces aren't efficient markets with instant arbitrage. Anyone
    # can list an item well below a standing buy order and it just sits
    # there unsold - sellers don't necessarily know about the buy order,
    # and nothing auto-matches them - so this gap is exactly the kind of
    # real opportunity this whole feature exists to surface, not a data
    # bug to suppress).
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
    # (Killstreak Kits/Fabricators never reach this point at all now - see
    # the early return near the top of this function - so no special-case
    # is needed here anymore.)
    backpacktf_search_link = build_classifieds_url(
        lookup_name, listing.quality, listing.particle_id,
        killstreak_tier=listing.killstreak_tier,
        australium=australium, spell=primary_spell,
        paint=listing.paint, craftable=listing.craftable,
        killstreaker=listing.killstreaker, sheen=listing.sheen,
    )

    # Real item picture for the Telegram alert, straight from Valve's own
    # schema (confirmed OK to hotlink directly - see steam_schema.py).
    # Tries the same name in two forms: first as backpack.tf/mannco.store
    # display it (only quality stripped) in case Valve's schema happens to
    # index australium/non-craftable variants under that fuller name too
    # (as IGetPrices does), then the fully bare base name as a fallback -
    # not certain which convention the schema itself uses, so both are
    # tried rather than guessing one and missing pictures for no reason.
    image_url = None
    if name_to_image_url:
        bare_name_for_image = strip_variant_prefixes(lookup_name)
        image_url = name_to_image_url.get(lookup_name) or name_to_image_url.get(bare_name_for_image)
        if image_url is None:
            log.warning(
                "No image found for %s - tried %r and %r against %d loaded names. If this "
                "keeps happening for real items, the schema's name format may not match either "
                "of these.",
                listing.name, lookup_name, bare_name_for_image, len(name_to_image_url),
            )
    elif listing.source != "stntrading.eu":
        # stntrading.eu genuinely has no schema-backed name resolution path
        # here, so staying silent for it is correct - but any OTHER source
        # having no mapping at all usually means steam_api_key is missing
        # or the schema fetch failed. Logged once (not per-item, which
        # would spam identically on every single alert if the key is
        # simply not configured) so it's still discoverable via /errors.
        global _logged_no_image_mapping
        if not _logged_no_image_mapping:
            _logged_no_image_mapping = True
            log.warning("No image mapping loaded at all - alerts will have no pictures until this is fixed.")

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
        "image_url": image_url,
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
        "suggested_keys": suggested_keys,
        "stn_buy_keys": stn_buy_keys,
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
