"""
Configuration loading for the TF2 Deal Watcher.

All secrets and tunable settings live in config.json (copy config.example.json
to config.json and fill it in — see README.md).
"""

import json
import os
import sys

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

DEFAULTS = {
    # --- API keys (required) ---
    "backpacktf_api_key": "",       # https://next.backpack.tf/account/api-access
    # backpack.tf USER TOKEN (separate from the API key above) - needed to
    # compare against backpack.tf's own LIVE listings (not just the static
    # community price list). Get it from the same page as the API key
    # above: https://next.backpack.tf/account/api-access . Expires after
    # ~90 days and needs 2FA to renew - see README. Without it, the
    # watcher still works, just using the slower-moving community-
    # suggested price as the reference instead of live listings.
    "backpacktf_token": "",
    "mannco_api_key": "",           # https://mannco.store/seller
    "telegram_bot_token": "",       # from @BotFather
    "telegram_chat_id": "",         # your numeric chat id (see README)

    # Optional but recommended: a free Steam Web API key
    # (https://steamcommunity.com/dev/apikey). Used only to resolve
    # Unusual particle effect names to their numeric IDs so Unusual hats
    # can be cross-checked against backpack.tf too. Without it, Unusual
    # items are skipped (Strange/Australium weapons still work fine).
    "steam_api_key": "",

    # marketplace.tf needs no API key (reads their public /deals page -
    # see marketplacetf_client.py). How often to check it, in seconds.
    "marketplacetf_poll_seconds": 300,

    # stntrading.eu (optional - only used for items you explicitly watch
    # via /watchstn in Telegram; see stntrading_client.py for why this
    # source can't discover new deals on its own the way the others do).
    # Get a key from https://stntrading.eu/dev/apikey - leave blank to
    # disable this source entirely.
    "stntrading_api_key": "",
    "stntrading_poll_seconds": 300,

    # --- Filtering rules ---
    # Which TF2 item qualities to watch. "Strange" also covers Australium
    # weapons, since Australiums are always Strange quality in TF2.
    # "Unique" is TF2's default/most common quality - watching it opens up
    # by far the largest item pool (this is what unlocks Killstreak Kits,
    # see watched_categories below, and any other Unique item worth
    # tracking) - expect noticeably more alert volume with it on.
    "watched_qualities": ["Unusual", "Strange", "Unique"],

    # Item categories to watch, independent of quality - lets you say
    # "only weapons" or "only cosmetics" via Telegram (/addcategory,
    # /removecategory). Categories, confirmed against Valve's own item
    # schema: "weapon", "cosmetic" (hats/misc), "taunt", "killstreak_kit"
    # (Killstreak Kits/Fabricators - note these are Unique-quality tools,
    # so also need "Unique" in watched_qualities to actually show up),
    # "other" (anything else).
    "watched_categories": ["weapon", "cosmetic", "taunt", "killstreak_kit", "other"],

    # Item types to always ignore (case-insensitive substring match against
    # the item's "type" field returned by Mannco.store).
    "excluded_types": ["War Paint"],

    # Minimum item value to bother alerting on, expressed in TF2 keys.
    "min_price_keys": 5,

    # How far below the backpack.tf reference price a Mannco.store listing
    # must be, in percent, to trigger a notification. Low bars like this
    # (versus the more conservative 15% this project started with) surface
    # far more candidates - combined with "Unique" above, expect a real
    # jump in alert volume and in backpack.tf API traffic per alert
    # candidate (each one now also runs the killstreak-tier consistency
    # check, itself up to 3 extra requests for weapons - see
    # bptf_client.MAX_CONCURRENT_REQUESTS for the safeguard against that).
    "discount_threshold_percent": 5,

    # How many OTHER active backpack.tf listings for the exact same item
    # (name+quality+effect) must exist before we trust their minimum price
    # as "the market rate". 1 means "at least one other seller". Higher is
    # safer against one-off outlier listings, but skips thinner markets.
    # Note: outlier listings (e.g. a stray 1-key troll listing among
    # 100-key ones) are already filtered out before this count is taken,
    # see bptf_client._filter_price_outliers.
    "min_other_listings": 1,

    # Liquidity filter: skip a deal if this item's price hasn't been
    # revised by the community in more than this many days - a discount
    # on something nobody's actively trading is more likely a forgotten
    # price than a real find. backpack.tf's actual sale-confirmation data
    # is a paid Premium feature (not available via the free API this
    # project uses) - this uses price-suggestion recency as the closest
    # free proxy for "is this item actively traded".
    "max_days_since_price_update": 90,

    # --- Operational settings ---
    # How often (seconds) to refresh the backpack.tf price list and the
    # mannco.store key exchange rate. Price data doesn't change second to
    # second, so every 10-15 minutes is plenty.
    "price_refresh_seconds": 900,

    # How often (seconds) to re-check the mannco.store login (JWT) is valid.
    "jwt_refresh_seconds": 3600,

    # Avoid re-notifying about the same listing id twice.
    "seen_listings_max": 20000,

    # How long (seconds) to cache a Steam inventory-privacy check per
    # seller, so an active seller doesn't get re-checked on every listing.
    "inventory_cache_seconds": 900,

    # How long (seconds) to cache a backpack.tf live-listings snapshot for
    # one item, so a burst of updates for the same item doesn't hammer the
    # snapshot endpoint.
    "snapshot_cache_seconds": 20,
}


def load_config():
    if not os.path.exists(CONFIG_PATH):
        sys.exit(
            "config.json not found. Copy config.example.json to config.json "
            "and fill in your API keys first (see README.md)."
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        user_cfg = json.load(f)

    cfg = dict(DEFAULTS)
    cfg.update(user_cfg)

    missing = [
        k
        for k in ("backpacktf_api_key", "mannco_api_key", "telegram_bot_token", "telegram_chat_id")
        if not cfg.get(k)
    ]
    if missing:
        sys.exit(f"config.json is missing required values: {', '.join(missing)}")

    return cfg
