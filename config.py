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
    # compare against backpack.tf's own LIVE listings. Get it from the
    # same page as the API key above: https://next.backpack.tf/account/
    # api-access . Expires after ~90 days and needs 2FA to renew - see
    # README. REQUIRED for meaningful comparisons: this project no longer
    # falls back to the slower-moving community-suggested price when live
    # listings are insufficient (removed after repeated real reports of
    # that fallback showing stale/wrong numbers) - without a live
    # snapshot to compare against, an item is now skipped rather than
    # guessed at, so a missing/expired token means most things get
    # skipped, not silently mispriced.
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
    "excluded_types": ["War Paint", "Badge"],

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

    # After alerting on an item from a given seller, don't alert again on
    # basically the same item FROM THAT SAME SELLER (same
    # source/seller/name/effect/paint/killstreaker/sheen - see send_deal
    # in main.py) for this many minutes - covers a seller bumping/
    # re-listing the same offer repeatedly. Scoped to the seller
    # specifically: a DIFFERENT seller's listing of the same kind of item
    # is a genuinely different opportunity and is never held back by this.
    "item_type_cooldown_minutes": 60,

    # Item names (substring match, case-insensitive) that always get a
    # priority marker on the alert, on top of every Unusual automatically
    # getting one - well-known "hype" items that are worth reacting to
    # first even outside the Unusual pool, since demand (and so how fast
    # they sell) isn't only about quality. This is a starting list, not a
    # definitive one - edit freely, what counts as "hyped" changes over
    # time and is inherently a judgment call, not something to treat as
    # authoritative.
    "priority_item_names": ["Max's Severed Head"],

    # Proactive health check (see health_check_loop in main.py) - if this
    # many NEW warnings/errors pile up within one interval, sends a
    # Telegram message about it unprompted, instead of waiting for
    # someone to notice a bad alert and go check /errors by hand.
    "health_check_interval_minutes": 180,
    "health_check_error_threshold": 5,
    # When the threshold above is crossed, also auto-pause deal alerts
    # (like /pause) so the problem can't be missed among regular alerts -
    # monitoring itself keeps running, only the alerting pauses, and it's
    # undone with /resume. Set to false to get the warning message only,
    # without the auto-pause.
    "health_check_auto_pause": True,

    # Liquidity filter: skip a deal if this item's price hasn't been
    # revised by the community in more than this many days - a discount
    # on something nobody's actively trading is more likely a forgotten
    # price than a real find. backpack.tf's actual sale-confirmation data
    # is a paid Premium feature (not available via the free API this
    # project uses) - this uses price-suggestion recency as the closest
    # free proxy for "is this item actively traded".
    "max_days_since_price_update": 90,

    # --- Operational settings ---
    # How often (seconds) to refresh backpack.tf's whole price list -
    # this genuinely does change often enough to need re-checking every
    # 10-15 minutes (unlike the key price below, which is its own,
    # separate, much slower cadence).
    "price_refresh_seconds": 900,

    # How often (seconds) to refresh the mannco.store key USD price
    # specifically - separate from the line above on purpose, per direct
    # feedback that this was refreshing every 15 minutes despite the
    # key's own price being stable week to week, and each attempt is a
    # real mannco.store API round-trip that can fail on its own (adding
    # log noise for no benefit). Defaults to once a week; the last
    # successfully-fetched value is kept and reused between refreshes,
    # including if a given weekly attempt fails.
    "key_price_refresh_seconds": 7 * 24 * 3600,

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

    # --- backpack.tf request pacing ---
    # How many backpack.tf requests (snapshot + price-history) can be in
    # flight at once. Lower is safer against rate-limiting, higher means
    # a burst of qualifying items clears faster. Real production logs
    # showed sustained 429s even at 4, so this is deliberately
    # conservative - raise it only if /stats and /errors show plenty of
    # spare headroom, not by default assumption.
    "bptf_max_concurrent_requests": 4,

    # Minimum time (seconds) between the START of any two backpack.tf
    # requests, regardless of how many are concurrently allowed above.
    # This is what actually caps the SUSTAINED request rate over time - a
    # concurrency cap alone doesn't: 4 requests in flight, each finishing
    # quickly, can still add up to more than backpack.tf tolerates per
    # second. Raised from an original 0.4s (~2.5 req/sec) after real logs
    # showed sustained rate-limiting continuing even at that pace -
    # repeated 429s cycling through the full adaptive backoff range (10s
    # up to the 300s ceiling), with the bot spending most of a 5-minute
    # window in cooldown and evaluating literally zero deals as a direct
    # result. Still no confirmed exact number for backpack.tf's real
    # limit, so 1.5s (~0.67 req/sec sustained) is a considerably more
    # conservative starting point, chosen because the previous one was
    # demonstrably still too fast, not because this one is measured
    # correct either - if 429s keep recurring even at this pace, raise it
    # further; if /stats and /errors show plenty of headroom, it's safe
    # to lower.
    "bptf_min_request_interval_seconds": 1.5,
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
