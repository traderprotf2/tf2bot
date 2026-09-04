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
    # backpack.tf USER TOKEN (separate from the API key). Same page as
    # above. Expires ~90 days, needs 2FA to renew - see README. Required:
    # without a live listings snapshot to compare against, an item is
    # skipped rather than priced off a stale community estimate.
    "backpacktf_token": "",

    # Optional additional backpack.tf accounts ({api_key, token} pairs) to
    # run requests across in parallel - confirmed OK with backpack.tf,
    # each account still respects its own rate limit independently (see
    # bptf_min_request_interval_seconds). N accounts ≈ N× throughput.
    # Example: [{"api_key": "...", "token": "..."}, {...}]
    "backpacktf_accounts": [],

    "mannco_api_key": "",           # https://mannco.store/seller - only used for the key USD price
    "telegram_bot_token": "",       # from @BotFather
    "telegram_chat_id": "",         # your numeric chat id (see README)

    # --- Filtering rules ---
    # TF2 qualities to watch. "Strange" also covers Australium weapons
    # (always Strange quality). "Unique" is the default/most common
    # quality and unlocks Killstreak Kits (see watched_categories) - opens
    # up by far the largest item pool.
    "watched_qualities": ["Unusual", "Strange", "Unique"],

    # Item categories to watch (/addcategory, /removecategory in Telegram):
    # "weapon", "cosmetic", "taunt", "killstreak_kit" (needs "Unique" in
    # watched_qualities too), "other".
    "watched_categories": ["weapon", "cosmetic", "taunt", "killstreak_kit", "other"],

    # Item types/names to always ignore (case-insensitive substring
    # match). "Tool" and "Crate" also get a second, schema-based check
    # (see classify_category) - this list is the backstop. Strangifier/
    # Party Favor/Craft Item rely on this text match alone (no confirmed
    # structured field for them yet), so a miss here is possible.
    "excluded_types": ["War Paint", "Badge", "Tool", "Strangifier", "Craft Item", "Crate", "Party Favor"],

    # Minimum item value to bother alerting on, in TF2 keys.
    "min_price_keys": 5,
    # None = no upper limit (search at any price above the minimum) -
    # see runtime_settings.py's own docstring for why. Runtime-mutable
    # via /maxprice in Telegram, same as min_price_keys/minprice.
    "max_price_keys": None,

    # Discount vs. the live buy order needed to trigger an alert, in percent.
    "discount_threshold_percent": 5,

    # How many other live sell listings of the exact same item are needed
    # before their minimum counts as informational "Было: X" context.
    # Not required for a deal to fire - see matcher.py's evaluate_listing
    # (the actual decision now runs on the buy order, not this).
    "min_other_listings": 1,

    # Don't re-alert on the same item from the SAME seller for this many
    # minutes after an alert (covers a bump/re-list). Scoped per-seller -
    # a different seller's listing of the same item is never held back.
    "item_type_cooldown_minutes": 60,

    # Item names (substring, case-insensitive) that always get a priority
    # marker, alongside every Unusual automatically getting one. Starting
    # list, not definitive - edit freely.
    "priority_item_names": ["Max's Severed Head"],

    # Proactive health check: if this many new warnings/errors pile up
    # within one interval, sends a Telegram message unprompted.
    "health_check_interval_minutes": 180,
    "health_check_error_threshold": 5,
    # Auto-pause alerts (like /pause) when the threshold above is hit, so
    # the problem can't be missed among regular alerts; undone with
    # /resume. Set false for a warning message only, no auto-pause.
    "health_check_auto_pause": True,

    # Liquidity filter: skip items whose community price hasn't been
    # revised in this many days (proxy for "not actively traded" - real
    # sale-confirmation data is a paid backpack.tf feature). Only applied
    # when fetch_price_history_data (below) is true.
    "max_days_since_price_update": 90,

    # OFF by default. Powers the liquidity check above and the "average
    # price (~30 days)" alert line, both via a separate, throttled
    # backpack.tf call (~11s+). The main reference-price/buy-order lookup
    # no longer needs this (self-collected local store, near-instant) -
    # set true to bring these two informational extras back at that cost.
    "fetch_price_history_data": False,

    # --- Operational settings ---
    "price_refresh_seconds": 900,          # backpack.tf whole price-list refresh
    "key_price_refresh_seconds": 7 * 24 * 3600,  # mannco.store key USD price (stable, so slow cadence)
    "jwt_refresh_seconds": 3600,           # mannco.store login re-check
    "seen_listings_max": 20000,            # dedup cache size
    "inventory_cache_seconds": 900,        # Steam inventory-privacy check, per seller
    "snapshot_cache_seconds": 20,          # backpack.tf live-listings snapshot cache, per item

    # --- backpack.tf request pacing ---
    # Max concurrent backpack.tf requests (snapshot + price-history).
    # Kept conservative - real 429s were seen even at 4.
    "bptf_max_concurrent_requests": 4,

    # Minimum seconds between the start of any two backpack.tf requests -
    # this is what actually caps sustained rate, not the concurrency cap
    # alone. Based on backpack.tf's own confirmed 429 response
    # (maxWindowRequests: 6, windowSeconds: 60 on the default tier - i.e.
    # 10s/request minimum). 11s leaves a small margin for default-tier
    # keys; drop to ~1.1s if this bot's token has backpack.tf Premium.
    "bptf_min_request_interval_seconds": 11.0,
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
