"""
TF2 Deal Watcher
================
Watches backpack.tf's classifieds websocket for newly-listed TF2 items
(Unusual hats, Strange weapons, Australiums, ...) priced well below their
current live buy order, and sends a Telegram alert.

The reference price is backpack.tf's own live buy order for that exact
item (name + quality + effect + ...), self-collected from the same
websocket stream into a local store (see bptf_client.py) - never
backpack.tf's community-suggested price, which real cases showed
disagreeing with what was actually for sale.

Usage:
    python3 main.py

See README.md for setup instructions.
"""

import asyncio
import collections
import concurrent.futures
import json
import logging
import os
import signal
import time

import bptf_client
import bptf_ws
import error_log
import mannco_client
import matcher
import runtime_settings
import steam_inventory
import unusual_effects
import telegram_commands
import telegram_notify
from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")

_error_buffer = error_log.install()

# Only cosmetics can carry a Paint Can colour in TF2; only weapons can
# carry a Killstreak tier. Used to strip out either field if upstream
# data ever looks physically impossible.
COSMETIC_SLOTS = {"head", "misc"}
WEAPON_SLOTS = {"primary", "secondary", "melee", "pda", "pda2", "building"}

# Taunts have item_slot == "taunt". Killstreak Kits/Fabricators are Tool
# items with no weapon/cosmetic slot - identified by Valve's consistent
# naming instead ("Killstreak Kit", "Specialized Killstreak Kit", etc.).
# NOTE: kits/fabricators are Unique-quality tools, not Strange/Unusual -
# watching this category alone won't surface anything unless "Unique" is
# also in watched_qualities.


def classify_category(name, slot=None):
    """backpack.tf's `slot` schema field is the reliable classifier."""
    name = name or ""
    # Valve names every killstreak-kit tier consistently, "Kit"/
    # "Fabricator" always the last word - catches "Killstreak Rocket
    # Launcher Kit" too, not just the literal substring "Killstreak Kit".
    if "Killstreak" in name and (name.endswith("Kit") or name.endswith("Fabricator")):
        return "killstreak_kit"

    # Unusualifiers are TOOLS (consumed to apply an Unusual effect to a
    # taunt) - checked BEFORE the "Taunt: " check below, since their own
    # name legitimately contains "Taunt: " (naming which taunt they
    # apply to, e.g. "Taunt: The Box Trot Unusualifier"), which would
    # otherwise misclassify them as an actual, performable taunt - a
    # real, confirmed case: enabling only the "taunt" category started
    # showing Unusualifier tools too, which isn't what "taunt" means to
    # someone using that filter.
    if name.endswith("Unusualifier"):
        return "other"

    # Valve always names taunt items "Taunt: <Name>" - reliable regardless
    # of whether slot happens to be populated.
    if "Taunt: " in name:
        return "taunt"

    if slot is not None:
        if slot == "taunt":
            return "taunt"
        if slot in WEAPON_SLOTS:
            return "weapon"
        if slot in COSMETIC_SLOTS:
            return "cosmetic"
        # Badges/medals equip in their own slot, distinct from head/misc
        # cosmetics (confirmed via the official wiki) - a reliable,
        # slot-based signal independent of the item's name text.
        if slot == "medal":
            return "badge"

    return "other"


class Watcher:
    def __init__(self, cfg):
        self.cfg = cfg
        self._effective_cfg_cache = None
        self._effective_cfg_cache_version = None
        # The primary account (backpacktf_api_key/backpacktf_token) is
        # always included, plus any extra accounts in backpacktf_accounts
        # - confirmed with backpack.tf that running several accounts'
        # requests in parallel is fine, so this is what makes N accounts
        # give roughly N times the throughput of one (see
        # bptf_client._AccountPool). A single account (the default, most
        # common case) behaves exactly as before - this list just has
        # one entry in it.
        bptf_accounts = [{"api_key": cfg["backpacktf_api_key"], "token": cfg.get("backpacktf_token", "")}]
        # Extra accounts set via /setaccounts (persisted to their own
        # file - see _save_accounts/_load_accounts) take priority over
        # whatever's in config.json, since they're the more recent,
        # explicit choice; config.json's own backpacktf_accounts is the
        # fallback for a fresh install that's never used /setaccounts.
        saved_accounts = self._load_accounts()
        bptf_accounts.extend(saved_accounts if saved_accounts is not None else cfg.get("backpacktf_accounts", []))
        # Kept in sync with what actually ended up in the pool (the
        # extras only, matching this key's own meaning elsewhere) - so
        # proactive_buy_order_refresh_loop's own worker_count (computed
        # from this same key) reflects accounts loaded from disk here,
        # not just ones set via /setaccounts during THIS run.
        cfg["backpacktf_accounts"] = bptf_accounts[1:]
        # At least one concurrent request slot per account, so N accounts
        # can genuinely have N requests in flight together rather than
        # being bottlenecked by a smaller concurrency cap sized for the
        # single-account case - cfg's own value still wins if explicitly
        # set higher than that.
        max_concurrent = max(cfg.get("bptf_max_concurrent_requests", 4), len(bptf_accounts))
        bptf_client.configure_request_pacing(
            bptf_accounts,
            max_concurrent,
            cfg.get("bptf_min_request_interval_seconds", 11.0),
        )
        self.bptf = bptf_client.BackpackTFPriceList(
            cfg["backpacktf_api_key"],
            cfg.get("backpacktf_token", ""),
            cfg["snapshot_cache_seconds"],
        )
        self.mannco = mannco_client.ManncoClient(cfg["mannco_api_key"], cfg["jwt_refresh_seconds"])
        self.telegram = telegram_notify.TelegramNotifier(cfg["telegram_bot_token"], cfg["telegram_chat_id"])
        # Bundled static data (unusual_effects.py), not a live Steam API
        # call - no API key dependency, works even if misconfigured.
        self.particle_name_to_id = dict(unusual_effects.NAME_TO_ID)
        self.particle_id_to_name = {v: k for k, v in self.particle_name_to_id.items()}
        self.inventory_checker = steam_inventory.SteamInventoryChecker(
            cache_ttl_seconds=cfg.get("inventory_cache_seconds", 900)
        )
        self.runtime = runtime_settings.RuntimeSettings.load(cfg)
        self.mannco_key_usd_cents = None
        self.seen = collections.deque(maxlen=cfg["seen_listings_max"])
        self.seen_set = set()

        # Funnel counters for /stats - shows where volume narrows down
        # (quality/category filter vs evaluate_listing's own checks).
        # Reset on each /stats read.
        self.stats = collections.Counter()
        self.stats_since = time.time()

        # Item-type cooldown state - see send_deal(). Keyed by (source,
        # display_name, particle, paint, killstreaker, sheen) -> last
        # alert timestamp.
        self.item_type_last_alerted = {}

        # One raw-sample log per item "kind" since startup - see the
        # sampling calls in handle_bptf_event.
        self._sampled_item_kinds = set()

        # name -> identity_key(s), for /checkitem's name-based lookup
        # (identity keys themselves may be defindex-anchored, not
        # name-based). Unbounded on purpose - distinct item NAMES are a
        # small set relative to listing volume.
        self._name_to_identity_keys = collections.defaultdict(set)

        # (name, quality_name) -> {"ts": last proactive-refresh time (0 =
        # never), "category": classify_category() result} - drives
        # proactive_buy_order_refresh_loop below, every watched quality.
        # Capped at MAX_KNOWN_SCAN_ITEMS, LRU-evicted (see the population
        # site in handle_bptf_event) - unbounded growth here once caused
        # an OOM kill. Not persisted - rebuilds within minutes.
        self._known_scan_items = {}
        self.MAX_KNOWN_SCAN_ITEMS = 2000

        # particle_id -> {"name_hint", "item_name", "quality", "count",
        # "first_seen"} for every resolved-but-unlisted Unusual effect
        # id seen since startup (never in unusual_effects.py's own
        # bundled data or the current schema) - lets the bundled
        # database grow from what this project actually observes
        # trading, rather than only from manual research, per explicit
        # request. Loaded from/saved to disk (see _load_unknown_
        # effects/_save_unknown_effects) so a discovery isn't lost to a
        # routine restart before anyone's reviewed it - unlike
        # _known_scan_items above, these are rare and worth keeping.
        self._unknown_particle_ids = {}
        self._load_unknown_effects()

        if not cfg.get("backpacktf_token"):
            log.warning(
                "No backpacktf_token configured - comparisons will use backpack.tf's "
                "community-suggested prices only, not live listings. See README."
            )

    def _mark_seen(self, dedup_id):
        if dedup_id in self.seen_set:
            return False
        if len(self.seen) == self.seen.maxlen:
            oldest = self.seen[0]
            self.seen_set.discard(oldest)
        self.seen.append(dedup_id)
        self.seen_set.add(dedup_id)
        return True

    def effective_cfg(self):
        """
        The dict matcher.evaluate_listing() reads filter settings from -
        config.json's static values, overlaid with whatever is currently
        set at runtime via Telegram commands (see runtime_settings.py) -
        so a command takes effect on the very next listing event.

        Cached, keyed by self.runtime._version (bumped only by
        RuntimeSettings.save(), i.e. on an actual mutation) rather than
        rebuilt fresh on every call - this runs once per evaluate_listing()
        call, synchronously on the main event loop, for a result that's
        identical on the overwhelming majority of calls (settings only
        change when a command is actually issued).
        """
        version = self.runtime._version
        if self._effective_cfg_cache is not None and self._effective_cfg_cache_version == version:
            return self._effective_cfg_cache
        merged = {
            **self.cfg,
            "min_price_keys": self.runtime.min_price_keys,
            "max_price_keys": self.runtime.max_price_keys,
            "min_profit_keys": self.runtime.min_profit_keys,
            "watched_qualities": self.runtime.watched_qualities,
            "watched_categories": self.runtime.watched_categories,
            "discount_threshold_percent": self.runtime.discount_threshold_percent,
            "max_days_since_price_update": self.runtime.max_days_since_price_update,
            "priority_item_names": self.runtime.priority_item_names,
            # A real, confirmed bug this closes: missing here meant
            # /australium in Telegram only ever changed what /status
            # displayed, never the actual filter is_watched() reads -
            # cfg.get("australium_only") always saw config.json's own
            # static value (itself never even defined until now, so
            # always None/falsy) regardless of what was toggled.
            "australium_only": self.runtime.australium_only,
        }
        self._effective_cfg_cache = merged
        self._effective_cfg_cache_version = version
        return merged

    def refresh_prices(self):
        self.bptf.refresh()

    async def _run_telegram(self, func, *args):
        """Runs a self.telegram.* call on its own small, dedicated
        thread pool (see run() for why) instead of the shared default
        pool asyncio.to_thread() would use. Falls back to the default
        pool if the dedicated one hasn't been set up yet (e.g. anything
        that calls send_deal directly without going through run() first,
        such as tests) - a slower path is fine there, correctness isn't
        affected either way."""
        executor = getattr(self, "_telegram_executor", None)
        if executor is None:
            return await asyncio.to_thread(func, *args)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, func, *args)

    def refresh_mannco_key_price(self):
        rate = self.mannco.get_key_price_usd_cents()
        if rate:
            self.mannco_key_usd_cents = rate
            log.info("mannco.store key price: $%.2f", rate / 100)
        else:
            log.warning("Could not refresh mannco.store key price; keeping previous value.")

    async def price_refresh_loop(self):
        while True:
            try:
                await asyncio.to_thread(self.refresh_prices)
            except Exception:
                log.exception("Price refresh failed, will retry next cycle.")
            await asyncio.sleep(self.cfg["price_refresh_seconds"])

    async def key_price_refresh_loop(self):
        """
        The mannco.store key price refreshes on its own, much slower
        cadence than price_refresh_loop above - its USD price is stable
        week to week, unlike backpack.tf's whole price list. Retries
        sooner (retry_seconds below) after any failure rather than
        waiting the full weekly cycle - mannco.store's login has hit real
        rate limits before, and this rate feeds every USD<->keys
        conversion in the project.
        """
        retry_seconds = 300  # 5 minutes - only used while no value has been obtained yet
        while True:
            try:
                await asyncio.to_thread(self.refresh_mannco_key_price)
            except Exception:
                log.exception("mannco.store key price refresh failed, will retry next cycle.")
            if self.mannco_key_usd_cents:
                await asyncio.sleep(self.cfg.get("key_price_refresh_seconds", 7 * 24 * 3600))
            else:
                log.warning("mannco.store key price still unavailable - retrying in %ss instead of "
                             "waiting for the full weekly cycle.", retry_seconds)
                await asyncio.sleep(retry_seconds)

    async def local_store_prune_loop(self):
        """Periodically removes expired entries from
        self.bptf.local_listings (see LocalListingStore in
        bptf_client.py) so a long-running process doesn't accumulate
        unbounded memory - the per-read freshness filtering already
        ignores stale entries on its own, this just actually frees
        them."""
        while True:
            await asyncio.sleep(600)
            try:
                await asyncio.to_thread(self.bptf.local_listings.prune_expired)
            except Exception:
                log.exception("Local listing store prune failed, will retry next cycle.")

    async def alert_cooldowns_prune_loop(self):
        """Periodically removes expired entries from
        item_type_last_alerted (see send_deal's own cooldown check) -
        a real, confirmed gap: unlike every other per-item/per-listing
        collection in this project (LocalListingStore's own buckets,
        _known_scan_items), this one had no cap or cleanup at all, so it
        grew by one entry for every unique (source, seller, item,
        attributes) combination that ever alerted, for the whole life
        of a long-running process, with no eviction - the same class of
        unbounded growth that caused a real OOM kill elsewhere, just on
        a slower timescale since alerts are far rarer than raw events.
        An entry past its own cooldown window is already useless for
        the check it exists for, so pruning it costs nothing."""
        while True:
            await asyncio.sleep(1800)
            try:
                cooldown_seconds = self.cfg.get("item_type_cooldown_minutes", 60) * 60
                now = time.time()
                expired = [
                    key for key, ts in self.item_type_last_alerted.items()
                    if now - ts >= cooldown_seconds
                ]
                for key in expired:
                    del self.item_type_last_alerted[key]
                if expired:
                    log.info("Pruned %d expired alert cooldown(s).", len(expired))
            except Exception:
                log.exception("Alert cooldown prune failed, will retry next cycle.")

    # Floor between two proactive refreshes of the SAME item, regardless
    # of how "stale" it looks relative to others - without this, a
    # small known-items set (very possible early after startup, or with
    # few categories watched) relative to worker count would have
    # workers just hammering the same handful of items back-to-back with
    # no real benefit, rather than correctly idling until a refresh is
    # genuinely due. 30 minutes comfortably keeps every item far fresher
    # than LocalListingStore's own multi-hour trust windows need.
    PROACTIVE_MIN_REFRESH_INTERVAL_SECONDS = 1800

    async def _proactive_unusual_refresh_worker(self, worker_id: int):
        """
        One worker of proactive_buy_order_refresh_loop below. Picks the
        single stalest known (item, quality) pair, claims it (bumps its
        timestamp synchronously, before awaiting - no race between two
        workers picking the same item), then spends its own time on the
        network request. N workers means N requests in flight, each
        paced by _AccountPool's own per-account throttle.
        """
        while True:
            # Only consider items whose category is CURRENTLY watched
            # (re-checked every pick, not just at discovery time - see
            # class comment) AND that are actually due for a refresh
            # (see PROACTIVE_MIN_REFRESH_INTERVAL_SECONDS above) - the
            # second condition is what makes workers correctly idle,
            # rather than uselessly re-hammering the same tiny set of
            # items, whenever known items are fewer than workers.
            now = time.time()
            eligible = {
                k: v for k, v in self._known_scan_items.items()
                if v["category"] in self.runtime.watched_categories
                and now - v["ts"] >= self.PROACTIVE_MIN_REFRESH_INTERVAL_SECONDS
            }
            if not eligible:
                await asyncio.sleep(5)
                continue
            scan_key = min(eligible, key=lambda k: eligible[k]["ts"])
            item_name, item_quality = scan_key
            self._known_scan_items[scan_key]["ts"] = time.time()  # claimed immediately, before awaiting
            try:
                recorded = await asyncio.to_thread(
                    self.bptf.fetch_and_record_all_buy_orders, item_name, item_quality
                )
                self.stats["proactive_unusual_scans"] += 1
                self.stats["proactive_unusual_buy_orders_recorded"] += recorded
            except Exception:
                log.exception("Proactive buy-order scan failed for %s / %s (worker %d).",
                               item_name, item_quality, worker_id)

    async def proactive_buy_order_refresh_loop(self):
        """
        Keeps buy-order data for every Unusual item this project has
        seen traded "perpetually fresh", rather than only fetching one
        on demand when a matching sell listing arrives (still the
        fallback for anything this hasn't gotten to yet - see fetch_
        live_buy_order_keys).

        One worker per configured account (more accounts = more workers
        = faster full-cycle time - backpack.tf confirmed running several
        accounts in parallel is fine, see _AccountPool). Each worker
        refreshes whichever known item has gone longest without one.
        """
        # +1 for the primary account (backpacktf_api_key/token) -
        # cfg["backpacktf_accounts"] holds only the EXTRA accounts (see
        # __init__), matching that key's meaning everywhere else, but
        # the primary account is also a real slot in the pool and can
        # also do proactive-scan work.
        worker_count = 1 + len(self.cfg.get("backpacktf_accounts") or [])
        log.info("Starting %d proactive Unusual buy-order refresh worker(s).", worker_count)
        await asyncio.gather(*(
            self._proactive_unusual_refresh_worker(i) for i in range(worker_count)
        ))

    async def local_store_snapshot_loop(self):
        """
        Periodically saves self.bptf.local_listings to disk (see
        LocalListingStore.save_to_disk) so the data isn't lost on every
        restart - a real, direct point made clear that updating the bot
        (which happens often) was wiping the store's entire accumulated
        knowledge each time, undermining the accuracy this whole
        architecture exists for right when it mattered most. Every 90
        seconds is a deliberate middle ground: frequent enough that a
        restart loses at most that much of the very latest data, rare
        enough that the disk write (done off the event loop, via
        to_thread, so it never costs any processing speed) isn't
        happening on anything like a per-event basis.
        """
        while True:
            await asyncio.sleep(90)
            try:
                await asyncio.to_thread(self.bptf.local_listings.save_to_disk, bptf_client.LOCAL_LISTINGS_STATE_PATH)
                await asyncio.to_thread(self._save_alert_cooldowns)
            except Exception:
                log.exception("Local listing store snapshot save failed, will retry next cycle.")

    async def health_check_loop(self):
        """
        Proactively pings Telegram only when something looks worth
        knowing about (a meaningful pile-up of new warnings/errors - see
        error_log.py) - not a routine "all good" check-in, which would
        just be noise.

        When the threshold is crossed, this PAUSES deal alerts (the same
        /pause a person can trigger by hand) rather than stopping the
        process - a full stop risks systemd just restarting it (defeating
        the point) and misses real, time-sensitive deals while down.
        Pausing keeps monitoring alive in the background and is fully
        reversible with /resume once /errors has been checked.
        """
        last_error_count = _error_buffer.total_emitted
        interval_seconds = self.cfg.get("health_check_interval_minutes", 180) * 60
        threshold = self.cfg.get("health_check_error_threshold", 5)
        auto_pause = self.cfg.get("health_check_auto_pause", True)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                new_error_count = _error_buffer.total_emitted - last_error_count
                if new_error_count >= threshold:
                    current_errors = _error_buffer.recent(1000)
                    recent_messages = [e["message"][:150] for e in current_errors]
                    summary = "\n".join(f"• {m}" for m in recent_messages[-5:])
                    pause_note = ""
                    if auto_pause and not self.runtime.paused:
                        self.runtime.paused = True
                        self.runtime.save()
                        pause_note = (
                            "\n\n⏸ <b>Уведомления о сделках приостановлены</b>, чтобы это точно "
                            "не потерялось - мониторинг продолжает работать в фоне. Посмотри /errors "
                            "и напиши, что нашлось - или просто /resume, если показалось лишним."
                        )
                    await asyncio.to_thread(
                        self.telegram.send,
                        f"⚠️ <b>За последние {interval_seconds // 60:.0f} мин. накопилось "
                        f"{new_error_count} предупреждений/ошибок</b>.\n\nПоследние:\n{summary}"
                        f"{pause_note}",
                    )
                last_error_count = _error_buffer.total_emitted
            except Exception:
                log.exception("Health check loop itself failed - continuing anyway.")
    def _build_alert_keyboard(self, deal: dict):
        """
        Inline URL buttons for the alert (Trade/Buy, classifieds search)
        instead of plain text links in the message body. Telegram buttons
        need a real https URL, so anything without one (e.g. a closed-
        inventory listing with no direct trade link) is simply omitted.
        """
        buttons = []
        if deal.get("trade_closed"):
            if deal.get("profile_link"):
                buttons.append({"text": "👤 Профиль продавца", "url": deal["profile_link"]})
        elif deal.get("link"):
            buy_labels = {"backpack.tf": "🔗 Трейд с продавцом"}
            buttons.append({"text": buy_labels.get(deal["source"], "🔗 Открыть"), "url": deal["link"]})
        if deal.get("backpacktf_search_link"):
            buttons.append({"text": "📋 Объявления на backpack.tf", "url": deal["backpacktf_search_link"]})
        return [buttons] if buttons else None

    def format_alert(self, deal: dict) -> str:
        import html
        effect = f" ({html.escape(deal['particle_name'], quote=False)})" if deal["particle_name"] else ""
        variant = f" [{html.escape(deal['variant_label'], quote=False)}]" if deal.get("variant_label") else ""
        # Civilian..Elite grade - already correctly captured for
        # comparison (see matcher.py's own "grade" field, sourced from
        # main.py's "rarity" field fix), but previously never shown
        # here either, so a graded item's alert never actually told the
        # person which grade matched. {Curly braces} to stay visually
        # distinct from (effect) and [variant] when more than one shows
        # on the same line.
        grade = f" {{{html.escape(deal['grade'], quote=False)}}}" if deal.get("grade") else ""
        usd = f", ${deal['price_usd']:.2f}" if deal["price_usd"] else ""

        # Every dynamic string below is backpack.tf-sourced text going
        # into an HTML-parse-mode message - html.escape()d, since an
        # unescaped "<"/"&"/">" in any of them would break Telegram's
        # HTML parser and silently drop the whole message.
        special_lines = []
        if deal["paint"]:
            special_lines.append(f"🎨 Краска: {html.escape(deal['paint'], quote=False)}")
        if deal["spells"]:
            special_lines.append(f"👻 Спелл(ы): {html.escape(', '.join(deal['spells']), quote=False)}")
        if deal.get("strange_parts"):
            special_lines.append(f"🔧 Strange Part(ы): {html.escape(', '.join(deal['strange_parts']), quote=False)}")
        if deal.get("killstreaker"):
            special_lines.append(f"🔥 Killstreaker (эффект в глазах): {html.escape(deal['killstreaker'], quote=False)}")
        if deal.get("sheen"):
            special_lines.append(f"✨ Sheen (цвет вспышки при убийствах): {html.escape(deal['sheen'], quote=False)}")
        if deal.get("trade_closed"):
            special_lines.append("🔒 Инвентарь продавца закрыт — трейд напрямую недоступен")
        special_block = ("\n" + "\n".join(special_lines)) if special_lines else ""

        avg_line = ""
        if deal["average_keys"] is not None:
            avg_line = f"\nСредняя цена (~30 дней): {deal['average_keys']:.2f} ключей"

        # Now purely informational (may not exist at all - the discount
        # decision itself runs entirely on the buy order, see
        # evaluate_listing's own comments in matcher.py for why), so
        # this line is only shown when a sell-side reference actually
        # happened to be available too, never required for an alert to fire.
        sell_reference_line = ""
        if deal.get("previous_low_keys") is not None:
            sell_reference_line = f"\nБыло: {deal['previous_low_keys']:.2f} ключей"

        buy_order_line = ""
        if deal.get("buy_order_keys") is not None:
            profit = deal.get("flip_profit_keys")
            profit_note = ""
            if profit is not None and profit > 0:
                profit_note = f" (перепродажа даёт +{profit:.2f} ключей прибыли)"
            buy_order_line = (
                f"\n💰 Buy order на backpack.tf: {deal['buy_order_keys']:.2f} ключей "
                f"({deal.get('buy_order_count', 0)} шт.){profit_note}"
            )
            if deal.get("unpainted_reference"):
                # No buy order exists at all for this specific paint (an
                # unpopular colour can genuinely have none, ever) - this
                # is the item's own UNPAINTED buy order instead, as a
                # conservative floor reference, NOT a live buy order for
                # this exact painted item. Said plainly so this doesn't
                # read as a guaranteed instant flip the way every other
                # buy order line here does.
                buy_order_line += (
                    "\n⚠️ Buy order на непокрашенный вариант - на именно эту краску "
                    "buy order'ов нет вообще, гарантированной перепродажи по этой цене нет"
                )

        stn_buy_line = ""
        if deal.get("stn_buy_keys") is not None:
            stn_buy_line = f"\n🏦 Buy order на STN.Trading: {deal['stn_buy_keys']:.2f} ключей"

        liquidity_line = ""
        days_since = deal.get("days_since_price_update")
        if days_since is not None:
            liquidity_line = f"\n📊 Последняя переоценка цены: {days_since:.0f} дн. назад"

        seller_note_line = ""
        if deal.get("seller_note"):
            # Real, confirmed field (see the listing construction in
            # handle_bptf_event) - the seller's own comment on their
            # listing, shown verbatim like the "Description" field in the
            # Discord example this was modeled on. HTML-escaped since
            # it's arbitrary user text being dropped into an HTML-parsed
            # message - an unescaped "<" or "&" in someone's note would
            # otherwise break the whole message's formatting.
            seller_note_line = f"\n📝 Продавец пишет: [<i>{html.escape(deal['seller_note'], quote=False)}</i>]"

        # Only called out when true - "this seller's had this item up
        # before" is the unremarkable default and doesn't need a line of
        # its own, similar to how the Discord example this was modeled
        # on treats "first time listing" as the notable case worth a
        # History field, not "seen before". Uses the SAME per-seller
        # identity tracking that already powers the cooldown in
        # send_deal - not a new signal, just surfacing an existing one.
        history_line = ""
        if deal.get("is_first_time_seller_item"):
            history_line = "\n🆕 Впервые видим этот лот от этого продавца"

        # Trade/classifieds links are now buttons under the message (see
        # _build_alert_keyboard), not text in the body - keeps the body
        # focused on data, actions live in the buttons, closer to how the
        # real Discord example this was modeled on separates the two.
        # (The closed-inventory note itself is already shown via
        # special_lines above - no need to repeat it here.)
        links_block = ""

        priority_prefix = "⭐ ПРИОРИТЕТ (ликвидно/хайп) ⭐\n" if deal.get("is_priority") else ""
        # display_name HTML-escaped defensively - not yet confirmed as
        # the actual cause of any specific missed alert, but a real,
        # concrete risk given this project has already seen backpack.tf's
        # own "name" field contain unexpected raw internal tokens (the
        # #Attrib_Particle artifact) - an unescaped "<"/"&" in some future
        # such token would break Telegram's HTML parser and silently drop
        # the WHOLE message, the same way an unescaped seller_note would
        # (already fixed, see seller_note_line above).
        safe_display_name = html.escape(deal["display_name"], quote=False)
        return (
            f"{priority_prefix}"
            f"🔥 <b>-{deal['discount_percent']:.0f}%</b> — {deal['source']}\n"
            f"<b>{safe_display_name}</b>{effect}{variant}{grade}"
            f"{special_block}\n"
            f"Цена в объявлении: <b>{deal['price_keys']:.2f} ключей</b>{usd}"
            f"{sell_reference_line}"
            f"{avg_line}"
            f"{buy_order_line}"
            f"{stn_buy_line}"
            f"{liquidity_line}"
            f"{seller_note_line}"
            f"{history_line}"
            f"{links_block}"
        )

    ALERT_COOLDOWNS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_cooldowns_state.json")

    def _save_alert_cooldowns(self):
        """
        Persists item_type_last_alerted (the per-seller "don't re-alert
        within N minutes" cooldown - see send_deal) to disk, the same
        list/dict-of-entries pattern LocalListingStore's own save_to_disk
        uses for its tuple keys. Added after a real, confirmed case: this
        dict was purely in-memory, so a restart (routine during a
        deploy, or systemd's own auto-restart) silently wiped every
        cooldown, letting the exact same seller/item combination alert
        again well within what should have been a 60-minute window.
        """
        try:
            serializable = [
                {"key": list(key), "ts": ts}
                for key, ts in self.item_type_last_alerted.items()
            ]
            tmp_path = f"{self.ALERT_COOLDOWNS_STATE_PATH}.{os.getpid()}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(serializable, f)
            os.replace(tmp_path, self.ALERT_COOLDOWNS_STATE_PATH)
        except Exception:
            log.exception("Could not save alert cooldowns to disk.")

    def _load_alert_cooldowns(self):
        if not os.path.exists(self.ALERT_COOLDOWNS_STATE_PATH):
            return
        try:
            with open(self.ALERT_COOLDOWNS_STATE_PATH, "r", encoding="utf-8") as f:
                serializable = json.load(f)
            for item in serializable:
                self.item_type_last_alerted[tuple(item["key"])] = item["ts"]
            log.info("Loaded %d saved alert cooldowns from disk.", len(self.item_type_last_alerted))
        except Exception:
            log.exception("Could not load alert cooldowns from disk - starting fresh.")

    UNKNOWN_EFFECTS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "unknown_effects_state.json")

    def _load_unknown_effects(self):
        if not os.path.exists(self.UNKNOWN_EFFECTS_STATE_PATH):
            return
        try:
            with open(self.UNKNOWN_EFFECTS_STATE_PATH, "r", encoding="utf-8") as f:
                self._unknown_particle_ids = {int(k): v for k, v in json.load(f).items()}
            log.info("Loaded %d previously-discovered unknown effect id(s) from disk.",
                      len(self._unknown_particle_ids))
        except Exception:
            log.exception("Could not load unknown-effects state from disk - starting fresh.")

    def _save_unknown_effects(self):
        try:
            tmp_path = f"{self.UNKNOWN_EFFECTS_STATE_PATH}.{os.getpid()}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in self._unknown_particle_ids.items()}, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.UNKNOWN_EFFECTS_STATE_PATH)
        except Exception:
            log.exception("Could not save unknown-effects state to disk.")

    ACCOUNTS_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backpacktf_accounts_state.json")

    def _save_accounts(self, accounts):
        """
        Persists cfg["backpacktf_accounts"] to its OWN dedicated file,
        deliberately separate from runtime_state.json - these are live
        API credentials, and keeping them in their own, narrowly-scoped
        file limits the damage if that OTHER file is ever accidentally
        committed to git again (a real, confirmed case this session -
        see runtime_state.json's own history), rather than credentials
        riding along with ordinary filter settings in the same blast
        radius.
        """
        try:
            tmp_path = f"{self.ACCOUNTS_STATE_PATH}.{os.getpid()}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(accounts, f)
            os.replace(tmp_path, self.ACCOUNTS_STATE_PATH)
        except Exception:
            log.exception("Could not save backpack.tf accounts to disk.")

    def _load_accounts(self):
        if not os.path.exists(self.ACCOUNTS_STATE_PATH):
            return None
        try:
            with open(self.ACCOUNTS_STATE_PATH, "r", encoding="utf-8") as f:
                accounts = json.load(f)
            if isinstance(accounts, list) and accounts:
                return accounts
        except Exception:
            log.exception("Could not load backpack.tf accounts from disk.")
        return None

    def _set_accounts(self, raw_text: str) -> str:
        """
        /setaccounts, followed by one "api_key token" pair per line
        (space or comma between the two) - configures the multi-account
        pool (see _AccountPool) from Telegram, no config.json editing.
        The account pool itself updates immediately; the proactive
        scanner's worker COUNT only grows after a restart (noted in the
        reply below).
        """
        lines = [ln.strip() for ln in raw_text.strip().splitlines() if ln.strip()]
        if not lines:
            return (
                "Пришли по одной паре api_key и token на строку, например:\n"
                "ключ1 токен1\nключ2 токен2\nключ3 токен3"
            )
        accounts = []
        bad_lines = []
        for i, line in enumerate(lines, start=1):
            parts = line.replace(",", " ").split()
            if len(parts) != 2:
                bad_lines.append(i)
                continue
            accounts.append({"api_key": parts[0], "token": parts[1]})
        if bad_lines:
            return (
                f"Не понял строку(и) {', '.join(map(str, bad_lines))} - в каждой должно быть "
                f"ровно два значения (api_key и token), через пробел или запятую. Ничего не изменил."
            )
        self.cfg["backpacktf_accounts"] = accounts
        # Primary account (backpacktf_api_key/token) is ALWAYS included
        # alongside whatever /setaccounts adds - same as __init__ builds
        # the pool at startup. Without this, the account this project
        # was originally configured with would simply stop being used
        # the moment /setaccounts is run - a real loss of throughput,
        # not the intended gain, if the person doesn't think to re-paste
        # their original credentials into the new list too.
        primary = {"api_key": self.cfg["backpacktf_api_key"], "token": self.cfg.get("backpacktf_token", "")}
        pool_accounts = [primary] + accounts
        max_concurrent = max(self.cfg.get("bptf_max_concurrent_requests", 4), len(pool_accounts))
        bptf_client.configure_request_pacing(
            pool_accounts, max_concurrent, self.cfg.get("bptf_min_request_interval_seconds", 11.0),
        )
        self._save_accounts(accounts)
        return (
            f"Настроено дополнительных аккаунтов: {len(accounts)} (плюс основной - итого "
            f"{len(pool_accounts)} в пуле). Пул запросов и лимит одновременных запросов "
            f"обновлены сразу.\n"
            f"Для проактивного сканирования Unusual (см. /stats) число воркеров тоже "
            f"вырастет до {len(pool_accounts)}, но только после перезапуска службы - "
            f"systemctl restart tf2-deal-watcher."
        )

    def _format_unknown_effects(self) -> str:
        """
        /unknowneffects - lists every Unusual particle effect id seen
        since the bundled database (unusual_effects.py) was last
        updated, that isn't in it (or the live schema) yet - see the
        discovery logic in handle_bptf_event. Lets the database grow
        from real observed trading over time, rather than only from
        periodic manual research.
        """
        if not self._unknown_particle_ids:
            return "Пока не встречалось ни одного неизвестного эффекта Unusual."
        lines = [f"Неизвестных эффектов: {len(self._unknown_particle_ids)}\n"]
        for particle_id, info in sorted(self._unknown_particle_ids.items(), key=lambda kv: -kv[1]["count"]):
            name_bit = f" — похоже на {info['name_hint']!r}" if info.get("name_hint") else ""
            lines.append(
                f"ID {particle_id}{name_bit} (сырое поле: {info.get('raw_particle_name')!r}), "
                f"впервые на {info.get('item_name')!r} [{info.get('quality')}], "
                f"встречалось {info.get('count')} раз(а)"
            )
        lines.append(
            "\nЧтобы добавить: впиши точное название эффекта и это ID в NAME_TO_ID "
            "в unusual_effects.py."
        )
        return "\n".join(lines)

    def _check_item(self, name_query: str) -> str:
        """
        /checkitem <name> - shows exactly what the local store currently
        holds for a given item, by name, instead of requiring aggregate
        /stats numbers to be interpreted to guess where one specific
        expected alert went missing.
        """
        name_query = name_query.strip()
        if not name_query:
            return "Укажи название предмета: /checkitem Max's Severed Head"

        query_lower = name_query.lower()
        matched_names = [n for n in self._name_to_identity_keys if query_lower in n]
        if not matched_names:
            return (
                f"Бот ещё ни разу не видел ни одного события (buy или sell) с названием, "
                f"содержащим {name_query!r}, с момента последнего запуска."
            )

        store = self.bptf.local_listings
        lines = []
        for matched_name in sorted(matched_names)[:5]:
            for key in self._name_to_identity_keys[matched_name]:
                (_, quality_name, particle_id, paint_dec, craftable, spell, ks_tier, australium,
                 texture, killstreaker, sheen) = key
                bits = [quality_name]
                if not craftable:
                    bits.append("Non-Craftable")
                if ks_tier:
                    bits.append(f"KS-tier {ks_tier}")
                if australium:
                    bits.append("Australium")
                if particle_id is not None:
                    bits.append(f"particle={particle_id}")
                if paint_dec is not None:
                    bits.append(f"paint={paint_dec}")
                if spell:
                    bits.append(f"spell={spell}")
                if texture:
                    bits.append(f"grade={texture}")
                if killstreaker:
                    bits.append(f"killstreaker={killstreaker}")
                if sheen:
                    bits.append(f"sheen={sheen}")
                variant_desc = ", ".join(bits)

                buy_price, buy_count = store.get_max_buy_price(key)
                sell_price, sell_count = store.get_min_sell_price(key)
                now = time.time()
                entries = store.get_all_entries(key)
                buy_ages = [now - e["ts"] for e in entries if e["intent"] == "buy"]
                sell_ages = [now - e["ts"] for e in entries if e["intent"] == "sell"]

                buy_line = (
                    f"💰 Buy order: {buy_price:.2f} ключей ({buy_count} шт., самый свежий "
                    f"{min(buy_ages)/60:.0f} мин назад)" if buy_price is not None
                    else f"💰 Buy order: нет в пределах {bptf_client.LocalListingStore.BUY_ORDER_SAFETY_NET_SECONDS // 3600}ч"
                    + (f" (есть {len(buy_ages)} более старых, самый свежий {min(buy_ages)/60:.0f} мин назад)" if buy_ages else " (не видели вообще)")
                )
                sell_line = (
                    f"🏷 Sell: {sell_price:.2f} ключей ({sell_count} шт., самый свежий "
                    f"{min(sell_ages)/60:.0f} мин назад)" if sell_price is not None
                    else "🏷 Sell: нет свежих данных" + (f" (есть {len(sell_ages)} старых)" if sell_ages else "")
                )
                lines.append(f"<b>{matched_name}</b> [{variant_desc}]\n{buy_line}\n{sell_line}")

        header = f"Найдено вариантов: {sum(len(self._name_to_identity_keys[n]) for n in matched_names)}"
        if len(matched_names) > 5:
            header += f" (показаны первые 5 из {len(matched_names)} совпавших названий)"
        return header + "\n\n" + "\n\n".join(lines)

    async def send_deal(self, deal):
        if "#attrib" in deal["display_name"].lower():
            # Safety-net diagnostic: if the #Attrib_Particle strip in
            # handle_bptf_event ever misses a case, logs the full deal
            # dict so the next occurrence is diagnosable from real data.
            log.warning(
                "DIAGNOSTIC SAMPLE (display_name still contains an unstripped artifact "
                "after clean_display_name) - full deal dict: %r", deal,
            )
        # Per-SELLER cooldown: don't re-alert on the same item from the
        # SAME seller within a short window, but a DIFFERENT seller's
        # listing of the same item is a genuinely different opportunity.
        # Scoping the cooldown to just the item type (no seller) was
        # wrong - it silently suppressed every other seller's listing of
        # the same item for a full hour after the first one alerted.
        identity_key = (
            deal["source"], deal.get("seller_id"), deal["display_name"],
            deal.get("particle_name"), deal.get("paint"), deal.get("killstreaker"), deal.get("sheen"),
        )
        now = time.time()
        cooldown_seconds = self.cfg.get("item_type_cooldown_minutes", 60) * 60
        last_alerted = self.item_type_last_alerted.get(identity_key)
        if last_alerted is not None and (now - last_alerted) < cooldown_seconds:
            log.info(
                "Suppressing repeat alert for %s (%s, seller %s) - alerted %.0f min ago, within "
                "the %d min per-seller cooldown.",
                deal["display_name"], deal["source"], deal.get("seller_id"), (now - last_alerted) / 60,
                self.cfg.get("item_type_cooldown_minutes", 60),
            )
            self.stats["suppressed_item_cooldown"] += 1
            return
        # Surfaced in the alert itself (see format_alert's history_line) -
        # not just used internally for the cooldown above. Genuinely
        # "first time" only when this exact seller+item combo has never
        # been seen before at all (last_alerted was None going into the
        # check above) - a re-list that passed the cooldown because
        # enough time had elapsed is a real re-list, not a first sighting.
        deal["is_first_time_seller_item"] = last_alerted is None

        log.info(
            "DEAL [%s]: %s - %.2f keys vs %s keys buy order (%.0f%% off)",
            deal["source"], deal["display_name"],
            deal["price_keys"],
            f"{deal['previous_low_keys']:.2f}" if deal["previous_low_keys"] is not None else "?",
            deal["discount_percent"],
        )
        alert_text = self.format_alert(deal)
        keyboard = self._build_alert_keyboard(deal)
        sent_message_id = await self._run_telegram(self.telegram.send, alert_text, keyboard)
        if sent_message_id is not None:
            # Cooldown recorded only on a CONFIRMED send - recording it
            # unconditionally meant a failed send (e.g. Telegram
            # rejecting malformed HTML) still blocked every retry for
            # this seller+item for the full cooldown window even though
            # nothing was actually received.
            self.item_type_last_alerted[identity_key] = now
        else:
            log.warning(
                "Telegram send failed for %s (%s, seller %s) - cooldown NOT recorded, "
                "so the next matching event can retry.",
                deal["display_name"], deal["source"], deal.get("seller_id"),
            )

    # -- backpack.tf side ---------------------------------------------------

    async def handle_bptf_event(self, payload: dict):
        if payload.get("_bptf_event_type") == "delete":
            # Processed regardless of pause state - keeping the local
            # store accurate is a separate concern from whether alerts
            # are currently being sent, and costs nothing to do anyway.
            listing_id = payload.get("id")
            if listing_id is not None:
                # Dispatched to a thread - remove_listing() takes the
                # same lock record() does, so this must never run
                # directly on the event loop.
                await asyncio.to_thread(self.bptf.local_listings.remove_listing, str(listing_id))
            return

        if self.runtime.paused:
            return
        self.stats["bptf_received"] += 1

        listing_id = payload.get("id")
        if listing_id is None:
            return
        # Dedup key includes the raw currencies, not just the listing id -
        # a price CHANGE on an already-seen listing id must still be
        # evaluated fresh. This matters even if backpack.tf's "bump"
        # normally keeps the same listing id: a real forum thread from
        # their own community confirms sellers deliberately delete+relist
        # (or otherwise update in place) specifically TO change price -
        # "if that relisting involves a price change" is discussed as a
        # known, real scenario, not a hypothetical. Deduping by id alone
        # would silently swallow exactly that kind of price-drop event.
        currencies = payload.get("currencies") or {}
        price_fingerprint = tuple(sorted(currencies.items()))
        if not self._mark_seen(f"bptf:{listing_id}:{price_fingerprint}"):
            self.stats["bptf_deduped"] += 1
            return

        item = bptf_client.safe_dict(payload.get("item"))
        quality_obj = bptf_client.safe_dict(item.get("quality"))
        quality = quality_obj.get("name")

        name = item.get("name") or item.get("marketName") or item.get("baseName")
        if not name:
            return
        # backpack.tf's own particle-name resolution occasionally fails,
        # leaving a raw internal token in the name instead of (or
        # alongside) the real effect name, e.g. "Sakura Smoke Bomb Blast
        # Bowl (#Attrib_Particle288)". Stripped here since an unreliable
        # suffix like this risks an identity-key mismatch, not just an
        # ugly display. Matches "#Attrib_Particle" anywhere (not a fixed
        # leading substring) since this project has hit non-ASCII
        # whitespace in other text fields before.
        if "#Attrib_Particle" in name:
            name = name.split("#Attrib_Particle")[0].rstrip("(").rstrip()

        # "Australium only" is ADDITIVE, not exclusive - same logic and
        # reasoning as is_watched() in matcher.py (kept there too, since
        # that function is shared by every source; this is a fast-path
        # duplicate for the highest-volume path). Real, confirmed gap
        # this closes: this early quality/category check used to have
        # NO such exception, so an Australium weapon was rejected right
        # here - before the name was even available to check for the
        # "Australium " prefix - whenever Strange/weapon weren't already
        # separately watched, regardless of the toggle.
        is_australium_weapon_hint = quality == "Strange" and name.startswith("Australium ")
        covered_by_australium_toggle = self.runtime.australium_only and is_australium_weapon_hint

        # Unusual is unconditionally watched - never toggleable via
        # /addquality-/removequality, unlike every other quality.
        if not covered_by_australium_toggle:
            if quality != "Unusual" and quality not in self.runtime.watched_qualities:
                self.stats["bptf_rejected_quality"] += 1
                return

        # Rejected here, before ever being recorded, not just later in
        # evaluate_listing's own excluded_types filter - an explicitly
        # banned item's data was otherwise still taking up store space
        # it could never be used for anyway.
        name_lower_for_exclusion = name.lower()
        if any(excluded.lower() in name_lower_for_exclusion
               for excluded in self.cfg.get("excluded_types", [])):
            self.stats["bptf_rejected_excluded_type"] += 1
            return

        # Base item's own numeric schema ID - preferred identity anchor
        # over name text, see listing_identity_key's own docstring.
        defindex = item.get("defindex")

        # Action items excluded unconditionally (not via watched_
        # categories, so re-enabling by mistake can't undo it) - they
        # have their own dedicated equip slot ("action"), a reliable
        # structural signal. Tool/Craft Item/Strangifier/Crate/Party
        # Favor don't have an equally reliable single field for this
        # data source, so those go through config.py's excluded_types
        # (name-text match) instead.
        if item.get("slot") == "action":
            self.stats["bptf_rejected_category"] += 1
            return

        # Checked here, alongside quality above, before the more
        # expensive particle/paint/spell/killstreak extraction below -
        # mirrors is_watched()'s own category check in matcher.py (kept
        # there too, since that function is shared by every source) -
        # this is just a fast-path duplicate for the highest-volume path.
        category = classify_category(name, item.get("slot"))
        if not covered_by_australium_toggle and category not in self.runtime.watched_categories:
            self.stats["bptf_rejected_category"] += 1
            return

        # Computed here, early, alongside category above, before the
        # expensive extraction below - nothing about this depends on
        # that extraction. Reuses `currencies`, already extracted above
        # for the dedup fingerprint.
        price_keys = self.bptf.currencies_to_keys(currencies)
        price_usd = None
        if price_keys is None and currencies.get("usd") and self.mannco_key_usd_cents:
            price_usd = float(currencies["usd"])
            price_keys = price_usd / (self.mannco_key_usd_cents / 100)
        elif price_keys is not None and self.mannco_key_usd_cents:
            price_usd = price_keys * (self.mannco_key_usd_cents / 100)
        if price_keys is None:
            return

        intent = payload.get("intent")
        # min/max price only makes sense against a SELL listing's own
        # asking price, never a buy order's - a buy order outside this
        # range is still exactly the reference data a later in-range
        # sell listing needs. Checked early (before the expensive
        # extraction below), gated on intent == "sell" specifically for
        # correctness, not just style. Still checked in matcher.py's
        # evaluate_listing too, as a backstop for other sources.
        if intent == "sell":
            if price_keys < self.runtime.min_price_keys:
                self.stats["bptf_rejected_price"] += 1
                return
            if self.runtime.max_price_keys is not None and price_keys > self.runtime.max_price_keys:
                self.stats["bptf_rejected_price"] += 1
                return

        if quality == "Unusual" and "bptf_unusual" not in self._sampled_item_kinds:
            self._sampled_item_kinds.add("bptf_unusual")
            log.warning(
                "DIAGNOSTIC SAMPLE (first Unusual seen this run) - raw item fields relevant to "
                "particle extraction: particle=%r, particleId=%r, attributes=%r",
                item.get("particle"), item.get("particleId"), item.get("attributes"),
            )

        particle_obj = bptf_client.safe_dict(item.get("particle"))
        particle_id = particle_obj.get("id")
        particle_name = particle_obj.get("name")

        if particle_id is None:
            # Fallback: flat field names.
            particle_id = item.get("particleId") or item.get("particle_id")
        if particle_id is None:
            # Fallback: raw attributes, defindex 134 = "attach particle
            # effect", value = the particle id.
            for attr in (item.get("attributes") or []):
                if isinstance(attr, dict) and attr.get("defindex") == 134:
                    raw_value = attr.get("value", attr.get("float_value"))
                    try:
                        particle_id = int(raw_value) if raw_value is not None else None
                    except (TypeError, ValueError):
                        particle_id = None
                    break
        if particle_id is None and quality == "Unusual":
            # Last resort: the websocket stream doesn't always include a
            # usable particle id via any of the fields above, even for
            # common, long-established effects. The item's own `name`
            # text still carries the effect as a literal prefix though
            # (backpack.tf bakes this in unconditionally) - matched
            # against the bundled effect list, picking the LONGEST
            # matching name so a short one never wins over a longer one
            # sharing the same first word ("Stardust" vs "Stardust
            # Pathway").
            _, particle_id, _ = bptf_client.find_effect_prefix(name)
        if particle_id is not None:
            # Prefer the schema's canonical name over the raw payload's
            # own name for this event - the raw one can vary in
            # phrasing/casing between two events for the SAME effect,
            # which would make strip_effect_prefix() below succeed for
            # one and silently fail for the other, splitting one effect
            # into two different `name` values that never match in the
            # identity key even though particle_id agrees. Falls back to
            # the raw name only for an id not yet in the bundled data.
            particle_name = self.particle_id_to_name.get(particle_id) or particle_name

            if particle_id not in self.particle_id_to_name:
                # Not in the bundled database (unusual_effects.py) or the
                # live schema - per explicit request, tracked here so the
                # bundled database can grow from what this project
                # actually observes trading, not just manual research.
                # Saved to disk only on first discovery of a given id -
                # a common unknown effect re-appearing many times updates
                # the in-memory count for context, but doesn't need a
                # disk write every single time.
                if particle_id not in self._unknown_particle_ids:
                    name_hint = particle_name if particle_name and not particle_name.startswith("#") else None
                    self._unknown_particle_ids[particle_id] = {
                        "name_hint": name_hint,
                        "raw_particle_name": particle_name,
                        "item_name": name,
                        "quality": quality,
                        "count": 1,
                        "first_seen": time.time(),
                    }
                    log.warning(
                        "DISCOVERED an Unusual effect id (%d) not in the bundled database - "
                        "name hint: %r, raw field: %r, seen on item: %r. See /unknowneffects "
                        "in Telegram, or unusual_effects.py's own docstring, to add it once "
                        "its real name is confirmed.",
                        particle_id, name_hint, particle_name, name,
                    )
                    await asyncio.to_thread(self._save_unknown_effects)
                else:
                    self._unknown_particle_ids[particle_id]["count"] += 1

        if particle_name and particle_name.startswith("#"):
            # item.particle.name is sometimes itself an unresolved raw
            # token ("#Attrib_ParticleNN"), not a display name - treated
            # as no resolved name at all, not shown to anyone.
            particle_name = None

        if particle_name:
            # item.name already includes the effect as a display prefix
            # ("Circling Heart Hot Dogger") - stripped here once so every
            # downstream use (display name, identity key, search link)
            # sees the same clean name. Left in, the effect would show
            # twice AND the search link would search for a nonexistent
            # item name while also passing particle= separately.
            name = bptf_client.strip_effect_prefix(name, particle_name)

        spells = [s.get("name") for s in (item.get("spells") or []) if isinstance(s, dict) and s.get("name")]
        # Same filter matcher.py's own evaluate_listing applies
        # (filter_spells_for_category) - keeps the recording side and
        # the lookup side from ever disagreeing about which bucket a
        # listing with a category-impossible spell belongs in.
        spells = matcher.filter_spells_for_category(spells, category)
        strange_parts = [p.get("name") for p in (item.get("strangeParts") or [])
                          if isinstance(p, dict) and p.get("name")]

        # Game-logic guard: paint only exists on cosmetics, killstreak
        # tiers only exist on weapons. `slot` tells us which is which -
        # if it's missing/unrecognised we drop both rather than guess.
        slot = item.get("slot")
        paint_obj = item.get("paint") or {}
        raw_paint = paint_obj.get("name") if isinstance(paint_obj, dict) else None
        paint = raw_paint if slot in COSMETIC_SLOTS else None

        # Exact RGB decimal straight from the source (color: '#b8383b'
        # decodes to Team Spirit RED exactly) - no RED/BLU guessing
        # needed for team-coloured paints.
        paint_decimal_hint = None
        if paint and slot in COSMETIC_SLOTS:
            raw_color = paint_obj.get("color") if isinstance(paint_obj, dict) else None
            if isinstance(raw_color, str):
                try:
                    paint_decimal_hint = int(raw_color.lstrip("#"), 16)
                except ValueError:
                    paint_decimal_hint = None

        if slot == "medal" and "bptf_medal_slot" not in self._sampled_item_kinds:
            self._sampled_item_kinds.add("bptf_medal_slot")
            log.warning("DIAGNOSTIC SAMPLE (first slot='medal' item seen this run) - name: %r", item.get("name"))

        if paint and paint in bptf_client.TEAM_COLOR_PAINT_RGB and "bptf_team_paint" not in self._sampled_item_kinds:
            self._sampled_item_kinds.add("bptf_team_paint")
            log.warning(
                "DIAGNOSTIC SAMPLE (first team-coloured paint seen this run, %r) - "
                "raw paint object: %r",
                paint, paint_obj,
            )
        raw_killstreak_tier = item.get("killstreakTier")
        killstreak_tier = raw_killstreak_tier if slot in WEAPON_SLOTS else None

        # Only Professional Killstreak (tier 3) is watched, plus plain
        # (tier 0) - tier 1/2 excluded. Checked early to skip the more
        # expensive parsing below for a tier that's never watched.
        if killstreak_tier in (1, 2):
            self.stats["bptf_rejected_category"] += 1
            return
        # Killstreak Kits/Fabricators have no weapon slot (killstreak_tier
        # is always None for them), so the same restriction applies by
        # name instead: only "Professional Killstreak ... Kit/Fabricator"
        # passes.
        if "Killstreak" in name and (name.endswith("Kit") or name.endswith("Fabricator")):
            if not name.startswith("Professional Killstreak "):
                self.stats["bptf_rejected_category"] += 1
                return

        # Killstreaker/sheen only exist on weapons with a killstreak tier
        # (sheen from tier 2+, killstreaker from tier 3 only - see
        # VALID_KILLSTREAKERS/VALID_SHEENS). Comes back None if the field
        # path guess is wrong, same as any other uncertain field here.
        killstreaker = None
        sheen = None
        if slot in WEAPON_SLOTS and killstreak_tier:
            if "bptf_killstreak_tier3" not in self._sampled_item_kinds and killstreak_tier >= 2:
                self._sampled_item_kinds.add("bptf_killstreak_tier3")
                log.warning(
                    "DIAGNOSTIC SAMPLE (first tier-2+ Killstreak weapon seen this run) - raw item "
                    "fields relevant to killstreaker/sheen extraction: killstreaker=%r, sheen=%r",
                    item.get("killstreaker"), item.get("sheen"),
                )
            killstreaker_obj = bptf_client.safe_dict(item.get("killstreaker"))
            raw_killstreaker = killstreaker_obj.get("name") if isinstance(killstreaker_obj, dict) else None
            if killstreak_tier >= 3 and raw_killstreaker in bptf_client.VALID_KILLSTREAKERS:
                killstreaker = raw_killstreaker
            sheen_obj = bptf_client.safe_dict(item.get("sheen"))
            raw_sheen = sheen_obj.get("name") if isinstance(sheen_obj, dict) else None
            if killstreak_tier >= 2 and raw_sheen in bptf_client.VALID_SHEENS:
                sheen = raw_sheen

        seller_steamid = payload.get("steamid")

        # wearTier (Factory New..Battle-Scarred) applies to any painted
        # weapon skin - both plain War Paint AND "Decorated Weapon"
        # quality (the Smissmas garland variant). These are NOT the same
        # thing and must not be treated alike: War Paint skins are
        # excluded (pattern seed can swing value by an order of
        # magnitude, data this project doesn't have), but Decorated
        # Weapons are explicitly wanted - excluding them here was a real,
        # confirmed mistake, conflating "has wearTier" with "is a plain
        # War Paint" when it isn't the same test. texture alone, WITHOUT
        # wearTier, means a graded COSMETIC (Civilian..Elite rarity
        # grade) - a separate, valuable category, distinct from both of
        # the above. Grade/texture is threaded into the identity key
        # (see listing_identity_key) rather than read from the item's
        # display name - some real items' names carry a different
        # grade's word than their true one.
        is_skin = bool(item.get("wearTier")) and quality != "Decorated Weapon"
        # Grade (Civilian..Elite rarity) - a real, confirmed field-name
        # correction: a third-party backpack.tf API wrapper's own
        # documented listing-object fields list "quality", "particle
        # effect", "rarity", "origin", "wear_tier", "killstreaker",
        # "strange_part" - "rarity" is the grade field, not "texture" as
        # this project assumed without ever confirming it against a
        # documented source. Reading only "texture" meant a graded
        # cosmetic's actual grade was never captured at all, pooling
        # every grade of the same item into one comparison - a name
        # collision this project has hit before. Checks "rarity" first,
        # "texture" as a fallback in case backpack.tf's real payload
        # still separately carries a War Paint pattern name under that
        # key for some item types.
        grade_obj = item.get("rarity") or item.get("texture")
        texture = grade_obj.get("name") if isinstance(grade_obj, dict) else grade_obj
        # Derived from the NAME TEXT, not the separate item.craftable
        # field - the two disagreed for some listings, so the displayed
        # name said "Non-Craftable X" while the buy order comparison
        # used craftable=True. backpack.tf's name text reliably carries
        # "Non-Craftable " as a literal prefix when applicable.
        craftable = not name.startswith("Non-Craftable ")

        # Records EVERY listing seen (both sell and buy intent) into the
        # self-collected local store - see LocalListingStore's own
        # docstring for why this replaced the deprecated snapshot
        # endpoint. Happens for every listing, not just qualifying
        # deals - a listing that isn't a bargain is still comparison
        # data a later listing of the same item needs.
        paint_value_for_identity = paint_decimal_hint if paint_decimal_hint is not None else (
            bptf_client.paint_rgb_decimal(paint) if paint else None
        )
        identity_key = bptf_client.listing_identity_key(
            name, quality, particle_id, paint_value_for_identity, bool(craftable),
            # ALL spells, sorted - not just the first. A real, confirmed
            # case: a two-spell item's buy order backing a one-spell (or
            # spell-less) sell listing's alert, since this used to track
            # only spells[0] - a second spell adds real value on its
            # own, so two items agreeing on spell #1 but differing on
            # whether a second exists were wrongly treated as identical.
            tuple(sorted(spells)) if spells else None, killstreak_tier, name.startswith("Australium "),
            texture=texture, defindex=defindex, killstreaker=killstreaker, sheen=sheen,
        )
        self._name_to_identity_keys[name.lower()].add(identity_key)
        # excluded_types already rejected above - anything reaching here
        # is guaranteed not excluded.
        is_currently_watched_quality = quality == "Unusual" or quality in self.runtime.watched_qualities
        scan_key = (name, quality)
        if is_currently_watched_quality and scan_key not in self._known_scan_items:
            # Hard cap, FIFO-evicted - see this dict's own comment in
            # __init__.
            if len(self._known_scan_items) >= self.MAX_KNOWN_SCAN_ITEMS:
                oldest_key = next(iter(self._known_scan_items))
                del self._known_scan_items[oldest_key]
            self._known_scan_items[scan_key] = {"ts": 0.0, "category": category}
        # Dispatched to a thread, not called directly on the event loop -
        # record() takes a plain threading.Lock shared with every
        # evaluate_listing() read too, and this function runs directly
        # on the asyncio event loop - if that lock were ever held by a
        # worker thread, this call would block the WHOLE event loop
        # (including Telegram's own command dispatch) waiting for it.
        await asyncio.to_thread(
            self.bptf.local_listings.record,
            identity_key, str(listing_id), seller_steamid, price_keys, intent,
        )

        if intent != "sell":
            # Buy-intent listings only feed the local store above -
            # nothing to alert on for an offer to BUY something, only
            # comparison data for pricing SELL listings later.
            self.stats["bptf_buy_recorded"] += 1
            return

        # Direct, pre-filled Steam trade-offer link to the seller, when
        # backpack.tf exposes one publicly - the "view on backpack.tf"
        # link is built separately in matcher.py for every deal.
        link = bptf_client.safe_dict(payload.get("user")).get("tradeOfferUrl")

        listing = matcher.NormalizedListing(
            source="backpack.tf",
            listing_id=str(listing_id),
            name=name,
            quality=quality,
            category=category,
            particle_id=particle_id,
            particle_name=particle_name,
            craftable=bool(craftable),
            price_keys=price_keys,
            price_usd=price_usd,
            link=link,
            extra_excluded_hint=is_skin,
            texture=texture,
            defindex=defindex,
            spells=spells,
            strange_parts=strange_parts,
            paint=paint,
            seller_steamid=seller_steamid,
            killstreak_tier=killstreak_tier,
            killstreaker=killstreaker,
            sheen=sheen,
            # "details" is at the LISTING level, not nested under "item".
            seller_note=(payload.get("details") or "").strip() or None,
            paint_decimal_hint=paint_decimal_hint,
        )

        self.stats["bptf_evaluated"] += 1
        deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg(), self.stats)
        if not deal:
            self.stats["bptf_rejected_by_checks"] += 1
            return
        self.stats["bptf_alerts"] += 1

        # Only bother checking inventory privacy for a deal that already
        # qualifies (same "expensive check only when it matters"
        # principle as the average-price lookup). Doesn't skip the alert
        # any more - a private inventory just means the trade-offer link
        # won't work, not that the deal itself is fake, so it's shown
        # with a clear warning and a link to the seller's profile instead
        # (you can still message them, or just judge for yourself).
        # Unknown (request failed / rate-limited) is treated as public -
        # better an occasional dead link than a wrongly-flagged good one.
        deal["trade_closed"] = False
        if seller_steamid:
            is_public = await asyncio.to_thread(self.inventory_checker.is_public, seller_steamid)
            if is_public is False:
                deal["trade_closed"] = True
                deal["profile_link"] = f"https://steamcommunity.com/profiles/{seller_steamid}"

        await self.send_deal(deal)

    # -- Telegram commands & button menu -------------------------------------

    async def telegram_command_loop(self):
        listener = telegram_commands.TelegramCommandListener(
            self.cfg["telegram_bot_token"], self.cfg["telegram_chat_id"]
        )
        await self._run_telegram(self.telegram.register_commands, telegram_commands.BOT_COMMANDS)
        log.info("Listening for Telegram commands and button taps...")

        while True:
            try:
                # Uses the SAME dedicated small thread pool as
                # _run_telegram's own sends, not the shared default
                # pool - get_updates() is a long-polling call occupying
                # a worker thread for the whole poll cycle, in a tight
                # while True loop, so if it shared the same pool as
                # evaluate_listing under real event volume, polling
                # couldn't get a thread promptly and the bot looked
                # totally unresponsive without actually crashing.
                loop = asyncio.get_running_loop()
                executor = getattr(self, "_telegram_executor", None)
                if executor is not None:
                    events = await loop.run_in_executor(executor, listener.get_updates)
                else:
                    events = await asyncio.to_thread(listener.get_updates)
            except Exception:
                log.exception("Telegram command polling error, retrying in 5s...")
                await asyncio.sleep(5)
                continue

            for event in events:
                try:
                    await self._handle_telegram_event(event)
                except Exception:
                    log.exception("Error handling Telegram event %r", event)

    async def _handle_telegram_event(self, event: dict):
        if event["type"] == "message":
            text = event["text"]
            if not text.startswith("/"):
                return
            command = text.split(maxsplit=1)[0].lower().lstrip("/").split("@")[0]
            if command in ("menu", "settings", "start"):
                menu_text, keyboard = telegram_commands.build_main_menu(self.runtime)
                await self._run_telegram(self.telegram.send, menu_text, keyboard)
            elif command == "errors":
                # Sent WITH a keyboard (pagination buttons) - unlike the
                # other typed commands below, which are plain text.
                errors_text, keyboard = telegram_commands.build_errors_view(_error_buffer.recent(100))
                await self._run_telegram(self.telegram.send, errors_text, keyboard)
            elif command == "checkitem":
                # Dispatched to a thread - a real, confirmed risk found
                # during a systematic Telegram-load sweep: this scans
                # self._name_to_identity_keys (unbounded, slowly growing
                # for the whole process lifetime) AND reads store._entries
                # directly (bypassing LocalListingStore's own lock
                # entirely, unlike every other read path in this
                # project) - both were running synchronously on the
                # event loop itself, the same event-loop-blocking risk
                # already fixed for record()/remove_listing() above.
                reply = await asyncio.to_thread(
                    self._check_item, text.split(maxsplit=1)[1] if " " in text else ""
                )
                await self._run_telegram(self.telegram.send, reply)
            elif command == "unknowneffects":
                reply = self._format_unknown_effects()
                await self._run_telegram(self.telegram.send, reply)
            elif command == "setaccounts":
                raw = text.split(maxsplit=1)[1] if " " in text or "\n" in text else ""
                reply = self._set_accounts(raw)
                await self._run_telegram(self.telegram.send, reply)
            else:
                reply = telegram_commands.handle_command(
                    text, self.runtime,
                    stats=self.stats, stats_since=self.stats_since,
                    currently_rate_limited=bptf_client.is_rate_limited(),
                )
                log.info("Telegram command: %r -> %s", text, reply.splitlines()[0])
                await self._run_telegram(self.telegram.send, reply)
                if command == "stats":
                    # Each /stats read shows "since last time you checked",
                    # not a lifetime total - reset right after sending so
                    # the next read starts a fresh window.
                    self.stats = collections.Counter()
                    self.stats_since = time.time()

        elif event["type"] == "callback_query":
            menu_text, keyboard = telegram_commands.handle_callback(
                event["data"], self.runtime, error_entries=_error_buffer.recent(100)
            )
            log.info("Telegram button: %r", event["data"])
            # Answer first so Telegram clears the button's loading spinner
            # even if the edit below is slow or fails.
            await self._run_telegram(self.telegram.answer_callback_query, event["id"])
            if event["message_id"] is not None:
                await self._run_telegram(self.telegram.edit_message, event["message_id"], menu_text, keyboard)

    async def _startup_sanity_check(self):
        """
        Checks a few core assumptions right after startup and sends an
        immediate Telegram warning if any look wrong, rather than waiting
        for someone to notice bad alerts. E.g. key_price_metal ending up
        None would silently break every metal->keys conversion.
        """
        problems = []

        if self.bptf.key_price_metal is None:
            problems.append(
                "key_price_metal не определился вообще - конвертация металл→ключи "
                "сломана для ВСЕХ предметов в металле."
            )
        elif not (10 <= self.bptf.key_price_metal <= 300):
            problems.append(
                f"key_price_metal выглядит неправдоподобно ({self.bptf.key_price_metal:.2f} "
                f"металла за ключ, обычно 30-80) - возможно, парсинг цены сломан."
            )

        if not self.mannco_key_usd_cents:
            problems.append("Цена ключа с mannco.store не определилась - конвертация цен оттуда сломана.")

        if not problems:
            log.info("Startup sanity check: all core values look reasonable.")
            return

        log.warning("Startup sanity check found %d issue(s): %s", len(problems), "; ".join(problems))
        message = "⚠️ <b>Проверка при запуске нашла возможные проблемы:</b>\n\n" + "\n\n".join(
            f"• {p}" for p in problems
        )
        await self._run_telegram(self.telegram.send, message)

    async def run(self):
        # Telegram gets its OWN small, dedicated thread pool, separate
        # from the default one asyncio.to_thread() uses for everything
        # else (evaluate_listing, Steam inventory checks, etc.) - a
        # shared pool let Telegram commands queue up behind listing
        # evaluations under load. A big shared pool alone isn't the fix
        # either: 200 real OS threads was too heavy on a small VPS. A
        # small dedicated pool for Telegram (it only needs a handful of
        # threads) solves the lag without the shared pool needing to be
        # huge - see _run_telegram below.
        self._telegram_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        # Restores whatever the local listing store had saved before -
        # comparison data available, not an empty store. Cleans up any
        # orphaned temp files from a prior run's interrupted save first
        # (see cleanup_stray_temp_files' own docstring for why these
        # accumulate) - harmless to load_from_disk either way, just
        # tidiness, done once here rather than leaving it to accumulate
        # further.
        bptf_client.LocalListingStore.cleanup_stray_temp_files(bptf_client.LOCAL_LISTINGS_STATE_PATH)
        await asyncio.to_thread(self.bptf.local_listings.load_from_disk, bptf_client.LOCAL_LISTINGS_STATE_PATH)
        self._load_alert_cooldowns()

        log.info("Starting up: loading initial prices...")
        try:
            await asyncio.to_thread(self.refresh_prices)
        except Exception:
            # A transient hiccup here shouldn't take down the whole
            # process - price_refresh_loop below retries on its own
            # schedule anyway, so it's fine to start the websocket
            # listeners now and let prices catch up shortly after.
            log.exception("Initial price load failed - continuing anyway, will retry on schedule.")

        try:
            # Fetched once immediately here too (not just on
            # key_price_refresh_loop's own, much slower schedule) so a
            # price is available right away at startup, rather than
            # leaving mannco_key_usd_cents unset for up to a week.
            await asyncio.to_thread(self.refresh_mannco_key_price)
        except Exception:
            log.exception("Initial mannco.store key price load failed - continuing anyway, will retry on schedule.")

        await self._startup_sanity_check()

        # Graceful-shutdown handling: saves the local listing store the
        # MOMENT the process is asked to stop, not just on
        # local_store_snapshot_loop's own 90-second timer - a restart
        # landing before that loop's first save would otherwise lose
        # everything collected since startup. SIGTERM (systemctl
        # restart/stop) and SIGINT (Ctrl+C) both trigger one last save.
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except NotImplementedError:
                # add_signal_handler isn't available on every platform
                # (e.g. Windows) - not expected for this project's real
                # deployment target (a Linux VPS under systemd), but
                # shouldn't crash startup if it's ever run somewhere it
                # isn't supported. KeyboardInterrupt still works via the
                # try/except around asyncio.run() in main() either way.
                pass

        main_tasks = asyncio.gather(
            self.price_refresh_loop(),
            self.key_price_refresh_loop(),
            self.telegram_command_loop(),
            self.health_check_loop(),
            self.local_store_prune_loop(),
            self.alert_cooldowns_prune_loop(),
            self.local_store_snapshot_loop(),
            self.proactive_buy_order_refresh_loop(),
            bptf_ws.stream_listing_events(self.handle_bptf_event),
        )
        shutdown_waiter = asyncio.create_task(shutdown_event.wait())

        await asyncio.wait([main_tasks, shutdown_waiter], return_when=asyncio.FIRST_COMPLETED)

        if shutdown_event.is_set():
            log.info("Shutdown signal received - saving local listing store before exiting...")
            try:
                await asyncio.to_thread(self.bptf.local_listings.save_to_disk, bptf_client.LOCAL_LISTINGS_STATE_PATH)
                await asyncio.to_thread(self._save_alert_cooldowns)
                log.info("Final save complete.")
            except Exception:
                log.exception("Final save on shutdown failed.")
            main_tasks.cancel()
            try:
                await main_tasks
            except (asyncio.CancelledError, Exception):
                pass
        else:
            # main_tasks finished first, not from a shutdown signal -
            # one of the (normally `while True`) loops actually raised.
            # Same emergency save a real shutdown would do, then let the
            # exception propagate - systemd's restart policy needs the
            # process to actually exit with the failure visible.
            shutdown_waiter.cancel()
            log.warning("A core loop exited unexpectedly - saving local listing store before the crash propagates...")
            try:
                await asyncio.to_thread(self.bptf.local_listings.save_to_disk, bptf_client.LOCAL_LISTINGS_STATE_PATH)
                await asyncio.to_thread(self._save_alert_cooldowns)
            except Exception:
                log.exception("Emergency save before crash failed.")
            await main_tasks


def main():
    cfg = load_config()
    watcher = Watcher(cfg)
    try:
        asyncio.run(watcher.run())
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
