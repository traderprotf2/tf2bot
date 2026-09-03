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
import steam_schema
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
        self.particle_name_to_id = steam_schema.fetch_particle_name_to_id(cfg.get("steam_api_key", ""))
        self.particle_id_to_name = {v: k for k, v in self.particle_name_to_id.items()}
        self.inventory_checker = steam_inventory.SteamInventoryChecker(
            cache_ttl_seconds=cfg.get("inventory_cache_seconds", 900)
        )
        self.runtime = runtime_settings.RuntimeSettings.load(cfg)
        self.mannco_key_usd_cents = None
        self.seen = collections.deque(maxlen=cfg["seen_listings_max"])
        self.seen_set = set()

        # Funnel counters, purely diagnostic - answers "is the bot even
        # seeing the volume I'd expect, and if so, where is it narrowing
        # down: the cheap quality/category filter, or the accuracy checks
        # inside evaluate_listing (discount threshold, liquidity, tier
        # consistency, community-price cross-check, etc)?" - see /stats
        # in Telegram. Reset whenever /stats is read, so each read shows
        # "since last time you checked", not a lifetime total.
        self.stats = collections.Counter()
        self.stats_since = time.time()

        # Item-type cooldown state - see send_deal(). Keyed by (source,
        # display_name, particle, paint, killstreaker, sheen) -> last
        # alert timestamp.
        self.item_type_last_alerted = {}

        # Which "kinds" of item structure we've already logged a raw
        # sample of since startup - see the sampling calls in
        # handle_bptf_event. One sample per kind is plenty; the point is
        # having real captured data on hand if extraction for that kind
        # ever turns out wrong, not building an ongoing log of every item.
        self._sampled_item_kinds = set()

        # Lightweight name -> identity_key(s) index, updated as events
        # arrive, purely to support /checkitem (see that command's own
        # handling) - lets a person look up "what does the bot's local
        # store currently know about item X" by NAME (identity keys
        # themselves may be defindex-anchored, not name-based - see
        # listing_identity_key's own docstring in bptf_client.py - so
        # this is the only way to search by name at all). Bounded size
        # isn't enforced here deliberately: distinct item NAMES (not
        # listings) are a small, slowly-growing set relative to the
        # listing volume itself.
        self._name_to_identity_keys = collections.defaultdict(set)

        # (name, quality_name) -> {"ts": last-proactive-refresh unix
        # timestamp (0.0 = never, sorts first), "category": this item's
        # classify_category() result} - drives proactive_buy_order_
        # refresh_loop below. Covers every watched quality, not just
        # Unusual.
        #
        # CAPPED at MAX_KNOWN_SCAN_ITEMS (LRU-evicted, see the population
        # site in handle_bptf_event) - a real, serious bug this session:
        # generalizing past Unusual-only meant this could grow to track
        # every (name, quality) combination ever seen across every
        # watched quality/category, unboundedly (nothing ever removed an
        # entry) - on a small VPS, this grew large enough that the OOM
        # killer terminated the whole process. Unusual alone stayed
        # naturally small (TF2 only has so many Unusual-eligible items);
        # generalizing without ALSO bounding the size was the mistake,
        # not the generalization itself. Not persisted across restarts -
        # rebuilds within minutes at the event volume this project sees.
        self._known_scan_items = {}
        self.MAX_KNOWN_SCAN_ITEMS = 2000

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
            "watched_qualities": self.runtime.watched_qualities,
            "watched_categories": self.runtime.watched_categories,
            "discount_threshold_percent": self.runtime.discount_threshold_percent,
            "max_days_since_price_update": self.runtime.max_days_since_price_update,
            "priority_item_names": self.runtime.priority_item_names,
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
        effect = f" ({deal['particle_name']})" if deal["particle_name"] else ""
        variant = f" [{deal['variant_label']}]" if deal.get("variant_label") else ""
        usd = f", ${deal['price_usd']:.2f}" if deal["price_usd"] else ""

        special_lines = []
        if deal["paint"]:
            special_lines.append(f"🎨 Краска: {deal['paint']}")
        if deal["spells"]:
            special_lines.append(f"👻 Спелл(ы): {', '.join(deal['spells'])}")
        if deal.get("strange_parts"):
            special_lines.append(f"🔧 Strange Part(ы): {', '.join(deal['strange_parts'])}")
        if deal.get("killstreaker"):
            special_lines.append(f"🔥 Killstreaker (эффект в глазах): {deal['killstreaker']}")
        if deal.get("sheen"):
            special_lines.append(f"✨ Sheen (цвет вспышки при убийствах): {deal['sheen']}")
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

        stn_buy_line = ""
        if deal.get("stn_buy_keys") is not None:
            stn_buy_line = f"\n🏦 Buy order на STN.Trading: {deal['stn_buy_keys']:.2f} ключей"

        liquidity_line = ""
        days_since = deal.get("days_since_price_update")
        if days_since is not None:
            liquidity_line = f"\n📊 Последняя переоценка цены: {days_since:.0f} дн. назад"

        seller_note_line = ""
        if deal.get("seller_note"):
            import html
            # Real, confirmed field (see the listing construction in
            # handle_bptf_event) - the seller's own comment on their
            # listing, shown verbatim like the "Description" field in the
            # Discord example this was modeled on. HTML-escaped since
            # it's arbitrary user text being dropped into an HTML-parsed
            # message - an unescaped "<" or "&" in someone's note would
            # otherwise break the whole message's formatting.
            seller_note_line = f"\n📝 Продавец пишет: [<i>{html.escape(deal['seller_note'])}</i>]"

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
        return (
            f"{priority_prefix}"
            f"🔥 <b>-{deal['discount_percent']:.0f}%</b> — {deal['source']}\n"
            f"<b>{deal['display_name']}</b>{effect}{variant}"
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
                bucket = store._entries.get(key, {})
                buy_ages = [now - e["ts"] for e in bucket.values() if e["intent"] == "buy"]
                sell_ages = [now - e["ts"] for e in bucket.values() if e["intent"] == "sell"]

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
            # Safety-net diagnostic: a real, repeated report that the
            # #Attrib_Particle strip in handle_bptf_event still isn't
            # catching every case, despite that fix testing correctly
            # against every example string reported so far - meaning the
            # REAL raw data must differ from what's been tested with in
            # some way not yet seen. Logs the full deal dict (the
            # closest thing to raw data available at this point) so the
            # next occurrence is diagnosable from real production data
            # instead of guessing at synthetic strings again.
            log.warning(
                "DIAGNOSTIC SAMPLE (display_name still contains an unstripped artifact "
                "after clean_display_name) - full deal dict: %r", deal,
            )
        # Per-SELLER cooldown: don't re-alert on the same item from the
        # SAME seller repeatedly (bumped/re-listed) within a short window
        # - but a DIFFERENT seller's listing of the same kind of item is a
        # genuinely different opportunity and must never be held back by
        # this. An earlier version of this scoped the cooldown to the item
        # type alone (no seller in the key), which was wrong per direct
        # correction: it was silently swallowing every other seller's
        # listing of the same item for a full hour after the first one
        # alerted - exactly the effect of "new items stopped showing up",
        # since they WERE being found, just suppressed by too broad a key.
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
        self.item_type_last_alerted[identity_key] = now
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
        await self._run_telegram(self.telegram.send, alert_text, keyboard)

    # -- backpack.tf side ---------------------------------------------------

    async def handle_bptf_event(self, payload: dict):
        if payload.get("_bptf_event_type") == "delete":
            # Processed regardless of pause state - keeping the local
            # store accurate is a separate concern from whether alerts
            # are currently being sent, and costs nothing to do anyway.
            listing_id = payload.get("id")
            if listing_id is not None:
                self.bptf.local_listings.remove_listing(str(listing_id))
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

        item = payload.get("item") or {}
        quality_obj = item.get("quality") or {}
        quality = quality_obj.get("name")
        # Unusual is unconditionally watched, per explicit request -
        # never toggleable via /addquality-/removequality, unlike every
        # other quality. Everything else still goes through the normal
        # watched_qualities check.
        if quality != "Unusual" and quality not in self.runtime.watched_qualities:
            self.stats["bptf_rejected_quality"] += 1
            return

        name = item.get("name") or item.get("marketName") or item.get("baseName")
        if not name:
            return
        # backpack.tf's own particle-name resolution occasionally fails
        # for some effects, leaving a raw, unresolved internal token
        # appended to the name instead of (or alongside) the real effect
        # name - a real, confirmed case: "Sakura Smoke Bomb Blast Bowl
        # (#Attrib_Particle288)". Stripped here, at the source, since an
        # unreliable suffix like this (present for some events of the
        # same effect, absent for others) risks the same kind of
        # identity-key mismatch already fixed twice this session for
        # similar name-text inconsistencies - not just an ugly display.
        #
        # Matches "#Attrib_Particle" ANYWHERE in the string, not a fixed
        # " (#Attrib_Particle" substring - a real, confirmed case: this
        # project has already hit backpack.tf using non-ASCII whitespace
        # in other text fields (seller notes), so requiring an exact
        # regular space before the marker risked silently not matching
        # at all if the same thing happens here. Trims any leftover
        # whitespace/punctuation right before the marker too.
        if "#Attrib_Particle" in name:
            name = name.split("#Attrib_Particle")[0].rstrip("(").rstrip()

        # Base item type's own numeric schema ID - confirmed real in
        # backpack.tf's payload (multiple third-party API docs: "item...
        # Contains keys like defindex and quality"). Preferred identity
        # anchor over name text - see listing_identity_key's own
        # docstring in bptf_client.py for why.
        defindex = item.get("defindex")

        # Action items excluded unconditionally, not via watched_categories
        # (so re-enabling by mistake via /addcategory can't undo it) - per
        # a direct, explicit request to never even consider Tool, Craft
        # Item, Strangifier, Crate, Party Favor, or Action items. Action
        # items specifically have their own dedicated equip slot ("action",
        # previously "MISC2") distinct from every other item type -
        # confirmed via the official TF2 wiki's own Action items article -
        # so this is a reliable, structural signal, not a name guess. The
        # other five categories don't have an equally reliable single
        # field confirmed for this project's exact data source, so they're
        # excluded via config.py's excluded_types instead (name/type text
        # match - the same, less-certain mechanism War Paint/Badge already
        # used). A more reliable, schema-based check (Valve's own
        # craft_class field confirms "tool" and "supply_crate" as real
        # values for Tool and Crate specifically) was researched and is a
        # good candidate for a future pass, but wasn't rushed into this
        # one - implementing a new schema-fetch system without room to
        # test it properly risked introducing exactly the kind of mistake
        # a large, hurried edit already caused once this session.
        if item.get("slot") == "action":
            self.stats["bptf_rejected_category"] += 1
            return

        # Category checked HERE, right alongside quality above and before
        # any of the more expensive extraction below (particle/paint/
        # spell/killstreak parsing) - per a direct request to stop that
        # work being done at all for a category that isn't even watched
        # (weapons specifically called out as the highest-volume category
        # on backpack.tf). This mirrors is_watched()'s own category check
        # in matcher.py - kept there too (not removed) since that
        # function is shared by every source, not just this one; this is
        # purely a fast-path duplicate for backpack.tf specifically,
        # where the volume is highest.
        category = classify_category(name, item.get("slot"))
        if category not in self.runtime.watched_categories:
            self.stats["bptf_rejected_category"] += 1
            return

        if quality == "Unusual" and "bptf_unusual" not in self._sampled_item_kinds:
            self._sampled_item_kinds.add("bptf_unusual")
            log.warning(
                "DIAGNOSTIC SAMPLE (first Unusual seen this run) - raw item fields relevant to "
                "particle extraction: particle=%r, particleId=%r, attributes=%r",
                item.get("particle"), item.get("particleId"), item.get("attributes"),
            )

        particle_obj = item.get("particle") or {}
        particle_id = particle_obj.get("id")
        particle_name = particle_obj.get("name")

        if particle_id is None:
            # Defensive fallback paths - not independently confirmed
            # against a real live backpack.tf Unusual payload (a repeated
            # report that NO Unusual alerts were ever seen from ANY
            # source raised real doubt about the primary path above).
            # Try a flat field name first...
            particle_id = item.get("particleId") or item.get("particle_id")
        if particle_id is None:
            # ...then raw Steam-schema-style attributes, where the
            # "attach particle effect" attribute (defindex 134, a
            # well-known constant in TF2 tooling) carries the particle id
            # as its value - plausible if backpack.tf passes through
            # relatively unprocessed inventory-style attribute data.
            for attr in (item.get("attributes") or []):
                if isinstance(attr, dict) and attr.get("defindex") == 134:
                    raw_value = attr.get("value", attr.get("float_value"))
                    try:
                        particle_id = int(raw_value) if raw_value is not None else None
                    except (TypeError, ValueError):
                        particle_id = None
                    break
        if particle_id is not None:
            # Prefer the schema-based canonical name over whatever the
            # raw payload's own "name" field happens to say for THIS
            # specific event, whenever we have one - a real, hard-to-spot
            # bug: strip_effect_prefix() below only strips the effect
            # prefix when its EXACT string matches the start of `name`
            # (case-sensitive), and if the raw payload's own particle
            # name ever varies in phrasing/casing between two events for
            # the SAME effect, the strip would succeed for one and
            # silently fail for the other - leaving the SAME effect
            # producing two DIFFERENT `name` values (one with the prefix
            # still attached), which never match in the identity key,
            # even though particle_id itself agrees. Using the one
            # canonical name tied to this particle_id, always, removes
            # that inconsistency at the source. Falls back to the raw
            # payload's own name only when this id isn't in the schema at
            # all (e.g. steam_api_key not configured, or a brand new
            # effect Valve hasn't indexed yet).
            particle_name = self.particle_id_to_name.get(particle_id) or particle_name

        if particle_name:
            # Confirmed real via a direct report: backpack.tf's own
            # item.name already includes the effect as a display prefix
            # ("Circling Heart Hot Dogger", not just "Hot Dogger") - left
            # in place, the effect ends up shown twice in an alert's own
            # text, AND (more importantly) the classifieds search link
            # ends up searching for a literal item named "Circling Heart
            # Hot Dogger" while ALSO passing particle=<id> separately,
            # which doesn't match anything real - the exact reason a
            # real alert's search link didn't work. Stripped here, once,
            # so every downstream use (display name, identity key,
            # search link) sees the same clean name consistently.
            name = bptf_client.strip_effect_prefix(name, particle_name)

        spells = [s.get("name") for s in (item.get("spells") or []) if isinstance(s, dict) and s.get("name")]
        strange_parts = [p.get("name") for p in (item.get("strangeParts") or [])
                          if isinstance(p, dict) and p.get("name")]

        # Game-logic guard: paint only exists on cosmetics, killstreak
        # tiers only exist on weapons. `slot` tells us which is which -
        # if it's missing/unrecognised we drop both rather than guess.
        slot = item.get("slot")
        paint_obj = item.get("paint") or {}
        raw_paint = paint_obj.get("name") if isinstance(paint_obj, dict) else None
        paint = raw_paint if slot in COSMETIC_SLOTS else None

        # Exact RGB decimal straight from the source (confirmed real via
        # a diagnostic sample: {'id': 5046, 'name': 'Team Spirit',
        # 'color': '#b8383b'} - '#b8383b' decodes, after stripping the
        # '#', to (184, 56, 59), an exact match for this project's own
        # "Team Spirit RED" value) - no RED/BLU guessing needed for
        # team-coloured paints, this is the exact colour the listing is.
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
            # Confirms the slot value classify_category() now checks for
            # (see there) against a real payload, the same way every
            # other slot/field assumption in this project has been
            # verified - the official wiki describes badges as equipping
            # "in the medal equip region", and this is the first real
            # listing to actually confirm (or contradict) that the raw
            # slot string backpack.tf sends is literally "medal".
            log.warning("DIAGNOSTIC SAMPLE (first slot='medal' item seen this run) - name: %r", item.get("name"))

        if paint and paint in bptf_client.TEAM_COLOR_PAINT_RGB and "bptf_team_paint" not in self._sampled_item_kinds:
            self._sampled_item_kinds.add("bptf_team_paint")
            # Real production evidence (a Steam econ-item parsing library,
            # danocmx/node-tf2-item-format) shows Steam's OWN raw item
            # description carries a single "Paint Color: <hex>" string per
            # item, not two RED/BLU alternatives - meaning there may be a
            # direct colour/hex field on THIS payload's paint object too,
            # which would be the correct single value to search with
            # instead of guessing between the two RGB variants (see
            # team_color_paint_decimals in matcher.py's evaluate_listing).
            # Logging the FULL raw object here, once, to find out.
            log.warning(
                "DIAGNOSTIC SAMPLE (first team-coloured paint seen this run, %r) - "
                "raw paint object: %r",
                paint, paint_obj,
            )
        raw_killstreak_tier = item.get("killstreakTier")
        killstreak_tier = raw_killstreak_tier if slot in WEAPON_SLOTS else None

        # Per explicit request: only Professional Killstreak (tier 3) is
        # watched now - plain (tier 0, no kit at all) still is too, only
        # tier 1 ("Killstreak") and tier 2 ("Specialized Killstreak") are
        # excluded. Checked here, early, same reasoning as the Action-
        # item/category checks above - skip the more expensive parsing
        # below entirely for a tier that's never going to be watched,
        # not just suppress the alert for it later.
        if killstreak_tier in (1, 2):
            self.stats["bptf_rejected_category"] += 1
            return
        # Killstreak Kits/Fabricators have no weapon slot (killstreak_tier
        # above is always None for them), so the same tier restriction is
        # applied by name instead, mirroring classify_category's own
        # Kit/Fabricator name-pattern detection. Only "Professional
        # Killstreak ... Kit/Fabricator" passes; plain "Killstreak ..."
        # and "Specialized Killstreak ..." are excluded the same way.
        if "Killstreak" in name and (name.endswith("Kit") or name.endswith("Fabricator")):
            if not name.startswith("Professional Killstreak "):
                self.stats["bptf_rejected_category"] += 1
                return

        # Killstreaker/sheen only exist on weapons with a killstreak tier
        # (sheen from tier 2+, killstreaker from tier 3 only - see
        # bptf_client.VALID_KILLSTREAKERS/VALID_SHEENS) - same game-logic
        # guard as paint/killstreak_tier above. Field path is a best-effort
        # guess following the same item.<attr>.name shape as particle/paint
        # above (not independently confirmed for killstreaker/sheen
        # specifically) - gracefully comes back None if wrong, same as any
        # other uncertain field in this project, rather than guessing at a
        # wrong value.
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
            killstreaker_obj = item.get("killstreaker") or {}
            raw_killstreaker = killstreaker_obj.get("name") if isinstance(killstreaker_obj, dict) else None
            if killstreak_tier >= 3 and raw_killstreaker in bptf_client.VALID_KILLSTREAKERS:
                killstreaker = raw_killstreaker
            sheen_obj = item.get("sheen") or {}
            raw_sheen = sheen_obj.get("name") if isinstance(sheen_obj, dict) else None
            if killstreak_tier >= 2 and raw_sheen in bptf_client.VALID_SHEENS:
                sheen = raw_sheen

        seller_steamid = payload.get("steamid")

        # wearTier is the actual weapon-skin-specific signal (Factory
        # New through Battle-Scarred wear only applies to War Paints/
        # Decorated weapons - confirmed via the official TF2 wiki's own
        # Grade article - cosmetics don't have a wear dimension at all).
        # texture alone, WITHOUT wearTier, means a graded COSMETIC (a
        # hat with an intrinsic Civilian-through-Elite rarity grade -
        # same wiki source, "Grade is an intrinsic sub-quality of
        # Decorated weapons AND certain cosmetic items") - a real,
        # separate, often valuable category that was previously being
        # unconditionally dropped here (is_skin used to fire on texture
        # alone), conflating it with actual weapon skins. Grade/texture
        # is captured below and threaded into the identity key instead -
        # see listing_identity_key's own docstring for why this can
        # never be read from the item's own display NAME text (backpack.
        # tf's own forums document real items whose name contains a
        # DIFFERENT grade's word than their true one).
        is_skin = bool(item.get("wearTier"))
        texture_obj = item.get("texture")
        texture = texture_obj.get("name") if isinstance(texture_obj, dict) else texture_obj
        # Derived from the NAME TEXT, not the separate item.craftable
        # field - a real, confirmed bug: the two disagreed for some
        # listings, so the alert's own displayed name (built straight
        # from this same name string - see clean_display_name) said
        # "Non-Craftable X" while the buy order actually being compared
        # against was looked up under craftable=True, because the
        # separate boolean field said so. backpack.tf's name text
        # reliably carries "Non-Craftable " as a literal prefix when
        # applicable (the same convention strip_variant_prefixes already
        # relies on for search links) - deriving from the SAME string
        # that gets displayed means the two can never disagree again.
        craftable = not name.startswith("Non-Craftable ")

        currencies = payload.get("currencies") or {}
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


        # Record EVERY listing this project sees (both sell AND buy
        # intent, now that bptf_ws.py passes buy-intent events through
        # too) into the self-collected local store - see LocalListingStore's
        # own docstring in bptf_client.py for why this replaced querying
        # backpack.tf's own (confirmed deprecated) snapshot endpoint.
        # This has to happen for EVERY listing, not just ones that end up
        # qualifying as a deal - a listing that isn't itself a bargain is
        # still exactly the kind of "what's the going rate" comparison
        # data a LATER listing of the same exact item needs.
        paint_value_for_identity = paint_decimal_hint if paint_decimal_hint is not None else (
            bptf_client.paint_rgb_decimal(paint) if paint else None
        )
        identity_key = bptf_client.listing_identity_key(
            name, quality, particle_id, paint_value_for_identity, bool(craftable),
            spells[0] if spells else None, killstreak_tier, name.startswith("Australium "),
            texture=texture, defindex=defindex, killstreaker=killstreaker, sheen=sheen,
        )
        self._name_to_identity_keys[name.lower()].add(identity_key)
        # Same name-text check evaluate_listing's own excluded_types
        # filter uses (matcher.py's item_type is never actually
        # populated for backpack.tf, so that filter is name-text-only in
        # practice too - see its own comment) - an Unusual item whose
        # name matches an excluded type would never pass evaluate_listing
        # anyway, so there's no point spending proactive-scan requests
        # keeping its buy-order data fresh.
        name_lower_for_exclusion = name.lower()
        is_excluded_type = any(
            excluded.lower() in name_lower_for_exclusion
            for excluded in self.cfg.get("excluded_types", [])
        )
        is_currently_watched_quality = quality == "Unusual" or quality in self.runtime.watched_qualities
        scan_key = (name, quality)
        if is_currently_watched_quality and not is_excluded_type and scan_key not in self._known_scan_items:
            # Hard cap, FIFO-evicted (oldest-inserted first - regular
            # dicts keep insertion order) - see this dict's own comment
            # in __init__ for why this cap exists at all: unbounded
            # growth here already took down the whole process once.
            if len(self._known_scan_items) >= self.MAX_KNOWN_SCAN_ITEMS:
                oldest_key = next(iter(self._known_scan_items))
                del self._known_scan_items[oldest_key]
            self._known_scan_items[scan_key] = {"ts": 0.0, "category": category}
        self.bptf.local_listings.record(
            identity_key, str(listing_id), seller_steamid, price_keys, intent,
        )

        if intent != "sell":
            # Buy-intent listings only ever feed the local store above -
            # there's no "deal" to evaluate or alert on for an offer to
            # BUY something, only a data point for pricing SELL listings
            # of the same item later.
            self.stats["bptf_buy_recorded"] += 1
            return

        # Direct, pre-filled Steam trade-offer link straight to the seller
        # - the fastest way to actually buy - when backpack.tf exposes one
        # publicly for them. The "view the offer on backpack.tf itself"
        # link is built uniformly for every deal in matcher.py, so this is
        # only the direct-purchase link; left empty if the seller doesn't
        # have one public (the search link still gets them there).
        link = (payload.get("user") or {}).get("tradeOfferUrl")

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
            # "details" is at the LISTING level (sibling of id/steamid/
            # price), not nested under "item" - confirmed independently
            # by two real sources reading/writing this exact field:
            # a GitHub library that parses live backpack.tf listings
            # ("details: string // Comment below the listing") and the
            # unofficial BackpackTF PyPI wrapper's create_listing
            # ("details - the listing comment, max 200 characters") -
            # both agree on the name, from opposite ends (read vs write).
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
                # _run_telegram's own sends (see run() for the full
                # reasoning) - NOT the shared default pool this used to
                # go through. A real report of the Telegram bot going
                # completely unresponsive traced to exactly this gap:
                # this fix's earlier version only moved outgoing sends
                # (_run_telegram) to a dedicated pool, but get_updates()
                # itself - a LONG-POLLING call that occupies a worker
                # thread for the whole poll cycle, called in a tight
                # while True loop - was still going through the shared
                # default pool the same way evaluate_listing does. Under
                # the real event volume this project handles, that
                # shared pool being busy meant polling itself couldn't
                # get a thread promptly, so the bot could go a long time
                # without even checking for new messages - not a crash,
                # just total unresponsiveness that looked exactly like one.
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
                reply = self._check_item(text.split(maxsplit=1)[1] if " " in text else "")
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

        if self.cfg.get("steam_api_key"):
            if len(self.particle_name_to_id) < 400:
                problems.append(
                    f"Загружено только {len(self.particle_name_to_id)} unusual-эффектов из схемы "
                    f"Steam (обычно 600+) - определение Unusual-предметов может работать не полностью."
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
        # from the one asyncio.to_thread() uses by default for
        # everything else (evaluate_listing, Steam inventory checks,
        # etc.) - NOT just a bigger shared pool. A real report showed
        # Telegram commands lagging once listing events started being
        # dispatched concurrently (bptf_ws.py's
        # _dispatch_semaphore, up to 60+60 potentially "in flight" at
        # once, each holding a worker thread for its entire throttled
        # request chain - tens of seconds of time.sleep() inside the
        # account pool's throttle) - Telegram's own to_thread calls were
        # queuing up behind all of that on the SAME small default pool.
        # The first fix here simply made the shared pool much bigger
        # (200 workers) - but a second, separate report then showed
        # something worse: 100% of evaluations failing right after that
        # change, on a small VPS (real hostname seen elsewhere: "vm-
        # nano") - 200 real OS threads is a real, sometimes-too-heavy
        # resource ask on a box that small, however roomy it looks on
        # paper. A dedicated small pool for Telegram specifically (it
        # only ever needs a handful of threads - calls are quick and
        # infrequent) solves the ORIGINAL lag without needing the shared
        # pool to be huge at all - see _run_telegram below, used
        # everywhere self.telegram.* used to go through plain
        # asyncio.to_thread.
        self._telegram_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        # Restores whatever the local listing store had saved right
        # before this run started (or a previous one, if this is the
        # first run since a save) - see LocalListingStore.load_from_disk
        # and local_store_snapshot_loop below for why this exists: a
        # real, direct point that restarting for every update (routine
        # during active development) was otherwise wiping out everything
        # the store had learned each time. Done before anything else
        # touches self.bptf.local_listings, so the very first real
        # listing evaluated after a restart already has recent
        # comparison data available, not an empty store.
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

        # Graceful-shutdown handling: saves the local listing store
        # (see LocalListingStore.save_to_disk / local_store_snapshot_loop
        # above) the MOMENT the process is asked to stop, not just on
        # local_store_snapshot_loop's own 90-second timer. Direct
        # feedback made clear why the timer alone isn't enough: this bot
        # gets restarted often during active development (an update is
        # exactly a restart), and if that restart happens to land before
        # the periodic loop's first save (up to 90 seconds after
        # startup, worst case), everything collected in that window was
        # simply lost when the process exited - not "written but stale",
        # never written at all. `systemctl restart`/`stop` send SIGTERM;
        # Ctrl+C / KeyboardInterrupt is SIGINT - both are handled the
        # same way here so either kind of stop triggers one last save
        # before the process actually exits, regardless of how much (or
        # how little) time has passed since the last periodic save.
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
            # main_tasks finished FIRST, and not because of a shutdown
            # signal - meaning one of the loops inside it actually
            # raised (every real loop in this project is `while True`,
            # so ordinary completion never happens on its own). Do the
            # same emergency save a real shutdown would (whatever data
            # has been collected is still worth keeping), then
            # deliberately let the original exception propagate out of
            # run() instead of swallowing it - systemd's restart policy
            # is what's supposed to bring the process back up after a
            # real crash, and it can only do that if the process
            # actually exits with the failure visible, the same as
            # before this shutdown-handling code existed at all.
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
