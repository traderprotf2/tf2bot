"""
TF2 Deal Watcher
================
Watches TWO live sources at once:
  - mannco.store's market websocket
  - backpack.tf's own classifieds websocket
for newly listed / re-priced TF2 items (Unusual hats, Strange weapons,
Australiums, ...) priced well below the going rate, and sends a Telegram
alert either way.

The reference "going rate" is backpack.tf's own LIVE listings for that
exact item (name + quality + effect) when available - i.e. this also
catches someone undercutting the rest of the market on backpack.tf
itself, not just mannco.store listings being cheap. Falls back to
backpack.tf's community-suggested price when live listings aren't
available for that item (or no backpacktf_token is configured).

Usage:
    python3 main.py

See README.md for setup instructions.
"""

import asyncio
import collections
import logging
import time

import bptf_client
import bptf_ws
import error_log
import mannco_client
import mannco_ws
import marketplacetf_client
import matcher
import runtime_settings
import steam_inventory
import steam_schema
import stntrading_client
import telegram_commands
import telegram_notify
from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")

_error_buffer = error_log.install()

# Only cosmetics (hats/misc) can carry a Paint Can colour in TF2 - weapons
# can never be painted (that's a common misconception; War Paint *skins*
# are a separate, unrelated system). Only weapons can carry a Killstreak
# tier - cosmetics can't. These whitelists are used to strip out either
# field if upstream data ever looks physically impossible, rather than
# ever showing a combination that can't exist in the game.
COSMETIC_SLOTS = {"head", "misc"}
WEAPON_SLOTS = {"primary", "secondary", "melee", "pda", "pda2", "building"}

# Real TF2 item taxonomy, confirmed against Valve's own item schema
# (backpack.tf/schema/... historical dumps) and the community-standard
# item.tf filter categories - not guessed:
#   - Taunts have item_slot == "taunt" (a distinct schema value, separate
#     from any weapon/cosmetic slot).
#   - Killstreak Kits and Killstreak Kit Fabricators are Tool items with
#     no weapon/cosmetic slot at all - Valve names them consistently
#     ("Killstreak Kit", "Specialized Killstreak Kit", "Professional
#     Killstreak Kit", plus the Fabricator variants), so a name-pattern
#     check is the most reliable signal, more so than any single slot
#     value. NOTE: kits/fabricators are Unique-quality tools, not
#     Strange/Unusual themselves - watching this category alone won't
#     surface anything unless "Unique" is also in watched_qualities.


def classify_category(name, slot=None, mannco_type=None):
    """
    Single classifier for both sources. `slot` is backpack.tf's reliable
    schema field; `mannco_type` is mannco.store's less-certain type string
    (same honesty caveat as mannco_paint() below - their exact TF2 type
    strings aren't documented, so this branch is keyword-based
    best-effort and falls back to "other" rather than guessing wrong).
    """
    name = name or ""
    # Valve names every tier consistently: "Killstreak <Weapon> Kit",
    # "Specialized Killstreak <Weapon> Kit", "Professional Killstreak Kit
    # Fabricator", etc. - "Kit"/"Fabricator" is always the last word, and
    # "Killstreak" always appears somewhere in the name. Checking for the
    # literal substring "Killstreak Kit" (an earlier version of this
    # check) misses the common case where a weapon name sits in between,
    # e.g. "Killstreak Rocket Launcher Kit" - caught by a real test.
    if "Killstreak" in name and (name.endswith("Kit") or name.endswith("Fabricator")):
        return "killstreak_kit"

    # Universal, source-independent signal: Valve always names taunt items
    # "Taunt: <Name>" (the effect/variant prefix, if any, comes before this,
    # e.g. "Shimmering Lights Taunt: Rocket Jockey" - confirmed against a
    # real, live marketplace.tf listing). Checked before the slot/type
    # branches below since it's more reliable than either for sources that
    # don't provide a slot or type at all (marketplace.tf, stntrading.eu).
    if "Taunt: " in name:
        return "taunt"

    if slot is not None:
        if slot == "taunt":
            return "taunt"
        if slot in WEAPON_SLOTS:
            return "weapon"
        if slot in COSMETIC_SLOTS:
            return "cosmetic"
        # Badges/medals/pins equip in their own dedicated slot, distinct
        # from head/misc cosmetics - confirmed via the official TF2 wiki's
        # own description of the item type ("equip in the medal equip
        # region"), not guessed. Given a direct request to stop wasting
        # requests on badges (illiquid, not real trading interest) and
        # that item_type/name-based exclusion alone was unreliable for
        # backpack.tf specifically (item_type is never populated for that
        # source - see handle_bptf_event - so exclusion fell back to
        # matching "badge" against the item NAME, which many real badges
        # don't literally contain), this is a second, independent,
        # slot-based signal that doesn't depend on the name at all.
        if slot == "medal":
            return "badge"
        return "other"

    if mannco_type is not None:
        t = mannco_type.lower()
        if "taunt" in t:
            return "taunt"
        if any(w in t for w in ("hat", "cosmetic", "misc")):
            return "cosmetic"
        if any(w in t for w in ("weapon", "rifle", "pistol", "launcher", "melee",
                                 "primary", "secondary", "sword", "axe", "bow")):
            return "weapon"

    return "other"


def mannco_effect_name(details: dict):
    for key in ("effect", "particle", "unusual_effect", "particleName"):
        value = details.get(key)
        if value:
            return value
    return None


def mannco_spells(details: dict):
    """
    Best-effort: mannco.store's item-details schema isn't fully documented
    for Halloween spells, so we try a couple of plausible shapes. Returns
    a list of spell name strings (possibly empty).
    """
    raw = details.get("spells")
    if not raw:
        return []
    names = []
    for s in raw:
        if isinstance(s, dict):
            n = s.get("name") or s.get("spellId")
            if n:
                names.append(str(n))
        elif isinstance(s, str):
            names.append(s)
    return names


def mannco_strange_parts(details: dict):
    """Same honesty caveat as mannco_spells() above - field name guessed
    by analogy, not confirmed against a real mannco.store example."""
    raw = details.get("strangeParts") or details.get("strange_parts")
    if not raw:
        return []
    names = []
    for p in raw:
        if isinstance(p, dict):
            n = p.get("name")
            if n:
                names.append(str(n))
        elif isinstance(p, str):
            names.append(p)
    return names


_sampled_mannco_team_paint = False


def mannco_paint(details: dict):
    """
    Same honesty caveat as mannco_spells() above, PLUS a hard game-logic
    check: only cosmetics can be painted in TF2, weapons never can. If
    mannco's "type" field doesn't look like a cosmetic, the paint value
    is dropped rather than shown - an impossible combination is worse
    than no data.
    """
    raw = details.get("paint")
    if not raw:
        return None

    item_type = (details.get("type") or "").lower()
    is_cosmetic_type = any(word in item_type for word in ("hat", "cosmetic", "misc"))
    if not is_cosmetic_type:
        return None

    if isinstance(raw, dict):
        name = raw.get("name")
        global _sampled_mannco_team_paint
        if name in bptf_client.TEAM_COLOR_PAINT_RGB and not _sampled_mannco_team_paint:
            _sampled_mannco_team_paint = True
            log.warning(
                "DIAGNOSTIC SAMPLE (first team-coloured paint seen this run from mannco.store, "
                "%r) - raw paint object: %r",
                name, raw,
            )
        return name
    if isinstance(raw, str):
        return raw
    return None


def mannco_paint_decimal_hint(details: dict):
    """
    The exact RGB decimal for this SPECIFIC listing's paint, when
    mannco.store's own payload gives one directly - confirmed real via
    the diagnostic sample above: a raw paint object of {'name': 'Team
    Spirit', 'color': 'b8383b', 'team': 'red'}, where 'b8383b' decodes to
    (184, 56, 59) - an exact match for this project's own independently-
    researched "Team Spirit RED" RGB value. This means mannco.store
    tells us directly which of the two colours a team-coloured item
    actually is, so there's no need to guess-and-try-both the way
    evaluate_listing does for backpack.tf (which has no equivalent
    confirmed field yet). Only meaningful for team-coloured paints -
    ordinary paints already resolve to one RGB value from the name alone
    (see paint_rgb_decimal), so this returns None for anything else,
    letting the normal name-based path handle it.
    """
    raw = details.get("paint")
    if not isinstance(raw, dict):
        return None
    color_hex = raw.get("color")
    if not color_hex or not isinstance(color_hex, str):
        return None
    try:
        return int(color_hex, 16)
    except ValueError:
        return None


class Watcher:
    def __init__(self, cfg):
        self.cfg = cfg
        bptf_client.configure_request_pacing(
            cfg.get("bptf_max_concurrent_requests", 4),
            cfg.get("bptf_min_request_interval_seconds", 11.0),
        )
        self.bptf = bptf_client.BackpackTFPriceList(
            cfg["backpacktf_api_key"],
            cfg.get("backpacktf_token", ""),
            cfg["snapshot_cache_seconds"],
        )
        self.mannco = mannco_client.ManncoClient(cfg["mannco_api_key"], cfg["jwt_refresh_seconds"])
        # Persistent (not re-created per call) so the same client can be
        # reused both by the watchlist poll loop below AND by the
        # per-deal STN-buy-order context lookup in evaluate_listing -
        # None when no key is configured, which every caller already
        # treats as "skip this source" (stntrading_client.py's own
        # methods return None/empty on a missing key too).
        self.stn_client = (
            stntrading_client.STNTradingClient(cfg["stntrading_api_key"])
            if cfg.get("stntrading_api_key") else None
        )
        self.telegram = telegram_notify.TelegramNotifier(cfg["telegram_bot_token"], cfg["telegram_chat_id"])
        self.particle_name_to_id = steam_schema.fetch_particle_name_to_id(cfg.get("steam_api_key", ""))
        self.particle_id_to_name = {v: k for k, v in self.particle_name_to_id.items()}
        # Case/whitespace-normalized lookup too, alongside the exact one -
        # mannco.store's effect-name field isn't documented (see
        # mannco_effect_name's own comment), so a mismatch as small as
        # "kill-a-watt" vs "Kill-A-Watt" or stray whitespace would
        # otherwise silently make every one of that effect's Unusuals
        # unmatchable, with no indication why.
        self.particle_name_to_id_normalized = {
            name.strip().lower(): pid for name, pid in self.particle_name_to_id.items()
        }
        self.defindex_to_name, self.name_to_image_url = steam_schema.fetch_defindex_to_name(
            cfg.get("steam_api_key", "")
        )
        if not self.defindex_to_name:
            log.warning(
                "No Steam schema defindex map available (steam_api_key missing or fetch failed) - "
                "marketplace.tf listings will be skipped, since their item names can't be split "
                "from the effect name without it. See README."
            )
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
        # Caches item_ids confirmed NOT TF2 (game != 440) - see
        # handle_mannco_event, which skips the details fetch entirely for
        # any item_id already in here. An item's game never changes, so
        # this is safe to keep for the whole run without expiring it the
        # way the recent-listings dedup cache does.
        self._known_non_tf2_item_ids = set()

        # Which "kinds" of item structure we've already logged a raw
        # sample of since startup - see the sampling calls in
        # handle_bptf_event. One sample per kind is plenty; the point is
        # having real captured data on hand if extraction for that kind
        # ever turns out wrong, not building an ongoing log of every item.
        self._sampled_item_kinds = set()

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
        The dict matcher.evaluate_listing() reads its filter settings
        from - static values from config.json, overlaid with whatever is
        currently set at runtime (via Telegram commands; see
        runtime_settings.py). Rebuilt on every call so a command takes
        effect on the very next listing event, no restart needed.
        """
        return {
            **self.cfg,
            "min_price_keys": self.runtime.min_price_keys,
            "watched_qualities": self.runtime.watched_qualities,
            "watched_categories": self.runtime.watched_categories,
            "discount_threshold_percent": self.runtime.discount_threshold_percent,
            "max_days_since_price_update": self.runtime.max_days_since_price_update,
        }

    def refresh_prices(self):
        self.bptf.refresh()

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
        The mannco.store key price gets its own, much slower refresh
        cadence, separate from price_refresh_loop above - a real
        complaint that this was refreshing every 15 minutes (the same
        cadence as backpack.tf's whole price list, which genuinely does
        need to be that fresh) despite the key's own USD price being
        stable week to week. Each attempt is a full mannco.store API
        round-trip (auth + request) that can fail on its own and was
        contributing repeated identical warnings to the logs for no real
        benefit - see key_price_refresh_seconds in config.py.

        RETRY-SOONER ON FAILURE: a real /stats report showed mannco.store
        AND marketplace.tf both going completely silent for 5+ hours
        straight (both depend on this same USD-per-key rate for price
        conversion), with the mannco.store key price stuck unavailable
        the whole time. At a weekly cadence with no retry, a single
        failed attempt - which does happen, mannco.store's own login has
        hit real rate limits before - would otherwise leave TWO sources
        completely dead for up to a week before the next scheduled try.
        This retries much sooner after any failure, and only settles
        into the full weekly cadence once a refresh has actually
        succeeded.
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

    async def health_check_loop(self):
        """
        Proactively pings Telegram only when something looks worth
        knowing about - NOT a routine "all good" check-in every few
        hours, which would just be noise. Specifically watches for a
        meaningful number of new warnings/errors piling up since the
        last check (see error_log.py) - the kind of thing that, across
        this project's history, otherwise only surfaced once someone
        noticed a bad alert and had to go dig through logs by hand.

        When the threshold is crossed, this also PAUSES deal alerts (the
        same /pause a person can trigger by hand) - not a full process
        stop. A full stop was the original ask, but risks two things: (1)
        systemd's own restart policy could just bring the process back up
        within seconds, defeating the point, and (2) actually stopping
        for longer would mean missing real, time-sensitive deals for
        however long it takes to notice - the exact opposite of what this
        project is for. Pausing achieves the same "impossible to miss"
        effect (no more deal alerts show up at all until /resume) while
        keeping the websocket connections and monitoring themselves alive
        in the background, and it's fully reversible with one command
        once the /errors output has been checked.
        """
        last_error_count = len(_error_buffer.recent(1000))
        interval_seconds = self.cfg.get("health_check_interval_minutes", 180) * 60
        threshold = self.cfg.get("health_check_error_threshold", 5)
        auto_pause = self.cfg.get("health_check_auto_pause", True)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                current_errors = _error_buffer.recent(1000)
                new_error_count = len(current_errors) - last_error_count
                if new_error_count >= threshold:
                    recent_messages = [e["message"][:150] for e in current_errors[-new_error_count:]]
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
                last_error_count = len(current_errors)
            except Exception:
                log.exception("Health check loop itself failed - continuing anyway.")

    # -- marketplace.tf (autonomous - scrapes their public /deals page) -----

    async def marketplacetf_poll_loop(self):
        client = marketplacetf_client.MarketplaceTFClient()
        poll_seconds = self.cfg.get("marketplacetf_poll_seconds", 300)
        log.info("Polling marketplace.tf deals every %ss", poll_seconds)
        while True:
            try:
                await self._check_marketplacetf_deals(client)
            except Exception:
                log.exception("marketplace.tf poll failed, will retry next cycle.")
            await asyncio.sleep(poll_seconds)

    async def _check_marketplacetf_deals(self, client):
        if self.runtime.paused:
            return
        if not self.mannco_key_usd_cents:
            # Same root cause and same fix as mannco.store's own
            # no-key-price return (see handle_mannco_event) - this
            # source depends on the SAME mannco.store USD-per-key rate
            # for its own price conversion, and a real /stats report
            # showed marketplace.tf reporting a flat "0 events" for 5+
            # hours straight with nothing in /errors explaining why -
            # traced to this exact return having no counter or log
            # message either, on a SEPARATE source from the one that
            # first exposed the same gap.
            if "mptf_no_key_price" not in self._sampled_item_kinds:
                self._sampled_item_kinds.add("mptf_no_key_price")
                log.warning(
                    "marketplace.tf polling is skipped - mannco.store key price is unavailable "
                    "(needed for USD-to-keys conversion) - will resume once it refreshes."
                )
            self.stats["mptf_no_key_price"] += 1
            return
        if not self.defindex_to_name:
            return  # can't safely resolve item names - see __init__ warning
        deals = await asyncio.to_thread(client.fetch_deals)
        for raw in deals:
            self.stats["mptf_received"] += 1
            # Include price in the dedup key too, same reasoning as the
            # mannco.store/backpack.tf handlers - a price drop on a SKU
            # already seen once must still be evaluated fresh, not
            # silently skipped because that SKU showed up before.
            dedup_id = f"mptf:{raw['sku']}:{round(raw['price_usd'], 2)}"
            if not self._mark_seen(dedup_id):
                self.stats["mptf_deduped"] += 1
                continue

            defindex, quality_name, particle_id, craftable = marketplacetf_client.parse_sku(raw["sku"])
            if quality_name is None:
                continue

            # CRITICAL: marketplace.tf's own display text combines the
            # effect name and the item name into one string with no
            # reliable separator (e.g. "Iridescence Crustaceous Cowl" -
            # "Iridescence" is the effect, "Crustaceous Cowl" is the hat).
            # An earlier version of this used that combined text directly
            # as the item name, which silently broke every backpack.tf
            # comparison for marketplace.tf listings (the combined string
            # matches nothing in backpack.tf's own data) - caught by a
            # real report where this showed a "discount" that wasn't
            # actually verified against backpack.tf at all. Resolving the
            # real name from the SKU's own defindex (unambiguous, from
            # Valve's schema) is the fix - and if it can't be resolved,
            # this listing is skipped rather than falling back to the
            # unreliable combined text.
            base_name = self.defindex_to_name.get(defindex)
            if not base_name:
                log.warning("Could not resolve marketplace.tf defindex %s to a name - skipping SKU %s.",
                            defindex, raw["sku"])
                continue
            particle_name = self.particle_id_to_name.get(particle_id) if particle_id is not None else None

            price_keys = (raw["price_usd"] * 100) / self.mannco_key_usd_cents

            listing = matcher.NormalizedListing(
                source="marketplace.tf",
                listing_id=dedup_id,
                name=base_name,
                quality=quality_name,
                category=classify_category(base_name),
                particle_id=particle_id,
                particle_name=particle_name,
                craftable=craftable,
                price_keys=price_keys,
                price_usd=raw["price_usd"],
                link=f"https://marketplace.tf/items/tf2/{raw['sku']}",
            )
            self.stats["mptf_evaluated"] += 1
            deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg(), self.name_to_image_url, self.stn_client)
            if deal:
                self.stats["mptf_alerts"] += 1
                await self.send_deal(deal)
            else:
                self.stats["mptf_rejected_by_checks"] += 1

    # -- stntrading.eu (opt-in watchlist - see /watchstn in Telegram) -------

    async def stntrading_poll_loop(self):
        if not self.cfg.get("stntrading_api_key"):
            return  # nothing to check without a key - skip the loop entirely
        client = self.stn_client
        poll_seconds = self.cfg.get("stntrading_poll_seconds", 300)
        log.info("Polling stntrading.eu watchlist every %ss (when non-empty)", poll_seconds)
        while True:
            try:
                await self._check_stn_watchlist(client)
            except Exception:
                log.exception("stntrading.eu poll failed, will retry next cycle.")
            await asyncio.sleep(poll_seconds)

    async def _check_stn_watchlist(self, client):
        if self.runtime.paused or not self.runtime.stn_watchlist:
            return
        for item_name in list(self.runtime.stn_watchlist):
            self.stats["stn_received"] += 1
            price_keys, _stock = await asyncio.to_thread(
                client.get_item_price_keys, item_name, self.bptf.key_price_metal
            )
            if price_keys is None:
                continue  # no Premium access, or nothing currently for sale - not an error

            quality = "Unique"
            for q in bptf_client.QUALITY_NAME_TO_ID:
                if q != "Unique" and item_name.startswith(q + " "):
                    quality = q
                    break
            bare_name = bptf_client.strip_quality_prefix(item_name, quality)

            # Re-checking the same watched item repeatedly at an unchanged
            # price shouldn't re-alert every poll - dedup on price too, so
            # a genuine price *change* still gets through.
            dedup_id = f"stn:{item_name}:{round(price_keys, 2)}"
            if not self._mark_seen(dedup_id):
                self.stats["stn_deduped"] += 1
                continue

            listing = matcher.NormalizedListing(
                source="stntrading.eu",
                listing_id=dedup_id,
                name=bare_name,
                quality=quality,
                category=classify_category(bare_name),
                craftable=True,
                price_keys=price_keys,
                price_usd=None,
                link="https://stntrading.eu/tf2",
            )
            self.stats["stn_evaluated"] += 1
            deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg(), self.name_to_image_url, self.stn_client)
            if deal:
                self.stats["stn_alerts"] += 1
                await self.send_deal(deal)
            else:
                self.stats["stn_rejected_by_checks"] += 1

    def _build_alert_keyboard(self, deal: dict):
        """
        Inline URL buttons for the alert - Trade/Buy and the classifieds
        search link as actual tappable buttons instead of plain text
        links in the message body, closer to how a real, well-regarded
        Discord alert bot presents the same kind of information (real
        example seen: Trade URL / Classifieds / Item History / Steam
        Profile as buttons under the embed). Telegram buttons need a
        real https URL, so anything without one (e.g. a closed-inventory
        listing with no direct trade link) is simply omitted from the
        row rather than shown as a dead button.
        """
        buttons = []
        if deal.get("trade_closed"):
            if deal.get("profile_link"):
                buttons.append({"text": "👤 Профиль продавца", "url": deal["profile_link"]})
        elif deal.get("link"):
            buy_labels = {
                "mannco.store": "🔗 Купить",
                "backpack.tf": "🔗 Трейд с продавцом",
                "marketplace.tf": "🔗 Купить",
                "stntrading.eu": "🔗 Открыть stntrading.eu",
            }
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

        suggested_line = ""
        if deal.get("suggested_keys") is not None:
            date_part = ""
            days_ago = deal.get("suggested_updated_days_ago")
            if days_ago is not None:
                import datetime
                updated_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime("%d.%m.%Y")
                date_part = f" — {updated_date}"
            suggested_line = (
                f"\n💬 Справочная цена (community): {deal['suggested_keys']:.2f} ключей{date_part}"
            )

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
            seller_note_line = f"\n📝 Продавец пишет: <i>{html.escape(deal['seller_note'])}</i>"

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
            f"Цена в объявлении: <b>{deal['price_keys']:.2f} ключей</b>{usd}\n"
            f"Было: {deal['previous_low_keys']:.2f} ключей"
            f"{avg_line}"
            f"{suggested_line}"
            f"{buy_order_line}"
            f"{stn_buy_line}"
            f"{liquidity_line}"
            f"{seller_note_line}"
            f"{history_line}"
            f"{links_block}"
        )

    async def send_deal(self, deal):
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
            "DEAL [%s]: %s - %.2f keys vs %.2f keys before (%.0f%% off)",
            deal["source"], deal["display_name"],
            deal["price_keys"], deal["previous_low_keys"], deal["discount_percent"],
        )
        alert_text = self.format_alert(deal)
        keyboard = self._build_alert_keyboard(deal)
        image_url = deal.get("image_url")
        sent_as_photo = False
        if image_url:
            # send_photo itself refuses (returns False, doesn't raise) if
            # the caption is over Telegram's 1024-char photo-caption limit
            # (plain messages allow 4096) - falls straight through to the
            # normal text-only send below either way, so a long alert or a
            # dead/blocked image URL never costs the alert itself.
            sent_as_photo = await asyncio.to_thread(self.telegram.send_photo, image_url, alert_text, keyboard)
        if not sent_as_photo:
            await asyncio.to_thread(self.telegram.send, alert_text, keyboard)

    # -- mannco.store side ------------------------------------------------

    async def handle_mannco_event(self, event: dict):
        if self.runtime.paused:
            return
        self.stats["mannco_received"] += 1

        data = event.get("data", {})
        item_id = data.get("itemId")
        listing_id = data.get("id")
        price_cents = data.get("price") if event.get("event") == "listing_added" else data.get("newPrice")

        if item_id is None or price_cents is None or listing_id is None:
            self.stats["mannco_malformed"] += 1
            return
        # Dedup key includes the price, not just the listing id - a price
        # CHANGE on an already-seen listing must still be evaluated fresh.
        # Confirmed this matters: mannco.store's own price_changed event
        # (the `newPrice` branch above) carries a real new price for an
        # EXISTING listing id, and deduping by id alone would have
        # silently swallowed every one of those, missing exactly the kind
        # of price-drop event this whole feature exists to catch.
        if not self._mark_seen(f"mannco:{listing_id}:{price_cents}"):
            self.stats["mannco_deduped"] += 1
            return

        # Skip the details fetch ENTIRELY for an item_id already known to
        # be a different game - direct feedback that other-game items
        # were being fetched and "messing up parsing" (each one is a real
        # mannco.store API call that only gets discarded afterward at the
        # game!=440 check below). mannco.store's own catalog item ids are
        # apparently shared across all 5 games it supports (440/730/570/
        # 252490/753), and the SAME popular CS2/Dota2/Rust item type
        # tends to generate many repeat listing events, so remembering
        # "this item_id isn't TF2" the first time avoids paying for that
        # same lookup again and again for the rest of this run.
        if item_id in self._known_non_tf2_item_ids:
            self.stats["mannco_wrong_game"] += 1
            return

        details = await asyncio.to_thread(self.mannco.get_item_details, item_id)
        if details is None:
            # A real production log showed this bucket silently absorbing
            # nearly an entire 5-minute window (4854 received, 0 reaching
            # evaluation, with no specific reason accounting for the gap)
            # - traced to mannco.store's own rate limiting on this exact
            # call, now throttled (see mannco_client.py). Counted
            # separately now so a repeat of that pattern is immediately
            # visible in /stats instead of leaving another unexplained
            # gap between received and evaluated.
            self.stats["mannco_details_failed"] += 1
            return
        if details.get("game") != 440:
            self._known_non_tf2_item_ids.add(item_id)
            self.stats["mannco_wrong_game"] += 1
            return

        quality = (details.get("quality") or "").strip(" ;")
        if quality not in self.runtime.watched_qualities:
            self.stats["mannco_rejected_quality"] += 1
            return

        particle_id = None
        particle_name = None
        if quality == "Unusual":
            particle_name = mannco_effect_name(details)
            if particle_name and self.particle_name_to_id:
                particle_id = self.particle_name_to_id.get(particle_name)
                if particle_id is None:
                    # Exact match failed - try case/whitespace-normalized,
                    # in case mannco.store formats the effect name
                    # slightly differently than Valve's own schema does.
                    particle_id = self.particle_name_to_id_normalized.get(particle_name.strip().lower())

        if not self.mannco_key_usd_cents:
            # A real /stats report showed a large, completely unexplained
            # gap between received and every other counted bucket for
            # mannco.store (9392 received, only 3665 accounted for across
            # every other reason) - traced to this exact return having no
            # counter at all. Given the key price now only refreshes
            # weekly (see key_price_refresh_loop in this file), a single
            # failed refresh can leave mannco_key_usd_cents unset - and
            # therefore every qualifying mannco.store item silently
            # dropped here - for up to a week, not just a brief blip.
            # Logged once per run (not per item) so a real, ongoing
            # outage is visible in /errors without flooding it.
            if "mannco_no_key_price" not in self._sampled_item_kinds:
                self._sampled_item_kinds.add("mannco_no_key_price")
                log.warning(
                    "mannco.store key price is unavailable - every qualifying mannco.store item "
                    "will be skipped until the next successful key-price refresh."
                )
            self.stats["mannco_no_key_price"] += 1
            return
        price_keys = price_cents / self.mannco_key_usd_cents

        # details["url"] is mannco's own URL slug for the item's market
        # page (confirmed field in their API docs). Best-effort link -
        # not verified live against the real site from where this was
        # written; if the path segment turns out wrong, easy one-line fix.
        slug = details.get("url")
        link = f"https://mannco.store/item/{slug}" if slug else "https://mannco.store/tf2"

        listing = matcher.NormalizedListing(
            source="mannco.store",
            listing_id=str(listing_id),
            name=details.get("name") or "",
            quality=quality,
            item_type=(details.get("type") or ""),
            category=classify_category(details.get("name"), mannco_type=details.get("type")),
            particle_id=particle_id,
            particle_name=particle_name,
            craftable=bool(details.get("craftable", True)),
            price_keys=price_keys,
            price_usd=price_cents / 100,
            link=link,
            spells=mannco_spells(details),
            strange_parts=mannco_strange_parts(details),
            paint=mannco_paint(details),
            paint_decimal_hint=mannco_paint_decimal_hint(details),
        )

        self.stats["mannco_evaluated"] += 1
        deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg(), self.name_to_image_url, self.stn_client)
        if deal:
            self.stats["mannco_alerts"] += 1
            await self.send_deal(deal)
        else:
            self.stats["mannco_rejected_by_checks"] += 1

    # -- backpack.tf side ---------------------------------------------------

    async def handle_bptf_event(self, payload: dict):
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
        if quality not in self.runtime.watched_qualities:
            self.stats["bptf_rejected_quality"] += 1
            return

        name = item.get("name") or item.get("marketName") or item.get("baseName")
        if not name:
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
        if particle_id is not None and not particle_name:
            # Resolve the name from the id via the schema we already have,
            # in case a fallback path above found the id but not a name.
            particle_name = self.particle_id_to_name.get(particle_id)

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

        # Exact RGB decimal straight from the source, same idea as
        # mannco_paint_decimal_hint - confirmed real for backpack.tf too
        # by the diagnostic sample below: a raw paint object of {'id':
        # 5046, 'name': 'Team Spirit', 'color': '#b8383b'} - '#b8383b'
        # decodes (after stripping the '#', unlike mannco.store's own
        # hex which has none) to (184, 56, 59), an exact match for this
        # project's own "Team Spirit RED" value. Team-coloured paints no
        # longer need the RED/BLU guess-and-check for backpack.tf either -
        # this goes straight to the exact colour the listing actually is.
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

        is_skin = bool(item.get("texture") or item.get("wearTier"))
        craftable = item.get("craftable")
        if craftable is None:
            craftable = True

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
            category=classify_category(name, slot=slot),
            particle_id=particle_id,
            particle_name=particle_name,
            craftable=bool(craftable),
            price_keys=price_keys,
            price_usd=price_usd,
            link=link,
            extra_excluded_hint=is_skin,
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
        deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg(), self.name_to_image_url, self.stn_client)
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
        await asyncio.to_thread(self.telegram.register_commands, telegram_commands.BOT_COMMANDS)
        log.info("Listening for Telegram commands and button taps...")

        while True:
            try:
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
                await asyncio.to_thread(self.telegram.send, menu_text, keyboard)
            elif command == "errors":
                # Sent WITH a keyboard (pagination buttons) - unlike the
                # other typed commands below, which are plain text.
                errors_text, keyboard = telegram_commands.build_errors_view(_error_buffer.recent(100))
                await asyncio.to_thread(self.telegram.send, errors_text, keyboard)
            else:
                reply = telegram_commands.handle_command(
                    text, self.runtime, has_stn_key=bool(self.cfg.get("stntrading_api_key")),
                    stats=self.stats, stats_since=self.stats_since,
                    currently_rate_limited=bptf_client.is_rate_limited(),
                )
                log.info("Telegram command: %r -> %s", text, reply.splitlines()[0])
                await asyncio.to_thread(self.telegram.send, reply)
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
            await asyncio.to_thread(self.telegram.answer_callback_query, event["id"])
            if event["message_id"] is not None:
                await asyncio.to_thread(self.telegram.edit_message, event["message_id"], menu_text, keyboard)

    async def _startup_sanity_check(self):
        """
        Proactively checks a few core assumptions against what actually
        got loaded, right after startup, and sends an immediate Telegram
        warning if any look wrong - rather than waiting for the user to
        notice bad alerts and report them. Each of these bounds traces
        back to a real, previously-hit bug in this project's history:
        key_price_metal ending up None (a parsing bug that broke every
        metal->keys conversion silently) and a schema fetch coming back
        suspiciously small (would mean marketplace.tf items and Unusual
        effect matching are both compromised) are exactly the kind of
        thing that otherwise wasn't visible until an alert already looked
        wrong.
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
            if len(self.defindex_to_name) < 5000:
                problems.append(
                    f"Загружено только {len(self.defindex_to_name)} записей defindex->название "
                    f"(обычно 10000+) - marketplace.tf и картинки предметов могут работать не полностью."
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
        await asyncio.to_thread(self.telegram.send, message)

    async def run(self):
        log.info("Starting up: loading initial prices...")
        try:
            await asyncio.to_thread(self.refresh_prices)
        except Exception:
            # A transient hiccup here (e.g. mannco.store still rate-limited
            # even after login()'s own retry) shouldn't take down the whole
            # process the way it used to - price_refresh_loop below retries
            # this on its own schedule anyway, so it's fine to start the
            # websocket listeners now and let prices catch up shortly after,
            # rather than crash-and-let-systemd-restart over it.
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

        await asyncio.gather(
            self.price_refresh_loop(),
            self.key_price_refresh_loop(),
            self.telegram_command_loop(),
            self.health_check_loop(),
            self.marketplacetf_poll_loop(),
            self.stntrading_poll_loop(),
            mannco_ws.stream_listing_events(self.handle_mannco_event),
            bptf_ws.stream_listing_events(self.handle_bptf_event),
        )


def main():
    cfg = load_config()
    watcher = Watcher(cfg)
    try:
        asyncio.run(watcher.run())
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
