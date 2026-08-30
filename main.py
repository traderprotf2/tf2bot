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

import bptf_client
import bptf_ws
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
        return raw.get("name")
    if isinstance(raw, str):
        return raw
    return None


class Watcher:
    def __init__(self, cfg):
        self.cfg = cfg
        self.bptf = bptf_client.BackpackTFPriceList(
            cfg["backpacktf_api_key"],
            cfg.get("backpacktf_token", ""),
            cfg["snapshot_cache_seconds"],
        )
        self.mannco = mannco_client.ManncoClient(cfg["mannco_api_key"], cfg["jwt_refresh_seconds"])
        self.telegram = telegram_notify.TelegramNotifier(cfg["telegram_bot_token"], cfg["telegram_chat_id"])
        self.particle_name_to_id = steam_schema.fetch_particle_name_to_id(cfg.get("steam_api_key", ""))
        self.particle_id_to_name = {v: k for k, v in self.particle_name_to_id.items()}
        self.defindex_to_name = steam_schema.fetch_defindex_to_name(cfg.get("steam_api_key", ""))
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
        if self.runtime.paused or not self.mannco_key_usd_cents:
            return
        if not self.defindex_to_name:
            return  # can't safely resolve item names - see __init__ warning
        deals = await asyncio.to_thread(client.fetch_deals)
        for raw in deals:
            dedup_id = f"mptf:{raw['sku']}"
            if not self._mark_seen(dedup_id):
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
            deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg())
            if deal:
                await self.send_deal(deal)

    # -- stntrading.eu (opt-in watchlist - see /watchstn in Telegram) -------

    async def stntrading_poll_loop(self):
        if not self.cfg.get("stntrading_api_key"):
            return  # nothing to check without a key - skip the loop entirely
        client = stntrading_client.STNTradingClient(self.cfg["stntrading_api_key"])
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
            deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg())
            if deal:
                await self.send_deal(deal)

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

        liquidity_line = ""
        days_since = deal.get("days_since_price_update")
        if days_since is not None:
            liquidity_line = f"\n📊 Последняя переоценка цены: {days_since:.0f} дн. назад"

        link_lines = []
        if deal.get("trade_closed"):
            link_lines.append(f"👤 Профиль продавца: {deal['profile_link']}")
        elif deal["link"]:
            buy_labels = {
                "mannco.store": "Купить",
                "backpack.tf": "Трейд с продавцом",
                "marketplace.tf": "Купить",
                "stntrading.eu": "Открыть stntrading.eu",
            }
            buy_label = buy_labels.get(deal["source"], "Открыть")
            link_lines.append(f"🔗 {buy_label}: {deal['link']}")
        if deal.get("backpacktf_search_link"):
            link_lines.append(f"📋 Объявление на backpack.tf: {deal['backpacktf_search_link']}")
        links_block = ("\n" + "\n".join(link_lines)) if link_lines else ""

        return (
            f"🔥 <b>-{deal['discount_percent']:.0f}%</b> — {deal['source']}\n"
            f"<b>{deal['display_name']}</b>{effect}{variant}"
            f"{special_block}\n"
            f"Цена в объявлении: <b>{deal['price_keys']:.2f} ключей</b>{usd}\n"
            f"Было: {deal['previous_low_keys']:.2f} ключей"
            f"{avg_line}"
            f"{buy_order_line}"
            f"{liquidity_line}"
            f"{links_block}"
        )

    async def send_deal(self, deal):
        log.info(
            "DEAL [%s]: %s - %.2f keys vs %.2f keys before (%.0f%% off)",
            deal["source"], deal["display_name"],
            deal["price_keys"], deal["previous_low_keys"], deal["discount_percent"],
        )
        await asyncio.to_thread(self.telegram.send, self.format_alert(deal))

    # -- mannco.store side ------------------------------------------------

    async def handle_mannco_event(self, event: dict):
        if self.runtime.paused:
            return

        data = event.get("data", {})
        item_id = data.get("itemId")
        listing_id = data.get("id")
        price_cents = data.get("price") if event.get("event") == "listing_added" else data.get("newPrice")

        if item_id is None or price_cents is None or listing_id is None:
            return
        if not self._mark_seen(f"mannco:{listing_id}"):
            return

        details = await asyncio.to_thread(self.mannco.get_item_details, item_id)
        if details is None or details.get("game") != 440:
            return

        quality = (details.get("quality") or "").strip(" ;")
        if quality not in self.runtime.watched_qualities:
            return

        particle_id = None
        particle_name = None
        if quality == "Unusual":
            particle_name = mannco_effect_name(details)
            if particle_name and self.particle_name_to_id:
                particle_id = self.particle_name_to_id.get(particle_name)

        if not self.mannco_key_usd_cents:
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
        )

        deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg())
        if deal:
            await self.send_deal(deal)

    # -- backpack.tf side ---------------------------------------------------

    async def handle_bptf_event(self, payload: dict):
        if self.runtime.paused:
            return

        listing_id = payload.get("id")
        if listing_id is None:
            return
        if not self._mark_seen(f"bptf:{listing_id}"):
            return

        item = payload.get("item") or {}
        quality_obj = item.get("quality") or {}
        quality = quality_obj.get("name")
        if quality not in self.runtime.watched_qualities:
            return

        name = item.get("name") or item.get("marketName") or item.get("baseName")
        if not name:
            return

        particle_obj = item.get("particle") or {}
        particle_id = particle_obj.get("id")
        particle_name = particle_obj.get("name")

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
        )

        deal = await asyncio.to_thread(matcher.evaluate_listing, listing, self.bptf, self.effective_cfg())
        if not deal:
            return

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
            else:
                reply = telegram_commands.handle_command(
                    text, self.runtime, has_stn_key=bool(self.cfg.get("stntrading_api_key"))
                )
                log.info("Telegram command: %r -> %s", text, reply.splitlines()[0])
                await asyncio.to_thread(self.telegram.send, reply)

        elif event["type"] == "callback_query":
            menu_text, keyboard = telegram_commands.handle_callback(event["data"], self.runtime)
            log.info("Telegram button: %r", event["data"])
            # Answer first so Telegram clears the button's loading spinner
            # even if the edit below is slow or fails.
            await asyncio.to_thread(self.telegram.answer_callback_query, event["id"])
            if event["message_id"] is not None:
                await asyncio.to_thread(self.telegram.edit_message, event["message_id"], menu_text, keyboard)

    async def run(self):
        log.info("Starting up: loading initial prices...")
        await asyncio.to_thread(self.refresh_prices)

        await asyncio.gather(
            self.price_refresh_loop(),
            self.telegram_command_loop(),
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
