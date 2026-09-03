"""
Telegram bot control surface: both typed commands (/pause, /minprice 10,
...) and an inline button menu (/menu) that does the same things by
tapping instead of typing.

Long-polling (GET /getUpdates) - no webhook/port needed. Handles two
kinds of Telegram updates:
  - "message": a typed command
  - "callback_query": a button tap in a menu this bot sent

Only events from the configured telegram_chat_id are ever acted on -
anyone else who happens to message the bot is silently ignored.
"""

import html
import logging
import time

import requests

from bptf_client import QUALITY_NAME_TO_ID

log = logging.getLogger("telegram_commands")

VALID_QUALITIES = list(QUALITY_NAME_TO_ID.keys())
VALID_CATEGORIES = ["weapon", "cosmetic", "taunt", "killstreak_kit", "other"]
PRICE_PRESETS = [1, 5, 10, 20, 50, 100, 200, 500]
DISCOUNT_PRESETS = [10, 15, 20, 25, 30, 40, 50, 70]
LIQUIDITY_PRESETS = [14, 30, 45, 60, 90, 120, 180, 365]

BOT_COMMANDS = [
    ("menu", "Открыть меню настроек"),
    ("status", "Текущие настройки"),
    ("pause", "Приостановить"),
    ("resume", "Возобновить"),
    ("help", "Список текстовых команд"),
]

HELP_TEXT = (
    "<b>Команды</b>\n"
    "/menu — меню с кнопками (рекомендуется)\n"
    "/status — текущие настройки\n"
    "/stats — сколько событий пришло по каждому источнику и где отсеялись "
    "(диагностика, если алертов кажется меньше, чем должно быть)\n"
    "/errors — последние предупреждения/ошибки из логов, без захода на сервер\n"
    "/pause — приостановить уведомления\n"
    "/resume — возобновить\n"
    "/minprice [число] — минимальная цена в ключах (без числа — показать текущую)\n"
    "/discount [число] — порог скидки в % (без числа — показать текущий)\n"
    "/liquidity [число] — игнорировать предметы без переоценки цены дольше N дней\n"
    "/qualities, /addquality Название, /removequality Название\n"
    "/categories, /addcategory weapon|cosmetic|taunt|killstreak_kit|other, /removecategory ...\n"
    "/australium — переключить режим \"только Australium-оружие\" (в меню: кнопка внутри Категорий)\n"
    "/checkitem Название — показать, что бот прямо сейчас знает про этот "
    "предмет (buy order, sell-референс, когда видели в последний раз)\n"
    "/priority, /addpriority Название, /removepriority Название — список "
    "приоритетных предметов (⭐ в алерте + живой запрос к backpack.tf, если "
    "в базе ещё нет buy order); Unusual и так всегда приоритетны\n"
    "/setaccounts — настроить несколько аккаунтов backpack.tf для "
    "/setaccounts — настроить несколько аккаунтов backpack.tf, чтобы "
    "ускорить проактивное сканирование Unusual, живые запросы по "
    "приоритетным предметам и проверку ликвидности/средней цены для "
    "любых предметов (по одной паре ключ+токен на строку)\n"
    "/help — это сообщение"
)


class TelegramCommandListener:
    def __init__(self, bot_token: str, chat_id: str):
        self.api_base = f"https://api.telegram.org/bot{bot_token}"
        self.chat_id = str(chat_id)
        self.offset = 0
        self.session = requests.Session()

    def get_updates(self):
        """
        One long-poll call (blocking - call via asyncio.to_thread).
        Returns a list of events from the configured chat only:
          {"type": "message", "text": "..."}
          {"type": "callback_query", "id": "...", "data": "...", "message_id": 123}
        Advances the internal offset so nothing is processed twice.
        """
        try:
            resp = self.session.get(
                f"{self.api_base}/getUpdates",
                params={"offset": self.offset, "timeout": 25,
                        "allowed_updates": '["message","callback_query"]'},
                timeout=35,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            log.warning("Telegram getUpdates request failed, will retry.")
            return []

        if not data.get("ok"):
            return []

        events = []
        for update in data.get("result", []):
            self.offset = update["update_id"] + 1

            message = update.get("message")
            if message:
                chat = message.get("chat") or {}
                text = message.get("text")
                if text and str(chat.get("id")) == self.chat_id:
                    events.append({"type": "message", "text": text.strip()})
                elif text:
                    log.warning("Ignored a message from an unrecognised chat id %s", chat.get("id"))
                continue

            callback = update.get("callback_query")
            if callback:
                cb_message = callback.get("message") or {}
                chat = cb_message.get("chat") or {}
                if str(chat.get("id")) == self.chat_id:
                    events.append({
                        "type": "callback_query",
                        "id": callback.get("id"),
                        "data": callback.get("data", ""),
                        "message_id": cb_message.get("message_id"),
                    })
                else:
                    log.warning("Ignored a callback from an unrecognised chat id %s", chat.get("id"))
        return events


# -- button menu screens --------------------------------------------------

def build_main_menu(runtime):
    text = (
        "⚙️ <b>Настройки</b>\n\n"
        f"Статус: {'⏸ на паузе' if runtime.paused else '▶️ работает'}\n"
        f"Мин. цена: {runtime.min_price_keys:g} ключей\n"
        f"Скидка от рынка: от {runtime.discount_threshold_percent:g}%\n"
        f"Ликвидность: не старше {runtime.max_days_since_price_update:g} дн.\n"
        f"Качества: {', '.join(runtime.watched_qualities) or '—'}\n"
        f"Категории: {', '.join(runtime.watched_categories) or '—'}"
    )
    pause_button = (
        {"text": "▶️ Возобновить", "callback_data": "t:pause"}
        if runtime.paused else
        {"text": "⏸ Пауза", "callback_data": "t:pause"}
    )
    keyboard = [
        [pause_button],
        [{"text": "💰 Мин. цена", "callback_data": "m:price"},
         {"text": "🎯 Скидка %", "callback_data": "m:discount"}],
        [{"text": "✨ Качества", "callback_data": "m:qual"},
         {"text": "📦 Категории", "callback_data": "m:cat"}],
        [{"text": "📈 Ликвидность", "callback_data": "m:liquidity"}],
        [{"text": "🔑 Аккаунты backpack.tf", "callback_data": "m:accounts"}],
    ]
    return text, keyboard


def build_accounts_menu(runtime):
    text = (
        "🔑 <b>Аккаунты backpack.tf</b>\n\n"
        "Несколько аккаунтов ускоряют: проактивное сканирование Unusual, "
        "живые запросы по приоритетным предметам (/priority) и проверку "
        "ликвидности/средней цены для любых предметов. Отправь команду:\n\n"
        "<code>/setaccounts\nключ1 токен1\nключ2 токен2\nключ3 токен3</code>\n\n"
        "По одной паре api_key и token (через пробел или запятую) на строку - "
        "основной аккаунт из конфига добавляется автоматически, его повторять не нужно."
    )
    keyboard = [[{"text": "⬅️ Назад", "callback_data": "m:main"}]]
    return text, keyboard


def build_qualities_menu(runtime):
    text = "✨ <b>Качества</b>\nНажми, чтобы включить/выключить."
    keyboard = []
    row = []
    for q in VALID_QUALITIES:
        mark = "✅" if q in runtime.watched_qualities else "⬜"
        row.append({"text": f"{mark} {q}", "callback_data": f"t:q:{q}"})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "⬅️ Назад", "callback_data": "m:main"}])
    return text, keyboard


def build_categories_menu(runtime):
    text = (
        "📦 <b>Категории</b>\n"
        "weapon = оружие, cosmetic = шапки/аксессуары, taunt = насмешки,\n"
        "killstreak_kit = наборы киллстриков (обычно Unique-качества — "
        "включи ещё и Unique в /qualities, иначе они не пройдут фильтр "
        "по качеству), other = всё остальное\n\n"
        f"+ Australium-оружие: {'✅ включено' if runtime.australium_only else '⬜ выключено'} "
        "(добавляет Australium-оружие в поиск ДОПОЛНИТЕЛЬНО, не требуя отдельно "
        "включать weapon и Strange выше — остальные категории/качества продолжают "
        "работать как обычно, ничего не отключается)"
    )
    keyboard = []
    row = []
    for c in VALID_CATEGORIES:
        mark = "✅" if c in runtime.watched_categories else "⬜"
        row.append({"text": f"{mark} {c}", "callback_data": f"t:c:{c}"})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    australium_mark = "✅" if runtime.australium_only else "⬜"
    keyboard.append([{"text": f"{australium_mark} + Australium-оружие", "callback_data": "t:australium"}])
    keyboard.append([{"text": "⬅️ Назад", "callback_data": "m:main"}])
    return text, keyboard


def build_price_menu(runtime):
    text = (
        f"💰 <b>Минимальная цена</b>\n"
        f"Сейчас: {runtime.min_price_keys:g} ключей\n\n"
        f"Нужно другое число — просто напиши боту, например: /minprice 33"
    )
    keyboard = []
    row = []
    for v in PRICE_PRESETS:
        is_current = abs(runtime.min_price_keys - v) < 1e-9
        label = f"🔘{v}" if is_current else str(v)
        row.append({"text": label, "callback_data": f"p:{v}"})
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "⬅️ Назад", "callback_data": "m:main"}])
    return text, keyboard


def build_discount_menu(runtime):
    text = (
        f"🎯 <b>Порог скидки</b>\n"
        f"Сейчас: алерт при цене от {runtime.discount_threshold_percent:g}% ниже рынка\n\n"
        f"Меньше % — больше алертов (и больше шума). Своё число —\n"
        f"/discount 12"
    )
    keyboard = []
    row = []
    for v in DISCOUNT_PRESETS:
        is_current = abs(runtime.discount_threshold_percent - v) < 1e-9
        label = f"🔘{v}%" if is_current else f"{v}%"
        row.append({"text": label, "callback_data": f"d:{v}"})
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "⬅️ Назад", "callback_data": "m:main"}])
    return text, keyboard


def build_liquidity_menu(runtime):
    text = (
        f"📈 <b>Порог ликвидности</b>\n"
        f"Сейчас: игнорировать предметы без переоценки цены на\n"
        f"backpack.tf дольше {runtime.max_days_since_price_update:g} дн.\n\n"
        f"Смысл: если скидка есть, а предмет никто давно не продавал\n"
        f"и не переоценивал — скорее всего, это не реальная сделка, а\n"
        f"просто забытая цена. Больше значение — строже отсекаем "
        f"неликвид.\n"
        f"Честная оговорка: backpack.tf не отдаёт историю подтверждённых\n"
        f"продаж бесплатно (это платная Premium-функция) — используется\n"
        f"дата последней переоценки цены сообществом как ближайший\n"
        f"бесплатный показатель активности по предмету.\n\n"
        f"Своё число — /liquidity 45"
    )
    keyboard = []
    row = []
    for v in LIQUIDITY_PRESETS:
        is_current = abs(runtime.max_days_since_price_update - v) < 1e-9
        label = f"🔘{v}" if is_current else str(v)
        row.append({"text": label, "callback_data": f"l:{v}"})
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "⬅️ Назад", "callback_data": "m:main"}])
    return text, keyboard


def handle_callback(data: str, runtime, error_entries=None):
    """Applies one button tap and returns (text, keyboard) for the menu
    screen to show afterwards (the caller edits the tapped message in
    place with this)."""
    if data.startswith("e:p:"):
        try:
            page = int(data[4:])
        except ValueError:
            page = 0
        return build_errors_view(error_entries or [], page)
    if data == "m:main":
        return build_main_menu(runtime)
    if data == "m:qual":
        return build_qualities_menu(runtime)
    if data == "m:cat":
        return build_categories_menu(runtime)
    if data == "m:price":
        return build_price_menu(runtime)
    if data == "m:discount":
        return build_discount_menu(runtime)
    if data == "m:liquidity":
        return build_liquidity_menu(runtime)
    if data == "m:accounts":
        return build_accounts_menu(runtime)

    if data == "t:pause":
        runtime.paused = not runtime.paused
        runtime.save()
        return build_main_menu(runtime)

    if data.startswith("t:q:"):
        quality = data[4:]
        if quality in runtime.watched_qualities:
            runtime.watched_qualities.remove(quality)
        elif quality in VALID_QUALITIES:
            runtime.watched_qualities.append(quality)
        runtime.save()
        return build_qualities_menu(runtime)

    if data.startswith("t:c:"):
        category = data[4:]
        if category in runtime.watched_categories:
            runtime.watched_categories.remove(category)
        elif category in VALID_CATEGORIES:
            runtime.watched_categories.append(category)
        runtime.save()
        return build_categories_menu(runtime)

    if data == "t:australium":
        runtime.australium_only = not runtime.australium_only
        runtime.save()
        return build_categories_menu(runtime)

    if data.startswith("p:"):
        try:
            runtime.min_price_keys = float(data[2:])
            runtime.save()
        except ValueError:
            pass
        return build_price_menu(runtime)

    if data.startswith("d:"):
        try:
            runtime.discount_threshold_percent = float(data[2:])
            runtime.save()
        except ValueError:
            pass
        return build_discount_menu(runtime)

    if data.startswith("l:"):
        try:
            runtime.max_days_since_price_update = float(data[2:])
            runtime.save()
        except ValueError:
            pass
        return build_liquidity_menu(runtime)

    return build_main_menu(runtime)


# -- typed commands (still supported alongside the button menu) ----------

def _find_quality(name: str):
    for q in VALID_QUALITIES:
        if q.lower() == name.lower():
            return q
    return None


def _find_category(name: str):
    for c in VALID_CATEGORIES:
        if c.lower() == name.lower():
            return c
    return None


def _format_stats(stats, stats_since, currently_rate_limited=False) -> str:
    """
    Answers "is the bot even seeing the volume I'd expect, and if so,
    where's it narrowing down" - a real question that came up when alert
    volume looked lower than the site's overall activity would suggest.
    Shows the funnel per source since the last time /stats was read (see
    main.py - the counters reset after every read), not a lifetime total,
    so this always reflects "what's happened lately". Also flags whether
    backpack.tf's rate-limit cooldown is active RIGHT NOW - a direct
    answer to "is the bot currently being throttled", rather than having
    to infer it from the funnel numbers.
    """
    if stats is None:
        return "Статистика недоступна."
    if stats_since is not None:
        minutes = max(1, round((time.time() - stats_since) / 60))
        header = f"📊 <b>За последние {minutes} мин.</b>\n\n"
    else:
        header = "📊 <b>Статистика</b>\n\n"
    if currently_rate_limited:
        header += "⏳ Сейчас в кулдауне после 429 от backpack.tf - оценка временно приостановлена.\n\n"

    if not stats:
        return header + "Событий пока не было."

    lines = []
    total_alerts = 0
    sources = [
        ("bptf", "backpack.tf"),
    ]
    for prefix, label in sources:
        received = stats.get(f"{prefix}_received", 0)
        evaluated = stats.get(f"{prefix}_evaluated", 0)
        alerts = stats.get(f"{prefix}_alerts", 0)
        deduped = stats.get(f"{prefix}_deduped", 0)
        rejected_quality = stats.get(f"{prefix}_rejected_quality", 0)
        rejected_category = stats.get(f"{prefix}_rejected_category", 0)
        rejected_checks = stats.get(f"{prefix}_rejected_by_checks", 0)
        total_alerts += alerts

        if received == 0 and evaluated == 0:
            lines.append(f"<b>{label}</b>: 0 событий - проверь подключение (см. журнал)")
            continue

        detail_bits = []
        if deduped:
            detail_bits.append(f"{deduped} повтор(ов)")
        if rejected_quality:
            detail_bits.append(f"{rejected_quality} не по качеству")
        if rejected_category:
            detail_bits.append(f"{rejected_category} не по категории")
        if rejected_checks:
            detail_bits.append(f"{rejected_checks} отсеяно проверками точности")
        # Breaks that single bucket down further - see evaluate_listing's
        # own reject() helper in matcher.py for why: a real /stats report
        # (263558 received, 23610 evaluated, only 31 found for
        # backpack.tf) made clear that one aggregate number couldn't
        # answer the actual question - was this "genuinely not a deal"
        # (discount_too_small) or "nothing to compare it against yet"
        # (no_reference_data, the local store's own cold-start reality
        # for this exact item, not a bug)? These two are usually the
        # overwhelming majority of rejected_checks, so they're shown
        # separately; smaller-volume reasons (min_price, kit_category,
        # no_particle_id, unmapped_paint, tier_inconsistency) stay folded
        # into the aggregate above rather than cluttering this line
        # further.
        no_ref_data = stats.get(f"{prefix}_rejected_no_live_buy_order", 0)
        if no_ref_data:
            detail_bits.append(f"{no_ref_data} нет живого buy order")
        too_small = stats.get(f"{prefix}_rejected_discount_too_small", 0)
        if too_small:
            detail_bits.append(f"{too_small} скидка меньше порога")
        # The REST of evaluate_listing's own reject() reasons (see
        # matcher.py) - added after a real report where only 107 of 494
        # rejected-by-checks items were accounted for by the two bits
        # above, leaving 387 with no visibility into why at all. These
        # are lower-volume in the common case (most rejections are one
        # of the two above), which is why they were folded into the
        # aggregate originally - but "usually low volume" isn't the same
        # as "never worth seeing", and there was no way to tell the two
        # apart without this.
        min_price = stats.get(f"{prefix}_rejected_min_price", 0)
        if min_price:
            detail_bits.append(f"{min_price} ниже мин. цены")
        kit_cat = stats.get(f"{prefix}_rejected_kit_category", 0)
        if kit_cat:
            detail_bits.append(f"{kit_cat} killstreak kit")
        no_particle = stats.get(f"{prefix}_rejected_no_particle_id", 0)
        if no_particle:
            detail_bits.append(f"{no_particle} не разрешён эффект")
        unmapped_paint = stats.get(f"{prefix}_rejected_unmapped_paint", 0)
        if unmapped_paint:
            detail_bits.append(f"{unmapped_paint} неизвестная краска")
        tier_inconsistent = stats.get(f"{prefix}_rejected_tier_inconsistency", 0)
        if tier_inconsistent:
            detail_bits.append(f"{tier_inconsistent} нестабильные тиры")
        # backpack.tf only - buy-intent listings recorded into the local
        # store (see LocalListingStore) rather than evaluated for a deal.
        # Added after a real report where this bucket alone accounted
        # for the majority of "received" with no visible explanation
        # anywhere in the numbers shown.
        buy_recorded = stats.get(f"{prefix}_buy_recorded", 0)
        if buy_recorded:
            detail_bits.append(f"{buy_recorded} buy-заявок записано в базу")
        detail = f" ({', '.join(detail_bits)})" if detail_bits else ""

        lines.append(
            f"<b>{label}</b>: получено {received} → оценено {evaluated} → "
            f"найдено {alerts}{detail}"
        )

    lines.append(f"\nИтого алертов: {total_alerts}")

    scans = stats.get("proactive_unusual_scans", 0)
    if scans:
        recorded = stats.get("proactive_unusual_buy_orders_recorded", 0)
        lines.append(
            f"\n🔄 Проактивное сканирование Unusual: {scans} запрос(ов), "
            f"{recorded} buy-заявок обновлено"
        )

    return header + "\n".join(lines)


ERRORS_PAGE_SIZE = 15


def build_errors_view(error_entries, page: int = 0):
    """
    Paginated /errors view - text + an inline keyboard with ◀️/▶️ buttons.
    Page 0 is the newest ERRORS_PAGE_SIZE entries; higher page numbers go
    further back in time. error_entries is oldest-first (as stored by
    error_log.TelegramErrorBuffer), so page 0 is the END of that list.
    """
    import datetime

    if not error_entries:
        return "Ошибок и предупреждений не было (или бот только что запущен).", []

    total = len(error_entries)
    total_pages = max(1, (total + ERRORS_PAGE_SIZE - 1) // ERRORS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))

    end = total - page * ERRORS_PAGE_SIZE
    start = max(0, end - ERRORS_PAGE_SIZE)
    shown = error_entries[start:end]

    lines = [
        f"⚠️ <b>Предупреждения/ошибки {start + 1}-{end} из {total}</b> "
        f"(стр. {page + 1}/{total_pages}, сначала новые):\n"
    ]
    for entry in reversed(shown):
        ts = datetime.datetime.fromtimestamp(entry.get("time", 0)).strftime("%d.%m %H:%M:%S")
        level = entry.get("level", "?")
        logger_name = entry.get("logger", "?")
        message = entry.get("message", "")
        if len(message) > 220:
            message = message[:220] + "…"
        # Escaped before going into this HTML-parse_mode message - a
        # real, persistent failure of /errors itself traced to this:
        # some logged messages legitimately contain raw HTML (e.g. an
        # HTTP error page's body, logged verbatim by this project's own
        # status-code diagnostics for backpack.tf's deprecated endpoint -
        # a 503 response can come back as an actual <html>...</html>
        # error page, not JSON). Left unescaped, that raw markup breaks
        # the ENTIRE Telegram message as invalid HTML, and the person
        # never even sees an error about it - the one tool meant to
        # surface exactly this kind of problem was the one silently
        # disabled by it. logger_name is also escaped defensively, even
        # though logger names in this project are all hardcoded, plain
        # identifiers - message content is the one field that can
        # contain genuinely anything.
        lines.append(f"[{ts}] <b>{html.escape(level)}</b> {html.escape(logger_name)}: {html.escape(message)}")
    text = "\n".join(lines)

    nav_row = []
    if page < total_pages - 1:
        nav_row.append({"text": "◀️ Старее", "callback_data": f"e:p:{page + 1}"})
    if page > 0:
        nav_row.append({"text": "Новее ▶️", "callback_data": f"e:p:{page - 1}"})
    keyboard = [nav_row] if nav_row else []
    return text, keyboard


def _format_errors(error_entries) -> str:
    """Kept for any caller still wanting a plain-text version (e.g. a
    non-Telegram context) - the real /errors command now uses
    build_errors_view above for pagination."""
    text, _ = build_errors_view(list(error_entries) if error_entries else [], page=0)
    return text


def handle_command(text: str, runtime, stats=None, stats_since=None,
                    currently_rate_limited: bool = False, error_entries=None) -> str:
    """Parses one typed command and applies it to `runtime`, returning
    the reply text. Any state change is saved to disk before returning."""
    parts = text.strip().split(maxsplit=1)
    command = parts[0].lower().lstrip("/").split("@")[0]
    arg = parts[1].strip() if len(parts) > 1 else ""

    if command == "help":
        return HELP_TEXT

    if command == "stats":
        return _format_stats(stats, stats_since, currently_rate_limited)

    if command == "errors":
        return _format_errors(error_entries)

    if command == "status":
        state = "⏸ на паузе" if runtime.paused else "▶️ работает"
        australium_line = "\nТолько Australium: ✅ включено" if runtime.australium_only else ""
        return (
            f"{state}\n"
            f"Минимальная цена: {runtime.min_price_keys:g} ключей\n"
            f"Порог скидки: {runtime.discount_threshold_percent:g}%\n"
            f"Порог ликвидности: {runtime.max_days_since_price_update:g} дн.\n"
            f"Качества: {', '.join(runtime.watched_qualities) or '(пусто)'}\n"
            f"Категории: {', '.join(runtime.watched_categories) or '(пусто)'}"
            f"{australium_line}"
        )

    if command == "australium":
        runtime.australium_only = not runtime.australium_only
        runtime.save()
        state = "включён - показываю только Australium-оружие" if runtime.australium_only else "выключен"
        return f"Режим \"только Australium\" {state}."

    if command == "pause":
        runtime.paused = True
        runtime.save()
        return "⏸ Приостановлено. /resume — включить обратно."

    if command == "resume":
        runtime.paused = False
        runtime.save()
        return "▶️ Возобновлено."

    if command == "minprice":
        if not arg:
            return f"Сейчас: {runtime.min_price_keys:g} ключей. Чтобы поменять: /minprice 10"
        try:
            value = float(arg.replace(",", "."))
        except ValueError:
            return f"Не понял число: {arg!r}. Пример: /minprice 10"
        if value < 0:
            return "Цена не может быть отрицательной."
        runtime.min_price_keys = value
        runtime.save()
        return f"Минимальная цена теперь {value:g} ключей."

    if command == "discount":
        if not arg:
            return f"Сейчас: от {runtime.discount_threshold_percent:g}%. Чтобы поменять: /discount 12"
        try:
            value = float(arg.replace(",", "."))
        except ValueError:
            return f"Не понял число: {arg!r}. Пример: /discount 12"
        if value < 0 or value > 100:
            return "Процент должен быть от 0 до 100."
        runtime.discount_threshold_percent = value
        runtime.save()
        return f"Порог скидки теперь {value:g}%."

    if command == "liquidity":
        if not arg:
            return (
                f"Сейчас: игнорировать предметы без переоценки цены дольше "
                f"{runtime.max_days_since_price_update:g} дн. Чтобы поменять: /liquidity 45"
            )
        try:
            value = float(arg.replace(",", "."))
        except ValueError:
            return f"Не понял число: {arg!r}. Пример: /liquidity 45"
        if value < 0:
            return "Число дней не может быть отрицательным."
        runtime.max_days_since_price_update = value
        runtime.save()
        return f"Порог ликвидности теперь {value:g} дн."

    if command == "qualities":
        return (
            f"Сейчас отслеживаются: {', '.join(runtime.watched_qualities) or '(пусто)'}\n"
            f"+ Unusual — всегда включён, без переключения\n"
            f"Доступные для переключения значения: {', '.join(VALID_QUALITIES)}"
        )

    if command == "addquality":
        if arg.strip().lower() == "unusual":
            return "Unusual и так всегда отслеживается, переключать не нужно."
        quality = _find_quality(arg)
        if not quality:
            return f"Не знаю качество {arg!r}. Доступные: {', '.join(VALID_QUALITIES)}"
        if quality in runtime.watched_qualities:
            return f"{quality} уже отслеживается."
        runtime.watched_qualities.append(quality)
        runtime.save()
        return f"Добавил {quality}. Сейчас: {', '.join(runtime.watched_qualities)}"

    if command == "removequality":
        if arg.strip().lower() == "unusual":
            return "Unusual нельзя отключить - он отслеживается всегда, по дизайну."
        quality = _find_quality(arg)
        if not quality or quality not in runtime.watched_qualities:
            return f"{arg!r} и так не отслеживается. Сейчас: {', '.join(runtime.watched_qualities) or '(пусто)'}"
        runtime.watched_qualities.remove(quality)
        runtime.save()
        return f"Убрал {quality}. Сейчас: {', '.join(runtime.watched_qualities) or '(пусто, алерты не будут приходить)'}"

    if command == "categories":
        return (
            f"Сейчас отслеживаются: {', '.join(runtime.watched_categories) or '(пусто)'}\n"
            f"Доступные значения: {', '.join(VALID_CATEGORIES)}"
        )

    if command == "priority":
        return (
            f"Приоритетные предметы (⭐, плюс живой запрос к backpack.tf, если в "
            f"локальной базе ещё нет buy order): {', '.join(runtime.priority_item_names) or '(пусто)'}\n"
            f"Unusual-качество и так всегда приоритетное, независимо от этого списка."
        )

    if command == "addpriority":
        if not arg:
            return "Укажи точное название предмета: /addpriority Max's Severed Head"
        if any(arg.lower() == n.lower() for n in runtime.priority_item_names):
            return f"{arg!r} уже в списке приоритетных."
        runtime.priority_item_names.append(arg)
        runtime.save()
        return f"Добавил {arg!r}. Сейчас: {', '.join(runtime.priority_item_names)}"

    if command == "removepriority":
        if not arg:
            return "Укажи точное название предмета: /removepriority Max's Severed Head"
        match = next((n for n in runtime.priority_item_names if n.lower() == arg.lower()), None)
        if not match:
            return f"{arg!r} и так не в списке. Сейчас: {', '.join(runtime.priority_item_names) or '(пусто)'}"
        runtime.priority_item_names.remove(match)
        runtime.save()
        return f"Убрал {match!r}. Сейчас: {', '.join(runtime.priority_item_names) or '(пусто)'}"

    if command == "addcategory":
        category = _find_category(arg)
        if not category:
            return f"Не знаю категорию {arg!r}. Доступные: {', '.join(VALID_CATEGORIES)}"
        if category in runtime.watched_categories:
            return f"{category} уже отслеживается."
        runtime.watched_categories.append(category)
        runtime.save()
        return f"Добавил {category}. Сейчас: {', '.join(runtime.watched_categories)}"

    if command == "removecategory":
        category = _find_category(arg)
        if not category or category not in runtime.watched_categories:
            return f"{arg!r} и так не отслеживается. Сейчас: {', '.join(runtime.watched_categories) or '(пусто)'}"
        runtime.watched_categories.remove(category)
        runtime.save()
        return f"Убрал {category}. Сейчас: {', '.join(runtime.watched_categories) or '(пусто, алерты не будут приходить)'}"

    return f"Не знаю команду {command!r}. /help — список команд, /menu — меню с кнопками."
