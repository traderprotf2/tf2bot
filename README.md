# TF2 Deal Watcher — установка

## Шаг 1. Собери реквизиты

- **backpack.tf API key + access token** — https://next.backpack.tf/account/api-access
  (войти через Steam, сгенерировать оба на этой странице)
- **mannco.store API key** — https://mannco.store/seller (войти через Steam)
- **Telegram bot token** — в Telegram: `@BotFather` → `/newbot`
- **Telegram chat id** — напиши своему боту любое сообщение, затем открой
  `https://api.telegram.org/botTOKEN/getUpdates` (замени TOKEN на токен бота),
  найди `"chat":{"id":ЧИСЛО`

## Шаг 2. Арендуй сервер

VPS с **Ubuntu 24.04** — Hetzner Cloud, DigitalOcean или любой другой.
Подключение: Windows — PuTTY, Mac/Linux — Терминал (`ssh root@IP`).

## Шаг 3. Выложи код на GitHub

1. https://github.com → **New repository** → **Public** → создать
2. На странице репозитория: **Add file → Upload files** → перетащи туда
   все файлы из архива → **Commit changes**
3. Скопируй ссылку репозитория: кнопка **Code → HTTPS**

## Шаг 4. Установи на сервере

```bash
apt-get update -qq && apt-get install -y -qq git
git clone ССЫЛКА_НА_РЕПО /root/tf2-deal-watcher
cd /root/tf2-deal-watcher
bash install.sh
```

Скрипт запросит реквизиты из Шага 1 — вставляй по очереди.

## Шаг 5. Включи автообновление

```bash
sudo bash setup-autoupdate.sh ССЫЛКА_НА_РЕПО
```

Скрипт выведет **Payload URL**, **Content type** и **Secret**. Дальше на
GitHub: репозиторий → **Settings → Webhooks → Add webhook** → вставь эти
три значения (Content type: `application/json`, события: **Just the
push event**) → **Add webhook**.

## Готово

- Настройки — напиши боту в Telegram `/menu`
- Статус: `systemctl status tf2-deal-watcher`
- Логи: `journalctl -u tf2-deal-watcher -f`
- Перезапуск: `systemctl restart tf2-deal-watcher`

## Обновление базы Unusual-эффектов (по желанию, вручную)

Бот работает полностью без Steam API — база эффектов (`unusual_effects.py`)
встроена и не требует сети. Если хочешь пополнить её самыми новыми
эффектами (добавленными после её последнего сбора), можно один раз
запустить `tools/update_effects_from_schema.py` с бесплатным Steam Web
API ключом (https://steamcommunity.com/dev/apikey) — он скачает
актуальную схему прямо у Valve и допишет в файл только то, чего там ещё
нет, не трогая остальное:

```bash
python3 tools/update_effects_from_schema.py ТВОЙ_STEAM_API_КЛЮЧ
```

Это разовая, необязательная утилита обслуживания — сам бот её никогда
не вызывает и в ней не нуждается для работы.
