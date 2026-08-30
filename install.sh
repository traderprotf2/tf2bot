#!/usr/bin/env bash
#
# TF2 Deal Watcher — automated installer.
#
# Usage (on the server, after uploading/unzipping this folder):
#     bash install.sh
#
# This script:
#   1. Installs Python + dependencies (apt + pip)
#   2. Asks you for your API keys ONCE and writes config.json
#   3. Registers the watcher as a systemd service (starts on boot,
#      restarts automatically if it ever crashes)
#   4. Starts it immediately
#
# Safe to re-run: if config.json already exists it won't ask again, it
# will just reinstall/restart the service (useful after you `git pull` or
# re-upload updated files).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$(id -u)" -ne 0 ]; then
    echo "Запусти этот скрипт с правами root (например: sudo bash install.sh)"
    exit 1
fi

echo "==> Устанавливаю Python и зависимости системы..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip >/dev/null

echo "==> Создаю виртуальное окружение и ставлю зависимости..."
python3 -m venv venv
venv/bin/pip install --quiet --upgrade pip
venv/bin/pip install --quiet -r requirements.txt

if [ ! -f config.json ]; then
    echo ""
    echo "==> Первая настройка: введи свои данные (Enter — оставить пустым/по умолчанию)"
    echo ""

    read -rp "backpack.tf API key: " BACKPACKTF_KEY
    read -rp "backpack.tf access token (необязательно, но настоятельно рекомендуется — см. README, Enter чтобы пропустить): " BACKPACKTF_TOKEN
    read -rp "mannco.store API key: " MANNCO_KEY
    read -rp "Telegram bot token: " TG_TOKEN
    read -rp "Telegram chat id: " TG_CHAT_ID
    read -rp "Steam Web API key (необязательно, Enter чтобы пропустить): " STEAM_KEY
    read -rp "Минимальная цена предмета в ключах [5]: " MIN_KEYS
    MIN_KEYS="${MIN_KEYS:-5}"
    read -rp "Минимальная скидка от цены backpack.tf в % [5]: " DISCOUNT
    DISCOUNT="${DISCOUNT:-5}"

    python3 - "$BACKPACKTF_KEY" "$BACKPACKTF_TOKEN" "$MANNCO_KEY" "$TG_TOKEN" "$TG_CHAT_ID" "$STEAM_KEY" "$MIN_KEYS" "$DISCOUNT" <<'PYEOF'
import json, sys

backpacktf_key, backpacktf_token, mannco_key, tg_token, tg_chat_id, steam_key, min_keys, discount = sys.argv[1:9]

with open("config.example.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

cfg["backpacktf_api_key"] = backpacktf_key
cfg["backpacktf_token"] = backpacktf_token
cfg["mannco_api_key"] = mannco_key
cfg["telegram_bot_token"] = tg_token
cfg["telegram_chat_id"] = tg_chat_id
cfg["steam_api_key"] = steam_key
cfg["min_price_keys"] = float(min_keys)
cfg["discount_threshold_percent"] = float(discount)

with open("config.json", "w", encoding="utf-8") as f:
    json.dump(cfg, f, ensure_ascii=False, indent=2)

print("config.json создан.")
PYEOF
else
    echo "==> config.json уже существует, пропускаю настройку (удали файл, если хочешь ввести данные заново)."
fi

echo "==> Настраиваю автозапуск (systemd)..."
sed \
    -e "s#WorkingDirectory=.*#WorkingDirectory=${SCRIPT_DIR}#" \
    -e "s#ExecStart=.*#ExecStart=${SCRIPT_DIR}/venv/bin/python3 ${SCRIPT_DIR}/main.py#" \
    tf2-deal-watcher.service > /etc/systemd/system/tf2-deal-watcher.service

systemctl daemon-reload
systemctl enable tf2-deal-watcher >/dev/null
systemctl restart tf2-deal-watcher

sleep 3
echo ""
echo "======================================================"
echo " Готово! Watcher запущен и работает в фоне 24/7."
echo ""
echo " Проверить статус:   systemctl status tf2-deal-watcher"
echo " Смотреть логи:       journalctl -u tf2-deal-watcher -f"
echo " Остановить:          systemctl stop tf2-deal-watcher"
echo " Перезапустить:       systemctl restart tf2-deal-watcher"
echo " Изменить настройки:  nano config.json   (потом: systemctl restart tf2-deal-watcher)"
echo "======================================================"
systemctl status tf2-deal-watcher --no-pager || true
