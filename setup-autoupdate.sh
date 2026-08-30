#!/usr/bin/env bash
#
# One-time setup for instant, push-triggered updates (no polling).
#
# Usage:
#     sudo bash setup-autoupdate.sh https://github.com/YOUR_USERNAME/YOUR_REPO.git
#
# After this: whenever you edit a file on your GitHub repo's page (or
# push from anywhere), GitHub calls this server immediately and it pulls
# + restarts right away - nothing runs on a timer.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$(id -u)" -ne 0 ]; then
    echo "Запусти с правами root: sudo bash setup-autoupdate.sh <ссылка-на-репозиторий>"
    exit 1
fi

REPO_URL="${1:-}"
if [ -z "$REPO_URL" ]; then
    read -rp "Ссылка на твой git-репозиторий (например https://github.com/username/tf2-deal-watcher.git): " REPO_URL
fi
if [ -z "$REPO_URL" ]; then
    echo "Ссылка не может быть пустой."
    exit 1
fi

read -rp "Порт для приёма вебхуков от GitHub [9001]: " WEBHOOK_PORT
WEBHOOK_PORT="${WEBHOOK_PORT:-9001}"

echo "==> Устанавливаю git..."
apt-get update -qq
apt-get install -y -qq git >/dev/null

if [ ! -d .git ]; then
    echo "==> Подключаю эту папку к $REPO_URL ..."
    git init -q
    git remote add origin "$REPO_URL"
else
    git remote remove origin 2>/dev/null || true
    git remote add origin "$REPO_URL"
fi

git fetch origin --quiet

BRANCH="main"
if ! git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    BRANCH="master"
fi

echo "==> Синхронизирую код с веткой $BRANCH ..."
git reset --hard "origin/$BRANCH" --quiet
git branch -M "$BRANCH" 2>/dev/null || true
git branch --set-upstream-to="origin/$BRANCH" "$BRANCH" 2>/dev/null || true

chmod +x auto-update.sh install.sh

# Generate a fresh webhook secret if one doesn't already exist (re-running
# this script keeps your existing secret rather than breaking the GitHub
# webhook you already configured).
if [ ! -f webhook_secret.txt ]; then
    echo "==> Генерирую секретный ключ для проверки вебхука..."
    python3 -c "import secrets; print(secrets.token_hex(20))" > webhook_secret.txt
fi
chmod 600 webhook_secret.txt
WEBHOOK_SECRET="$(cat webhook_secret.txt)"

echo "==> Устанавливаю приёмник вебхуков (порт $WEBHOOK_PORT)..."
sed \
    -e "s#WorkingDirectory=.*#WorkingDirectory=${SCRIPT_DIR}#" \
    -e "s#Environment=WEBHOOK_PORT=.*#Environment=WEBHOOK_PORT=${WEBHOOK_PORT}#" \
    -e "s#ExecStart=.*#ExecStart=${SCRIPT_DIR}/venv/bin/python3 ${SCRIPT_DIR}/webhook_listener.py#" \
    tf2-deal-watcher-webhook.service > /etc/systemd/system/tf2-deal-watcher-webhook.service

systemctl daemon-reload
systemctl enable --now tf2-deal-watcher-webhook

# Best-effort: open the port if ufw is active. Harmless no-op otherwise -
# most cloud VPS images have no local firewall (it's managed at the
# provider level instead), in which case this block just does nothing.
if command -v ufw >/dev/null 2>&1 && ufw status | grep -q "Status: active"; then
    echo "==> Открываю порт $WEBHOOK_PORT в ufw..."
    ufw allow "${WEBHOOK_PORT}/tcp" >/dev/null || true
fi

SERVER_IP="$(curl -s --max-time 5 https://ifconfig.me || echo "ТВОЙ_IP")"

echo ""
echo "======================================================================"
echo " Готово! Осталось подключить вебхук на стороне GitHub:"
echo ""
echo " 1. Открой свой репозиторий на GitHub -> Settings -> Webhooks -> Add webhook"
echo " 2. Payload URL:   http://${SERVER_IP}:${WEBHOOK_PORT}/webhook"
echo " 3. Content type:  application/json"
echo " 4. Secret:        ${WEBHOOK_SECRET}"
echo " 5. Which events:  Just the push event"
echo " 6. Нажми Add webhook"
echo ""
echo " GitHub сразу пришлёт тестовый 'ping' - на странице Webhooks у тебя"
echo " появится галочка (успех) или крестик (ошибка) под 'Recent Deliveries'."
echo ""
echo " С этого момента любое изменение файла в репозитории (даже прямо в"
echo " браузере на GitHub) применяется на сервере за секунды, без таймеров."
echo ""
echo " Логи вебхука:   journalctl -u tf2-deal-watcher-webhook -f"
echo " Логи обновлений: journalctl -t tf2-deal-watcher-update -f"
echo "======================================================================"
