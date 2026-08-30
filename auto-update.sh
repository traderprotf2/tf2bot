#!/usr/bin/env bash
#
# Pulls the latest code from git; if anything changed, reinstalls
# dependencies (in case requirements.txt changed) and restarts the
# watcher service. Does nothing if there's no update.
#
# Triggered instantly by webhook_listener.py whenever GitHub sends a push
# notification (see setup-autoupdate.sh) - not on a timer. You can also
# run it manually any time: sudo bash auto-update.sh
#
# config.json is never touched: it's git-ignored, and `git reset --hard`
# only affects files that are tracked by git.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

TAG="tf2-deal-watcher-update"
log() { echo "$1" | systemd-cat -t "$TAG" 2>/dev/null || echo "$1"; }

if [ ! -d .git ]; then
    log "Папка не подключена к git — автообновление ещё не настроено (запусти setup-autoupdate.sh)."
    exit 0
fi

BRANCH="main"
if ! git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    BRANCH="master"
fi

BEFORE="$(git rev-parse HEAD)"

if ! git fetch origin --quiet; then
    log "Не удалось подключиться к git-репозиторию, пропускаю проверку до следующего раза."
    exit 0
fi

git reset --hard "origin/$BRANCH" --quiet

AFTER="$(git rev-parse HEAD)"

if [ "$BEFORE" != "$AFTER" ]; then
    log "Найдено обновление: $BEFORE -> $AFTER. Обновляю зависимости и перезапускаю службу..."
    venv/bin/pip install --quiet -r requirements.txt || true
    systemctl restart tf2-deal-watcher
    log "Готово, служба перезапущена на новой версии."
fi
