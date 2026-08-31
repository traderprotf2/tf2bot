"""
Runtime-mutable settings: the subset of the watcher's behaviour that can
be changed live from Telegram (see telegram_commands.py), as opposed to
config.json which only changes when you edit the file on the server.

Persisted to runtime_state.json next to the other project files, so a
setting made via Telegram (e.g. "/pause", "/minprice 10") survives a
restart - including the automatic restart that happens every time an
update is pulled in (see auto-update.sh). Without this, every code
update would silently reset the user's live-configured filters back to
config.json's shipped defaults.

Deliberately NOT included here: API keys/tokens and anything else from
config.json. Only the filter/operational knobs the user explicitly asked
to control from Telegram.
"""

import json
import logging
import os
import threading

log = logging.getLogger("runtime_settings")

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runtime_state.json")


class RuntimeSettings:
    def __init__(self, min_price_keys, watched_qualities, watched_categories,
                 discount_threshold_percent, max_days_since_price_update,
                 stn_watchlist=None, paused=False, australium_only=False):
        self.paused = paused
        self.australium_only = australium_only
        self.min_price_keys = min_price_keys
        self.watched_qualities = list(watched_qualities)
        self.watched_categories = list(watched_categories)
        self.discount_threshold_percent = discount_threshold_percent
        self.max_days_since_price_update = max_days_since_price_update
        self.stn_watchlist = list(stn_watchlist or [])
        self._lock = threading.Lock()

    @classmethod
    def load(cls, cfg):
        """
        Starts from config.json's values, then overlays whatever was last
        saved via a Telegram command (if any) - so Telegram-set values
        win after the first command, but a brand new deployment behaves
        exactly like config.json says until you change something.
        """
        settings = cls(
            min_price_keys=cfg["min_price_keys"],
            watched_qualities=cfg["watched_qualities"],
            watched_categories=cfg.get("watched_categories", ["weapon", "cosmetic", "taunt", "killstreak_kit", "other"]),
            discount_threshold_percent=cfg["discount_threshold_percent"],
            max_days_since_price_update=cfg.get("max_days_since_price_update", 90),
            stn_watchlist=[],
            paused=False,
        )
        if os.path.exists(STATE_PATH):
            try:
                with open(STATE_PATH, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                settings.paused = bool(saved.get("paused", settings.paused))
                settings.australium_only = bool(saved.get("australium_only", settings.australium_only))
                settings.min_price_keys = float(saved.get("min_price_keys", settings.min_price_keys))
                settings.watched_qualities = list(saved.get("watched_qualities", settings.watched_qualities))
                watched_categories = list(saved.get("watched_categories", settings.watched_categories))
                # Migration: "hat" was renamed "cosmetic" when the category
                # list grew to also cover taunts and killstreak kits.
                settings.watched_categories = [
                    "cosmetic" if c == "hat" else c for c in watched_categories
                ]
                settings.discount_threshold_percent = float(
                    saved.get("discount_threshold_percent", settings.discount_threshold_percent)
                )
                settings.max_days_since_price_update = float(
                    saved.get("max_days_since_price_update", settings.max_days_since_price_update)
                )
                settings.stn_watchlist = list(saved.get("stn_watchlist", settings.stn_watchlist))
                log.info("Loaded saved runtime settings from %s", STATE_PATH)
            except Exception:
                log.exception("Could not read %s, starting from config.json defaults instead.", STATE_PATH)
        return settings

    def save(self):
        with self._lock:
            data = {
                "paused": self.paused,
                "australium_only": self.australium_only,
                "min_price_keys": self.min_price_keys,
                "watched_qualities": self.watched_qualities,
                "watched_categories": self.watched_categories,
                "discount_threshold_percent": self.discount_threshold_percent,
                "max_days_since_price_update": self.max_days_since_price_update,
                "stn_watchlist": self.stn_watchlist,
            }
            try:
                tmp_path = STATE_PATH + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, STATE_PATH)
            except Exception:
                log.exception("Could not save runtime settings to %s", STATE_PATH)
