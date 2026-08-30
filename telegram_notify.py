"""
Telegram bot notifier + the low-level HTTP calls needed for an inline
button menu (Bot API is just plain HTTP, no extra library needed):

  send()                 - send a message, optionally with an inline keyboard
  edit_message()          - update an existing message's text/keyboard in place
  answer_callback_query() - required after every button tap, or Telegram
                            leaves the tap showing a loading spinner
"""

import logging

import requests

log = logging.getLogger("telegram")


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_base = f"https://api.telegram.org/bot{bot_token}"
        self.session = requests.Session()

    def _post(self, method: str, payload: dict):
        try:
            resp = self.session.post(f"{self.api_base}/{method}", json=payload, timeout=15)
            if resp.status_code != 200:
                log.error("Telegram %s failed (%s): %s", method, resp.status_code, resp.text)
                return None
            return resp.json()
        except Exception:
            log.exception("Telegram %s failed.", method)
            return None

    def send(self, text: str, keyboard=None):
        """
        keyboard, if given, is a list of rows, each a list of
        {"text": ..., "callback_data": ...} dicts - the inline keyboard
        layout. Returns the sent message_id, or None on failure.
        """
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            # The trade-offer/classifieds links generate large, cluttering
            # previews (a Steam or backpack.tf page preview) - a real
            # complaint that they crowd out the alert itself.
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = {"inline_keyboard": keyboard}
        result = self._post("sendMessage", payload)
        if result and result.get("ok"):
            return result["result"]["message_id"]
        return None

    def send_photo(self, photo_url: str, caption: str):
        """
        Sends a photo (hotlinked by URL, not uploaded) with an HTML
        caption. Telegram caption length is capped at 1024 characters -
        shorter than the 4096 a plain text message allows - so this
        returns False (not raising) when the caption is too long, and
        the caller (main.py's send_deal) falls back to a plain send()
        instead rather than having Telegram silently reject an
        over-length caption. Returns True on success, False on any
        failure (bad/dead image URL included) - callers should fall back
        to plain send() either way, not treat this as fatal.
        """
        if len(caption) > 1024:
            return False
        payload = {
            "chat_id": self.chat_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "HTML",
        }
        result = self._post("sendPhoto", payload)
        return bool(result and result.get("ok"))

    def edit_message(self, message_id, text: str, keyboard=None):
        """Updates an existing message's text/keyboard in place (used to
        make button menus feel like a live panel instead of a new
        message per tap)."""
        payload = {
            "chat_id": self.chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
        }
        payload["reply_markup"] = {"inline_keyboard": keyboard} if keyboard else {"inline_keyboard": []}
        self._post("editMessageText", payload)

    def answer_callback_query(self, callback_query_id: str, text: str = None):
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._post("answerCallbackQuery", payload)

    def register_commands(self, commands: list):
        """commands: list of (command, description) tuples, shown in
        Telegram's "/" autocomplete and menu button. Best-effort - a
        failure here shouldn't stop the watcher from starting."""
        self._post("setMyCommands", {
            "commands": [{"command": c, "description": d} for c, d in commands]
        })
