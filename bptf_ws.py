"""
backpack.tf live market websocket.

Docs: https://next.backpack.tf/developer/websocket
wss://ws.backpack.tf/events  (no authentication needed to connect)

The socket sends a JSON *array* of events per message, each shaped like:
    {"id": "...", "event": "listing-update" | "listing-delete" | ..., "payload": {...}}

For 'listing-update', payload is a full listing object. Confirmed shape
(from a real captured example - see comments below), the parts we care
about:

{
  "id": "440_11488812491",              # "{appid}_{assetid}" - unique per listing
  "steamid": "...",
  "appid": 440,
  "currencies": {"keys": 14, "metal": 33},   # or {"usd": 349.99} for some bots
  "intent": "sell" | "buy",
  "status": "active",
  "item": {
    "appid": 440,
    "baseName": "Gunslinger",                       # name with NO quality/killstreak prefix
    "name": "Strange Professional Killstreak Gunslinger",   # full display name
    "quality": {"id": 11, "name": "Strange", "color": "..."},
    "particle": {"id": 13, "name": "Burning Flames", ...},  # only present on Unusuals
    "australium": true,                              # only present on australiums
    "killstreakTier": 3,
    "tradable": true,
    "craftable": true,
    "texture": {...},                                 # only present on War Paint skins
    "wearTier": {...}                                  # only present on War Paint skins
  }
}

This event schema isn't in backpack.tf's official docs (their docs only
describe the event *names*, not the payload fields) - the shape above is
reconstructed from a real logged example. It's the one part of this
project most likely to need a small adjustment after the first real run;
see bptf_listener.py's DEBUG logging if something looks off.
"""

import asyncio
import json
import logging

import websockets

log = logging.getLogger("bptf_ws")

WS_URL = "wss://ws.backpack.tf/events"
RECONNECT_DELAY_SECONDS = 5

# The connection can look open (no exception, no clean close) while
# silently having stopped delivering anything - a known failure mode for
# long-lived websockets behind flaky network paths. If nothing at all
# arrives for this long, treat the connection as stale and reconnect
# rather than trusting it. backpack.tf's stream is busy enough site-wide
# that total silence for 5 minutes is already a strong signal something
# is wrong, not just a quiet moment.
IDLE_TIMEOUT_SECONDS = 300


async def stream_listing_events(on_event):
    """
    Connects to backpack.tf's market stream forever, calling
    `on_event(payload_dict)` for every active TF2 'sell' listing-update.
    Automatically reconnects on any connection drop OR if the connection
    goes idle for IDLE_TIMEOUT_SECONDS.
    """
    while True:
        try:
            log.info("Connecting to backpack.tf market stream...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                log.info("Connected to backpack.tf market stream.")
                while True:
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        log.warning(
                            "No backpack.tf messages for %ss - connection looks stale, reconnecting.",
                            IDLE_TIMEOUT_SECONDS,
                        )
                        break

                    try:
                        events = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(events, list):
                        events = [events]

                    for e in events:
                        if e.get("event") != "listing-update":
                            continue
                        payload = e.get("payload")
                        if not payload:
                            continue
                        if payload.get("appid") != 440:
                            continue
                        if payload.get("intent") != "sell":
                            continue
                        if payload.get("status") not in (None, "active"):
                            continue
                        await on_event(payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("backpack.tf stream connection error, reconnecting in %ss...", RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
