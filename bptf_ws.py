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

    max_size=None (no cap) on the connection - confirmed via a real
    production log that backpack.tf sends batched messages over 1 MiB
    (the `websockets` library's own default cap), which without this
    gets the connection closed with "message too big" and forces a
    reconnect - losing whatever was in that batch and repeating every
    time a large-enough batch comes through, not a one-off.

    Each event is dispatched as its own concurrent task (see
    _dispatch_event below), not awaited one at a time in this loop -
    per direct feedback that a single killstreak weapon's own
    (correctly rate-limited) request chain could take up to ~44s
    end-to-end, and awaiting that FULLY before even starting the NEXT
    listing's evaluation meant the real bottleneck wasn't backpack.tf's
    6-requests/60s limit itself, it was this loop only ever having ONE
    listing "in flight" at a time. The shared throttle/semaphore in
    bptf_client.py (not this loop) is what actually keeps every
    concurrent task's requests within that same limit - running many
    listings' evaluations concurrently doesn't request anything faster
    than one at a time would, it just stops the gaps between a single
    listing's own spaced-out requests from sitting completely idle
    instead of being used for other listings' requests.
    """
    while True:
        try:
            log.info("Connecting to backpack.tf market stream...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=None) as ws:
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
                        asyncio.create_task(_dispatch_event(on_event, payload))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("backpack.tf stream connection error, reconnecting in %ss...", RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# Bounds how many listings' evaluations can be concurrently "in flight"
# at once (each one mostly just waiting its turn on the shared request
# throttle, not actually using much of its own resources meanwhile) -
# not a rate limit itself (bptf_client.py's throttle/semaphore already
# is one), just a sane ceiling against a burst of thousands of events
# arriving in one batch spinning up thousands of simultaneous tasks.
_dispatch_semaphore = asyncio.Semaphore(60)


async def _dispatch_event(on_event, payload):
    async with _dispatch_semaphore:
        try:
            await on_event(payload)
        except Exception:
            # A task created with asyncio.create_task() that raises is
            # never awaited here, so an unhandled exception would
            # otherwise only surface as an easy-to-miss "Task exception
            # was never retrieved" warning at garbage-collection time,
            # not a clear log entry when it actually happened.
            log.exception("Unhandled error while processing a backpack.tf listing event.")
