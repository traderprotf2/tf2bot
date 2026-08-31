"""
Mannco.store live market websocket.

Docs: https://docs.mannco.store/api-reference/websocket/market-stream
wss://api.mannco.store/ws

Broadcasts events for the whole site (all games), including:
  listing_added   {id, assetId, itemId, price}      <- what we care about
  price_changed   {id, assetId, itemId, oldPrice, newPrice}
  listing_removed {id}
  buyorder_*       (ignored here)

Prices are integer USD cents.
"""

import asyncio
import json
import logging

import websockets

log = logging.getLogger("mannco_ws")

WS_URL = "wss://api.mannco.store/ws"
RECONNECT_DELAY_SECONDS = 5

# Same rationale as bptf_ws.py: a connection can go silently stale
# without raising an exception. Reconnect proactively if nothing at all
# arrives for this long.
IDLE_TIMEOUT_SECONDS = 300


async def stream_listing_events(on_event):
    """
    Connects to the market stream forever, calling `on_event(event_dict)`
    for every 'listing_added' and 'price_changed' message. Automatically
    reconnects on any connection drop OR if the connection goes idle for
    IDLE_TIMEOUT_SECONDS.

    Each event is dispatched as its own concurrent task (see
    _dispatch_event below), not awaited one at a time in this loop - same
    reasoning as bptf_ws.py's identical change: a single listing's own
    full evaluation chain can take a while even when correctly
    rate-limited, and awaiting that FULLY before even starting the NEXT
    listing left real request capacity sitting idle between one
    listing's own spaced-out requests instead of using it for others.
    mannco_client.py's own throttle is what actually keeps the real
    request rate in check regardless of how many listings are being
    evaluated concurrently.
    """
    while True:
        try:
            log.info("Connecting to mannco.store market stream...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                log.info("Connected to market stream.")
                while True:
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        log.warning(
                            "No mannco.store messages for %ss - connection looks stale, reconnecting.",
                            IDLE_TIMEOUT_SECONDS,
                        )
                        break

                    try:
                        event = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    event_type = event.get("event")
                    if event_type in ("listing_added", "price_changed"):
                        asyncio.create_task(_dispatch_event(on_event, event))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Market stream connection error, reconnecting in %ss...", RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# Same reasoning as bptf_ws.py's identical constant - a sane ceiling on
# concurrently in-flight listing evaluations, not a rate limit itself
# (mannco_client.py's own throttle already is one).
_dispatch_semaphore = asyncio.Semaphore(60)


async def _dispatch_event(on_event, event):
    async with _dispatch_semaphore:
        try:
            await on_event(event)
        except Exception:
            log.exception("Unhandled error while processing a mannco.store market event.")
