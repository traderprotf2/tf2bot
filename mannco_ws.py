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
                        await on_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Market stream connection error, reconnecting in %ss...", RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
