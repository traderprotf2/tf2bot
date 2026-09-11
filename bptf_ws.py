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
import time

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


async def stream_listing_events(on_event, on_drop=None):
    """
    Connects to backpack.tf's market stream forever, calling
    `on_event(payload_dict)` for every active TF2 'sell' listing-update.
    Automatically reconnects on any connection drop OR if the connection
    goes idle for IDLE_TIMEOUT_SECONDS.

    on_drop(payload_dict), if given, is called for a SELL event (or a
    delete) specifically dropped because the dispatch backlog was
    completely full (see _spawn_dispatch's own comments) - NOT for a
    dropped buy event, which main.py's own proactive scanner already
    re-covers on its normal schedule regardless (a fresh, complete
    snapshot each cycle, not an incremental catch-up), so flagging those
    individually would add complexity without adding anything the
    scanner wasn't already going to do. A dropped sell event is
    different: it's what an actual alert would have come from, and by
    design this only fires in the rare case that even the buy-side
    reserve wasn't enough headroom - see on_drop's caller in main.py for
    what it actually does with this (an accelerated re-scan of that one
    item, rather than waiting out its normal interval).

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
            # ping_timeout raised from 20s to 60s (ping_interval kept at
            # 20s) - a real report showed frequent disconnects with
            # "keepalive" in the stack trace, at a real sustained load of
            # ~60+ events/second. Under that load the event loop can be
            # momentarily busy enough that a pong reply is delayed past a
            # tight 20s timeout even though the connection itself is
            # perfectly healthy - a false-positive disconnect, not a real
            # one. Each disconnect is a real gap in coverage (backpack.tf's
            # websocket has no "replay what I missed" on reconnect - a
            # buy order that updates during that gap is simply never
            # seen), so cutting down on FALSE disconnects directly means
            # fewer missed events, not just fewer log lines.
            # compression=None disables permessage-deflate entirely - a
            # real, confirmed finding: tracemalloc (Python's own stdlib
            # memory profiler, see the /memtop command) showed only ~7MB
            # tracked while this project's actual process RSS was
            # climbing into the hundreds of MB / low GB before yet
            # another OOM kill - meaning the real growth was happening
            # OUTSIDE anything Python's own allocator (and therefore
            # tracemalloc) can see at all. zlib - which permessage-
            # deflate uses under the hood for every compressed frame -
            # keeps its compression window and internal buffers in raw C
            # memory, invisible to tracemalloc, and is a well-known real
            # source of native memory growth on long-lived compressed
            # connections; every reconnect (and this project forces one
            # on buffer-limit-exceeded now, see the branch above) is
            # itself a place old compression state could fail to be
            # fully released. This project's own traffic (JSON text
            # events) compresses well, so this trades some bandwidth for
            # ruling out an entire class of native memory growth
            # tracemalloc structurally cannot see or help diagnose
            # further - a worthwhile trade given how much time this
            # project has already spent chasing OOM incidents blind.
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=60, max_size=None, compression=None
            ) as ws:
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

                    force_reconnect = False
                    for e in events:
                        event_type = e.get("event")
                        if event_type == "listing-delete":
                            # Real, confirmed event type (backpack.tf's
                            # own developer docs) - lets the local store
                            # (see bptf_client.py's LocalListingStore)
                            # drop a listing immediately instead of
                            # waiting for it to time out of its
                            # freshness window naturally. Payload here is
                            # much smaller than an update's (just an id,
                            # not a full item) - dispatched with an
                            # explicit marker so handle_bptf_event can
                            # tell the two apart without guessing from
                            # shape alone.
                            delete_payload = e.get("payload") or {}
                            delete_payload["_bptf_event_type"] = "delete"
                            _spawn_dispatch(on_event, delete_payload, on_drop)
                            continue
                        if event_type == "buffer-limit-exceeded":
                            # Real, documented backpack.tf event (their own
                            # developer docs, verbatim): "you will not
                            # receive any further events until the buffer
                            # clears" - and "due to the nature of the data
                            # buffer, you won't see this event until after
                            # the problem arises". Before this fix, this
                            # event type fell straight through the generic
                            # "not listing-update, ignore" branch below -
                            # completely silent, no log line, nothing -
                            # meaning a connection could go quietly dead
                            # (deliver zero further listings) with no trace
                            # anywhere explaining why, indistinguishable
                            # from "nothing interesting happened to alert
                            # on" from every other part of this project's
                            # own logging. The docs don't promise this
                            # self-heals - breaking out to force a fresh
                            # reconnect is the only way to be SURE events
                            # resume, rather than trusting an already-
                            # degraded connection to recover on its own.
                            # force_reconnect (checked right after this
                            # for loop) is what actually exits the message
                            # loop below - a bare break here would only
                            # exit THIS for loop, not that one.
                            log.warning(
                                "backpack.tf: buffer-limit-exceeded received - this connection "
                                "will deliver no further events until reconnected. Forcing a "
                                "fresh connection now."
                            )
                            force_reconnect = True
                            break
                        if event_type == "client-limit-exceeeded":
                            # Also real and documented (backpack.tf's own
                            # spelling, three e's, kept verbatim so a log
                            # search for the exact event name still
                            # matches): too many concurrent connections
                            # from this IP - backpack.tf closes the
                            # connection itself right after sending this,
                            # so the outer reconnect will fire regardless,
                            # but logging it explicitly turns "mysteriously
                            # stopped receiving events" into an actionable
                            # signal (e.g. another script - a manual
                            # diagnostic test, a second instance of this
                            # same project - sharing this same IP/
                            # connection budget) instead of silence.
                            log.warning(
                                "backpack.tf: client-limit-exceeeded received - too many "
                                "concurrent connections from this IP. backpack.tf will close "
                                "this connection; reconnecting after that happens."
                            )
                            continue
                        if event_type != "listing-update":
                            continue
                        payload = e.get("payload")
                        if not payload:
                            continue
                        if payload.get("appid") != 440:
                            continue
                        # BOTH intents now passed through - a real, major
                        # finding: buy-intent events used to be dropped
                        # right here, meaning this project's own "buy
                        # order" numbers NEVER came from anything seen on
                        # this websocket at all, only from the (now-
                        # confirmed-deprecated, see bptf_client.py's
                        # LocalListingStore docstring) snapshot API call.
                        # main.py's handle_bptf_event records "buy"
                        # events into the local store without running the
                        # full sell-side deal evaluation on them.
                        if payload.get("intent") not in ("sell", "buy"):
                            continue
                        if payload.get("status") not in (None, "active"):
                            continue
                        _spawn_dispatch(on_event, payload, on_drop)

                    if force_reconnect:
                        break
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

# Hard cap on TOTAL pending dispatch tasks, not just the semaphore's own
# "60 running at once" - a real, confirmed gap the semaphore alone left
# open: it bounds how many _dispatch_event calls actively RUN at a time,
# but nothing bounded how many could be QUEUED waiting for a semaphore
# slot to free up. Every queued-but-not-yet-run task keeps its own
# parsed event payload alive (via _background_tasks' own strong
# reference below, and the task's own coroutine frame) for as long as
# it waits - and under sustained real event volume, this project's own
# automatic tracemalloc logging (see main.py's memory_guard_loop) kept
# finding ~190MB sitting in json's own decoder, tied to exactly this
# queuing, even AFTER record() (the highest-frequency consumer inside
# each handler) got its own dedicated thread pool - meaning the backlog
# was forming here, before record() is ever reached, not inside it.
# 300 (5x the semaphore's own concurrency) gives real buffering room for
# a legitimate short burst without ever letting the queue itself become
# unbounded - past that, an event is dropped (logged, rate-limited so
# the drop itself can't also become a memory/log-spam problem) rather
# than accepted into an ever-growing backlog. A dropped event is a real,
# missed listing - a strictly better trade than the alternative this
# project kept observing: the WHOLE process OOM-killed, losing every
# event, not just the overflow.
MAX_PENDING_DISPATCH_TASKS = 300
# Reserved headroom for sell events (and deletes) specifically, once the
# backlog gets deep - per explicit request: buy-intent events are far
# higher volume (this project's own /stats: buy-order counts routinely
# run 5-10x sell counts) and lower urgency (they feed the local price
# reference, refreshed independently by the proactive scanner too), while
# sell events are what an actual alert comes from - losing THOSE under
# pressure is a direct, felt loss (a missed deal) in a way a dropped buy
# update mostly isn't. Below this lower threshold, buy events are ALSO
# still accepted normally; only once the backlog is already this deep do
# buy events start getting turned away first, preserving the remaining
# room up to the full cap for sell events (and deletes, which matter for
# correctness - an unprocessed delete could leave a stale, already-sold
# listing looking live) specifically.
BUY_DISPATCH_RESERVE_THRESHOLD = 250
_dispatch_overflow_last_logged = 0.0
_DISPATCH_OVERFLOW_LOG_INTERVAL_SECONDS = 60

# Holds a strong reference to every task created via _spawn_dispatch
# below, for as long as it's running - a real, confirmed Python pitfall
# found during a systematic sweep of files this project hadn't audited
# yet: asyncio.create_task()'s own docs warn that a task with no
# reference kept anywhere else "can be garbage collected" mid-execution
# without warning, since the event loop itself only holds a weak
# reference. Every dispatch here was calling create_task() and
# immediately discarding the only reference to it - the standard,
# documented fix is exactly this: keep it in a set, drop it via a
# done-callback once it actually finishes on its own.
_background_tasks = set()


def _log_dispatch_overflow():
    """Shared, rate-limited warning for both overflow paths in
    _spawn_dispatch below (the hard cap, and the buy-specific reserve
    threshold) - one shared cooldown, not two independent ones, so a
    dropped buy event followed shortly by a dropped sell event still
    only logs once per _DISPATCH_OVERFLOW_LOG_INTERVAL_SECONDS window,
    not once per reason."""
    global _dispatch_overflow_last_logged
    now = time.time()
    if now - _dispatch_overflow_last_logged >= _DISPATCH_OVERFLOW_LOG_INTERVAL_SECONDS:
        log.warning(
            "Dispatch backlog under pressure (cap %d, buy reserve threshold %d) - dropping "
            "some new events until it drains (processing genuinely isn't keeping up with "
            "arrival rate right now).",
            MAX_PENDING_DISPATCH_TASKS, BUY_DISPATCH_RESERVE_THRESHOLD,
        )
        _dispatch_overflow_last_logged = now


def _spawn_dispatch(on_event, payload, on_drop=None):
    backlog_size = len(_background_tasks)
    # is_high_priority: sell events (what an alert actually comes from)
    # and deletes (correctness - a stale listing looking live if its own
    # delete never gets processed) - everything else (buy updates) is
    # the lower-priority majority this reserve protects against. See
    # BUY_DISPATCH_RESERVE_THRESHOLD's own comment for the reasoning.
    is_high_priority = payload.get("intent") == "sell" or payload.get("_bptf_event_type") == "delete"
    if backlog_size >= MAX_PENDING_DISPATCH_TASKS:
        _log_dispatch_overflow()
        # Only high-priority (sell/delete) drops get flagged for an
        # accelerated re-scan - see stream_listing_events' own docstring
        # on on_drop for why a dropped BUY event deliberately does NOT
        # trigger this (the scanner already re-covers it on its own
        # schedule regardless, so there's nothing extra to catch up on).
        if is_high_priority and on_drop is not None:
            try:
                on_drop(payload)
            except Exception:
                log.exception("on_drop callback failed for a dropped high-priority event.")
        return
    if not is_high_priority and backlog_size >= BUY_DISPATCH_RESERVE_THRESHOLD:
        _log_dispatch_overflow()
        return
    task = asyncio.create_task(_dispatch_event(on_event, payload))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


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
