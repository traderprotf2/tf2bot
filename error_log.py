"""
Captures WARNING-and-above log records from anywhere in the bot into a
bounded, disk-persisted buffer, so they can be reviewed via /errors in
Telegram - a fast, low-friction way to see "what actually went wrong
recently" without needing SSH+journalctl access for every investigation.

This doesn't replace journalctl - that still has the full, unfiltered
log (every INFO line, every "Connected to..." message, etc). This is
specifically the filtered subset worth a human's attention: warnings
(like an Unusual item whose effect couldn't be resolved) and real
errors (a failed request, an unhandled exception) - the two levels that
kept turning out to be exactly what needed investigating, across many
rounds of "here's a screenshot, what's wrong" in this project's history.

Persisted to disk (not just kept in memory) specifically because a
CRASH is one of the most important things to be able to review
afterwards - an in-memory-only buffer would be lost in exactly the
moment it mattered most.
"""

import collections
import json
import logging
import os
import threading
import time

MAX_ERRORS_KEPT = 100
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "error_log.jsonl")
# Rewrite (trim) the on-disk file every this many new entries, so it
# doesn't grow forever over a long-running bot's lifetime - keeps
# somewhat more than MAX_ERRORS_KEPT on disk as a safety margin, since
# the in-memory buffer is already the tighter, authoritative cap for
# what /errors actually shows.
_TRIM_EVERY_N_WRITES = 25
_TRIM_KEEP_LINES = MAX_ERRORS_KEPT * 2


class TelegramErrorBuffer(logging.Handler):
    """
    A standard logging.Handler - attach it to the root logger (see
    install() below) and it transparently captures WARNING+ records from
    every module (bptf_client, mannco_client, matcher, main, ...)
    without those modules needing to know it exists.
    """

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.buffer = collections.deque(maxlen=MAX_ERRORS_KEPT)
        self._lock = threading.Lock()
        self._trim_lock = threading.Lock()
        self._writes_since_trim = 0
        self._load_from_disk()

    def _load_from_disk(self):
        if not os.path.exists(LOG_FILE_PATH):
            return
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()[-MAX_ERRORS_KEPT:]
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    self.buffer.append(json.loads(line))
                except (json.JSONDecodeError, ValueError):
                    continue
        except Exception:
            pass  # best-effort - a broken/missing log file shouldn't block startup

    def emit(self, record):
        # A logging handler must never itself raise - that could take
        # down whatever was being logged in the first place, which would
        # be a uniquely bad way for an error-tracking feature to fail.
        try:
            entry = {
                "time": time.time(),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            }
            with self._lock:
                self.buffer.append(entry)
            # Disk write happens OUTSIDE the lock - a real report showed
            # /errors (recent() below, which needs this same lock) never
            # responding across several attempts during a run generating
            # a very high volume of log calls (thousands/minute). Every
            # emit() previously held the lock for the ENTIRE duration of
            # a synchronous disk write (a real, blocking file open+write
            # syscall), so a reader could be starved indefinitely if
            # writes kept arriving faster than each one's disk I/O
            # completed. Readers only ever need the in-memory buffer
            # (self.buffer, appended above), never the file on disk - so
            # there is no correctness reason for a read to wait on a
            # write's disk I/O in the first place.
            try:
                with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            except Exception:
                pass
            self._writes_since_trim += 1
            if self._writes_since_trim >= _TRIM_EVERY_N_WRITES:
                self._trim_file()
                self._writes_since_trim = 0
        except Exception:
            pass

    def _trim_file(self):
        # Also deliberately outside self._lock (same reasoning as the
        # write above) - and now further guarded by its OWN lock
        # (_trim_lock, not self._lock) so two trims can't run
        # concurrently and corrupt the file between them, without that
        # guard ever blocking a plain read() or a plain append.
        if not self._trim_lock.acquire(blocking=False):
            # Another trim is already in progress - skip this one rather
            # than wait; the next write's trim check will catch up.
            return
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > _TRIM_KEEP_LINES:
                with open(LOG_FILE_PATH, "w", encoding="utf-8") as f:
                    f.writelines(lines[-_TRIM_KEEP_LINES:])
        except Exception:
            pass
        finally:
            self._trim_lock.release()

    def recent(self, n=20):
        with self._lock:
            return list(self.buffer)[-n:]


_buffer_handler = None


def install():
    """
    Attaches the buffer to the root logger. Call once at startup, before
    anything else logs. Returns the handler instance (main.py keeps a
    reference so the /errors command can read from it).
    """
    global _buffer_handler
    if _buffer_handler is not None:
        return _buffer_handler
    _buffer_handler = TelegramErrorBuffer()
    _buffer_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(_buffer_handler)
    return _buffer_handler
