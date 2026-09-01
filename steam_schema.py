"""
Steam Web API schema client.

Maps Unusual particle effect *names* (what mannco.store gives us, e.g.
"Kill-a-Watt") to the numeric particle *IDs* backpack.tf's price list is
indexed by, and item *defindex* numbers to their base display name.
Straight from Valve's official Steam Web API, so authoritative, no
hand-maintained table - this matters more than it sounds: marketplace.tf's
page only gives combined display text like "Iridescence Crustaceous Cowl",
which LOOKS like one name but is actually two concatenated - "Iridescence"
(the effect) and "Crustaceous Cowl" (the item). Splitting that by parsing
the text is unreliable (an effect name or item name could itself contain
a word that looks like a boundary). The SKU that comes with it
("31194;5;u188") already has both halves as unambiguous numbers -
defindex 31194 and particle 188 - so resolving each against Valve's own
schema is the only actually-reliable way to recover the real item name,
rather than guessing where to split the display text.

Requires a free Steam Web API key: https://steamcommunity.com/dev/apikey
(any Steam account can generate one instantly). Without one, the watcher
still works for backpack.tf/mannco.store, but marketplace.tf listings are
skipped entirely (see main.py) rather than risk alerting on a
misidentified item - not a fallback anyone should want silently guessing.
"""

import logging

import requests

log = logging.getLogger("steam_schema")

SCHEMA_OVERVIEW_URL = "https://api.steampowered.com/IEconItems_440/GetSchemaOverview/v0001/"
SCHEMA_ITEMS_URL = "https://api.steampowered.com/IEconItems_440/GetSchemaItems/v1/"


def fetch_particle_name_to_id(steam_api_key: str):
    """
    Returns {effect_name: particle_id}. Returns {} on any failure (caller
    should treat that as "Unusual cross-check unavailable" rather than a
    fatal error).
    """
    if not steam_api_key:
        return {}

    try:
        resp = requests.get(SCHEMA_OVERVIEW_URL, params={"key": steam_api_key}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        particles = data.get("result", {}).get("attribute_controlled_attached_particles", [])
        mapping = {}
        for p in particles:
            name = p.get("name")
            pid = p.get("id")
            if name is not None and pid is not None:
                mapping[name] = int(pid)
        if not mapping:
            # A real /stats report showed 0 effects loaded with NO
            # accompanying exception anywhere in /errors - meaning the
            # request itself succeeded (200 OK, valid JSON) but the
            # expected data wasn't in it, which the try/except below
            # can't catch since nothing actually raised. Most likely
            # cause: an invalid/expired Steam API key still returns 200
            # with an empty or error-shaped body rather than a 4xx here.
            # Logging the raw response is the only way to tell which.
            log.warning(
                "Steam schema request succeeded but yielded 0 particle effects - raw response: %r",
                data,
            )
        log.info("Loaded %d unusual particle effects from Steam schema.", len(mapping))
        return mapping
    except Exception:
        log.exception("Could not fetch Steam TF2 schema; Unusual cross-checks vs backpack.tf will be skipped.")
        return {}


def fetch_defindex_to_name(steam_api_key: str, max_pages: int = 20):
    """
    Returns {defindex: item_name} for every item in the TF2 schema (a
    much bigger fetch than the particle overview - paginated by Valve's
    API via a `next` cursor). Needed to correctly split marketplace.tf's
    combined "<effect> <item name>" display text back into its real
    components - see the module docstring.

    Per-alert images were removed on explicit request, to cut every bit
    of load associated with them - this function no longer builds an
    image mapping at all (not just "unused", genuinely not fetched or
    assembled), only the name mapping marketplace.tf actually needs.

    Returns {} on any failure or if no key is given - caller (main.py)
    treats a missing name mapping as "can't safely process
    marketplace.tf listings" (not a fatal error - skipping that source
    beats guessing at a name split).
    """
    if not steam_api_key:
        return {}

    name_mapping = {}
    start = None
    try:
        for _ in range(max_pages):
            params = {"key": steam_api_key}
            if start is not None:
                params["start"] = start
            resp = requests.get(SCHEMA_ITEMS_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("result", {})
            items = result.get("items", [])
            for item in items:
                name = item.get("item_name") or item.get("name")
                defindex = item.get("defindex")
                if name is not None and defindex is not None:
                    name_mapping[int(defindex)] = name
            start = result.get("next")
            if not start:
                break
        if not name_mapping:
            # Same reasoning as fetch_particle_name_to_id's identical
            # fix - a real report showed both this and that function
            # returning 0 items with no exception anywhere to explain
            # why, meaning the request itself succeeded but the expected
            # data wasn't there. Logging the first page's raw response
            # (not every page - this could run for up to 20 pages) is
            # enough to tell whether it's an auth problem, a genuinely
            # empty result, or something else entirely.
            log.warning(
                "Steam schema items request succeeded but yielded 0 items - raw first-page "
                "response: %r",
                data,
            )
        log.info("Loaded %d defindex->name mappings from Steam schema.", len(name_mapping))
        return name_mapping
    except Exception:
        log.exception("Could not fetch full Steam TF2 item schema; marketplace.tf listings will be skipped.")
        return name_mapping
