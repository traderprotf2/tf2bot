"""
Steam Web API schema client.

Maps Unusual particle effect *names* (e.g. "Kill-a-Watt") to the numeric
particle *IDs* backpack.tf's price list is indexed by. Straight from
Valve's official Steam Web API, so authoritative, no hand-maintained
table.

Requires a free Steam Web API key: https://steamcommunity.com/dev/apikey
(any Steam account can generate one instantly). Without one, the watcher
still works, just without Unusual particle-effect cross-checking.
"""

import logging

import requests

log = logging.getLogger("steam_schema")

SCHEMA_OVERVIEW_URL = "https://api.steampowered.com/IEconItems_440/GetSchemaOverview/v0001/"


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
            # A real report showed 0 effects loaded with no exception
            # anywhere - the request succeeded (200 OK, valid JSON) but
            # the expected data wasn't in it (most likely an invalid/
            # expired key still returning 200 with an empty body).
            log.warning(
                "Steam schema request succeeded but yielded 0 particle effects - raw response: %r",
                data,
            )
        log.info("Loaded %d unusual particle effects from Steam schema.", len(mapping))
        return mapping
    except Exception:
        log.exception("Could not fetch Steam TF2 schema; Unusual cross-checks vs backpack.tf will be skipped.")
        return {}
