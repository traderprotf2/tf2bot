"""
One-time (or occasional) maintenance script - NOT run by the bot itself,
never part of its normal startup or operation. Run this by hand, once,
to pull the CURRENT, complete Unusual particle effect list straight
from Valve's own game schema and merge it into unusual_effects.py,
so the bundled database (currently hand-compiled from public wiki/
GitHub sources, complete only through Smissmas 2022) catches up to
whatever effects exist as of whenever you run this - including
anything added in 2023-2026 that no single documented source had
combined into one place yet.

This is the SAME data source the bot's old, removed Steam API
dependency used to fetch live at every startup - the difference is
this script fetches it ONCE, by hand, and bakes the result into a
static file the bot then uses forever after with zero network calls
of its own. Running this script does not reintroduce any runtime
dependency on Steam's API - it's a one-time enrichment of the bundled
data, not something the bot calls itself.

Requires a free Steam Web API key: https://steamcommunity.com/dev/apikey
(the same kind this project used to require unconditionally - now
needed only for this optional, one-time refresh, never for the bot to
actually run).

Usage:
    python3 tools/update_effects_from_schema.py YOUR_STEAM_API_KEY

Rewrites unusual_effects.py in place, keeping every existing entry
(never removes anything already there) and adding any new (name -> id)
pairs the live schema has that the bundle doesn't yet. If a name
already exists with a DIFFERENT id than the schema reports, the
schema's id wins (it's the authoritative source) and a note is
printed so you can see exactly what changed.
"""
import json
import re
import sys
import urllib.request
from pathlib import Path

SCHEMA_URL_ENDPOINT = "https://api.steampowered.com/IEconItems_440/GetSchemaURL/v1/"
EFFECTS_FILE = Path(__file__).resolve().parent.parent / "unusual_effects.py"


def fetch_schema_url(api_key: str) -> str:
    url = f"{SCHEMA_URL_ENDPOINT}?key={api_key}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    schema_url = data.get("result", {}).get("items_game_url")
    if not schema_url:
        raise RuntimeError(f"GetSchemaURL did not return an items_game_url: {data!r}")
    return schema_url


def fetch_items_game_text(schema_url: str) -> str:
    with urllib.request.urlopen(schema_url, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def extract_particle_effects(items_game_text: str) -> dict:
    """
    Targeted, lightweight parser for JUST the
    "attribute_controlled_attached_particles" block of items_game.txt
    (a Valve KeyValues/VDF file) - not a general VDF parser, since this
    one block has a simple, regular structure: a numbered sub-block per
    effect, each containing a "name" field.

        "attribute_controlled_attached_particles"
        {
            "30"
            {
                "system"    "unusual_ghost_flame_blue"
                "name"        "Blizzardy Storm"
                ...
            }
            ...
        }

    Finds the block by its marker string, then tracks brace depth from
    there to find exactly where it ends, then regex-extracts every
    "ID" { ... "name" "NAME" ... } pair within that span only - not the
    whole file, so a "name" field belonging to some unrelated block
    elsewhere in this huge file can never be mistaken for an effect.
    """
    marker = '"attribute_controlled_attached_particles"'
    start = items_game_text.find(marker)
    if start == -1:
        raise RuntimeError(
            "Could not find attribute_controlled_attached_particles in the "
            "schema text - Valve may have renamed this block."
        )
    brace_start = items_game_text.find("{", start)
    depth = 0
    i = brace_start
    while i < len(items_game_text):
        if items_game_text[i] == "{":
            depth += 1
        elif items_game_text[i] == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    block = items_game_text[brace_start:i + 1]

    effects = {}
    # Matches: "30" { ... "name" "Blizzardy Storm" ... } - non-greedy up
    # to the next numbered sub-block's own opening, so each sub-block's
    # own "name" is captured, not a later one's.
    for m in re.finditer(r'"(\d+)"\s*\{(.*?)\n\t\t\}', block, re.DOTALL):
        effect_id, body = m.group(1), m.group(2)
        name_match = re.search(r'"name"\s*"([^"]+)"', body)
        if name_match:
            effects[name_match.group(1)] = int(effect_id)
    return effects


def merge_and_write(live_effects: dict):
    current_source = EFFECTS_FILE.read_text(encoding="utf-8")
    namespace = {}
    exec(compile(current_source, str(EFFECTS_FILE), "exec"), namespace)
    existing = dict(namespace["NAME_TO_ID"])

    added, changed = [], []
    merged = dict(existing)
    for name, effect_id in live_effects.items():
        if name not in merged:
            merged[name] = effect_id
            added.append((name, effect_id))
        elif merged[name] != effect_id:
            changed.append((name, merged[name], effect_id))
            merged[name] = effect_id

    if not added and not changed:
        print("Already up to date - nothing to add or change.")
        return

    print(f"Adding {len(added)} new effect(s):")
    for name, effect_id in sorted(added, key=lambda x: x[1]):
        print(f"    {effect_id:>5}  {name}")
    if changed:
        print(f"\nCorrecting {len(changed)} id mismatch(es) (schema is authoritative):")
        for name, old_id, new_id in changed:
            print(f"    {name}: {old_id} -> {new_id}")

    lines = ["    \"{}\": {},".format(name.replace('"', '\\"'), effect_id)
             for name, effect_id in sorted(merged.items(), key=lambda x: x[1])]
    new_dict_body = "\n".join(lines)

    new_source = re.sub(
        r"NAME_TO_ID = \{.*?\n\}",
        "NAME_TO_ID = {\n" + new_dict_body + "\n}",
        current_source,
        count=1,
        flags=re.DOTALL,
    )
    EFFECTS_FILE.write_text(new_source, encoding="utf-8")
    print(f"\nWrote {len(merged)} total effects to {EFFECTS_FILE}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 update_effects_from_schema.py YOUR_STEAM_API_KEY")
        sys.exit(1)
    api_key = sys.argv[1]

    print("Fetching current schema URL from Steam...")
    schema_url = fetch_schema_url(api_key)
    print(f"Schema URL: {schema_url}")

    print("Downloading items_game.txt (this file is large, may take a moment)...")
    items_game_text = fetch_items_game_text(schema_url)
    print(f"Downloaded {len(items_game_text):,} characters.")

    print("Extracting particle effects...")
    live_effects = extract_particle_effects(items_game_text)
    print(f"Found {len(live_effects)} effects in the live schema.")

    merge_and_write(live_effects)


if __name__ == "__main__":
    main()
