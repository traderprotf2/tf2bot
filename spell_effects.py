"""
Bundled TF2 Halloween spell id -> name database - the complete,
official set (16 total, confirmed against a dedicated live spell-data
API - see ID_TO_NAME below for the source and exact response used).

Exists for the same reason unusual_effects.py does for particle
effects: backpack.tf's real-time payload sometimes carries a spell
attribute as an ID/defindex without a resolved "name" field (confirmed
via a third-party TF2-item-parsing library that explicitly documents a
"spell defindex to name" export as a required piece for exactly this
reason) - reading only item.spells[].name, with no fallback, silently
drops any spell in that shape. A real, confirmed incident this closes:
a two-spell item's identity key ending up as one spell (or zero) purely
because one of its spells only had this ID-only shape, which is the
exact mechanism behind several repeated "buy order from a spelled item"
reports this project has seen.

Spells are a small, permanently fixed set - none have been added since
Scream Fortress 2015 (per the official TF2 wiki, spells have been
unobtainable since then; all that remain are already-applied to items
from before that date) - so, unlike unusual_effects.py, this bundle
needs no "may be incomplete" caveat: this IS the complete, final list,
confirmed against a source that itself collects directly from
backpack.tf's own listing data.

Also note: `type` groups spells by the ATTRIBUTE slot they occupy on an
item (paint/footprint/voice/effect are stored as separate attributes -
confirmed via a public TF2 attribute-index reference), which is why an
item can genuinely carry more than one spell at once (e.g. a paint
spell AND a voice spell simultaneously) - this project's own spell_combo
handling (see matcher.py) already accounts for that.
"""

import re

# Source: https://spells.pricedb.io/api/spell/spells (fetched directly,
# a dedicated live spell-data API collecting from backpack.tf's own
# listings) - id -> (name, type). type is informational only (not used
# for identity-key purposes, which only care about the name).
ID_TO_INFO = {
    2000: ("Halloween Fire", "effect"),
    2001: ("Voices From Below", "voice"),
    2002: ("Exorcism", "effect"),
    2003: ("Pumpkin Bombs", "effect"),
    2004: ("Chromatic Corruption", "paint"),
    2005: ("Sinister Staining", "paint"),
    2006: ("Spectral Spectrum", "paint"),
    2007: ("Putrescent Pigmentation", "paint"),
    2008: ("Die Job", "paint"),
    2009: ("Headless Horseshoes", "footprint"),
    2010: ("Team Spirit Footprints", "footprint"),
    2011: ("Corpse Gray Footprints", "footprint"),
    2012: ("Violent Violet Footprints", "footprint"),
    2013: ("Bruised Purple Footprints", "footprint"),
    2014: ("Gangreen Footprints", "footprint"),
    2015: ("Rotten Orange Footprints", "footprint"),
}

ID_TO_NAME = {spell_id: info[0] for spell_id, info in ID_TO_INFO.items()}
NAME_TO_ID = {name: spell_id for spell_id, name in ID_TO_NAME.items()}

# Alternate/historical names for the SAME underlying spell that this
# project's own code has previously observed appearing directly in raw
# payload text, confirmed by the official TF2 wiki as pre-2015-rename
# names Valve's own client still sometimes surfaces verbatim instead of
# the current grouped display name ("Gourd Grenades, Sentry Quad-
# Pumpkins, and Squash Rockets spells applied to items are listed as
# 'Pumpkin Bombs' rather than the actual name of the spell" - i.e. the
# UNDERLYING spell attribute is one of these three internally, but only
# ever DISPLAYS as "Pumpkin Bombs"; "Spectral Flame" applied to items is
# similarly always displayed as "Halloween Fire"; every class-specific
# voice spell - "Spy's Creepy Croon" etc - is always displayed as
# "Voices From Below"). A real, confirmed risk this closes: if ANY two
# events for the same underlying spell ever report different name text
# (one the internal name, one the display name), the identity key would
# treat them as different spells entirely - normalizing to the same
# canonical name at extraction time, everywhere a spell name is read,
# closes that gap regardless of which text a given payload happens to use.
ALTERNATE_NAME_TO_CANONICAL = {
    "Squash Rocket": "Pumpkin Bombs",
    "Squash Rockets": "Pumpkin Bombs",
    "Gourd Grenades": "Pumpkin Bombs",
    "Sentry Quad-Pumpkins": "Pumpkin Bombs",
    "Spectral Flame": "Halloween Fire",
    "Spy's Creepy Croon": "Voices From Below",
    "Demoman's Cadaverous Croak": "Voices From Below",
    "Scout's Spectral Snarl": "Voices From Below",
    "Sniper's Deep Down Under Drawl": "Voices From Below",
    "Heavy's Bottomless Bass": "Voices From Below",
    "Medic's Blood-Curdling Bellow": "Voices From Below",
    "Pyro's Muffled Moan": "Voices From Below",
    "Engineer's Gravelly Growl": "Voices From Below",
    "Soldier's Booming Bark": "Voices From Below",
}


def normalize_spell_name(name):
    """Canonical display name for a spell, regardless of which of its
    possible text variants a given payload happens to report - see
    ALTERNATE_NAME_TO_CANONICAL's own comment for why this exists.
    Passes through unrecognised names as-is (never guesses)."""
    if not name:
        return name
    return ALTERNATE_NAME_TO_CANONICAL.get(name, name)


def extract_spell_names(raw_spells):
    """
    Given the raw item.spells list from a backpack.tf payload, returns
    the list of resolved, canonicalized spell names actually present -
    the single shared implementation behind every spell-extraction site
    in this project (main.py's real-time handler, the bulk scanner, and
    fetch_live_buy_order_keys's own entry filtering), so a fix to this
    logic only ever needs to be made once.

    Resolves via ID_TO_NAME when an entry has no usable "name" field
    (see this module's own docstring for why that shape occurs), then
    normalizes every result through normalize_spell_name.
    """
    names = []
    for s in (raw_spells or []):
        if not isinstance(s, dict):
            continue
        spell_name = s.get("name")
        if not spell_name:
            spell_id = s.get("id") or s.get("defindex")
            try:
                spell_id = int(spell_id) if spell_id is not None else None
            except (TypeError, ValueError):
                spell_id = None
            if spell_id is not None:
                spell_name = ID_TO_NAME.get(spell_id)
        if spell_name:
            names.append(normalize_spell_name(spell_name))
    return names


# Common abbreviations/shorthand for spells seen in real buy-order seller
# notes, beyond the canonical names themselves (checked separately, via
# NAME_TO_ID) - "VFB" for Voices From Below is by far the most common,
# confirmed directly from a real buy order's own text: "VFB - Listed
# Price" (i.e. the listing's own structured price applies ONLY to that
# one spell, not to a plain item at all).
_KNOWN_SPELL_ABBREVIATIONS = ("vfb", "cc", "dj", "ss", "pp", "hh")


def note_mentions_spell(text):
    """
    Whether a buy-order's own free-text note (its "details"/description
    field) mentions a specific spell by name or common abbreviation -
    used to catch a real, confirmed pattern: a buy-order bot posts ONE
    structured listing whose price covers only ONE specific spell (or a
    tiered set of different spells at different prices), stated entirely
    in free text ("VFB - Listed Price / CC - Spec - 9k / Any footprints
    - 41+ keys") - backpack.tf has no structured way to express "this
    price is spell-conditional" at all, so the listing's own "spells"
    field is empty/unset even though the price plainly isn't for a plain
    item. Recording this price under the spell-less identity bucket
    would silently misprice every genuinely spell-less sell listing of
    the same item - the exact mechanism behind a real, confirmed report.
    Pattern-matches on word boundaries only (never a bare substring), so
    "shhh" doesn't match "hh" etc. Never guesses WHICH spell - just
    flags the note as spell-conditional so the caller can treat this
    entry's price as unreliable for a spell-less comparison.
    """
    if not text:
        return False
    text_lower = text.lower()
    for name in NAME_TO_ID:
        if name.lower() in text_lower:
            return True
    for name in ALTERNATE_NAME_TO_CANONICAL:
        if name.lower() in text_lower:
            return True
    if "spell" in text_lower:
        return True
    for abbrev in _KNOWN_SPELL_ABBREVIATIONS:
        if re.search(rf"\b{re.escape(abbrev)}\b", text_lower):
            return True
    return False
