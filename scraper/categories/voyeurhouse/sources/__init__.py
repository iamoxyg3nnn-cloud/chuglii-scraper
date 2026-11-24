"""Voyeur House source package.

One category, one adapter per isolated website (mirrors the OnlyFans layout):

- ``voyeurhouseorg``       — voyeur-house.org
- ``hotscopesvoyeurhouse`` — hotscopes.net/videos/voyeur-house
- ``reallifecamto``        — reallifecam.to/videos
- ``hotscopesspygasm``     — hotscopes.net/videos/spygasm?o=mr
- ``hotscopesreallifecam`` — hotscopes.net/videos/reallifecam?o=mr

Each adapter writes to its own table through its own RPC family. There is no
cross-site dedupe, merge, or canonical cache; ``source_site`` keys every record.
"""

from . import avs_base  # noqa: F401 - must load before the AVS adapters
from . import voyeurhouseorg
from . import hotscopesvoyeurhouse, reallifecamto, hotscopesspygasm, hotscopesreallifecam

SOURCES = {
    "voyeurhouseorg": voyeurhouseorg,
    "hotscopesvoyeurhouse": hotscopesvoyeurhouse,
    "reallifecamto": reallifecamto,
    "hotscopesspygasm": hotscopesspygasm,
    "hotscopesreallifecam": hotscopesreallifecam,
}

SOURCE_LABELS = {key: mod.LABEL for key, mod in SOURCES.items()}

__all__ = [
    "avs_base",
    "voyeurhouseorg",
    "hotscopesvoyeurhouse",
    "reallifecamto",
    "hotscopesspygasm",
    "hotscopesreallifecam",
    "SOURCES",
    "SOURCE_LABELS",
]
