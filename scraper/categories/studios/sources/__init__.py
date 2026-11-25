"""
Studios Sources Package

Phase A: madouer (madouer.cc, MacCMS). Phase B: dasj91 (91dasj.com, Typecho).
Phase C adds madouhd.
"""
from . import madouer
from . import dasj91

SOURCES = {
    "madouer": madouer,
    "dasj91": dasj91,
}

__all__ = ["madouer", "dasj91", "SOURCES"]
