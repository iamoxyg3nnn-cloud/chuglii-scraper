"""
Studios Sources Package

Phase A: madouer (madouer.cc, MacCMS). Phase B/C add dasj91 + madouhd.
"""
from . import madouer

SOURCES = {
    "madouer": madouer,
}

__all__ = ["madouer", "SOURCES"]
