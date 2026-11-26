"""Isolated Viral MMS source adapters."""
from . import spicymms
from . import desi49
from . import mydesi
from . import indianbfclips

SOURCES = {
    "spicymms": spicymms,
    "desi49": desi49,
    "mydesi": mydesi,
    "indianbfclips": indianbfclips,
}

__all__ = ["spicymms", "desi49", "mydesi", "indianbfclips", "SOURCES"]
