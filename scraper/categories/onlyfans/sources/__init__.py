"""
OnlyFans Sources Package
"""
from . import influencersgonewild
from . import sheeshfans

SOURCES = {
    "influencersgonewild": influencersgonewild,
    "sheeshfans": sheeshfans
}

__all__ = ["influencersgonewild", "sheeshfans", "SOURCES"]
