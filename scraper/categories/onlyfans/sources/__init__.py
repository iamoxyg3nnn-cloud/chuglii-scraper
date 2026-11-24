"""
OnlyFans Sources Package
"""
from . import internetchicks
from . import influencersgonewild
from . import sheeshfans

SOURCES = {
    "internetchicks": internetchicks,
    "influencersgonewild": influencersgonewild,
    "sheeshfans": sheeshfans
}

__all__ = ["internetchicks", "influencersgonewild", "sheeshfans", "SOURCES"]
