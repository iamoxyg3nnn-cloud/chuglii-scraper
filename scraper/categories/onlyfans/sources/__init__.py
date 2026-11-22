"""
OnlyFans Sources Package
"""
from . import internetchicks
from . import influencersgonewild
from . import sheeshfans
from . import pimpbunny

SOURCES = {
    "internetchicks": internetchicks,
    "influencersgonewild": influencersgonewild,
    "sheeshfans": sheeshfans,
    "pimpbunny": pimpbunny
}

__all__ = ["internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny", "SOURCES"]
