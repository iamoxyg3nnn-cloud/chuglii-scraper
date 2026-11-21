"""
Forwarder to Viral MMS polish module under scraper/categories/viral-mms/polish.py
"""
import sys
import importlib.util
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
VIRAL_MMS_POLISH = CURRENT_DIR / "categories" / "viral-mms" / "polish.py"

if not VIRAL_MMS_POLISH.exists():
    raise FileNotFoundError(f"Missing polish module at {VIRAL_MMS_POLISH}")

spec = importlib.util.spec_from_file_location("viral_mms_polish", str(VIRAL_MMS_POLISH))
_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_module)

for _attr in dir(_module):
    if not _attr.startswith("_"):
        globals()[_attr] = getattr(_module, _attr)
