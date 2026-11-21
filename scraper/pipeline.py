"""
Forwarder to Viral MMS pipeline under scraper/categories/viral-mms/pipeline.py
"""
import sys
import importlib.util
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
VIRAL_MMS_PIPELINE = CURRENT_DIR / "categories" / "viral-mms" / "pipeline.py"

if not VIRAL_MMS_PIPELINE.exists():
    raise FileNotFoundError(f"Missing pipeline module at {VIRAL_MMS_PIPELINE}")

spec = importlib.util.spec_from_file_location("viral_mms_pipeline", str(VIRAL_MMS_PIPELINE))
_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_module)

for _attr in dir(_module):
    if not _attr.startswith("_"):
        globals()[_attr] = getattr(_module, _attr)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Viral MMS Ingestion Pipeline Forwarder")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to scrape")
    parser.add_argument("--filter", type=str, default="latest", help="Filter mode")
    parser.add_argument("--category", type=str, default=None, help="Target specific subcategory")
    parser.add_argument("--enrich", action="store_true", help="Enable deep detail page fetch")

    args = parser.parse_args()
    run_scraper_pipeline(pages=args.pages, filter_mode=args.filter, category=args.category, enrich_details=args.enrich)
