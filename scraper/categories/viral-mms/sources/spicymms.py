"""Source adapter for SpicyMMS (https://www.spicymms.com)."""
from .kvs_base import KvsViralMmsAdapter

BASE_URL = "https://www.spicymms.com"
SOURCE_NAME = "spicymms"

_ADAPTER = KvsViralMmsAdapter(
    source_site=SOURCE_NAME,
    base_url=BASE_URL,
    label="SpicyMMS",
)

scrape_listing = _ADAPTER.scrape_listing
enrich_detail = _ADAPTER.enrich_detail
