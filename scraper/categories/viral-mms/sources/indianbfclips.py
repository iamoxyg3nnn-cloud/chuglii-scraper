"""Source adapter for IndianBFClips (https://www.indianbfclips.com)."""
from .kvs_base import KvsViralMmsAdapter

BASE_URL = "https://www.indianbfclips.com"
SOURCE_NAME = "indianbfclips"

_ADAPTER = KvsViralMmsAdapter(
    source_site=SOURCE_NAME,
    base_url=BASE_URL,
    label="IndianBFClips",
)

scrape_listing = _ADAPTER.scrape_listing
enrich_detail = _ADAPTER.enrich_detail
