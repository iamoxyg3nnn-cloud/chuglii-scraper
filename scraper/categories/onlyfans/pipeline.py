"""
Unified Multi-Site OnlyFans Scraper & Cross-Site Deduplication Pipeline
Targets:
  1. Internet Chicks (https://internetchicks.com)
  2. Influencers Gone Wild (https://influencersgonewild.com)
  3. SheeshFans (https://sheeshfans.com)

Features:
 - Multi-tier cross-site deduplication (Locker hash, Semantic token fingerprint, Jaccard overlap)
 - Automatic stream mirror merging (combines VOE, PlayMogo, Streamtape, DoodStream, Direct CDN MP4)
 - Multi-threaded detail enrichment (ThreadPoolExecutor)
 - Zero-dependency stealth scraping via Scrapling Fetcher
 - Resilient per-site checkpoint state persistence
 - Continuous 24/7 daemon watcher mode
"""

import os
import sys
import re
import json
import time
import random
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(PROJECT_ROOT), str(SCRAPER_DIR), str(CURRENT_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from scraper.categories.onlyfans.polish import polish_onlyfans_record
    from scraper.categories.onlyfans.deduplicator import CrossSiteDeduplicator
    from scraper.categories.onlyfans.sources import SOURCES
except ImportError:
    from polish import polish_onlyfans_record
    from deduplicator import CrossSiteDeduplicator
    from sources import SOURCES

from scrapling import Fetcher

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"

# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint & Cache Persistence
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoints() -> Dict[str, Any]:
    """Loads per-site checkpoints: Supabase-first with local fallback."""
    state = {
        "internetchicks": {"current_page": 1, "max_pages": 513},
        "influencersgonewild": {"current_page": 1, "max_pages": 200},
        "sheeshfans": {"current_page": 1, "max_pages": 200},
        "pimpbunny": {"current_page": 1, "max_pages": 154},
        "total_ingested": 0,
        "total_merged": 0
    }
    if CHECKPOINT_FILE.exists():
        try:
            data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                state.update(data)
        except Exception:
            pass
    return state

def save_checkpoints(state: Dict[str, Any]) -> None:
    """Saves per-site checkpoints locally and syncs to Supabase."""
    try:
        from scraper.db import set_scraper_checkpoint
        set_scraper_checkpoint("onlyfans_multi", state)
    except Exception:
        pass
    try:
        state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        CHECKPOINT_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[!] Warning writing checkpoint file: {e}")

def load_known_ids() -> Set[str]:
    """Loads post ID set formatted as site:post_id."""
    known_ids: Set[str] = set()
    if KNOWN_IDS_FILE.exists():
        try:
            data = json.loads(KNOWN_IDS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                known_ids.update(str(x) for x in data)
        except Exception:
            pass
    return known_ids

def save_known_ids(known_ids: Set[str]) -> None:
    try:
        KNOWN_IDS_FILE.write_text(json.dumps(sorted(list(known_ids))), encoding="utf-8")
    except Exception as e:
        print(f"[!] Warning writing known IDs cache: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Unified Concurrent Crawler
# ─────────────────────────────────────────────────────────────────────────────

def enrich_items_concurrently(source_module, items: List[Dict[str, Any]], max_workers: int = 5) -> List[Dict[str, Any]]:
    """Fetches details for listing items concurrently."""
    if not items:
        return []
    enriched = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(source_module.enrich_detail, item): item for item in items}
        for future in as_completed(futures):
            try:
                res = future.result()
                enriched.append(res)
            except Exception as e:
                item = futures[future]
                print(f"[!] Error enriching item {item.get('detail_url')}: {e}")
                enriched.append(item)
    return enriched

def run_site_crawl(
    site_key: str,
    start_page: int,
    max_pages: int,
    deduplicator: CrossSiteDeduplicator,
    known_ids: Set[str],
    dry_run: bool = False,
    jitter: float = 1.0
) -> Dict[str, Any]:
    """Runs ingestion and deduplication for a single source site."""
    source_mod = SOURCES.get(site_key)
    if not source_mod:
        print(f"[!] Unknown site key: {site_key}")
        return {"items_found": 0, "items_new": 0, "items_merged": 0}

    print(f"\n=======================================================")
    print(f"CRAWLING SOURCE: {site_key.upper()} (Pages {start_page} to {start_page + max_pages - 1})")
    print(f"=======================================================")

    stats = {
        "items_found": 0,
        "items_new": 0,
        "items_merged": 0,
        "skipped_known": 0
    }

    for page_idx in range(max_pages):
        page_num = start_page + page_idx
        cards, detected_max = source_mod.scrape_listing(page_num)

        if not cards:
            print(f"[-] No cards found on {site_key} page {page_num}. Ending site crawl.")
            break

        stats["items_found"] += len(cards)

        # Skip items already processed from this specific site
        new_cards = [c for c in cards if f"{site_key}:{c.get('source_post_id')}" not in known_ids]
        skipped = len(cards) - len(new_cards)
        stats["skipped_known"] += skipped

        print(f"    [{site_key}] Page {page_num}: {len(cards)} cards, {len(new_cards)} new, {skipped} previously seen.")

        if not new_cards:
            time.sleep(jitter)
            continue

        # Concurrent detail enrichment
        enriched = enrich_items_concurrently(source_mod, new_cards, max_workers=5)

        # Polish and pass through Cross-Site Deduplicator
        batch_to_upsert = []
        for raw in enriched:
            try:
                # Discard non-video entries (e.g. photo sets or galleries without video streams)
                raw_servers = raw.get("servers") or []
                raw_stream = raw.get("full_video_url")
                if not raw_stream and not raw_servers:
                    is_photo_set = raw.get("gallery_url") or any(w in raw.get("title", "").lower() for w in ["photo", "photos", "set", "gallery"])
                    if is_photo_set:
                        known_ids.add(f"{site_key}:{raw.get('source_post_id')}")
                        print(f"      [SKIP PHOTO SET] '{raw.get('title')}' is a photo set/gallery. Skipped.")
                    else:
                        print(f"      [SKIP UNRESOLVED] '{raw.get('title')}' has no stream (enrichment failed or anti-bot gated). Not marked known.")
                    continue

                polished = polish_onlyfans_record(raw)
                if not polished.get("full_video_url") and not (polished.get("raw_payload", {}).get("servers") or polished.get("servers")):
                    known_ids.add(f"{site_key}:{raw.get('source_post_id')}")
                    print(f"      [SKIP NON-VIDEO] '{polished.get('title')}' has no valid stream after polish. Skipped.")
                    continue

                processed_record, is_new = deduplicator.process_candidate(polished)
                batch_to_upsert.append(processed_record)

                if is_new:
                    stats["items_new"] += 1
                else:
                    stats["items_merged"] += 1
                    print(f"      [🔀 DUPLICATE MERGED] '{polished.get('title')}' merged into canonical entry!")

                # Mark site:post_id in known cache
                known_ids.add(f"{site_key}:{raw.get('source_post_id')}")
            except Exception as e:
                print(f"[!] Error polishing {site_key} post {raw.get('source_post_id')}: {e}")

        # Batch upsert to database
        if not dry_run and batch_to_upsert:
            # Deduplicate batch by (source_site, source_post_id) to avoid Postgres error 21000
            deduped_batch_dict = {}
            for r in batch_to_upsert:
                k = (r.get("source_site"), str(r.get("source_post_id")))
                deduped_batch_dict[k] = r
            final_batch = list(deduped_batch_dict.values())
            try:
                from scraper.db import upsert_onlyfans_videos_batch
                upsert_onlyfans_videos_batch(final_batch)
            except Exception as e:
                print(f"[!] Supabase upsert error: {e}")

        save_known_ids(known_ids)
        time.sleep(jitter + random.uniform(0.3, 0.8))

    return stats

def run_unified_pipeline(
    sites: List[str],
    pages_per_site: int = 2,
    dry_run: bool = False,
    save_json: bool = False,
    jitter: float = 1.0
) -> None:
    """Executes the master cross-site deduplicating pipeline across selected sites."""
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    deduplicator = CrossSiteDeduplicator()

    # Pre-populate deduplicator with existing cached records if available
    export_cache_file = CURRENT_DIR / "onlyfans_canonical_cache.json"
    if export_cache_file.exists():
        try:
            cached_data = json.loads(export_cache_file.read_text(encoding="utf-8"))
            for r in cached_data:
                deduplicator.register_existing(r)
            print(f"[*] Pre-loaded {len(cached_data)} canonical records into deduplication engine.")
        except Exception:
            pass

    total_stats = {"items_found": 0, "items_new": 0, "items_merged": 0, "skipped_known": 0}

    for site in sites:
        site_cp = checkpoints.setdefault(site, {"current_page": 1, "max_pages": 500})
        start_p = site_cp.get("current_page", 1)
        res = run_site_crawl(
            site_key=site,
            start_page=start_p,
            max_pages=pages_per_site,
            deduplicator=deduplicator,
            known_ids=known_ids,
            dry_run=dry_run,
            jitter=jitter
        )
        for k in total_stats:
            total_stats[k] += res.get(k, 0)
        site_cp["current_page"] = start_p + pages_per_site
        checkpoints[site] = site_cp

    checkpoints["total_ingested"] = len(deduplicator.canonical_records)
    checkpoints["total_merged"] = total_stats["items_merged"]
    save_checkpoints(checkpoints)

    canonical_list = list(deduplicator.canonical_records.values())
    export_cache_file.write_text(json.dumps(canonical_list, indent=2), encoding="utf-8")
    print(f"\n[+] Synchronized {len(canonical_list)} clean canonical records to {export_cache_file.name}")

    if not dry_run and canonical_list:
        try:
            from scraper.db import upsert_onlyfans_videos_batch
            # Upsert in chunks of 50 to avoid request size limits
            for i in range(0, len(canonical_list), 50):
                chunk = canonical_list[i:i + 50]
                upsert_onlyfans_videos_batch(chunk)
            print(f"[✓] Successfully synced all {len(canonical_list)} canonical records to Supabase scraper.onlyfans_videos")
        except Exception as e:
            print(f"[!] Warning syncing canonical records to Supabase: {e}")

    print("\n" + "=" * 60)
    print("UNIFIED MULTI-SITE ONLYFANS SCRAPE SUMMARY")
    print(f"Sites crawled          : {', '.join(sites)}")
    print(f"Raw items found        : {total_stats['items_found']}")
    print(f"Unique new videos      : {total_stats['items_new']}")
    print(f"Duplicates merged      : {total_stats['items_merged']}")
    print(f"Known items skipped    : {total_stats['skipped_known']}")
    print(f"Total canonical videos : {len(deduplicator.canonical_records)}")
    print("=" * 60 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Multi-Site OnlyFans Scraper & Deduplication Pipeline")
    parser.add_argument("--site", choices=["all", "internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny"], default="all",
                        help="Target site to crawl (default: all)")
    parser.add_argument("--pages", type=int, default=2, help="Number of pages per site (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Scrape and deduplicate without writing to Supabase")
    parser.add_argument("--save-json", action="store_true", help="Save merged canonical records to JSON")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background watcher mode")
    parser.add_argument("--delay", type=float, default=1.0, help="Polite delay between pages in seconds")

    args = parser.parse_args()

    target_sites = ["internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny"] if args.site == "all" else [args.site]

    if args.daemon:
        print("[*] Starting continuous 24/7 cross-site watcher daemon...")
        while True:
            run_unified_pipeline(target_sites, pages_per_site=1, dry_run=args.dry_run, save_json=args.save_json, jitter=args.delay)
            time.sleep(600)
        return

    run_unified_pipeline(
        sites=target_sites,
        pages_per_site=args.pages,
        dry_run=args.dry_run,
        save_json=args.save_json,
        jitter=args.delay
    )

if __name__ == "__main__":
    main()
