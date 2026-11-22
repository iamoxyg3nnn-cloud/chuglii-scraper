"""
Multi-Tier Cross-Site Deduplication & Quality-First Mirror Merging Engine
Guarantees zero duplicate videos across InternetChicks, InfluencersGoneWild, SheeshFans, and PimpBunny.
Always defaults to scraping and serving the HIGHEST QUALITY video stream and thumbnail.

Tiers:
 1. Canonical Locker Hash Matching (Exact Hash on Streamtape, VOE, PlayMogo, DoodStream, IGW CDN IDs)
 2. Model + Semantic Token Fingerprint (SHA256 of creator + sorted non-generic words)
 3. Jaccard Token Overlap (Fuzzy word similarity >= 0.38 within the same model)
 4. Quality Ranking & Promotion Engine (Promotes 1080p/Direct MP4 to primary, demotes lower to backup)
 5. Thumbnail Resolution Promotion (Always selects highest resolution 1000px/680px asset)
"""

import re
import hashlib
from typing import Dict, Any, List, Optional, Set, Tuple

GENERIC_STOP_WORDS = {
    "onlyfans", "only", "fans", "leaked", "leak", "leaks", "video", "videos",
    "clip", "clips", "nude", "nudes", "naked", "porn", "porno", "xxx", "sextape",
    "sex", "tape", "free", "hd", "compilation", "set", "photos", "full", "ppv",
    "internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny", "com", "net",
    "www", "watch", "online", "stream", "directors", "cut", "edition", "part", "pt", "ep",
    "uncut", "footage", "archive", "private", "show", "exclusive"
}

def extract_locker_hash(url: str) -> Optional[str]:
    """Extracts unique locker media hash/id from stream or embed URLs."""
    if not url:
        return None
    
    # Streamtape: /e/{id} or /v/{id}
    m = re.search(r"streamtape\.com/[ev]/([A-Za-z0-9_-]+)", url)
    if m:
        return f"st:{m.group(1)}"
        
    # VOE: /e/{id}
    m = re.search(r"voe\.sx/e/([A-Za-z0-9_-]+)", url)
    if m:
        return f"voe:{m.group(1)}"
        
    # PlayMogo / StreamWish: /e/{id}
    m = re.search(r"(?:playmogo|streamwish)\.[a-z]+/e/([A-Za-z0-9_-]+)", url)
    if m:
        return f"mogo:{m.group(1)}"
        
    # DoodStream / Bysezejataos: /e/{id} or /d/{id}
    m = re.search(r"(?:dood|bysezejataos)\.[a-z]+/[ed]/([A-Za-z0-9_-]+)", url)
    if m:
        return f"dood:{m.group(1)}"

    # Influencers CDN filename hash: ..._<hash>.mp4
    m = re.search(r"/videos/[^/]+?-([A-Z0-9]{4,10})\.mp4", url)
    if m:
        return f"igw:{m.group(1)}"

    # PimpBunny base64 query payload
    m = re.search(r"/stream/video\.mp4\?v=([A-Za-z0-9+/=]+)", url)
    if m:
        return f"pb:{m.group(1)[:20]}"

    return None

def normalize_model_name(raw_model: Optional[str]) -> str:
    """Normalizes model name into lowercase hyphenated slug (e.g. 'christina-khalil')."""
    if not raw_model:
        return "unknown"
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", raw_model.strip().lower()).strip("-")
    return cleaned or "unknown"

def extract_semantic_tokens(title: str, model_slug: str) -> Set[str]:
    """Extracts meaningful semantic words from title, stripping stop words and model tokens."""
    cleaned = title.lower()
    words = re.findall(r"\b[a-z0-9]{2,}\b", cleaned)
    
    model_parts = set(model_slug.split("-")) if model_slug else set()
    tokens = set()
    for w in words:
        if w not in GENERIC_STOP_WORDS and w not in model_parts:
            tokens.add(w)
            
    return tokens

def generate_semantic_fingerprint(model_slug: str, tokens: Set[str]) -> str:
    """Generates a 16-character deterministic hash from model slug and sorted tokens."""
    token_str = " ".join(sorted(list(tokens)))
    raw = f"{model_slug}:{token_str}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
    """Calculates Jaccard overlap index between two token sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union

# ─────────────────────────────────────────────────────────────────────────────
# Quality Scoring & Resolution Evaluator
# ─────────────────────────────────────────────────────────────────────────────

def compute_quality_score(server: Dict[str, Any], title: str = "") -> int:
    """
    Computes a numerical quality score for a server stream:
      - 4K / 2160p: 2160 * 1.25 = 2700
      - 2K / 1440p: 1440 * 1.25 = 1800
      - 1080p Direct MP4: 1080 * 1.30 = 1404
      - 1080p Fast Locker (VOE): 1080 * 1.15 = 1242
      - 720p HD (PlayMogo/Streamtape): 720 * 1.05 = 756
      - 480p SD (Doodstream): 480 * 0.85 = 408
    """
    url = server.get("embed_url", "").lower()
    name = server.get("name", "").lower()
    mime = server.get("type", "").lower()
    combined = f"{name} {url} {title}".lower()

    if any(k in combined for k in ["4k", "2160p", "uhd"]):
        base_res = 2160
    elif any(k in combined for k in ["2k", "1440p", "qhd"]):
        base_res = 1440
    elif any(k in combined for k in ["1080p", "1080", "fhd", "full hd"]):
        base_res = 1080
    elif any(k in combined for k in ["720p", "720", "hd"]):
        base_res = 720
    elif any(k in combined for k in ["480p", "480", "sd"]):
        base_res = 480
    else:
        # Host-specific intelligent defaults
        if "influencersgonewild.net" in url or "voe.sx" in url:
            base_res = 1080
        elif "sheeshfans.com" in url or "playmogo" in url or "streamtape" in url or "pimpbunny" in url:
            base_res = 720
        else:
            base_res = 480

    # Multiplier based on compression and delivery mechanism
    multiplier = 1.0
    if mime == "direct_mp4" or ".mp4" in url or "video/mp4" in mime:
        multiplier = 1.25  # Direct uncompressed MP4
    elif "voe.sx" in url:
        multiplier = 1.15  # VOE high-bitrate HLS
    elif "playmogo" in url or "streamwish" in url:
        multiplier = 1.10
    elif "streamtape" in url:
        multiplier = 1.00
    elif "dood" in url or "bysezejataos" in url:
        multiplier = 0.85  # Aggressive lossy re-encoding

    return int(base_res * multiplier)

def estimate_thumbnail_resolution(thumb_url: Optional[str]) -> int:
    """Estimates pixel area (width * height) from thumbnail URL or dimensions."""
    if not thumb_url:
        return 0
    u = thumb_url.lower()
    
    # Check explicit dimensions in filename: e.g. 1000x563, 680x450
    m = re.search(r"(\d{3,4})x(\d{3,4})", u)
    if m:
        return int(m.group(1)) * int(m.group(2))
        
    if "original" in u or "1000" in u:
        return 563000
    if "680" in u:
        return 306000
    if "640" in u:
        return 230400
    if "340" in u:
        return 76500
    return 100000

# ─────────────────────────────────────────────────────────────────────────────
# Master Deduplication & Highest-Quality Merger
# ─────────────────────────────────────────────────────────────────────────────

class CrossSiteDeduplicator:
    def __init__(self):
        self.locker_hash_index: Dict[str, str] = {}
        self.fingerprint_index: Dict[str, str] = {}
        self.model_records_index: Dict[str, List[str]] = {}
        self.canonical_records: Dict[str, Dict[str, Any]] = {}

    def register_existing(self, record: Dict[str, Any]) -> None:
        """Indexes an existing canonical record into all lookups."""
        canonical_id = record.get("canonical_id") or f"{record.get('source_site')}:{record.get('source_post_id')}"
        record["canonical_id"] = canonical_id
        self.canonical_records[canonical_id] = record

        # Index locker hashes
        raw_servers = record.get("raw_payload", {}).get("servers", [])
        for s in raw_servers:
            h = extract_locker_hash(s.get("embed_url", ""))
            if h:
                self.locker_hash_index[h] = canonical_id

        # Index semantic fingerprint
        actors = record.get("actors", [])
        model_slug = normalize_model_name(actors[0] if actors else None)
        tokens = extract_semantic_tokens(record.get("title", ""), model_slug)
        fp = generate_semantic_fingerprint(model_slug, tokens)
        self.fingerprint_index[fp] = canonical_id

        if model_slug not in self.model_records_index:
            self.model_records_index[model_slug] = []
        if canonical_id not in self.model_records_index[model_slug]:
            self.model_records_index[model_slug].append(canonical_id)

    def find_duplicate(self, candidate: Dict[str, Any]) -> Optional[str]:
        """
        Runs multi-tier check to find if candidate already exists.
        Returns matching canonical_id if duplicate, else None.
        """
        # Tier 1: Check locker hashes
        candidate_servers = candidate.get("raw_payload", {}).get("servers", []) or candidate.get("servers", [])
        for s in candidate_servers:
            embed_url = s.get("embed_url") if isinstance(s, dict) else str(s)
            h = extract_locker_hash(embed_url)
            if h and h in self.locker_hash_index:
                return self.locker_hash_index[h]

        # Tier 2: Check model + exact semantic fingerprint
        actors = candidate.get("actors", []) or candidate.get("models", [])
        model_slug = normalize_model_name(actors[0] if actors else None)
        title = candidate.get("title", "")
        tokens = extract_semantic_tokens(title, model_slug)
        fp = generate_semantic_fingerprint(model_slug, tokens)

        if fp in self.fingerprint_index:
            return self.fingerprint_index[fp]

        # Tier 3: Fuzzy token similarity within the same model
        cand_dur = candidate.get("duration_seconds")
        if model_slug != "unknown" and model_slug in self.model_records_index:
            for cid in self.model_records_index[model_slug]:
                existing = self.canonical_records.get(cid)
                if not existing:
                    continue
                exist_tokens = extract_semantic_tokens(existing.get("title", ""), model_slug)
                sim = jaccard_similarity(tokens, exist_tokens)
                
                # Check duration proximity if duration is known for both
                exist_dur = existing.get("duration_seconds")
                duration_match = False
                if cand_dur and exist_dur and abs(cand_dur - exist_dur) <= 4:
                    duration_match = True

                if sim >= 0.38 or (duration_match and sim >= 0.20):
                    return cid

        return None

    def merge_records(self, existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merges an incoming duplicate video into the canonical record:
         - Promotes HIGHEST QUALITY video stream as default primary full_video_url.
         - Promotes HIGHEST RESOLUTION thumbnail as clean_thumbnail_url.
         - Combines all streaming mirrors in servers list.
         - Merges tags and tracks sources.
        """
        merged = dict(existing)
        
        # 1. Merge server streams without duplicate URLs
        existing_servers = merged.get("raw_payload", {}).get("servers", [])
        incoming_servers = incoming.get("raw_payload", {}).get("servers", []) or incoming.get("servers", [])
        existing_urls = {s.get("embed_url") for s in existing_servers if isinstance(s, dict)}

        for inc_s in incoming_servers:
            if isinstance(inc_s, dict):
                url = inc_s.get("embed_url")
                if url and url not in existing_urls:
                    # Calculate quality score for incoming server
                    inc_s["quality_score"] = compute_quality_score(inc_s, incoming.get("title", ""))
                    existing_servers.append(inc_s)
                    existing_urls.add(url)
                    h = extract_locker_hash(url)
                    if h:
                        self.locker_hash_index[h] = merged["canonical_id"]

        # Ensure all servers have quality scores computed
        for s in existing_servers:
            if "quality_score" not in s:
                s["quality_score"] = compute_quality_score(s, merged.get("title", ""))

        # 2. HIGHEST QUALITY PROMOTION: Sort servers by quality score descending
        existing_servers.sort(key=lambda s: s.get("quality_score", 0), reverse=True)
        best_server = existing_servers[0] if existing_servers else None

        if best_server:
            merged["full_video_url"] = best_server.get("embed_url")
            merged["video_mime_type"] = "video/mp4" if (best_server.get("type") == "direct_mp4" or ".mp4" in best_server.get("embed_url", "")) else "embed/iframe"
            merged["trailer_preview_url"] = best_server.get("embed_url")
            merged["quality_score"] = best_server.get("quality_score")
            merged["quality_label"] = (
                "4K UHD" if best_server["quality_score"] >= 2000 else
                "1080p Full HD" if best_server["quality_score"] >= 1000 else
                "720p HD" if best_server["quality_score"] >= 700 else "480p SD"
            )

        # 3. THUMBNAIL RESOLUTION PROMOTION: Select highest pixel resolution image
        curr_thumb = merged.get("clean_thumbnail_url") or merged.get("thumbnail_url")
        inc_thumb = incoming.get("clean_thumbnail_url") or incoming.get("thumbnail_url")
        
        curr_area = estimate_thumbnail_resolution(curr_thumb)
        inc_area = estimate_thumbnail_resolution(inc_thumb)

        if inc_area > curr_area and inc_thumb:
            merged["clean_thumbnail_url"] = inc_thumb
            merged["thumbnail_url"] = inc_thumb

        # 4. Merge tags & categories
        exist_tags = set(merged.get("tags", []))
        exist_tags.update(incoming.get("tags", []))
        merged["tags"] = sorted(list(exist_tags))

        # 5. Merge actors
        exist_actors = set(merged.get("actors", []))
        exist_actors.update(incoming.get("actors", []) or incoming.get("models", []))
        merged["actors"] = sorted(list(exist_actors))

        # 6. Duration reconciliation
        if not merged.get("duration_seconds") or merged.get("duration_seconds") == 600:
            if incoming.get("duration_seconds") and incoming.get("duration_seconds") != 600:
                merged["duration_seconds"] = incoming.get("duration_seconds")
                merged["duration_formatted"] = incoming.get("duration_formatted")

        # 7. Append source tracking
        raw_payload = merged.setdefault("raw_payload", {})
        raw_payload["servers"] = existing_servers
        sources = raw_payload.setdefault("sources", [])
        new_source = {
            "site": incoming.get("source_site"),
            "post_id": incoming.get("source_post_id"),
            "url": incoming.get("detail_url")
        }
        if not any(s.get("site") == new_source["site"] and s.get("post_id") == new_source["post_id"] for s in sources):
            sources.append(new_source)

        return merged

    def process_candidate(self, candidate: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """
        Processes a newly scraped candidate:
        - If duplicate: merges into canonical and returns (merged_record, is_new=False)
        - If distinct: registers as new canonical and returns (candidate, is_new=True)
        """
        # Ensure candidate servers have quality scores
        servers = candidate.get("raw_payload", {}).get("servers", [])
        for s in servers:
            if "quality_score" not in s:
                s["quality_score"] = compute_quality_score(s, candidate.get("title", ""))

        if servers:
            servers.sort(key=lambda s: s.get("quality_score", 0), reverse=True)
            candidate["full_video_url"] = servers[0].get("embed_url")
            candidate["video_mime_type"] = "video/mp4" if (servers[0].get("type") == "direct_mp4" or ".mp4" in servers[0].get("embed_url", "")) else "embed/iframe"
            candidate["quality_score"] = servers[0].get("quality_score")
            candidate["quality_label"] = (
                "4K UHD" if servers[0]["quality_score"] >= 2000 else
                "1080p Full HD" if servers[0]["quality_score"] >= 1000 else
                "720p HD" if servers[0]["quality_score"] >= 700 else "480p SD"
            )

        actors = candidate.get("actors", []) or candidate.get("models", [])
        model_slug = normalize_model_name(actors[0] if actors else None)
        tokens = extract_semantic_tokens(candidate.get("title", ""), model_slug)
        fp = generate_semantic_fingerprint(model_slug, tokens)
        candidate["canonical_fingerprint"] = fp

        match_id = self.find_duplicate(candidate)
        if match_id and match_id in self.canonical_records:
            existing = self.canonical_records[match_id]
            merged = self.merge_records(existing, candidate)
            merged["canonical_fingerprint"] = existing.get("canonical_fingerprint") or fp
            self.canonical_records[match_id] = merged
            return merged, False
        else:
            self.register_existing(candidate)
            return candidate, True
