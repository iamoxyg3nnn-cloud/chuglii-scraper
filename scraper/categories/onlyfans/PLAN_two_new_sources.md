# Plan: add borntobefuck.com + uncut-x.com to the OnlyFans category

Status: **analysis complete (v2), implementation not started**
Date: 2026-11-26
Supersedes v1 — v1 badly underestimated borntobefuck's catalogue and missed that
most of its archive has no stream.

---

## 0. What changed after deeper checking (read this first)

| v1 claim | v2 reality |
|---|---|
| btb ≈ 600 videos | id space runs **2 → 46,681**; ~**23,000 estimated live** videos |
| `/videos?page=1..50` is the catalogue | that is a **1-month rolling window** (597). `sort=oldest` is a *separate* window (961, ids ~2–1600) |
| all btb streams are live | **ids below ~16,400 have no stream at all** — the player fragment returns 200 with no m3u8 |
| uncut-x ≈ 4,445 videos | **1,365 usable** (3,080 R2 objects are 404; 42,998 photos 100 % dead) |
| metadata comes from listings | btb watch page has everything; the *fragment* has only title/thumb/m3u8 — this decides the crawl strategy |

Nothing below is inferred; every number was measured on 2026-11-26.

---

## 1. `borntobefuck.com`

### 1.1 Surface map (from `window.routes` in the page)

```js
{ home:"/", search:"/search", search_results:"/search/results", videos:"/videos",
  watch_prefix:"/watch/", live_prefix:"/live/@", channels:"/channels",
  channel_handle:"/@", categories:"/categories", tags:"/tags" }
```

| Surface | Behaviour | Usable for discovery? |
|---|---|---|
| `/videos?page=1..50` | newest window, 597 ids, ids 45,100–46,681, 2026-10-27→11-25 | yes (incremental) |
| `/videos?page=1..80&sort=oldest` | oldest window, 961 ids, ids 2–1,600, 2023-06→2026-11 | barely (old = streamless) |
| `/categories` | **94 categories**; each paginates to **~80 pages ≈ 960 videos** | **primary discovery** |
| `/tags` | **419 tags**, tag pages paginate | secondary |
| `/channels?page=N` | paginated, ~24–26 handles/page | model roster |
| `/@{handle}` | 5 ids, **no `?page=`** (p2 empty) | weak |
| `/search?q=` | results are not server-rendered (0 ids) | no |
| `?sort=views\|popular\|top\|…` | **ignored** (identical to default) | no |
| sitemap | absent (`/sitemap.xml` 404) | no |

Two windows barely overlap — **1 shared id** between newest (597) and oldest (961).
The middle of the id space is reachable *only* through category/tag pages and
direct id access.

### 1.2 The stream cutoff (the dead-video rule for this site)

`GET /videos/{id}/player?lang=en` + `Accept: text/html` +
`X-Requested-With: XMLHttpRequest` + `Referer: /watch/{id}` returns a fragment.
It contains `index.m3u8` **only for ids ≥ ~16,400**:

```
id      2, 54, 300, 800, 1600, 5000, 12248  -> fragment 200, NO m3u8
id  16459 … 26821 … 35000 … 46681           -> m3u8 present, master 200
binary search: cutoff between 16346 and 16459
```

So the 2023–2024 archive is **structurally unplayable** and must never be ingested.
This one rule removes ~16,400 ids from the crawl for free.

Density estimate above the cutoff — 22 ids sampled evenly across 16,500–46,600:

```
live (m3u8 + master 200)      17   (77 %)
watch 200 but no stream        4
error                          1
```
⇒ **~23,000 live videos** in 16,459–46,681.

### 1.3 Metadata

| Field | Listing card `div.video.ranked` | Watch page | Player fragment |
|---|---|---|---|
| title | `img.thumbnail[title]` (−" Porn Video BornToBeFuck") | `og:title` | `<title>` |
| duration | `.time[data-duration]` (sec) | `.video-duration` (`04:50`) | — |
| thumbnail | `img.thumbnail[src]` | poster / `og:image` | `poster` |
| preview | `img.thumbnail[data-preview-url]` (webp) | — | — |
| model | `a.profil[href]` → `/@{handle}` | `/@{handle}` | — |
| country | `a.card-country[href]` → `/categories/{c}` | same | — |
| tags | — | `/tags/{tag}` | — |
| views | `data-views` | `data-views` | — |
| published | `data-published` (unix) | `data-published` | — |
| stream | — | — | `index.m3u8` |

Watch-page values are polluted by the "related videos" block — parse must scope to
the main video container, not the whole document.

### 1.4 Stream format
`https://content.zavastream.com/{tenant}/{uuid}/index.m3u8` — unsigned, stable,
720p (`1350 kb/s`) + 480p (`825 kb/s`). Requires only `Referer: /watch/{id}`.
Segments are relative (`720p/seg_00000.ts`), so a proxy must rewrite variant +
segment URLs.

---

## 2. `uncut-x.com`

SPA over a public Supabase/PostgREST API (`gmmxzxdkmqhhmugapsok.supabase.co`,
anon JWT embedded in `/assets/index-*.js`). No server-rendered HTML.

| Table | Rows (anon) | Verdict |
|---|---|---|
| `videos` | 4,445 | **1,365 usable** |
| `models` | 427 | usable (profile images on Supabase storage) |
| `categories` | 20 | usable |
| `photos` | 42,998 | **100 % dead R2 → skip the table entirely** |

Alive/dead split of `videos.video_url`:

| Host | Rows | Probe |
|---|---|---|
| `uncut.{hash}.r2.cloudflarestorage.com` | 3,080 | **26/26 → 404** (signed URLs mint fine, objects gone) |
| `media.uncut-x.com/cards/.../video.mp4` | 1,365 | **12/12 → 206 `video/mp4`** |

Quality of the 1,365 alive rows:

```
proper (non-uuid) titles : 1365 / 1365
views > 0                : 1365 / 1365
thumbnails alive         : 14/14 (206)  — Supabase storage + media.uncut-x.com
duration present         :  198 / 1365   <-- gap
orientation present      :    2 / 1365   <-- gap
created_at               : 2026-03-09 -> 2026-11-24
```

⇒ duration and orientation must be **derived at ingest with ffprobe** (the project
already has an ffprobe step for HLS quality).

Dead-row fingerprint (free pre-filter, no network):
`video_url LIKE 'https://uncut.%.r2.cloudflarestorage.com/%'` **and** `title` is a
UUID ⇒ 592 rows are obviously dead by shape alone.

The `r2-signed-url` edge function still answers but only confirms the objects are
gone — use it as a one-off audit tool, never in the hot path.

---

## 3. Crawl strategy

### 3.1 borntobefuck — two phases (listings first, id sweep for the gaps)

**Phase A — advertised catalogue (safe, rich metadata)**
1. `/categories` → 94 slugs.
2. For each: `/categories/{slug}?page=1..N` until a page returns 0 ids (cap 80).
3. `/channels?page=1..N` → model roster (for `actors` normalisation).
4. Merge with `/videos?page=1..50` (newest, needed for freshness).
5. Dedupe by video id → estimate **10–15k** unique (measured: 6 categories alone
   gave 4,304 unique with 25 % overlap).
Cost: ~7,700 requests. At the shared 1.3 req/s/host throttle ≈ **1h40m** — fits in
one session.

**Phase B — id sweep for the uncovered middle**
- Range `16,459 … 46,681` (30,222 ids), stored as a shard cursor in the checkpoint.
- 1 request/id (`/videos/{id}/player`) → live? then 1 watch-page request for metadata.
- **Must be sharded**: at ~1.3 req/s a full sweep is ~6.4h, longer than a 5h session.
  Suggested 10k ids/session across 3 runs.
- Skip ids already known from Phase A (no fragment request).

**Never** request ids < 16,400 — they cannot have a stream.

### 3.2 uncut-x — single pass, one request per page
```
GET /rest/v1/videos?select=<cols>&video_url=like.https://media.uncut-x.com/*
    &order=created_at.desc&limit=1000&offset=N
```
- Ingest only the 1,365 alive-host rows (server-side filter, ~2 pages of 1000).
- Import `models` (427) for actor names/slugs and `categories` (20).
- Skip `photos` entirely.
- ffprobe each mp4 once for duration/orientation (`Range` + ffprobe on the URL).

---

## 4. Dead-video gate (three layers)

### Layer 1 — ingest gate (`stream_check.py`)
```python
verify_stream(url, *, kind, referer) -> (alive: bool, status: int, detail: str)
```
- **HLS**: master 200 + `#EXTM3U` + ≥1 `#EXT-X-STREAM-INF` → first variant 200 +
  `#EXTINF` → `seg_00000.ts` 200/206 + `video/*`.
- **MP4**: `Range: bytes=0-1023` → 200/206 + `video/*`.
- **btb shortcut**: no m3u8 in the player fragment ⇒ dead, zero extra requests.
- **uncut shortcut**: host is `r2.cloudflarestorage.com` ⇒ dead, zero requests.
- A `400`/`403` is **never** "alive" — that is exactly how the R2 bucket lied.

### Layer 2 — scheduled revalidation (`verify_dead.py`)
- `scraper.stream_health(url_hash pk, source_site, host, http_status, is_alive, checked_at)`
- Nightly: re-probe `status='active' AND last_verified_at < now()-'7 days'`, 200/night.
- Failure ⇒ `status='dead'` (soft delete, never `DELETE`, keeps DMCA addressable).

### Layer 3 — playback guard
- `media-proxy` 404 ⇒ card renders "unavailable"; write a `stream_health` row so
  Layer 2 promotes it to dead.

---

## 5. Storage (isolated, same pattern as the existing two sites)

Migration `20261127_onlyfans_borntobefuck_uncutx.sql`:
1. Add `'borntobefuck','uncutx'` to every `p_site NOT IN (...)` allow-list (4 places).
2. `scraper.borntobefuck_videos`, `scraper.uncutx_videos` — same column shape as the
   existing two, plus `last_verified_at TIMESTAMPTZ`.
3. Public wrappers `scraper_insert_onlyfans_borntobefuck_videos(items)` /
   `scraper_insert_onlyfans_uncutx_videos(items)` pinning `p_site`.
4. `scraper.stream_health`.
5. Reuse the existing RLS pattern (`service_role` ALL, anon SELECT `status='active'`).

Python: `ONLYFANS_SITES`, `DEFAULT_CHECKPOINTS`, `ONLYFANS_SOURCE_TABLE_RPC`,
`sources/__init__.py`.

**Crawl cursor for Phase B** rides on the existing checkpoint mechanism
(`onlyfans_multi` → `borntobefuck.sweep_cursor`), so an interrupted sweep resumes.

---

## 6. Frontend

- `hooks/useOnlyfansVideos.ts`: add `borntobefuck`, `uncutx` to `ONLYFANS_SITES`
  + labels ("BornToBeFuck", "Uncut-X"). Tabs are generated from this constant.
- `api-server/routes/media-proxy.ts`: `REFERER_MAP['content.zavastream.com'] =
  'https://borntobefuck.com/'`; allowlist `media.uncut-x.com`.
- `api-server/routes/player.ts`: `borntobefuck` resolver — fetch the player fragment,
  extract master m3u8, rewrite variant + segment URLs through the proxy.
- uncut-x plays direct (public mp4, no referer) — simplest path in the whole system.

---

## 7. Phases and effort

| Phase | Work | Effort | Yield |
|---|---|---|---|
| P0 | migration, `db.py`, `pipeline.py`, empty source modules | 0.5 d | wiring only |
| P1 | uncut-x source (PostgREST + ffprobe + probe) | 0.5 d | **1,365** |
| P2 | btb source, listing/player-fragment extraction, `verify_stream` | 1 d | listing scrape works |
| P3 | btb Phase A backfill (94 categories) | 0.5 d + 1 run | ~10–15k |
| P4 | frontend constants + proxy referer + player resolver | 0.5 d | playable |
| P5 | `verify_dead.py` + nightly workflow step | 0.5 d | self-healing |
| P6 | btb Phase B id sweep (sharded, 3 runs) | 0.5 d + 3 runs | remainder → ~23k |

≈ **4 dev-days** plus background runs.

---

## 8. Risks

1. **Scale.** btb is a 23k-video site, not a 600-video one. Phase B is the only thing
   that makes "scrape everything" true, and it must be sharded across sessions.
2. **uncut-x anon key rotation** — env override `UNCUTX_ANON_KEY` + runtime extraction
   from `/assets/index-*.js`, failing loudly if the shape changes.
3. **btb CF on `/videos/*/player`** — works with `Accept` + `X-Requested-With`;
   otherwise `net.py` escalates to the browser automatically (slower, still correct).
4. **Category cap of ~960** means Phase A **skews recent**. Do not present Phase A as
   "the full archive".
5. **Content policy** — both are OnlyFans-leak aggregators. Keep the existing NCII
   blocklist in front of them; `status='dead'` soft-delete keeps takedowns addressable.
6. **207k media requests** if we probe every segment later — only ever probe
   master + variant + first segment, never the whole playlist.

---

## 9. Verification checklist

- [ ] `scrape_listing(1)` > 0 cards for both; btb `detected_max` reflects category depth
- [ ] btb player-fragment extraction returns master m3u8 for id 46681 and **None** for id 12248
- [ ] uncut-x ingest rejects every `r2.cloudflarestorage.com` row without a request
- [ ] deliberately feed one dead R2 row + one streamless btb id ⇒ both rejected at Layer 1
- [ ] ffprobe fills duration for the uncut-x rows that lack it
- [ ] interruption during Phase B resumes from `sweep_cursor` (no re-walk)
- [ ] `pytest tests/` still green (48 tests)
