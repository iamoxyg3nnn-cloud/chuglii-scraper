# Chuglii Multi-Site Auto-Scraper Fleet

24/7 Automated Cloud Scraper Fleet for **Chuglii** covering:
1. **Viral MMS**: Ingests fresh Indian / Desi viral leaks into scraper.videos.
2. **SupJav Japanese JAV**: Ingests fresh Japanese videos into scraper.japanese_videos with strictly verified working mirrors (VOE HD, VidSpeed, LuluStream).

## Architecture & Features
- **Anti-Queue Concurrency (cancel-in-progress: true)**: Guarantees runs never backlog or queue up.
- **24/7 Self-Chaining**: Sessions run for 5 hours in GitHub Actions and automatically autotrigger their successor before concluding.
- **Adaptive Human Politeness**: Uses Chrome 124 TLS JA3/JA4 fingerprinting (curl_cffi) with randomized jitter and exponential backoff.
- **Sub-Millisecond Deduplication**: Skips already-stored videos in <1ms without redundant network requests.
