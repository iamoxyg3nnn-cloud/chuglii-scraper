-- Quarantine legacy HotScopes/SpyGasm rows whose only source was a now-parked
-- Gounlimited embed. They stay in the isolated SpyGasm table as remembered
-- dead IDs, but disappear from active feeds and detail lookups.

UPDATE scraper.hotscopesspygasm_videos
   SET status = 'dead',
       dead_reason = 'gounlimited_parked',
       last_checked_at = now(),
       updated_at = now()
 WHERE source_site = 'hotscopesspygasm'
   AND status = 'active'
   AND (
     video_mime_type = 'embed/iframe'
     OR full_video_url ILIKE 'https://hotscopes.io/embed/%'
   );
