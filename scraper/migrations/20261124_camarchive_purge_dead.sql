-- Purge dead/unplayable rows already written during the initial validation run.
-- Going forward the pipeline skips dead videos entirely (never writes them).

DELETE FROM scraper.hotscopesvoyeurhouse_videos
 WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '';

DELETE FROM scraper.reallifecamto_videos
 WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '';

DELETE FROM scraper.hotscopesspygasm_videos
 WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '';

DELETE FROM scraper.hotscopesreallifecam_videos
 WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '';
