-- Service-role queue for the sprite-storyboard backfill (PostgREST only
-- exposes `public`, so table access goes through a SECURITY DEFINER RPC).
CREATE OR REPLACE FUNCTION public.scraper_list_missing_storyboards(
    p_table TEXT, p_limit INT DEFAULT 4
) RETURNS TABLE (video_id TEXT, video_url TEXT)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = scraper, public AS $$
BEGIN
    IF p_table NOT IN ('videos','japanese_videos','myanmar_videos',
                       'onlyfans_videos','voyeurhouse_videos',
                       'hotscopesvoyeurhouse_videos','reallifecamto_videos',
                       'hotscopesspygasm_videos','hotscopesreallifecam_videos',
                       'chinese_videos') THEN
        RAISE EXCEPTION 'table not allowed: %', p_table;
    END IF;
    RETURN QUERY EXECUTE format(
        'SELECT id::text, full_video_url::text FROM scraper.%I
         WHERE preview_thumbs IS NULL AND full_video_url IS NOT NULL
         ORDER BY scraped_at DESC LIMIT $1', p_table)
    USING p_limit;
END $$;
GRANT EXECUTE ON FUNCTION public.scraper_list_missing_storyboards(TEXT, INT) TO service_role;
