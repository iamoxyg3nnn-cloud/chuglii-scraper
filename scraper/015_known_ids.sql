DROP FUNCTION IF EXISTS public.scraper_get_known_post_ids(text, int);
DROP FUNCTION IF EXISTS public.scraper_get_known_post_ids(text);

CREATE OR REPLACE FUNCTION public.scraper_get_known_post_ids(p_site text, p_limit int DEFAULT 100000)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    v_result jsonb;
BEGIN
    IF p_site IN ('viral-mms', 'mmsvibex') THEN
        SELECT COALESCE(jsonb_agg(source_post_id), '[]'::jsonb)
        INTO v_result
        FROM (
            SELECT source_post_id
            FROM scraper.videos
            WHERE source_post_id IS NOT NULL
            ORDER BY scraped_at DESC
            LIMIT p_limit
        ) sub;
    ELSIF p_site IN ('japanese', 'supjav', 'jav') THEN
        SELECT COALESCE(jsonb_agg(source_post_id), '[]'::jsonb)
        INTO v_result
        FROM (
            SELECT source_post_id
            FROM scraper.japanese_videos
            WHERE source_post_id IS NOT NULL
            ORDER BY scraped_at DESC
            LIMIT p_limit
        ) sub;
    ELSIF p_site IN ('myanmar', 'buumal') THEN
        SELECT COALESCE(jsonb_agg(source_post_id), '[]'::jsonb)
        INTO v_result
        FROM (
            SELECT source_post_id
            FROM scraper.myanmar_videos
            WHERE source_post_id IS NOT NULL
            ORDER BY scraped_at DESC
            LIMIT p_limit
        ) sub;
    ELSE
        v_result := '[]'::jsonb;
    END IF;
    
    RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_known_post_ids(text, int) TO anon, authenticated, service_role;
NOTIFY pgrst, 'reload schema';
