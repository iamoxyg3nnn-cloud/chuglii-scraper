-- Diagnostic RPC for verifying Cam Archive isolation and dead-row purging.

CREATE OR REPLACE FUNCTION public.scraper_camarchive_audit()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    v_result jsonb := '[]'::jsonb;
    v_row record;
BEGIN
    FOR v_row IN
        SELECT 'hotscopesvoyeurhouse' AS site, count(*) AS total,
               count(*) FILTER (WHERE status = 'active') AS active,
               count(*) FILTER (WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '') AS dead
          FROM scraper.hotscopesvoyeurhouse_videos
        UNION ALL
        SELECT 'reallifecamto', count(*),
               count(*) FILTER (WHERE status = 'active'),
               count(*) FILTER (WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '')
          FROM scraper.reallifecamto_videos
        UNION ALL
        SELECT 'hotscopesspygasm', count(*),
               count(*) FILTER (WHERE status = 'active'),
               count(*) FILTER (WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '')
          FROM scraper.hotscopesspygasm_videos
        UNION ALL
        SELECT 'hotscopesreallifecam', count(*),
               count(*) FILTER (WHERE status = 'active'),
               count(*) FILTER (WHERE status <> 'active' OR full_video_url IS NULL OR full_video_url = '')
          FROM scraper.hotscopesreallifecam_videos
    LOOP
        v_result := v_result || jsonb_build_object(
            'site', v_row.site,
            'total', v_row.total,
            'active', v_row.active,
            'dead', v_row.dead
        );
    END LOOP;
    RETURN v_result;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_camarchive_audit() TO service_role, anon, authenticated;
