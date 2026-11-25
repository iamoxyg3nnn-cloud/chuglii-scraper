-- Partial quality update by id (probe backfill): resolution / quality_label /
-- quality_score + raw_payload merge. Service-role only.
CREATE OR REPLACE FUNCTION public.scraper_update_chinese_quality(items jsonb)
RETURNS int
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE updated_count int := 0;
BEGIN
    WITH v AS (
        SELECT x->>'id' AS id,
               x->>'resolution' AS resolution,
               x->>'quality_label' AS quality_label,
               (x->>'quality_score')::int AS quality_score,
               COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    )
    UPDATE scraper.chinese_videos cv SET
        resolution    = v.resolution,
        quality_label = v.quality_label,
        quality_score = COALESCE(v.quality_score, cv.quality_score),
        raw_payload   = cv.raw_payload || v.raw_payload,
        updated_at    = now()
    FROM v
    WHERE cv.id::text = v.id;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END $$;
GRANT EXECUTE ON FUNCTION public.scraper_update_chinese_quality(jsonb) TO service_role;
