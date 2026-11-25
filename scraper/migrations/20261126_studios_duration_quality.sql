-- Studios: extend the per-site quality-update RPCs to also carry duration.
-- Used by scraper/categories/studios/duration_backfill.py to backfill
-- duration_seconds / duration_formatted on existing rows (keyed by id).

CREATE OR REPLACE FUNCTION public.scraper_update_madouer_quality(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH upd AS (
        UPDATE scraper.madouer_videos sv
        SET quality_label = COALESCE(x.quality_label, sv.quality_label),
            quality_score = COALESCE(x.quality_score, sv.quality_score),
            resolution = COALESCE(x.resolution, sv.resolution),
            duration_seconds = COALESCE(x.duration_seconds, sv.duration_seconds),
            duration_formatted = COALESCE(x.duration_formatted, sv.duration_formatted),
            updated_at = now()
        FROM (
            SELECT (x->>'id') AS id, (x->>'quality_label') AS quality_label,
                   (x->>'quality_score')::int AS quality_score, (x->>'resolution') AS resolution,
                   (x->>'duration_seconds')::int AS duration_seconds,
                   (x->>'duration_formatted') AS duration_formatted
            FROM jsonb_array_elements(items) AS x
        ) x
        WHERE sv.id::text = x.id
            RETURNING 1
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

CREATE OR REPLACE FUNCTION public.scraper_update_dasj91_quality(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH upd AS (
        UPDATE scraper.dasj91_videos sv
        SET quality_label = COALESCE(x.quality_label, sv.quality_label),
            quality_score = COALESCE(x.quality_score, sv.quality_score),
            resolution = COALESCE(x.resolution, sv.resolution),
            duration_seconds = COALESCE(x.duration_seconds, sv.duration_seconds),
            duration_formatted = COALESCE(x.duration_formatted, sv.duration_formatted),
            updated_at = now()
        FROM (
            SELECT (x->>'id') AS id, (x->>'quality_label') AS quality_label,
                   (x->>'quality_score')::int AS quality_score, (x->>'resolution') AS resolution,
                   (x->>'duration_seconds')::int AS duration_seconds,
                   (x->>'duration_formatted') AS duration_formatted
            FROM jsonb_array_elements(items) AS x
        ) x
        WHERE sv.id::text = x.id
            RETURNING 1
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;
