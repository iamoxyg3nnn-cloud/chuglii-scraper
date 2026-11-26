-- OnlyFans: keep the global read table in step with the per-site tables.
--
-- The isolation work created per-site tables (scraper.<site>_videos) and the
-- ingest path dual-writes into both those and the original
-- scraper.onlyfans_videos. But the READ path the site actually uses --
-- public.scraper_get_onlyfans_videos -- still selects from the global table.
--
-- Consequence found in production: the dead-video gate soft-deleted three
-- "Video Removed" rows in scraper.borntobefuck_videos and the site kept showing
-- them, because only the per-site copy was updated. The same applied to the
-- measured-quality repair: every uncut-x row still displayed the old
-- hardcoded "1080p".
--
-- So every mutation must land on both tables until the read path is moved off
-- the global table. These RPCs are the only way the scraper mutates them.

CREATE OR REPLACE FUNCTION public.scraper_onlyfans_mark_verified(
    p_site text,
    p_alive_ids text[],
    p_dead_ids text[]
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
    alive_count integer := 0;
    dead_count integer := 0;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM scraper.onlyfans_sources WHERE site_key = p_site) THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';

    IF p_alive_ids IS NOT NULL AND array_length(p_alive_ids, 1) > 0 THEN
        EXECUTE format(
            'UPDATE scraper.%I SET last_verified_at = now() WHERE source_post_id = ANY($1)',
            table_key
        ) USING p_alive_ids;
        GET DIAGNOSTICS alive_count = ROW_COUNT;
    END IF;

    IF p_dead_ids IS NOT NULL AND array_length(p_dead_ids, 1) > 0 THEN
        -- Soft delete in BOTH tables: the per-site copy is the archive of
        -- record, the global one is what the public feed reads today.
        EXECUTE format(
            'UPDATE scraper.%I SET last_verified_at = now(), status = ''dead'' WHERE source_post_id = ANY($1)',
            table_key
        ) USING p_dead_ids;
        GET DIAGNOSTICS dead_count = ROW_COUNT;

        UPDATE scraper.onlyfans_videos
           SET status = 'dead', updated_at = now()
         WHERE source_site = p_site
           AND source_post_id = ANY(p_dead_ids);
    END IF;

    RETURN jsonb_build_object('success', true, 'alive', alive_count, 'dead', dead_count);
END;
$$;

-- Measured-quality updates (uncut-x has no resolution field, so the label is
-- derived from the file itself). Applies to both tables for the same reason.
CREATE OR REPLACE FUNCTION public.scraper_onlyfans_set_quality(
    p_site text,
    p_items jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
    site_count integer := 0;
    global_count integer := 0;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM scraper.onlyfans_sources WHERE site_key = p_site) THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    IF p_items IS NULL OR jsonb_typeof(p_items) <> 'array' THEN
        RAISE EXCEPTION 'p_items must be a JSON array of {post_id,label,score}';
    END IF;
    table_key := p_site || '_videos';

    CREATE TEMP TABLE _q ON COMMIT DROP AS
        SELECT (item->>'post_id') AS post_id,
               (item->>'label')   AS label,
               ((item->>'score')::int) AS score
        FROM jsonb_array_elements(p_items) AS item
        WHERE item->>'post_id' IS NOT NULL;

    EXECUTE format(
        'UPDATE scraper.%I AS v SET quality_label = q.label, quality_score = q.score '
        'FROM _q q WHERE v.source_post_id = q.post_id',
        table_key
    );
    GET DIAGNOSTICS site_count = ROW_COUNT;

    UPDATE scraper.onlyfans_videos AS v
       SET quality_label = q.label, quality_score = q.score, updated_at = now()
      FROM _q q
     WHERE v.source_site = p_site
       AND v.source_post_id = q.post_id;
    GET DIAGNOSTICS global_count = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'site_rows', site_count, 'global_rows', global_count);
END;
$$;

REVOKE ALL ON FUNCTION public.scraper_onlyfans_set_quality(text, jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_set_quality(text, jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';
