-- ============================================================================
-- Duration self-healing: the scraper repairs its own unknown durations
--
-- Two sources cannot know a duration from their pages (influencersgonewild
-- publishes none at all; sheeshfans sometimes has og:video:duration), so their
-- rows were stored with the table default 600 s. Fixing the ingest is not
-- enough: every row already in the archive keeps the lie, and any future gap
-- would need another manual repair.
--
-- This adds the state the scraper needs to heal itself on every run:
--
--   duration_checked_at  -- "we measured, or tried and could not"
--   scraper_onlyfans_duration_backlog()    -- what still needs measuring
--   scraper_onlyfans_mark_duration_checked() -- give up quietly on dead media
--
-- so the nightly job drains a bounded batch instead of a human running a
-- one-off script.
-- ============================================================================

-- 1. Track when a row's duration was established (NULL = never established).
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.table_name
          FROM information_schema.columns c
         WHERE c.table_schema = 'scraper'
           AND c.column_name  = 'duration_seconds'
           AND (c.table_name = 'onlyfans_videos' OR c.table_name LIKE '%\_videos')
           AND c.table_name NOT IN (
                 'studios_videos', 'voyeurhouse_videos', 'chinese_videos',
                 'japanese_videos', 'myanmar_videos', 'fangsung_videos',
                 'samusar_videos', 'reallifecamto_videos'
               )
    LOOP
        EXECUTE format(
            'ALTER TABLE scraper.%I ADD COLUMN IF NOT EXISTS duration_checked_at timestamptz', tbl);
    END LOOP;
END;
$$;

-- 2. A row that arrives WITH a duration is already established; stamp it so the
--    backlog only contains unknowns.
CREATE OR REPLACE FUNCTION scraper.stamp_duration_checked()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.duration_seconds IS NOT NULL AND NEW.duration_seconds > 0 THEN
        NEW.duration_checked_at := COALESCE(NEW.duration_checked_at, now());
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.table_name
          FROM information_schema.columns c
         WHERE c.table_schema = 'scraper'
           AND c.column_name  = 'duration_checked_at'
           AND (c.table_name = 'onlyfans_videos' OR c.table_name LIKE '%\_videos')
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_stamp_duration_checked ON scraper.%I', tbl);
        EXECUTE format(
            'CREATE TRIGGER trg_stamp_duration_checked
             BEFORE INSERT OR UPDATE ON scraper.%I
             FOR EACH ROW EXECUTE FUNCTION scraper.stamp_duration_checked()', tbl);
    END LOOP;
END;
$$;

-- 3. Existing rows: the sources that really publish durations are trusted as-is.
--    The two that cannot are only trusted when the value is not the 600 s
--    sentinel, so the backlog picks those lies up.
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.table_name
          FROM information_schema.columns c
         WHERE c.table_schema = 'scraper'
           AND c.column_name  = 'duration_checked_at'
           AND (c.table_name = 'onlyfans_videos' OR c.table_name LIKE '%\_videos')
    LOOP
        EXECUTE format($f$
            UPDATE scraper.%I
               SET duration_checked_at = now()
             WHERE duration_checked_at IS NULL
               AND duration_seconds IS NOT NULL
               AND duration_seconds > 0
               AND ( source_site NOT IN ('influencersgonewild', 'sheeshfans')
                     OR duration_seconds <> 600 )
        $f$, tbl);
    END LOOP;
END;
$$;

-- 4. Writing a measured duration also marks the row checked.
CREATE OR REPLACE FUNCTION public.scraper_onlyfans_set_duration(
    p_site  text,
    p_items jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'scraper', 'public'
AS $$
DECLARE
    table_key text := lower(btrim(p_site)) || '_videos';
    site_rows integer := 0;
    global_rows integer := 0;
BEGIN
    EXECUTE format($f$
        UPDATE scraper.%I v
           SET duration_seconds   = (i->>'seconds')::int,
               duration_formatted = CASE
                     WHEN (i->>'seconds')::int >= 3600 THEN
                         ((i->>'seconds')::int / 3600)::text || ':' ||
                         lpad((((i->>'seconds')::int %% 3600) / 60)::text, 2, '0') || ':' ||
                         lpad(((i->>'seconds')::int %% 60)::text, 2, '0')
                     ELSE
                         ((i->>'seconds')::int / 60)::text || ':' ||
                         lpad(((i->>'seconds')::int %% 60)::text, 2, '0')
                   END,
               duration_checked_at = now()
          FROM jsonb_array_elements($1) AS i
         WHERE v.source_post_id = i->>'post_id'
           AND (i->>'seconds')::int > 0
    $f$, table_key) USING p_items;
    GET DIAGNOSTICS site_rows = ROW_COUNT;

    UPDATE scraper.onlyfans_videos v
       SET duration_seconds   = (i->>'seconds')::int,
           duration_formatted = CASE
                 WHEN (i->>'seconds')::int >= 3600 THEN
                     ((i->>'seconds')::int / 3600)::text || ':' ||
                     lpad((((i->>'seconds')::int % 3600) / 60)::text, 2, '0') || ':' ||
                     lpad(((i->>'seconds')::int % 60)::text, 2, '0')
                 ELSE
                     ((i->>'seconds')::int / 60)::text || ':' ||
                     lpad(((i->>'seconds')::int % 60)::text, 2, '0')
               END,
           duration_checked_at = now()
      FROM jsonb_array_elements(p_items) AS i
     WHERE v.source_site = lower(btrim(p_site))
       AND v.source_post_id = i->>'post_id'
       AND (i->>'seconds')::int > 0;
    GET DIAGNOSTICS global_rows = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'site_rows', site_rows, 'global_rows', global_rows);
END;
$$;

-- 5. Rows that still need a measurement, oldest first, for one site.
CREATE OR REPLACE FUNCTION public.scraper_onlyfans_duration_backlog(
    p_site  text,
    p_limit integer DEFAULT 500
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'scraper', 'public'
AS $$
DECLARE
    rows jsonb;
BEGIN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'post_id', t.source_post_id,
               'url',     t.full_video_url,
               'seconds', t.duration_seconds
           )), '[]'::jsonb)
      INTO rows
      FROM (
        SELECT v.source_post_id, v.full_video_url, v.duration_seconds
          FROM scraper.onlyfans_videos v
         WHERE lower(btrim(v.source_site)) = lower(btrim(p_site))
           AND v.duration_checked_at IS NULL
           AND COALESCE(v.status, 'active') = 'active'
           AND COALESCE(v.full_video_url, '') <> ''
         ORDER BY v.published_at DESC NULLS LAST
         LIMIT greatest(1, least(COALESCE(p_limit, 500), 2000))
      ) t;

    RETURN jsonb_build_object('success', true, 'items', rows);
END;
$$;

-- 6. Give up quietly on media that cannot be measured (dead object, exotic
--    container) so the backlog cannot churn on the same rows forever.
CREATE OR REPLACE FUNCTION public.scraper_onlyfans_mark_duration_checked(
    p_site text,
    p_ids  text[]
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'scraper', 'public'
AS $$
DECLARE
    table_key text := lower(btrim(p_site)) || '_videos';
    site_rows integer := 0;
    global_rows integer := 0;
BEGIN
    EXECUTE format(
        'UPDATE scraper.%I SET duration_checked_at = now()
          WHERE source_post_id = ANY($1) AND duration_checked_at IS NULL',
        table_key) USING p_ids;
    GET DIAGNOSTICS site_rows = ROW_COUNT;

    UPDATE scraper.onlyfans_videos
       SET duration_checked_at = now()
     WHERE lower(btrim(source_site)) = lower(btrim(p_site))
       AND source_post_id = ANY(p_ids)
       AND duration_checked_at IS NULL;
    GET DIAGNOSTICS global_rows = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'site_rows', site_rows, 'global_rows', global_rows);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_duration_backlog(text, integer) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_mark_duration_checked(text, text[]) TO anon, authenticated, service_role;
