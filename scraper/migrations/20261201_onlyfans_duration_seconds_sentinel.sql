-- ============================================================================
-- duration_seconds also defaulted to 600 (10:00)
--
-- The previous migration fixed the DISPLAY string, but the seconds column had
-- the same lie baked in:
--
--     duration_seconds INTEGER DEFAULT 600
--     COALESCE(x.duration_seconds, 600)
--
-- influencersgonewild publishes no duration at all, so all 4,284 of its rows
-- were stored as exactly 600 s -- and because seconds and the formatted string
-- agreed, no consistency check could see it. 1,007 sheeshfans rows (rows whose
-- page had no og:video:duration and no direct mp4) were wrong the same way.
--
-- Unknown must stay unknown: the sources now measure the media header, and the
-- database stops inventing a value when they cannot.
-- ============================================================================

-- 1. Stop the insert functions from substituting 600 / '10:00'.
--    The definitions are patched in place so the rest of the body is untouched.
DO $$
DECLARE
    r record;
    d text;
    patched integer := 0;
BEGIN
    FOR r IN
        SELECT p.oid, p.proname
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public'
           AND p.proname IN ('scraper_insert_onlyfans_source_videos',
                             'scraper_insert_onlyfans_videos')
    LOOP
        d := pg_get_functiondef(r.oid);
        IF position('COALESCE(x.duration_seconds, 600)' in d) > 0 THEN
            d := replace(d, 'COALESCE(x.duration_seconds, 600)', 'x.duration_seconds');
            d := replace(d, 'COALESCE(x.duration_formatted, ''10:00'')', 'x.duration_formatted');
            EXECUTE d;
            patched := patched + 1;
            RAISE NOTICE 'patched %', r.proname;
        ELSE
            RAISE NOTICE 'no 600 default found in %', r.proname;
        END IF;
    END LOOP;
    RAISE NOTICE 'insert functions patched: %', patched;
END;
$$;

-- 2. Drop the column defaults that recreate the lie for any INSERT that omits
--    the column.
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
        EXECUTE format('ALTER TABLE scraper.%I ALTER COLUMN duration_seconds DROP DEFAULT', tbl);
    END LOOP;
END;
$$;

-- 3. An RPC to write a measured duration into both the per-site archive and the
--    global read table (the site reads the latter).
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
                         lpad((((i->>'seconds')::int % 3600) / 60)::text, 2, '0') || ':' ||
                         lpad(((i->>'seconds')::int % 60)::text, 2, '0')
                     ELSE
                         ((i->>'seconds')::int / 60)::text || ':' ||
                         lpad(((i->>'seconds')::int % 60)::text, 2, '0')
                   END
          FROM jsonb_array_elements(p_items) AS i
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
               END
      FROM jsonb_array_elements(p_items) AS i
     WHERE v.source_site = lower(btrim(p_site))
       AND v.source_post_id = i->>'post_id'
       AND (i->>'seconds')::int > 0;
    GET DIAGNOSTICS global_rows = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'site_rows', site_rows, 'global_rows', global_rows);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_set_duration(text, jsonb) TO anon, authenticated, service_role;
