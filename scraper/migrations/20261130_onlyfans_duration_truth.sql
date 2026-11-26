-- ============================================================================
-- Fix the 10:00 lie: derive duration_formatted from duration_seconds
--
-- The OnlyFans upsert functions stored
--     COALESCE(x.duration_formatted, '10:00')
-- and the tables declared `duration_formatted VARCHAR(32) DEFAULT '10:00'`.
-- Any record that arrived with a measured duration_seconds but no formatted
-- string (the id sweep does exactly that) was stamped "10:00", so the site
-- showed ten minutes for videos that are 2:24 or 38:13.
--
-- Two guards, because the root cause is a default and defaults come back:
--   1. a BEFORE INSERT/UPDATE trigger that derives the string whenever it is
--      missing, empty, or the '10:00' sentinel while the seconds disagree;
--   2. a repair of the rows already carrying the lie.
--
-- Sources that publish their own formatting (sheeshfans can send "111:24")
-- keep it: the trigger only touches missing/empty/sentinel values.
-- ============================================================================

CREATE OR REPLACE FUNCTION scraper.sync_duration_formatted()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    secs integer;
    derived text;
BEGIN
    secs := COALESCE(NEW.duration_seconds, 0);
    IF secs <= 0 THEN
        RETURN NEW;
    END IF;

    derived := CASE
        WHEN secs >= 3600 THEN
            (secs / 3600)::text || ':' ||
            lpad(((secs % 3600) / 60)::text, 2, '0') || ':' ||
            lpad((secs % 60)::text, 2, '0')
        ELSE
            (secs / 60)::text || ':' || lpad((secs % 60)::text, 2, '0')
    END;

    IF NEW.duration_formatted IS NULL
       OR btrim(NEW.duration_formatted) = ''
       OR (NEW.duration_formatted = '10:00' AND secs <> 600) THEN
        NEW.duration_formatted := derived;
    END IF;

    RETURN NEW;
END;
$$;

-- Attach to every OnlyFans archive table (per-site tables are created
-- dynamically, so enumerate them instead of hard-coding a list).
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
        EXECUTE format('DROP TRIGGER IF EXISTS trg_sync_duration_formatted ON scraper.%I', tbl);
        EXECUTE format(
            'CREATE TRIGGER trg_sync_duration_formatted
             BEFORE INSERT OR UPDATE ON scraper.%I
             FOR EACH ROW EXECUTE FUNCTION scraper.sync_duration_formatted()', tbl);
    END LOOP;
END;
$$;

-- Repair the rows already wrong (sentinel / empty, never a real 10:00).
DO $$
DECLARE
    tbl text;
    fixed integer;
    total integer := 0;
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
        EXECUTE format($f$
            UPDATE scraper.%I
               SET duration_formatted = CASE
                     WHEN duration_seconds >= 3600 THEN
                         (duration_seconds / 3600)::text || ':' ||
                         lpad(((duration_seconds %% 3600) / 60)::text, 2, '0') || ':' ||
                         lpad((duration_seconds %% 60)::text, 2, '0')
                     ELSE
                         (duration_seconds / 60)::text || ':' ||
                         lpad((duration_seconds %% 60)::text, 2, '0')
                   END
             WHERE duration_seconds > 0
               AND (duration_formatted IS NULL
                    OR btrim(duration_formatted) = ''
                    OR (duration_formatted = '10:00' AND duration_seconds <> 600))
        $f$, tbl);
        GET DIAGNOSTICS fixed = ROW_COUNT;
        total := total + fixed;
        RAISE NOTICE 'repaired %.% rows: %', 'scraper', tbl, fixed;
    END LOOP;
    RAISE NOTICE 'duration repair total: %', total;
END;
$$;

-- The sentinel default is what created the lie; stop it at the source.
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.table_name
          FROM information_schema.columns c
         WHERE c.table_schema = 'scraper'
           AND c.column_name  = 'duration_formatted'
           AND (c.table_name = 'onlyfans_videos' OR c.table_name LIKE '%\_videos')
           AND c.table_name NOT IN (
                 'studios_videos', 'voyeurhouse_videos', 'chinese_videos',
                 'japanese_videos', 'myanmar_videos', 'fangsung_videos',
                 'samusar_videos', 'reallifecamto_videos'
               )
    LOOP
        EXECUTE format('ALTER TABLE scraper.%I ALTER COLUMN duration_formatted DROP DEFAULT', tbl);
    END LOOP;
END;
$$;
