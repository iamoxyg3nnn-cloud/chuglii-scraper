-- ============================================================================
-- Fix scraper_onlyfans_set_duration: $1 inside EXECUTE, %% inside format()
--
-- Two separate faults in the first version, both surfacing as a 400 that made
-- every measured duration silently unwritable:
--
--   SQLSTATE 42703  column "p_items" does not exist
--       EXECUTE does not see plpgsql variable names; the dynamic statement must
--       refer to the USING parameter as $1.
--
--   SQLSTATE 22023  unrecognized format() type specifier " "
--       the derived duration uses the modulo operator, and a bare % inside a
--       format() string is a placeholder. It has to be written %%.
--
-- The function body is built with format($f$...$f$, table_key), and the derived
-- duration uses the modulo operator:
--
--     (seconds % 3600) / 60
--
-- Inside a format() string a bare % is a placeholder, so every call died with
--     unrecognized format() type specifier " "   (SQLSTATE 22023)
-- and no measured duration was ever written. Doubling the % fixes it.
-- ============================================================================

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
                   END
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
               END
      FROM jsonb_array_elements(p_items) AS i
     WHERE v.source_site = lower(btrim(p_site))
       AND v.source_post_id = i->>'post_id'
       AND (i->>'seconds')::int > 0;
    GET DIAGNOSTICS global_rows = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'site_rows', site_rows, 'global_rows', global_rows);
END;
$$;
