-- Ledger repair + audit for scraper.known_post_ids.
--
-- Incident (run 46093407022, 2026-11-28): borntobefuck.com's origin answered
-- HTTP 200 with `Internal server error.` for hours. sweep_ids.py probed 5,328
-- ids, could not read a title from any of them, and recorded EVERY one as
-- dead. verify_dead.py then re-checked 975 of the stored rows and found 974
-- alive. Because scraper_get_known_post_ids() returns the ledger regardless of
-- status, those false "dead" rows would have blocked re-ingest of live videos
-- forever.
--
-- This migration adds the two functions needed to (a) see a poisoning episode
-- and (b) undo one.

-- ─── 1. Forget ids so they can be probed again ──────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_forget_known_post_ids(
    p_site  TEXT,
    p_ids   JSONB DEFAULT '[]'::JSONB,
    p_from  BIGINT DEFAULT NULL,
    p_to    BIGINT DEFAULT NULL,
    p_status TEXT DEFAULT NULL,
    p_since TIMESTAMPTZ DEFAULT NULL,
    p_until TIMESTAMPTZ DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_site   TEXT := lower(btrim(COALESCE(p_site, '')));
    v_status TEXT := lower(btrim(COALESCE(p_status, '')));
    v_ids    TEXT[];
    v_count  INT := 0;
BEGIN
    IF v_site = '' THEN
        RETURN jsonb_build_object('success', false, 'error', 'missing site');
    END IF;

    SELECT COALESCE(array_agg(DISTINCT btrim(x)), '{}'::TEXT[])
      INTO v_ids
      FROM jsonb_array_elements_text(COALESCE(p_ids, '[]'::JSONB)) AS x
     WHERE btrim(x) <> '';

    DELETE FROM scraper.known_post_ids
     WHERE site = v_site
       AND (p_since IS NULL OR last_seen_at >= p_since)
       AND (p_until IS NULL OR last_seen_at <= p_until)
       AND (cardinality(v_ids) > 0 AND post_id = ANY (v_ids))
        OR (site = v_site
             AND (p_since IS NULL OR last_seen_at >= p_since)
             AND (p_until IS NULL OR last_seen_at <= p_until)
             AND (
             cardinality(v_ids) = 0
             AND (p_from IS NULL OR post_id ~ '^[0-9]+$' AND post_id::BIGINT >= p_from)
             AND (p_to   IS NULL OR post_id ~ '^[0-9]+$' AND post_id::BIGINT <= p_to)
             AND (v_status = '' OR status = v_status)
           ));
    GET DIAGNOSTICS v_count = ROW_COUNT;

    RETURN jsonb_build_object('success', true, 'forgotten', v_count);
END;
$func$;

-- ─── 2. Audit the ledger per site/status ────────────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_known_post_ids_summary(p_site TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_site TEXT := lower(btrim(COALESCE(p_site, '')));
    v_out  JSONB;
BEGIN
    SELECT COALESCE(
               jsonb_object_agg(status, jsonb_build_object(
                   'count', n,
                   'first_seen', first_seen,
                   'last_seen', last_seen
               )),
               '{}'::JSONB
           )
      INTO v_out
      FROM (
          SELECT status,
                 count(*)                AS n,
                 min(first_seen_at)      AS first_seen,
                 max(last_seen_at)       AS last_seen
            FROM scraper.known_post_ids
           WHERE v_site = '' OR site = v_site
           GROUP BY status
      ) s;
    RETURN COALESCE(v_out, '{}'::JSONB);
END;
$func$;

GRANT EXECUTE ON FUNCTION public.scraper_forget_known_post_ids(TEXT, JSONB, BIGINT, BIGINT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ)
    TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.scraper_known_post_ids_summary(TEXT)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';

-- ─── 3. Per-hour ledger histogram (spot an outage window) ────────────────────
CREATE OR REPLACE FUNCTION public.scraper_known_post_ids_histogram(p_site TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_site TEXT := lower(btrim(COALESCE(p_site, '')));
    v_out  JSONB;
BEGIN
    SELECT COALESCE(jsonb_agg(row_to_json(t)), '[]'::JSONB)
      INTO v_out
      FROM (
          SELECT to_char(date_trunc('hour', last_seen_at), 'YYYY-MM-DD HH24:00') AS hour,
                 status,
                 count(*) AS n
            FROM scraper.known_post_ids
           WHERE (v_site = '' OR site = v_site)
             AND last_seen_at >= timezone('utc'::text, now()) - interval '96 hours'
           GROUP BY 1, 2
           ORDER BY 1 DESC, 2
      ) t;
    RETURN COALESCE(v_out, '[]'::JSONB);
END;
$func$;

GRANT EXECUTE ON FUNCTION public.scraper_known_post_ids_histogram(TEXT)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
