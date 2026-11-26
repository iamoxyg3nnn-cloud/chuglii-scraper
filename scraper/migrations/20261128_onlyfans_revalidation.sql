-- OnlyFans: dead-stream revalidation (Layer 2 of the dead-video gate).
--
-- Layer 1 (stream_check.verify_stream at ingest) stops dead videos from ever
-- being stored. Layer 2 re-probes rows that were alive when ingested, because
-- CDN objects disappear later: 3,080 of uncut-x's 4,445 videos pointed at
-- Cloudflare R2 objects that had been deleted, and every one of them was
-- ingestible before the gate existed.
--
-- Two RPCs keep the nightly job on the same access path as everything else
-- (the scraper schema is not exposed via PostgREST):
--   scraper_onlyfans_revalidation_batch  -> rows whose stream is due a re-check
--   scraper_onlyfans_mark_verified       -> stamp results / soft-delete dead ones

-- Every site table gets the bookkeeping column, including the original two.
DO $$
DECLARE
    site_key text;
BEGIN
    FOR site_key IN SELECT s.site_key FROM scraper.onlyfans_sources s LOOP
        EXECUTE format(
            'ALTER TABLE scraper.%I ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ',
            site_key || '_videos'
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.scraper_onlyfans_revalidation_batch(
    p_site text,
    p_limit integer,
    p_max_age_days integer
)
RETURNS SETOF jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM scraper.onlyfans_sources WHERE site_key = p_site) THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';
    RETURN QUERY EXECUTE format($q$
        SELECT to_jsonb(v)
        FROM scraper.%I v
        WHERE v.status = 'active'
          AND v.full_video_url IS NOT NULL AND v.full_video_url <> ''
          AND (
              v.last_verified_at IS NULL
              OR v.last_verified_at < now() - (($1)::text || ' days')::interval
          )
        ORDER BY v.last_verified_at ASC NULLS FIRST, v.scraped_at ASC
        LIMIT $2
    $q$, table_key)
    USING greatest(p_max_age_days, 1), greatest(p_limit, 1);
END;
$$;

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
        -- Soft delete only: the row stays for DMCA / re-enable, but the public
        -- read RPCs filter status = 'active' so it disappears from the feed.
        EXECUTE format(
            'UPDATE scraper.%I SET last_verified_at = now(), status = ''dead'' WHERE source_post_id = ANY($1)',
            table_key
        ) USING p_dead_ids;
        GET DIAGNOSTICS dead_count = ROW_COUNT;
    END IF;

    RETURN jsonb_build_object('success', true, 'alive', alive_count, 'dead', dead_count);
END;
$$;

REVOKE ALL ON FUNCTION public.scraper_onlyfans_revalidation_batch(text, integer, integer) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.scraper_onlyfans_mark_verified(text, text[], text[]) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_revalidation_batch(text, integer, integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.scraper_onlyfans_mark_verified(text, text[], text[]) TO service_role;

NOTIFY pgrst, 'reload schema';
