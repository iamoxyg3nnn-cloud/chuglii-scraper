-- OnlyFans source isolation. Keep this migration unapplied until the scraper is ready.
DROP FUNCTION IF EXISTS public.scraper_upsert_onlyfans_videos(jsonb);
DROP FUNCTION IF EXISTS public.scraper_get_onlyfans_videos(text, text, text, text, text, integer, integer);
DROP FUNCTION IF EXISTS public.scraper_count_onlyfans_videos(text, text, text, text);
DROP FUNCTION IF EXISTS public.scraper_get_onlyfans_video_by_id(text);
DROP TABLE IF EXISTS scraper.onlyfans_videos CASCADE;

DO $$
DECLARE
    site_key text;
    table_key text;
BEGIN
    FOREACH site_key IN ARRAY ARRAY['influencersgonewild', 'sheeshfans'] LOOP
        table_key := site_key || '_videos';
        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS scraper.%I (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_site TEXT NOT NULL DEFAULT %L CHECK (source_site = %L),
                source_post_id TEXT NOT NULL,
                title TEXT NOT NULL,
                slug TEXT,
                detail_url TEXT NOT NULL,
                full_video_url TEXT,
                video_mime_type TEXT DEFAULT 'embed/iframe',
                trailer_preview_url TEXT,
                thumbnail_url TEXT,
                clean_thumbnail_url TEXT,
                quality_label VARCHAR(32) DEFAULT '1080p Full HD',
                quality_score INTEGER DEFAULT 1080,
                duration_seconds INTEGER DEFAULT 600,
                duration_formatted VARCHAR(32) DEFAULT '10:00',
                views_count BIGINT DEFAULT 0,
                views_raw VARCHAR(64) DEFAULT '0',
                rating_score NUMERIC(5,2) DEFAULT 95.0,
                rating_raw VARCHAR(32) DEFAULT '95%%',
                categories TEXT[] DEFAULT '{onlyfans}',
                tags TEXT[] DEFAULT '{}',
                actors TEXT[] DEFAULT '{}',
                status TEXT DEFAULT 'active',
                published_at TIMESTAMPTZ,
                scraped_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now(),
                raw_payload JSONB DEFAULT '{}'::jsonb
            )
        $ddl$, table_key, site_key, site_key);

        EXECUTE format('ALTER TABLE scraper.%I ENABLE ROW LEVEL SECURITY', table_key);
        EXECUTE format('DROP POLICY IF EXISTS %I ON scraper.%I', 'service_role_all_' || table_key, table_key);
        EXECUTE format('CREATE POLICY %I ON scraper.%I FOR ALL TO service_role USING (true) WITH CHECK (true)', 'service_role_all_' || table_key, table_key);
        EXECUTE format('DROP POLICY IF EXISTS %I ON scraper.%I', 'anon_select_' || table_key, table_key);
        EXECUTE format('CREATE POLICY %I ON scraper.%I FOR SELECT TO anon, authenticated USING (status = ''active'')', 'anon_select_' || table_key, table_key);

        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I (quality_score DESC)', 'idx_' || table_key || '_quality', table_key);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I (published_at DESC NULLS LAST)', 'idx_' || table_key || '_published', table_key);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I (views_count DESC)', 'idx_' || table_key || '_views', table_key);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I USING gin(tags)', 'idx_' || table_key || '_tags', table_key);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I USING gin(categories)', 'idx_' || table_key || '_categories', table_key);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON scraper.%I USING gin(actors)', 'idx_' || table_key || '_actors', table_key);
    END LOOP;
END;
$$;

-- Shared implementation is private; public RPC wrappers below pin every route to one source.
CREATE OR REPLACE FUNCTION public.scraper_insert_onlyfans_source_videos(p_site text, items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
    inserted_count integer;
BEGIN
    IF p_site NOT IN ('influencersgonewild', 'sheeshfans') THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';

    EXECUTE format($insert$
        INSERT INTO scraper.%I (
            source_site, source_post_id, title, slug, detail_url, full_video_url,
            video_mime_type, trailer_preview_url, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            views_count, views_raw, rating_score, rating_raw, categories, tags,
            actors, status, published_at, raw_payload
        )
        SELECT
            $2, x.source_post_id, COALESCE(x.title, ''), x.slug, COALESCE(x.detail_url, ''),
            x.full_video_url, COALESCE(x.video_mime_type, 'embed/iframe'), x.trailer_preview_url,
            x.thumbnail_url, x.clean_thumbnail_url, COALESCE(x.quality_label, '1080p Full HD'),
            COALESCE(x.quality_score, 1080), COALESCE(x.duration_seconds, 600),
            COALESCE(x.duration_formatted, '10:00'), COALESCE(x.views_count, 0),
            COALESCE(x.views_raw, '0'), COALESCE(x.rating_score, 95.0),
            COALESCE(x.rating_raw, '95%%'),
            COALESCE(ARRAY(SELECT jsonb_array_elements_text(x.categories)), ARRAY['onlyfans']::text[]),
            COALESCE(ARRAY(SELECT jsonb_array_elements_text(x.tags)), ARRAY[]::text[]),
            COALESCE(ARRAY(SELECT jsonb_array_elements_text(x.actors)), ARRAY[]::text[]),
            COALESCE(x.status, 'active'), x.published_at, COALESCE(x.raw_payload, '{}'::jsonb)
        FROM jsonb_to_recordset($1) AS x(
            source_post_id text, title text, slug text, detail_url text, full_video_url text,
            video_mime_type text, trailer_preview_url text, thumbnail_url text,
            clean_thumbnail_url text, quality_label text, quality_score integer,
            duration_seconds integer, duration_formatted text, views_count bigint,
            views_raw text, rating_score numeric, rating_raw text, categories jsonb,
            tags jsonb, actors jsonb, status text, published_at timestamptz, raw_payload jsonb
        )
    $insert$, table_key) USING items, p_site;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN jsonb_build_object('success', true, 'inserted', inserted_count, 'count', inserted_count);
END;
$$;

CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_source_videos(
    p_site text, p_tag text, p_category text, p_search text, p_sort text, p_limit integer, p_offset integer
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
    IF p_site NOT IN ('influencersgonewild', 'sheeshfans') THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';
    RETURN QUERY EXECUTE format($query$
        SELECT to_jsonb(v)
        FROM scraper.%I v
        WHERE ( $1 IS NULL OR lower(trim($1)) = 'all' OR lower(trim($1)) = ANY(v.tags) OR trim($1) = ANY(v.actors) )
          AND ( $2 IS NULL OR lower(trim($2)) = 'all' OR lower(trim($2)) = ANY(v.categories) )
          AND ( $3 IS NULL OR v.title ILIKE '%%' || trim($3) || '%%' )
          AND v.status = 'active'
          AND v.full_video_url IS NOT NULL AND v.full_video_url <> ''
        ORDER BY
          CASE WHEN $4 = 'views' THEN v.views_count END DESC NULLS LAST,
          CASE WHEN $4 = 'rating' THEN v.rating_score END DESC NULLS LAST,
          CASE WHEN $4 = 'oldest' THEN v.published_at END ASC NULLS LAST,
          CASE WHEN $4 = 'quality' THEN v.quality_score END DESC NULLS LAST,
          v.published_at DESC NULLS LAST, v.scraped_at DESC
        LIMIT $5 OFFSET $6
    $query$, table_key) USING p_tag, p_category, p_search, p_sort, p_limit, p_offset;
END;
$$;

CREATE OR REPLACE FUNCTION public.scraper_count_onlyfans_source_videos(p_site text, p_tag text, p_category text, p_search text)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
    result_count bigint;
BEGIN
    IF p_site NOT IN ('influencersgonewild', 'sheeshfans') THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';
    EXECUTE format($query$
        SELECT count(*) FROM scraper.%I v
        WHERE ( $1 IS NULL OR lower(trim($1)) = 'all' OR lower(trim($1)) = ANY(v.tags) OR trim($1) = ANY(v.actors) )
          AND ( $2 IS NULL OR lower(trim($2)) = 'all' OR lower(trim($2)) = ANY(v.categories) )
          AND ( $3 IS NULL OR v.title ILIKE '%%' || trim($3) || '%%' )
          AND v.status = 'active'
          AND v.full_video_url IS NOT NULL AND v.full_video_url <> ''
    $query$, table_key) INTO result_count USING p_tag, p_category, p_search;
    RETURN result_count;
END;
$$;

CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_source_video_by_id(p_site text, p_id text)
RETURNS SETOF jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    table_key text;
BEGIN
    IF p_site NOT IN ('influencersgonewild', 'sheeshfans') THEN
        RAISE EXCEPTION 'Unknown OnlyFans source: %', p_site;
    END IF;
    table_key := p_site || '_videos';
    RETURN QUERY EXECUTE format(
        'SELECT to_jsonb(v) FROM scraper.%I v WHERE v.id::text = $1 AND v.status = ''active'' LIMIT 1',
        table_key
    ) USING p_id;
END;
$$;

REVOKE ALL ON FUNCTION public.scraper_insert_onlyfans_source_videos(text, jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.scraper_get_onlyfans_source_videos(text, text, text, text, text, integer, integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.scraper_count_onlyfans_source_videos(text, text, text, text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.scraper_get_onlyfans_source_video_by_id(text, text) FROM PUBLIC, anon, authenticated, service_role;

DO $$
DECLARE
    site_key text;
BEGIN
    FOREACH site_key IN ARRAY ARRAY['influencersgonewild', 'sheeshfans'] LOOP
        EXECUTE format($insert_wrapper$
            CREATE OR REPLACE FUNCTION public.scraper_insert_onlyfans_%1$s_videos(items jsonb)
            RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path = scraper, public
            AS $body$ SELECT public.scraper_insert_onlyfans_source_videos(%2$L, items) $body$;
        $insert_wrapper$, site_key, site_key);

        EXECUTE format($get_wrapper$
            CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_%1$s_videos(
                p_tag text, p_category text, p_search text, p_sort text, p_limit integer, p_offset integer
            ) RETURNS SETOF jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = scraper, public
            AS $body$ SELECT * FROM public.scraper_get_onlyfans_source_videos(%2$L, p_tag, p_category, p_search, p_sort, p_limit, p_offset) $body$;
        $get_wrapper$, site_key, site_key);

        EXECUTE format($count_wrapper$
            CREATE OR REPLACE FUNCTION public.scraper_count_onlyfans_%1$s_videos(p_tag text, p_category text, p_search text)
            RETURNS bigint LANGUAGE sql STABLE SECURITY DEFINER SET search_path = scraper, public
            AS $body$ SELECT public.scraper_count_onlyfans_source_videos(%2$L, p_tag, p_category, p_search) $body$;
        $count_wrapper$, site_key, site_key);

        EXECUTE format($detail_wrapper$
            CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_%1$s_video_by_id(p_id text)
            RETURNS SETOF jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = scraper, public
            AS $body$ SELECT * FROM public.scraper_get_onlyfans_source_video_by_id(%2$L, p_id) $body$;
        $detail_wrapper$, site_key, site_key);

        EXECUTE format('GRANT EXECUTE ON FUNCTION public.scraper_insert_onlyfans_%s_videos(jsonb) TO service_role', site_key);
        EXECUTE format('GRANT EXECUTE ON FUNCTION public.scraper_get_onlyfans_%s_videos(text, text, text, text, integer, integer) TO anon, authenticated, service_role', site_key);
        EXECUTE format('GRANT EXECUTE ON FUNCTION public.scraper_count_onlyfans_%s_videos(text, text, text) TO anon, authenticated, service_role', site_key);
        EXECUTE format('GRANT EXECUTE ON FUNCTION public.scraper_get_onlyfans_%s_video_by_id(text) TO anon, authenticated, service_role', site_key);
    END LOOP;
END;
$$;

NOTIFY pgrst, 'reload schema';
