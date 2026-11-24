-- ============================================================================
-- CAM ARCHIVE — FOUR PHYSICALLY ISOLATED SOURCE DATABASES
-- hotscopes.net/videos/voyeur-house, reallifecam.to/videos,
-- hotscopes.net/videos/spygasm?o=mr, hotscopes.net/videos/reallifecam?o=mr
-- Each source has its own table + RPC family. No cross-source data sharing.
-- ============================================================================


-- ============================================================================
-- CAM ARCHIVE — hotscopesvoyeurhouse — ISOLATED DATABASE + RPC BRIDGE
-- Physically separate from every other category and source. No cross-source
-- dedupe, merge, fallback, or canonical cache. Dead videos are stored but
-- hidden by the public read RPCs via status/dead_reason.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.hotscopesvoyeurhouse_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'embed/iframe',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT '720p HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64) DEFAULT '0',
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{hotscopesvoyeurhouse}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    dead_reason TEXT,
    last_checked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_hotscopesvoyeurhouse_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.hotscopesvoyeurhouse_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_hotscopesvoyeurhouse_videos ON scraper.hotscopesvoyeurhouse_videos;
CREATE POLICY service_role_all_hotscopesvoyeurhouse_videos ON scraper.hotscopesvoyeurhouse_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_hotscopesvoyeurhouse_videos ON scraper.hotscopesvoyeurhouse_videos;
CREATE POLICY anon_select_hotscopesvoyeurhouse_videos ON scraper.hotscopesvoyeurhouse_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_source ON scraper.hotscopesvoyeurhouse_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_status ON scraper.hotscopesvoyeurhouse_videos(status);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_published ON scraper.hotscopesvoyeurhouse_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_views ON scraper.hotscopesvoyeurhouse_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_tags ON scraper.hotscopesvoyeurhouse_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_categories ON scraper.hotscopesvoyeurhouse_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesvoyeurhouse_actors ON scraper.hotscopesvoyeurhouse_videos USING gin(actors);

-- BATCH UPSERT (revives rows when a stream comes back)
CREATE OR REPLACE FUNCTION public.scraper_upsert_hotscopesvoyeurhouse_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            (x->>'source_site') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            (x->>'title') AS title,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'embed/iframe') AS video_mime_type,
            (x->>'trailer_preview_url') AS trailer_preview_url,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', '720p HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted')::varchar AS duration_formatted,
            COALESCE((x->>'views_count')::bigint, 0) AS views_count,
            COALESCE(x->>'views_raw', '0') AS views_raw,
            (x->>'rating_score')::numeric(5,2) AS rating_score,
            (x->>'rating_raw')::varchar AS rating_raw,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['hotscopesvoyeurhouse']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'actors') elem), ARRAY[]::text[]) AS actors,
            COALESCE(x->>'status', 'active') AS status,
            NULLIF(x->>'dead_reason', '') AS dead_reason,
            now() AS last_checked_at,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.hotscopesvoyeurhouse_videos (
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE
                WHEN EXCLUDED.full_video_url IS NOT NULL AND EXCLUDED.full_video_url <> ''
                THEN EXCLUDED.full_video_url
                ELSE scraper.hotscopesvoyeurhouse_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.hotscopesvoyeurhouse_videos.quality_score, 0)),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.hotscopesvoyeurhouse_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.hotscopesvoyeurhouse_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.hotscopesvoyeurhouse_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.hotscopesvoyeurhouse_videos.duration_formatted),
            views_count = EXCLUDED.views_count,
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.hotscopesvoyeurhouse_videos.rating_score),
            rating_raw = COALESCE(EXCLUDED.rating_raw, scraper.hotscopesvoyeurhouse_videos.rating_raw),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            status = EXCLUDED.status,
            dead_reason = EXCLUDED.dead_reason,
            last_checked_at = EXCLUDED.last_checked_at,
            published_at = COALESCE(EXCLUDED.published_at, scraper.hotscopesvoyeurhouse_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now()
        RETURNING (xmax = 0) AS is_insert
    )
    SELECT
        COUNT(*) FILTER (WHERE is_insert) AS ins,
        COUNT(*) FILTER (WHERE NOT is_insert) AS upd
    INTO inserted_count, updated_count
    FROM upserted;

    RETURN jsonb_build_object(
        'success', true,
        'inserted', inserted_count,
        'updated', updated_count,
        'total', inserted_count + updated_count
    );
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_hotscopesvoyeurhouse_videos(jsonb) TO service_role;

-- PUBLIC QUERY RPC (active rows with a playable URL only)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesvoyeurhouse_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.hotscopesvoyeurhouse_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesvoyeurhouse_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesvoyeurhouse_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_hotscopesvoyeurhouse_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.hotscopesvoyeurhouse_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '');
    RETURN v_count;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_count_hotscopesvoyeurhouse_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC (scoped to this one isolated table)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesvoyeurhouse_video_by_id(p_id text)
RETURNS SETOF scraper.hotscopesvoyeurhouse_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesvoyeurhouse_videos vv
    WHERE (vv.id::text = p_id OR vv.source_post_id = p_id OR vv.slug = p_id)
      AND vv.status = 'active'
    LIMIT 1;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesvoyeurhouse_video_by_id(text) TO anon, authenticated, service_role;

-- OPERATIONAL RPC: quarantine dead videos discovered by re-checks
CREATE OR REPLACE FUNCTION public.scraper_mark_hotscopesvoyeurhouse_dead(p_site text, p_post_id text, p_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    UPDATE scraper.hotscopesvoyeurhouse_videos
       SET status = 'dead',
           dead_reason = COALESCE(NULLIF(p_reason, ''), 'dead'),
           last_checked_at = now(),
           updated_at = now()
     WHERE source_site = p_site AND source_post_id = p_post_id;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_mark_hotscopesvoyeurhouse_dead(text, text, text) TO service_role;


-- ============================================================================
-- CAM ARCHIVE — reallifecamto — ISOLATED DATABASE + RPC BRIDGE
-- Physically separate from every other category and source. No cross-source
-- dedupe, merge, fallback, or canonical cache. Dead videos are stored but
-- hidden by the public read RPCs via status/dead_reason.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.reallifecamto_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'embed/iframe',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT '720p HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64) DEFAULT '0',
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{reallifecamto}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    dead_reason TEXT,
    last_checked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_reallifecamto_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.reallifecamto_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_reallifecamto_videos ON scraper.reallifecamto_videos;
CREATE POLICY service_role_all_reallifecamto_videos ON scraper.reallifecamto_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_reallifecamto_videos ON scraper.reallifecamto_videos;
CREATE POLICY anon_select_reallifecamto_videos ON scraper.reallifecamto_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_source ON scraper.reallifecamto_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_status ON scraper.reallifecamto_videos(status);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_published ON scraper.reallifecamto_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_views ON scraper.reallifecamto_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_tags ON scraper.reallifecamto_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_categories ON scraper.reallifecamto_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_ca_reallifecamto_actors ON scraper.reallifecamto_videos USING gin(actors);

-- BATCH UPSERT (revives rows when a stream comes back)
CREATE OR REPLACE FUNCTION public.scraper_upsert_reallifecamto_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            (x->>'source_site') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            (x->>'title') AS title,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'embed/iframe') AS video_mime_type,
            (x->>'trailer_preview_url') AS trailer_preview_url,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', '720p HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted')::varchar AS duration_formatted,
            COALESCE((x->>'views_count')::bigint, 0) AS views_count,
            COALESCE(x->>'views_raw', '0') AS views_raw,
            (x->>'rating_score')::numeric(5,2) AS rating_score,
            (x->>'rating_raw')::varchar AS rating_raw,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['reallifecamto']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'actors') elem), ARRAY[]::text[]) AS actors,
            COALESCE(x->>'status', 'active') AS status,
            NULLIF(x->>'dead_reason', '') AS dead_reason,
            now() AS last_checked_at,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.reallifecamto_videos (
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE
                WHEN EXCLUDED.full_video_url IS NOT NULL AND EXCLUDED.full_video_url <> ''
                THEN EXCLUDED.full_video_url
                ELSE scraper.reallifecamto_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.reallifecamto_videos.quality_score, 0)),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.reallifecamto_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.reallifecamto_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.reallifecamto_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.reallifecamto_videos.duration_formatted),
            views_count = EXCLUDED.views_count,
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.reallifecamto_videos.rating_score),
            rating_raw = COALESCE(EXCLUDED.rating_raw, scraper.reallifecamto_videos.rating_raw),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            status = EXCLUDED.status,
            dead_reason = EXCLUDED.dead_reason,
            last_checked_at = EXCLUDED.last_checked_at,
            published_at = COALESCE(EXCLUDED.published_at, scraper.reallifecamto_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now()
        RETURNING (xmax = 0) AS is_insert
    )
    SELECT
        COUNT(*) FILTER (WHERE is_insert) AS ins,
        COUNT(*) FILTER (WHERE NOT is_insert) AS upd
    INTO inserted_count, updated_count
    FROM upserted;

    RETURN jsonb_build_object(
        'success', true,
        'inserted', inserted_count,
        'updated', updated_count,
        'total', inserted_count + updated_count
    );
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_reallifecamto_videos(jsonb) TO service_role;

-- PUBLIC QUERY RPC (active rows with a playable URL only)
CREATE OR REPLACE FUNCTION public.scraper_get_reallifecamto_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.reallifecamto_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.reallifecamto_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_reallifecamto_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_reallifecamto_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.reallifecamto_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '');
    RETURN v_count;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_count_reallifecamto_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC (scoped to this one isolated table)
CREATE OR REPLACE FUNCTION public.scraper_get_reallifecamto_video_by_id(p_id text)
RETURNS SETOF scraper.reallifecamto_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.reallifecamto_videos vv
    WHERE (vv.id::text = p_id OR vv.source_post_id = p_id OR vv.slug = p_id)
      AND vv.status = 'active'
    LIMIT 1;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_reallifecamto_video_by_id(text) TO anon, authenticated, service_role;

-- OPERATIONAL RPC: quarantine dead videos discovered by re-checks
CREATE OR REPLACE FUNCTION public.scraper_mark_reallifecamto_dead(p_site text, p_post_id text, p_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    UPDATE scraper.reallifecamto_videos
       SET status = 'dead',
           dead_reason = COALESCE(NULLIF(p_reason, ''), 'dead'),
           last_checked_at = now(),
           updated_at = now()
     WHERE source_site = p_site AND source_post_id = p_post_id;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_mark_reallifecamto_dead(text, text, text) TO service_role;


-- ============================================================================
-- CAM ARCHIVE — hotscopesspygasm — ISOLATED DATABASE + RPC BRIDGE
-- Physically separate from every other category and source. No cross-source
-- dedupe, merge, fallback, or canonical cache. Dead videos are stored but
-- hidden by the public read RPCs via status/dead_reason.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.hotscopesspygasm_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'embed/iframe',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT '720p HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64) DEFAULT '0',
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{hotscopesspygasm}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    dead_reason TEXT,
    last_checked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_hotscopesspygasm_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.hotscopesspygasm_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_hotscopesspygasm_videos ON scraper.hotscopesspygasm_videos;
CREATE POLICY service_role_all_hotscopesspygasm_videos ON scraper.hotscopesspygasm_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_hotscopesspygasm_videos ON scraper.hotscopesspygasm_videos;
CREATE POLICY anon_select_hotscopesspygasm_videos ON scraper.hotscopesspygasm_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_source ON scraper.hotscopesspygasm_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_status ON scraper.hotscopesspygasm_videos(status);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_published ON scraper.hotscopesspygasm_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_views ON scraper.hotscopesspygasm_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_tags ON scraper.hotscopesspygasm_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_categories ON scraper.hotscopesspygasm_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesspygasm_actors ON scraper.hotscopesspygasm_videos USING gin(actors);

-- BATCH UPSERT (revives rows when a stream comes back)
CREATE OR REPLACE FUNCTION public.scraper_upsert_hotscopesspygasm_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            (x->>'source_site') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            (x->>'title') AS title,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'embed/iframe') AS video_mime_type,
            (x->>'trailer_preview_url') AS trailer_preview_url,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', '720p HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted')::varchar AS duration_formatted,
            COALESCE((x->>'views_count')::bigint, 0) AS views_count,
            COALESCE(x->>'views_raw', '0') AS views_raw,
            (x->>'rating_score')::numeric(5,2) AS rating_score,
            (x->>'rating_raw')::varchar AS rating_raw,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['hotscopesspygasm']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'actors') elem), ARRAY[]::text[]) AS actors,
            COALESCE(x->>'status', 'active') AS status,
            NULLIF(x->>'dead_reason', '') AS dead_reason,
            now() AS last_checked_at,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.hotscopesspygasm_videos (
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE
                WHEN EXCLUDED.full_video_url IS NOT NULL AND EXCLUDED.full_video_url <> ''
                THEN EXCLUDED.full_video_url
                ELSE scraper.hotscopesspygasm_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.hotscopesspygasm_videos.quality_score, 0)),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.hotscopesspygasm_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.hotscopesspygasm_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.hotscopesspygasm_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.hotscopesspygasm_videos.duration_formatted),
            views_count = EXCLUDED.views_count,
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.hotscopesspygasm_videos.rating_score),
            rating_raw = COALESCE(EXCLUDED.rating_raw, scraper.hotscopesspygasm_videos.rating_raw),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            status = EXCLUDED.status,
            dead_reason = EXCLUDED.dead_reason,
            last_checked_at = EXCLUDED.last_checked_at,
            published_at = COALESCE(EXCLUDED.published_at, scraper.hotscopesspygasm_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now()
        RETURNING (xmax = 0) AS is_insert
    )
    SELECT
        COUNT(*) FILTER (WHERE is_insert) AS ins,
        COUNT(*) FILTER (WHERE NOT is_insert) AS upd
    INTO inserted_count, updated_count
    FROM upserted;

    RETURN jsonb_build_object(
        'success', true,
        'inserted', inserted_count,
        'updated', updated_count,
        'total', inserted_count + updated_count
    );
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_hotscopesspygasm_videos(jsonb) TO service_role;

-- PUBLIC QUERY RPC (active rows with a playable URL only)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesspygasm_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.hotscopesspygasm_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesspygasm_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesspygasm_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_hotscopesspygasm_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.hotscopesspygasm_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '');
    RETURN v_count;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_count_hotscopesspygasm_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC (scoped to this one isolated table)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesspygasm_video_by_id(p_id text)
RETURNS SETOF scraper.hotscopesspygasm_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesspygasm_videos vv
    WHERE (vv.id::text = p_id OR vv.source_post_id = p_id OR vv.slug = p_id)
      AND vv.status = 'active'
    LIMIT 1;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesspygasm_video_by_id(text) TO anon, authenticated, service_role;

-- OPERATIONAL RPC: quarantine dead videos discovered by re-checks
CREATE OR REPLACE FUNCTION public.scraper_mark_hotscopesspygasm_dead(p_site text, p_post_id text, p_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    UPDATE scraper.hotscopesspygasm_videos
       SET status = 'dead',
           dead_reason = COALESCE(NULLIF(p_reason, ''), 'dead'),
           last_checked_at = now(),
           updated_at = now()
     WHERE source_site = p_site AND source_post_id = p_post_id;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_mark_hotscopesspygasm_dead(text, text, text) TO service_role;


-- ============================================================================
-- CAM ARCHIVE — hotscopesreallifecam — ISOLATED DATABASE + RPC BRIDGE
-- Physically separate from every other category and source. No cross-source
-- dedupe, merge, fallback, or canonical cache. Dead videos are stored but
-- hidden by the public read RPCs via status/dead_reason.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.hotscopesreallifecam_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'embed/iframe',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT '720p HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64) DEFAULT '0',
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{hotscopesreallifecam}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    dead_reason TEXT,
    last_checked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_hotscopesreallifecam_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.hotscopesreallifecam_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_hotscopesreallifecam_videos ON scraper.hotscopesreallifecam_videos;
CREATE POLICY service_role_all_hotscopesreallifecam_videos ON scraper.hotscopesreallifecam_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_hotscopesreallifecam_videos ON scraper.hotscopesreallifecam_videos;
CREATE POLICY anon_select_hotscopesreallifecam_videos ON scraper.hotscopesreallifecam_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_source ON scraper.hotscopesreallifecam_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_status ON scraper.hotscopesreallifecam_videos(status);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_published ON scraper.hotscopesreallifecam_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_views ON scraper.hotscopesreallifecam_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_tags ON scraper.hotscopesreallifecam_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_categories ON scraper.hotscopesreallifecam_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_ca_hotscopesreallifecam_actors ON scraper.hotscopesreallifecam_videos USING gin(actors);

-- BATCH UPSERT (revives rows when a stream comes back)
CREATE OR REPLACE FUNCTION public.scraper_upsert_hotscopesreallifecam_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            (x->>'source_site') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            (x->>'title') AS title,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'embed/iframe') AS video_mime_type,
            (x->>'trailer_preview_url') AS trailer_preview_url,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', '720p HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted')::varchar AS duration_formatted,
            COALESCE((x->>'views_count')::bigint, 0) AS views_count,
            COALESCE(x->>'views_raw', '0') AS views_raw,
            (x->>'rating_score')::numeric(5,2) AS rating_score,
            (x->>'rating_raw')::varchar AS rating_raw,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['hotscopesreallifecam']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'actors') elem), ARRAY[]::text[]) AS actors,
            COALESCE(x->>'status', 'active') AS status,
            NULLIF(x->>'dead_reason', '') AS dead_reason,
            now() AS last_checked_at,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.hotscopesreallifecam_videos (
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score,
            duration_seconds, duration_formatted, views_count, views_raw,
            rating_score, rating_raw, categories, tags, actors,
            status, dead_reason, last_checked_at, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE
                WHEN EXCLUDED.full_video_url IS NOT NULL AND EXCLUDED.full_video_url <> ''
                THEN EXCLUDED.full_video_url
                ELSE scraper.hotscopesreallifecam_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.hotscopesreallifecam_videos.quality_score, 0)),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.hotscopesreallifecam_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.hotscopesreallifecam_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.hotscopesreallifecam_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.hotscopesreallifecam_videos.duration_formatted),
            views_count = EXCLUDED.views_count,
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.hotscopesreallifecam_videos.rating_score),
            rating_raw = COALESCE(EXCLUDED.rating_raw, scraper.hotscopesreallifecam_videos.rating_raw),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            status = EXCLUDED.status,
            dead_reason = EXCLUDED.dead_reason,
            last_checked_at = EXCLUDED.last_checked_at,
            published_at = COALESCE(EXCLUDED.published_at, scraper.hotscopesreallifecam_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now()
        RETURNING (xmax = 0) AS is_insert
    )
    SELECT
        COUNT(*) FILTER (WHERE is_insert) AS ins,
        COUNT(*) FILTER (WHERE NOT is_insert) AS upd
    INTO inserted_count, updated_count
    FROM upserted;

    RETURN jsonb_build_object(
        'success', true,
        'inserted', inserted_count,
        'updated', updated_count,
        'total', inserted_count + updated_count
    );
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_hotscopesreallifecam_videos(jsonb) TO service_role;

-- PUBLIC QUERY RPC (active rows with a playable URL only)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesreallifecam_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.hotscopesreallifecam_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesreallifecam_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesreallifecam_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_hotscopesreallifecam_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.hotscopesreallifecam_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '');
    RETURN v_count;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_count_hotscopesreallifecam_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC (scoped to this one isolated table)
CREATE OR REPLACE FUNCTION public.scraper_get_hotscopesreallifecam_video_by_id(p_id text)
RETURNS SETOF scraper.hotscopesreallifecam_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.hotscopesreallifecam_videos vv
    WHERE (vv.id::text = p_id OR vv.source_post_id = p_id OR vv.slug = p_id)
      AND vv.status = 'active'
    LIMIT 1;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_get_hotscopesreallifecam_video_by_id(text) TO anon, authenticated, service_role;

-- OPERATIONAL RPC: quarantine dead videos discovered by re-checks
CREATE OR REPLACE FUNCTION public.scraper_mark_hotscopesreallifecam_dead(p_site text, p_post_id text, p_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $fn$
BEGIN
    UPDATE scraper.hotscopesreallifecam_videos
       SET status = 'dead',
           dead_reason = COALESCE(NULLIF(p_reason, ''), 'dead'),
           last_checked_at = now(),
           updated_at = now()
     WHERE source_site = p_site AND source_post_id = p_post_id;
END;
$fn$;

GRANT EXECUTE ON FUNCTION public.scraper_mark_hotscopesreallifecam_dead(text, text, text) TO service_role;


-- ============================================================================
-- END OF CAM ARCHIVE ISOLATED SCHEMA
-- ============================================================================
