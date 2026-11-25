-- ============================================================================
-- MIGRATION: Split studios_videos into per-site isolated tables
-- Date: 2026-11-26
-- Purpose: Every website owns its isolated database (onlyfans/voyeurhouse
--   pattern). scraper.studios_videos (shared by madouer + dasj91) is split
--   into scraper.madouer_videos and scraper.dasj91_videos, each with its own
--   RPC family (scraper_*_madouer_videos / scraper_*_dasj91_videos). Existing
--   rows are copied by source_site; the shared table and its RPCs are dropped.
--   The Chinese category page lists both sites in its site picker.
-- ============================================================================

-- ─── 1. Per-site tables (same schema as studios_videos) ─────────────────────

CREATE TABLE IF NOT EXISTS scraper.madouer_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL DEFAULT 'madouer',
    source_post_id TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'video',
    title TEXT NOT NULL,
    title_en TEXT,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'application/x-mpegURL',
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT 'HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    region TEXT,
    region_en TEXT,
    resolution VARCHAR(32),
    categories TEXT[] DEFAULT '{studios}',
    tags TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_madouer_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.madouer_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_madouer_videos ON scraper.madouer_videos;
CREATE POLICY service_role_all_madouer_videos ON scraper.madouer_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_madouer_videos ON scraper.madouer_videos;
CREATE POLICY anon_select_madouer_videos ON scraper.madouer_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_madouer_quality_score ON scraper.madouer_videos(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_madouer_scraped_at ON scraper.madouer_videos(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_madouer_source_post ON scraper.madouer_videos(source_site, source_post_id);

-- Copy existing rows from the shared studios table.
INSERT INTO scraper.madouer_videos
SELECT * FROM scraper.studios_videos
WHERE source_site = 'madouer'
ON CONFLICT (source_site, source_post_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS scraper.dasj91_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL DEFAULT 'dasj91',
    source_post_id TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'video',
    title TEXT NOT NULL,
    title_en TEXT,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,
    video_mime_type TEXT DEFAULT 'application/x-mpegURL',
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT 'HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    region TEXT,
    region_en TEXT,
    resolution VARCHAR(32),
    categories TEXT[] DEFAULT '{studios}',
    tags TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_dasj91_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.dasj91_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_dasj91_videos ON scraper.dasj91_videos;
CREATE POLICY service_role_all_dasj91_videos ON scraper.dasj91_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_dasj91_videos ON scraper.dasj91_videos;
CREATE POLICY anon_select_dasj91_videos ON scraper.dasj91_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_dasj91_quality_score ON scraper.dasj91_videos(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_dasj91_scraped_at ON scraper.dasj91_videos(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_dasj91_source_post ON scraper.dasj91_videos(source_site, source_post_id);

-- Copy existing rows from the shared studios table.
INSERT INTO scraper.dasj91_videos
SELECT * FROM scraper.studios_videos
WHERE source_site = 'dasj91'
ON CONFLICT (source_site, source_post_id) DO NOTHING;

-- ─── RPC family: madouer ───
CREATE OR REPLACE FUNCTION public.scraper_upsert_madouer_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            COALESCE(x->>'source_site', 'madouer') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            COALESCE(x->>'content_type', 'video') AS content_type,
            (x->>'title') AS title,
            (x->>'title_en') AS title_en,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'application/x-mpegURL') AS video_mime_type,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', 'HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted') AS duration_formatted,
            (x->>'region') AS region,
            (x->>'region_en') AS region_en,
            (x->>'resolution') AS resolution,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['madouer']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE(x->>'status', 'active') AS status,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.madouer_videos (
            source_site, source_post_id, content_type, title, title_en, slug,
            detail_url, full_video_url, video_mime_type, thumbnail_url,
            clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, region, region_en, resolution,
            categories, tags, status, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, content_type, title, title_en, slug,
            detail_url, full_video_url, video_mime_type, thumbnail_url,
            clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, region, region_en, resolution,
            categories, tags, status, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            title_en = COALESCE(EXCLUDED.title_en, scraper.madouer_videos.title_en),
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.madouer_videos.full_video_url),
            video_mime_type = EXCLUDED.video_mime_type,
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.madouer_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.madouer_videos.clean_thumbnail_url),
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.madouer_videos.quality_score, 0)),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.madouer_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.madouer_videos.duration_formatted),
            region = COALESCE(EXCLUDED.region, scraper.madouer_videos.region),
            region_en = COALESCE(EXCLUDED.region_en, scraper.madouer_videos.region_en),
            resolution = COALESCE(EXCLUDED.resolution, scraper.madouer_videos.resolution),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            status = EXCLUDED.status,
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
$$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_madouer_videos(jsonb) TO service_role;

-- QUERY RPC
CREATE OR REPLACE FUNCTION public.scraper_get_madouer_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.madouer_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT sv.*
    FROM scraper.madouer_videos sv
    WHERE (p_source_site IS NULL OR sv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR sv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = sv.region_en
           OR sv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%'
           OR sv.title_en ILIKE '%' || trim(p_search) || '%')
      AND sv.status = 'active'
      AND (sv.full_video_url IS NOT NULL AND sv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN sv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN sv.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN sv.scraped_at END ASC NULLS LAST,
        sv.scraped_at DESC,
        sv.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_madouer_videos(text, text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_madouer_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.madouer_videos sv
    WHERE (p_source_site IS NULL OR sv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR sv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = sv.region_en
           OR sv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%'
           OR sv.title_en ILIKE '%' || trim(p_search) || '%')
      AND sv.status = 'active'
      AND (sv.full_video_url IS NOT NULL AND sv.full_video_url <> '');
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_madouer_videos(text, text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_madouer_video_by_id(p_id text)
RETURNS SETOF scraper.madouer_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT sv.*
    FROM scraper.madouer_videos sv
    WHERE (sv.id::text = p_id OR sv.source_post_id = p_id OR sv.slug = p_id)
      AND sv.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_madouer_video_by_id(text) TO anon, authenticated, service_role;

-- PARTIAL UPDATE: English titles (service-role only)
CREATE OR REPLACE FUNCTION public.scraper_update_madouer_title_en(items jsonb)
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
        SET title_en = x.title_en, updated_at = now()
        FROM jsonb_array_elements(items) AS x
        WHERE sv.id::text = (x->>'id') AND COALESCE(x->>'title_en', '') <> ''
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_madouer_title_en(jsonb) TO service_role;

-- PARTIAL UPDATE: quality labels (service-role only)
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
            updated_at = now()
        FROM (
            SELECT (x->>'id') AS id, (x->>'quality_label') AS quality_label,
                   (x->>'quality_score')::int AS quality_score, (x->>'resolution') AS resolution
            FROM jsonb_array_elements(items) AS x
        ) x
        WHERE sv.id::text = x.id
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_madouer_quality(jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';

-- ─── RPC family: dasj91 ───
CREATE OR REPLACE FUNCTION public.scraper_upsert_dasj91_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    inserted_count int := 0;
    updated_count int := 0;
BEGIN
    WITH raw_items AS (
        SELECT DISTINCT ON ((x->>'source_site'), (x->>'source_post_id'))
            COALESCE(x->>'source_site', 'madouer') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            COALESCE(x->>'content_type', 'video') AS content_type,
            (x->>'title') AS title,
            (x->>'title_en') AS title_en,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'application/x-mpegURL') AS video_mime_type,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', 'HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted') AS duration_formatted,
            (x->>'region') AS region,
            (x->>'region_en') AS region_en,
            (x->>'resolution') AS resolution,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['dasj91']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE(x->>'status', 'active') AS status,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.dasj91_videos (
            source_site, source_post_id, content_type, title, title_en, slug,
            detail_url, full_video_url, video_mime_type, thumbnail_url,
            clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, region, region_en, resolution,
            categories, tags, status, published_at, raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, content_type, title, title_en, slug,
            detail_url, full_video_url, video_mime_type, thumbnail_url,
            clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, region, region_en, resolution,
            categories, tags, status, published_at, raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            title_en = COALESCE(EXCLUDED.title_en, scraper.dasj91_videos.title_en),
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.dasj91_videos.full_video_url),
            video_mime_type = EXCLUDED.video_mime_type,
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.dasj91_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.dasj91_videos.clean_thumbnail_url),
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.dasj91_videos.quality_score, 0)),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.dasj91_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.dasj91_videos.duration_formatted),
            region = COALESCE(EXCLUDED.region, scraper.dasj91_videos.region),
            region_en = COALESCE(EXCLUDED.region_en, scraper.dasj91_videos.region_en),
            resolution = COALESCE(EXCLUDED.resolution, scraper.dasj91_videos.resolution),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            status = EXCLUDED.status,
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
$$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_dasj91_videos(jsonb) TO service_role;

-- QUERY RPC
CREATE OR REPLACE FUNCTION public.scraper_get_dasj91_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.dasj91_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT sv.*
    FROM scraper.dasj91_videos sv
    WHERE (p_source_site IS NULL OR sv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR sv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = sv.region_en
           OR sv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%'
           OR sv.title_en ILIKE '%' || trim(p_search) || '%')
      AND sv.status = 'active'
      AND (sv.full_video_url IS NOT NULL AND sv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN sv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN sv.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN sv.scraped_at END ASC NULLS LAST,
        sv.scraped_at DESC,
        sv.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_dasj91_videos(text, text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_dasj91_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.dasj91_videos sv
    WHERE (p_source_site IS NULL OR sv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR sv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = sv.region_en
           OR sv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%'
           OR sv.title_en ILIKE '%' || trim(p_search) || '%')
      AND sv.status = 'active'
      AND (sv.full_video_url IS NOT NULL AND sv.full_video_url <> '');
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_dasj91_videos(text, text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_dasj91_video_by_id(p_id text)
RETURNS SETOF scraper.dasj91_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT sv.*
    FROM scraper.dasj91_videos sv
    WHERE (sv.id::text = p_id OR sv.source_post_id = p_id OR sv.slug = p_id)
      AND sv.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_dasj91_video_by_id(text) TO anon, authenticated, service_role;

-- PARTIAL UPDATE: English titles (service-role only)
CREATE OR REPLACE FUNCTION public.scraper_update_dasj91_title_en(items jsonb)
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
        SET title_en = x.title_en, updated_at = now()
        FROM jsonb_array_elements(items) AS x
        WHERE sv.id::text = (x->>'id') AND COALESCE(x->>'title_en', '') <> ''
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_dasj91_title_en(jsonb) TO service_role;

-- PARTIAL UPDATE: quality labels (service-role only)
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
            updated_at = now()
        FROM (
            SELECT (x->>'id') AS id, (x->>'quality_label') AS quality_label,
                   (x->>'quality_score')::int AS quality_score, (x->>'resolution') AS resolution
            FROM jsonb_array_elements(items) AS x
        ) x
        WHERE sv.id::text = x.id
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_dasj91_quality(jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';

-- ─── 3. Drop the shared studios table and its RPC family ───────────────────
DROP FUNCTION IF EXISTS public.scraper_upsert_studios_videos(jsonb);
DROP FUNCTION IF EXISTS public.scraper_get_studios_videos(text, text, text, text, text, text, int, int);
DROP FUNCTION IF EXISTS public.scraper_count_studios_videos(text, text, text, text, text);
DROP FUNCTION IF EXISTS public.scraper_get_studios_video_by_id(text);
DROP FUNCTION IF EXISTS public.scraper_update_studios_title_en(jsonb);
DROP FUNCTION IF EXISTS public.scraper_update_studios_quality(jsonb);
DROP TABLE IF EXISTS scraper.studios_videos;

NOTIFY pgrst, 'reload schema';
