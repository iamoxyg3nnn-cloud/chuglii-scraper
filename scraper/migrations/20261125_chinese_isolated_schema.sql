-- ============================================================================
-- CHINESE CATEGORY — ISOLATED DATABASE + RPC BRIDGE (multisite-ready)
-- Applies the same per-site isolation model used for OnlyFans / Voyeur House.
-- No cross-category dedupe. No fallback to scraper.videos or any other table.
-- Source sites are represented by `source_site` (first: 'uxj838238').
-- `content_type` splits playable HLS videos ('video') from magnet/torrent
-- entries ('torrent') so the frontend can stream videos directly.
-- Site exposes no publish dates; `published_at` stays NULL and `scraped_at`
-- drives the 'newest' sort.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.chinese_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,                     -- e.g. 'uxj838238'
    source_post_id TEXT NOT NULL,                  -- numeric id from /video/{id}.html or /torrent/{id}.html
    content_type TEXT NOT NULL DEFAULT 'video',    -- 'video' (HLS playable) | 'torrent' (magnet entry)
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,                           -- HLS m3u8 for videos; NULL for torrents
    video_mime_type TEXT DEFAULT 'application/x-mpegURL',
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT '720p HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,                      -- torrents only (site exposes minutes)
    duration_formatted VARCHAR(32),
    category_id TEXT,                              -- source-site category id, e.g. '20110496'
    category_name_cn TEXT,                         -- e.g. '国产精品'
    category_name_en TEXT,                         -- e.g. 'Chinese Premium'
    magnet_uri TEXT,                               -- torrents only
    magnet_hash TEXT,                              -- btih 40-hex
    torrent_file_url TEXT,                         -- direct .torrent download
    size_label VARCHAR(32),                        -- e.g. '238 MB'
    resolution VARCHAR(32),                        -- e.g. '1280x720'
    cdn_endpoints JSONB DEFAULT '[]'::jsonb,       -- fallback CDN api.php endpoints (videos)
    screenshots JSONB DEFAULT '[]'::jsonb,         -- torrent screenshot URLs
    categories TEXT[] DEFAULT '{"chinese"}',
    tags TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_chinese_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.chinese_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_chinese_videos ON scraper.chinese_videos;
CREATE POLICY service_role_all_chinese_videos ON scraper.chinese_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_chinese_videos ON scraper.chinese_videos;
CREATE POLICY anon_select_chinese_videos ON scraper.chinese_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_chinese_content_type ON scraper.chinese_videos(content_type);
CREATE INDEX IF NOT EXISTS idx_chinese_quality_score ON scraper.chinese_videos(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_source ON scraper.chinese_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_published ON scraper.chinese_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_scraped ON scraper.chinese_videos(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_category ON scraper.chinese_videos(category_id);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_tags ON scraper.chinese_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_chinese_videos_categories ON scraper.chinese_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_chinese_magnet_hash ON scraper.chinese_videos(magnet_hash);

-- BATCH UPSERT RPC (quality-first stream promotion, mirrors onlyfans behaviour)
CREATE OR REPLACE FUNCTION public.scraper_upsert_chinese_videos(items jsonb)
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
            (x->>'source_site') AS source_site,
            (x->>'source_post_id') AS source_post_id,
            COALESCE(x->>'content_type', 'video') AS content_type,
            (x->>'title') AS title,
            (x->>'slug') AS slug,
            (x->>'detail_url') AS detail_url,
            (x->>'full_video_url') AS full_video_url,
            COALESCE(x->>'video_mime_type', 'application/x-mpegURL') AS video_mime_type,
            (x->>'thumbnail_url') AS thumbnail_url,
            (x->>'clean_thumbnail_url') AS clean_thumbnail_url,
            COALESCE(x->>'quality_label', '720p HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 720) AS quality_score,
            (x->>'duration_seconds')::int AS duration_seconds,
            (x->>'duration_formatted') AS duration_formatted,
            (x->>'category_id') AS category_id,
            (x->>'category_name_cn') AS category_name_cn,
            (x->>'category_name_en') AS category_name_en,
            (x->>'magnet_uri') AS magnet_uri,
            (x->>'magnet_hash') AS magnet_hash,
            (x->>'torrent_file_url') AS torrent_file_url,
            (x->>'size_label') AS size_label,
            (x->>'resolution') AS resolution,
            COALESCE(x->'cdn_endpoints', '[]'::jsonb) AS cdn_endpoints,
            COALESCE(x->'screenshots', '[]'::jsonb) AS screenshots,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['chinese']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE(x->>'status', 'active') AS status,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.chinese_videos (
            source_site, source_post_id, content_type, title, slug, detail_url,
            full_video_url, video_mime_type, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            category_id, category_name_cn, category_name_en,
            magnet_uri, magnet_hash, torrent_file_url, size_label, resolution,
            cdn_endpoints, screenshots, categories, tags, status, published_at,
            raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, content_type, title, slug, detail_url,
            full_video_url, video_mime_type, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            category_id, category_name_cn, category_name_en,
            magnet_uri, magnet_hash, torrent_file_url, size_label, resolution,
            cdn_endpoints, screenshots, categories, tags, status, published_at,
            raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE
                WHEN COALESCE(EXCLUDED.quality_score, 0) >= COALESCE(scraper.chinese_videos.quality_score, 0)
                THEN EXCLUDED.full_video_url
                ELSE scraper.chinese_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.chinese_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.chinese_videos.clean_thumbnail_url),
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.chinese_videos.quality_score, 0)),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.chinese_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.chinese_videos.duration_formatted),
            category_id = COALESCE(EXCLUDED.category_id, scraper.chinese_videos.category_id),
            category_name_cn = COALESCE(EXCLUDED.category_name_cn, scraper.chinese_videos.category_name_cn),
            category_name_en = COALESCE(EXCLUDED.category_name_en, scraper.chinese_videos.category_name_en),
            magnet_uri = COALESCE(EXCLUDED.magnet_uri, scraper.chinese_videos.magnet_uri),
            magnet_hash = COALESCE(EXCLUDED.magnet_hash, scraper.chinese_videos.magnet_hash),
            torrent_file_url = COALESCE(EXCLUDED.torrent_file_url, scraper.chinese_videos.torrent_file_url),
            size_label = COALESCE(EXCLUDED.size_label, scraper.chinese_videos.size_label),
            resolution = COALESCE(EXCLUDED.resolution, scraper.chinese_videos.resolution),
            cdn_endpoints = EXCLUDED.cdn_endpoints,
            screenshots = EXCLUDED.screenshots,
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

GRANT EXECUTE ON FUNCTION public.scraper_upsert_chinese_videos(jsonb) TO service_role;

-- QUERY RPC (content_type aware; playable-URL filter applies to videos only)
CREATE OR REPLACE FUNCTION public.scraper_get_chinese_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.chinese_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT cv.*
    FROM scraper.chinese_videos cv
    WHERE (p_source_site IS NULL OR cv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR cv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR cv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = cv.category_name_en
           OR cv.category_id = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR cv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR cv.title ILIKE '%' || trim(p_search) || '%')
      AND cv.status = 'active'
      AND (cv.content_type <> 'video' OR (cv.full_video_url IS NOT NULL AND cv.full_video_url <> ''))
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN cv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN cv.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN cv.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN cv.scraped_at END ASC NULLS LAST,
        cv.scraped_at DESC,
        cv.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_chinese_videos(
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
    FROM scraper.chinese_videos cv
    WHERE (p_source_site IS NULL OR cv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR cv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR cv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = cv.category_name_en
           OR cv.category_id = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR cv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR cv.title ILIKE '%' || trim(p_search) || '%')
      AND cv.status = 'active'
      AND (cv.content_type <> 'video' OR (cv.full_video_url IS NOT NULL AND cv.full_video_url <> ''));
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_chinese_videos(text, text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_chinese_video_by_id(p_id text)
RETURNS SETOF scraper.chinese_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT cv.*
    FROM scraper.chinese_videos cv
    WHERE (cv.id::text = p_id OR cv.source_post_id = p_id OR cv.slug = p_id)
      AND cv.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_chinese_video_by_id(text) TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
