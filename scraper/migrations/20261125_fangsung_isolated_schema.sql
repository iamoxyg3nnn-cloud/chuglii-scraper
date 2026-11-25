-- ============================================================================
-- FANGSUNG CATEGORY — ISOLATED DATABASE + RPC BRIDGE
-- Same per-site isolation model as chinese_videos / onlyfans_videos.
-- No cross-category dedupe. No fallback to scraper.videos or any other table.
-- Single source site: 'fangsung' (www.fangsung.com). Video-only (static VOD
-- HLS, one rendition per video, no torrents on this site).
-- English titles stored in title_en at ingest (Google Translate), CJK kept
-- in title. Region/actress tags go to tags[]; `region` holds the primary
-- region term (日本AV/台湾/中国AV/...).
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.fangsung_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL DEFAULT 'fangsung',
    source_post_id TEXT NOT NULL,                  -- numeric id from /a片/{id}/{slug}
    content_type TEXT NOT NULL DEFAULT 'video',    -- always 'video' on this site
    title TEXT NOT NULL,                           -- original CJK title
    title_en TEXT,                                 -- English translation (UI display)
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,                           -- HLS m3u8 (bbcdn.xianshipian.com)
    video_mime_type TEXT DEFAULT 'application/x-mpegURL',
    thumbnail_url TEXT,                            -- cdn52.apianmeng.com/kk55/...
    clean_thumbnail_url TEXT,
    quality_label VARCHAR(32) DEFAULT 'HD',
    quality_score INTEGER DEFAULT 720,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    region TEXT,                                   -- primary region term, e.g. '中国AV'
    region_en TEXT,                                -- e.g. 'Chinese AV'
    resolution VARCHAR(32),                        -- probed, e.g. '1174x660'
    categories TEXT[] DEFAULT '{fangsung}',
    tags TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,                      -- site exposes none; stays NULL
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_fangsung_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.fangsung_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_fangsung_videos ON scraper.fangsung_videos;
CREATE POLICY service_role_all_fangsung_videos ON scraper.fangsung_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_fangsung_videos ON scraper.fangsung_videos;
CREATE POLICY anon_select_fangsung_videos ON scraper.fangsung_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_fangsung_quality_score ON scraper.fangsung_videos(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_fangsung_videos_source ON scraper.fangsung_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_fangsung_videos_scraped ON scraper.fangsung_videos(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_fangsung_videos_region ON scraper.fangsung_videos(region);
CREATE INDEX IF NOT EXISTS idx_fangsung_videos_tags ON scraper.fangsung_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_fangsung_videos_categories ON scraper.fangsung_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_fangsung_title_en ON scraper.fangsung_videos(title_en);

-- BATCH UPSERT RPC (first-write-wins for quality fields; listing data refreshes)
CREATE OR REPLACE FUNCTION public.scraper_upsert_fangsung_videos(items jsonb)
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
            COALESCE(x->>'source_site', 'fangsung') AS source_site,
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
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['fangsung']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE(x->>'status', 'active') AS status,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.fangsung_videos (
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
            title_en = COALESCE(EXCLUDED.title_en, scraper.fangsung_videos.title_en),
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.fangsung_videos.full_video_url),
            video_mime_type = EXCLUDED.video_mime_type,
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.fangsung_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.fangsung_videos.clean_thumbnail_url),
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.fangsung_videos.quality_score, 0)),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.fangsung_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.fangsung_videos.duration_formatted),
            region = COALESCE(EXCLUDED.region, scraper.fangsung_videos.region),
            region_en = COALESCE(EXCLUDED.region_en, scraper.fangsung_videos.region_en),
            resolution = COALESCE(EXCLUDED.resolution, scraper.fangsung_videos.resolution),
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

GRANT EXECUTE ON FUNCTION public.scraper_upsert_fangsung_videos(jsonb) TO service_role;

-- QUERY RPC
CREATE OR REPLACE FUNCTION public.scraper_get_fangsung_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.fangsung_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT fv.*
    FROM scraper.fangsung_videos fv
    WHERE (p_source_site IS NULL OR fv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR fv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR fv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = fv.region_en
           OR fv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR fv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR fv.title ILIKE '%' || trim(p_search) || '%'
           OR fv.title_en ILIKE '%' || trim(p_search) || '%')
      AND fv.status = 'active'
      AND (fv.full_video_url IS NOT NULL AND fv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN fv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN fv.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN fv.scraped_at END ASC NULLS LAST,
        fv.scraped_at DESC,
        fv.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_fangsung_videos(text, text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_fangsung_videos(
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
    FROM scraper.fangsung_videos fv
    WHERE (p_source_site IS NULL OR fv.source_site = p_source_site)
      AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR fv.content_type = lower(trim(p_content_type)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR fv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = fv.region_en
           OR fv.region = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR fv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR fv.title ILIKE '%' || trim(p_search) || '%'
           OR fv.title_en ILIKE '%' || trim(p_search) || '%')
      AND fv.status = 'active'
      AND (fv.full_video_url IS NOT NULL AND fv.full_video_url <> '');
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_fangsung_videos(text, text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_fangsung_video_by_id(p_id text)
RETURNS SETOF scraper.fangsung_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT fv.*
    FROM scraper.fangsung_videos fv
    WHERE (fv.id::text = p_id OR fv.source_post_id = p_id OR fv.slug = p_id)
      AND fv.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_fangsung_video_by_id(text) TO anon, authenticated, service_role;

-- PARTIAL UPDATE: English titles (service-role only)
CREATE OR REPLACE FUNCTION public.scraper_update_fangsung_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH upd AS (
        UPDATE scraper.fangsung_videos fv
        SET title_en = x.title_en, updated_at = now()
        FROM jsonb_array_elements(items) AS x
        WHERE fv.id::text = (x->>'id') AND COALESCE(x->>'title_en', '') <> ''
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_fangsung_title_en(jsonb) TO service_role;

-- PARTIAL UPDATE: quality labels (service-role only)
CREATE OR REPLACE FUNCTION public.scraper_update_fangsung_quality(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH upd AS (
        UPDATE scraper.fangsung_videos fv
        SET quality_label = COALESCE(x.quality_label, fv.quality_label),
            quality_score = COALESCE(x.quality_score, fv.quality_score),
            resolution = COALESCE(x.resolution, fv.resolution),
            updated_at = now()
        FROM (
            SELECT (x->>'id') AS id, (x->>'quality_label') AS quality_label,
                   (x->>'quality_score')::int AS quality_score, (x->>'resolution') AS resolution
            FROM jsonb_array_elements(items) AS x
        ) x
        WHERE fv.id::text = x.id
    )
    SELECT COUNT(*) INTO updated_count FROM upd;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_fangsung_quality(jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';
