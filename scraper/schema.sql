-- ============================================================================
-- ISOLATED SCRAPER DATABASE SCHEMA
-- ============================================================================
-- Completely segregated from 'public' (which holds Chuglii backend/frontend DVR fleet).
-- Schema name: 'scraper'
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS scraper;

-- 1. Security & Role Permissions
-- Only backend service_role has read/write access by default to maintain total isolation.
GRANT USAGE ON SCHEMA scraper TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA scraper GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA scraper GRANT ALL ON SEQUENCES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA scraper GRANT ALL ON FUNCTIONS TO service_role;

-- 2. Scraped Videos Table
CREATE TABLE IF NOT EXISTS scraper.videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,                     -- e.g. 'mmsvibex'
    source_post_id TEXT NOT NULL,                  -- Original post ID on target site (e.g. '31054')
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    
    -- Media URLs
    full_video_url TEXT,                           -- Direct MP4 stream URL
    video_mime_type TEXT DEFAULT 'video/mp4',
    trailer_preview_url TEXT,                      -- Preview trailer MP4
    thumbnail_url TEXT,                            -- Standard poster/thumbnail
    clean_thumbnail_url TEXT,                      -- Clean/watermark-free thumbnail if available
    
    -- Polished Metrics
    duration_seconds INTEGER,                      -- Total length in seconds (e.g. 276)
    duration_formatted VARCHAR(32),                -- Formatted length (e.g. '04:36')
    views_count BIGINT DEFAULT 0,                  -- Integer count (e.g. 1000)
    views_raw VARCHAR(64),                         -- Original scraped view text ('1K')
    rating_score NUMERIC(5,2),                     -- Percentage score (0.00 - 100.00)
    rating_raw VARCHAR(32),                        -- Original scraped rating text ('100%')
    
    -- Taxonomy (Indexed Arrays)
    categories TEXT[] DEFAULT '{}',                -- e.g. ['indian', 'desi']
    tags TEXT[] DEFAULT '{}',                      -- e.g. ['bhabhi', 'teen', 'amateur']
    actors TEXT[] DEFAULT '{}',                    -- e.g. ['sakshi-sexy-chubby-fun']
    
    -- State & Timestamps
    status TEXT DEFAULT 'active',                  -- 'active', 'archived', 'broken_link'
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    
    -- Audit & Raw Backup
    raw_payload JSONB DEFAULT '{}'::jsonb,         -- Full original payload preserved for safety
    
    -- Unique constraint for idempotent upserts
    CONSTRAINT uq_scraper_videos UNIQUE (source_site, source_post_id)
);

-- 3. Scraper Crawl Execution Logs
CREATE TABLE IF NOT EXISTS scraper.crawl_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,
    filter_mode VARCHAR(64) DEFAULT 'latest',
    pages_crawled INTEGER DEFAULT 0,
    items_scraped INTEGER DEFAULT 0,
    items_inserted INTEGER DEFAULT 0,
    items_updated INTEGER DEFAULT 0,
    status VARCHAR(32) DEFAULT 'running',          -- 'running', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ
);

-- 4. Enable Row Level Security (RLS)
ALTER TABLE scraper.videos ENABLE ROW LEVEL SECURITY;
ALTER TABLE scraper.crawl_runs ENABLE ROW LEVEL SECURITY;

-- Service role bypasses all policies; explicit policy for service_role
DROP POLICY IF EXISTS "service_role_all_videos" ON scraper.videos;
CREATE POLICY "service_role_all_videos" ON scraper.videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "service_role_all_crawl_runs" ON scraper.crawl_runs;
CREATE POLICY "service_role_all_crawl_runs" ON scraper.crawl_runs
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 5. Performance Indexes
CREATE INDEX IF NOT EXISTS idx_scraper_videos_source ON scraper.videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_published_at ON scraper.videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_views_count ON scraper.videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_duration ON scraper.videos(duration_seconds);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_scraped_at ON scraper.videos(scraped_at DESC);

-- GIN Indexes for array containment queries: WHERE tags @> ARRAY['desi']
CREATE INDEX IF NOT EXISTS idx_scraper_videos_tags ON scraper.videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_categories ON scraper.videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_scraper_videos_actors ON scraper.videos USING gin(actors);

-- Optional trigram index on title if extensions.pg_trgm exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS idx_scraper_videos_title_trgm ON scraper.videos USING gin(title extensions.gin_trgm_ops);
    END IF;
END $$;


-- 6. RPC Bridge Functions (allows PostgREST client access to the segregated scraper schema)
CREATE OR REPLACE FUNCTION public.scraper_upsert_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    v jsonb;
    inserted_count int := 0;
BEGIN
    FOR v IN SELECT * FROM jsonb_array_elements(items)
    LOOP
        INSERT INTO scraper.videos (
            source_site,
            source_post_id,
            title,
            slug,
            detail_url,
            full_video_url,
            video_mime_type,
            trailer_preview_url,
            thumbnail_url,
            clean_thumbnail_url,
            duration_seconds,
            duration_formatted,
            views_count,
            views_raw,
            rating_score,
            rating_raw,
            categories,
            tags,
            actors,
            published_at,
            raw_payload,
            updated_at
        ) VALUES (
            (v->>'source_site'),
            (v->>'source_post_id'),
            (v->>'title'),
            (v->>'slug'),
            (v->>'detail_url'),
            (v->>'full_video_url'),
            COALESCE(v->>'video_mime_type', 'video/mp4'),
            (v->>'trailer_preview_url'),
            (v->>'thumbnail_url'),
            (v->>'clean_thumbnail_url'),
            (v->>'duration_seconds')::int,
            (v->>'duration_formatted'),
            COALESCE((v->>'views_count')::bigint, 0),
            (v->>'views_raw'),
            (v->>'rating_score')::numeric,
            (v->>'rating_raw'),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'categories', '[]'::jsonb))),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'tags', '[]'::jsonb))),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'actors', '[]'::jsonb))),
            CASE WHEN (v->>'published_at') IS NOT NULL AND (v->>'published_at') <> '' 
                 THEN (v->>'published_at')::timestamptz ELSE NULL END,
            COALESCE(v->'raw_payload', '{}'::jsonb),
            now()
        )
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.videos.full_video_url),
            trailer_preview_url = COALESCE(EXCLUDED.trailer_preview_url, scraper.videos.trailer_preview_url),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.videos.duration_formatted),
            views_count = GREATEST(EXCLUDED.views_count, scraper.videos.views_count),
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.videos.rating_score),
            rating_raw = EXCLUDED.rating_raw,
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            published_at = COALESCE(EXCLUDED.published_at, scraper.videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now();

        inserted_count := inserted_count + 1;
    END LOOP;

    RETURN jsonb_build_object('success', true, 'count', inserted_count);
END;
$$;

REVOKE ALL ON FUNCTION public.scraper_upsert_videos(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.scraper_upsert_videos(jsonb) TO service_role;

-- Query helper
CREATE OR REPLACE FUNCTION public.scraper_get_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_limit int DEFAULT 20,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      AND (p_tag IS NULL OR tags @> ARRAY[p_tag])
    ORDER BY published_at DESC NULLS LAST, scraped_at DESC
    LIMIT p_limit OFFSET p_offset;
$$;

REVOKE ALL ON FUNCTION public.scraper_get_videos(text, text, int, int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.scraper_get_videos(text, text, int, int) TO anon, authenticated, service_role;


-- Stats Helper Function
CREATE OR REPLACE FUNCTION public.scraper_get_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT jsonb_build_object(
        'total_videos', count(*),
        'with_streams', count(full_video_url),
        'unique_sources', count(DISTINCT source_site)
    ) FROM scraper.videos;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_stats() TO anon, authenticated, service_role;


-- 7. Advanced Scraper Query & Navigation Bridge Functions

-- Get videos with Sort, Search, Tag, and Category filters
CREATE OR REPLACE FUNCTION public.scraper_get_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'viral-mms')
          OR lower(trim(p_category)) = ANY(categories)
          OR replace(lower(trim(p_category)), ' ', '-') = ANY(categories)
      )
      -- Tag / Section Filter
      AND (
          p_tag IS NULL 
          OR lower(trim(p_tag)) = 'all'
          OR lower(trim(p_tag)) = ANY(tags)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(tags)
          OR lower(trim(p_tag)) = ANY(categories)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(categories)
          OR (lower(trim(p_tag)) IN ('bangladeshi', 'bangladesi') AND ('bangladesi' = ANY(categories) OR 'bangladeshi' = ANY(tags)))
          OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || lower(replace(trim(p_tag), '-', ' ')) || '%'
      )
      -- Multi-Word Search
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR NOT EXISTS (
              SELECT 1
              FROM unnest(string_to_array(trim(regexp_replace(lower(p_search), '\s+', ' ', 'g')), ' ')) AS word
              WHERE word <> ''
                AND NOT (
                  lower(title) LIKE '%' || word || '%'
                  OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
                  OR lower(coalesce(array_to_string(actors, ' '), '')) LIKE '%' || word || '%'
                )
          )
      )
    ORDER BY
        CASE WHEN p_sort = 'views' THEN views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'longest' THEN duration_seconds END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN published_at END ASC NULLS LAST,
        published_at DESC NULLS LAST,
        scraped_at DESC
    LIMIT p_limit OFFSET p_offset;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- Count videos for accurate pagination
CREATE OR REPLACE FUNCTION public.scraper_count_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT count(*) FROM scraper.videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'viral-mms')
          OR lower(trim(p_category)) = ANY(categories)
          OR replace(lower(trim(p_category)), ' ', '-') = ANY(categories)
      )
      -- Tag / Section Filter
      AND (
          p_tag IS NULL 
          OR lower(trim(p_tag)) = 'all'
          OR lower(trim(p_tag)) = ANY(tags)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(tags)
          OR lower(trim(p_tag)) = ANY(categories)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(categories)
          OR (lower(trim(p_tag)) IN ('bangladeshi', 'bangladesi') AND ('bangladesi' = ANY(categories) OR 'bangladeshi' = ANY(tags)))
          OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || lower(replace(trim(p_tag), '-', ' ')) || '%'
      )
      -- Multi-Word Search
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR NOT EXISTS (
              SELECT 1
              FROM unnest(string_to_array(trim(regexp_replace(lower(p_search), '\s+', ' ', 'g')), ' ')) AS word
              WHERE word <> ''
                AND NOT (
                  lower(title) LIKE '%' || word || '%'
                  OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
                  OR lower(coalesce(array_to_string(actors, ' '), '')) LIKE '%' || word || '%'
                )
          )
      );
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_videos(text, text, text, text) TO anon, authenticated, service_role;

-- Get single video by UUID
CREATE OR REPLACE FUNCTION public.scraper_get_video_by_id(p_id uuid)
RETURNS SETOF scraper.videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.videos
    WHERE id = p_id
    LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_video_by_id(uuid) TO anon, authenticated, service_role;

-- Get related videos based on matching tags or category
CREATE OR REPLACE FUNCTION public.scraper_get_related_videos(
    p_video_id uuid,
    p_limit int DEFAULT 12
)
RETURNS SETOF scraper.videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    WITH target AS (
        SELECT tags, categories FROM scraper.videos WHERE id = p_video_id LIMIT 1
    )
    SELECT v.* FROM scraper.videos v, target t
    WHERE v.id <> p_video_id
      AND (v.tags && t.tags OR v.categories && t.categories)
    ORDER BY v.views_count DESC, v.published_at DESC NULLS LAST
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_related_videos(uuid, int) TO anon, authenticated, service_role;

-- ============================================================================
-- 8. ISOLATED JAPANESE VIDEOS SCHEMA & RPC BRIDGE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.japanese_videos (
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
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64),
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{"japanese"}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_japanese_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.japanese_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_japanese_videos ON scraper.japanese_videos;
CREATE POLICY service_role_all_japanese_videos ON scraper.japanese_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE INDEX IF NOT EXISTS idx_japanese_videos_source ON scraper.japanese_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_japanese_videos_published ON scraper.japanese_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_japanese_videos_views ON scraper.japanese_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_japanese_videos_tags ON scraper.japanese_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_japanese_videos_categories ON scraper.japanese_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_japanese_videos_actors ON scraper.japanese_videos USING gin(actors);

CREATE OR REPLACE FUNCTION public.scraper_upsert_japanese_videos(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    v jsonb;
    inserted_count int := 0;
BEGIN
    FOR v IN SELECT * FROM jsonb_array_elements(items)
    LOOP
        INSERT INTO scraper.japanese_videos (
            source_site,
            source_post_id,
            title,
            slug,
            detail_url,
            full_video_url,
            video_mime_type,
            trailer_preview_url,
            thumbnail_url,
            clean_thumbnail_url,
            duration_seconds,
            duration_formatted,
            views_count,
            views_raw,
            rating_score,
            rating_raw,
            categories,
            tags,
            actors,
            published_at,
            raw_payload,
            updated_at
        ) VALUES (
            (v->>'source_site'),
            (v->>'source_post_id'),
            (v->>'title'),
            (v->>'slug'),
            (v->>'detail_url'),
            (v->>'full_video_url'),
            COALESCE(v->>'video_mime_type', 'embed/iframe'),
            (v->>'trailer_preview_url'),
            (v->>'thumbnail_url'),
            (v->>'clean_thumbnail_url'),
            (v->>'duration_seconds')::int,
            (v->>'duration_formatted'),
            COALESCE((v->>'views_count')::bigint, 0),
            (v->>'views_raw'),
            (v->>'rating_score')::numeric,
            (v->>'rating_raw'),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'categories', '["japanese"]'::jsonb))),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'tags', '[]'::jsonb))),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'actors', '[]'::jsonb))),
            CASE WHEN (v->>'published_at') IS NOT NULL AND (v->>'published_at') <> '' 
                 THEN (v->>'published_at')::timestamptz ELSE NULL END,
            COALESCE(v->'raw_payload', '{}'::jsonb),
            now()
        )
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.japanese_videos.full_video_url),
            trailer_preview_url = COALESCE(EXCLUDED.trailer_preview_url, scraper.japanese_videos.trailer_preview_url),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.japanese_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.japanese_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.japanese_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.japanese_videos.duration_formatted),
            views_count = GREATEST(EXCLUDED.views_count, scraper.japanese_videos.views_count),
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.japanese_videos.rating_score),
            rating_raw = EXCLUDED.rating_raw,
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            published_at = COALESCE(EXCLUDED.published_at, scraper.japanese_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now();

        inserted_count := inserted_count + 1;
    END LOOP;

    RETURN jsonb_build_object('success', true, 'count', inserted_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_japanese_videos(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.scraper_get_japanese_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.japanese_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.japanese_videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'japanese')
          OR lower(trim(p_category)) = ANY(categories)
          OR replace(lower(trim(p_category)), ' ', '-') = ANY(categories)
      )
      -- Tag Filter
      AND (
          p_tag IS NULL 
          OR lower(trim(p_tag)) = 'all'
          OR lower(trim(p_tag)) = ANY(tags)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(tags)
          OR lower(trim(p_tag)) = ANY(categories)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(categories)
          OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || lower(replace(trim(p_tag), '-', ' ')) || '%'
      )
      -- Multi-Word Search (maker codes like FC2, SNOS, titles, tags, actors)
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR NOT EXISTS (
              SELECT 1
              FROM unnest(string_to_array(trim(regexp_replace(lower(p_search), '\s+', ' ', 'g')), ' ')) AS word
              WHERE word <> ''
                AND NOT (
                  lower(title) LIKE '%' || word || '%'
                  OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
                  OR lower(coalesce(array_to_string(actors, ' '), '')) LIKE '%' || word || '%'
                )
          )
      )
    ORDER BY
      CASE WHEN p_sort = 'views' THEN views_count END DESC NULLS LAST,
      CASE WHEN p_sort = 'rating' THEN rating_score END DESC NULLS LAST,
      CASE WHEN p_sort = 'longest' THEN duration_seconds END DESC NULLS LAST,
      CASE WHEN p_sort = 'oldest' THEN published_at END ASC NULLS LAST,
      published_at DESC NULLS LAST,
      scraped_at DESC
    LIMIT p_limit OFFSET p_offset;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_japanese_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.scraper_count_japanese_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL
)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT count(*) FROM scraper.japanese_videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'japanese')
          OR lower(trim(p_category)) = ANY(categories)
          OR replace(lower(trim(p_category)), ' ', '-') = ANY(categories)
      )
      -- Tag Filter
      AND (
          p_tag IS NULL 
          OR lower(trim(p_tag)) = 'all'
          OR lower(trim(p_tag)) = ANY(tags)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(tags)
          OR lower(trim(p_tag)) = ANY(categories)
          OR replace(lower(trim(p_tag)), ' ', '-') = ANY(categories)
          OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || lower(replace(trim(p_tag), '-', ' ')) || '%'
      )
      -- Multi-Word Search
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR NOT EXISTS (
              SELECT 1
              FROM unnest(string_to_array(trim(regexp_replace(lower(p_search), '\s+', ' ', 'g')), ' ')) AS word
              WHERE word <> ''
                AND NOT (
                  lower(title) LIKE '%' || word || '%'
                  OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                  OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
                  OR lower(coalesce(array_to_string(actors, ' '), '')) LIKE '%' || word || '%'
                )
          )
      );
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_japanese_videos(text, text, text, text) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.scraper_get_japanese_video_by_id(p_id uuid)
RETURNS SETOF scraper.japanese_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.japanese_videos WHERE id = p_id LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_japanese_video_by_id(uuid) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.scraper_get_japanese_related_videos(
    p_video_id uuid,
    p_limit int DEFAULT 8
)
RETURNS SETOF scraper.japanese_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    WITH current_video AS (
        SELECT tags, categories, actors FROM scraper.japanese_videos WHERE id = p_video_id LIMIT 1
    )
    SELECT jv.*
    FROM scraper.japanese_videos jv, current_video cv
    WHERE jv.id <> p_video_id
      AND (
        jv.tags && cv.tags
        OR jv.categories && cv.categories
        OR jv.actors && cv.actors
      )
    ORDER BY jv.published_at DESC NULLS LAST, jv.views_count DESC
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_japanese_related_videos(uuid, int) TO anon, authenticated, service_role;
