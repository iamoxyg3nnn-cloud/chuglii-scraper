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
    existing_id uuid;
BEGIN
    FOR v IN SELECT * FROM jsonb_array_elements(items)
    LOOP
        existing_id := NULL;

        -- A. Check if exact full_video_url exists
        IF (v->>'full_video_url') IS NOT NULL AND trim(v->>'full_video_url') <> '' THEN
            SELECT id INTO existing_id
            FROM scraper.videos
            WHERE full_video_url = (v->>'full_video_url')
            LIMIT 1;
        END IF;

        -- B. Check if exact title + duration exists
        IF existing_id IS NULL AND (v->>'duration_seconds')::int > 0 THEN
            SELECT id INTO existing_id
            FROM scraper.videos
            WHERE lower(trim(title)) = lower(trim(v->>'title'))
              AND duration_seconds = (v->>'duration_seconds')::int
            LIMIT 1;
        END IF;

        -- If existing duplicate found, update metrics and skip duplicate row insertion
        IF existing_id IS NOT NULL THEN
            UPDATE scraper.videos SET
                views_count = GREATEST(COALESCE((v->>'views_count')::bigint, 0), scraper.videos.views_count),
                rating_score = COALESCE((v->>'rating_score')::numeric, scraper.videos.rating_score),
                updated_at = now()
            WHERE id = existing_id;
            inserted_count := inserted_count + 1;
            CONTINUE;
        END IF;

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
        scraped_at DESC,
        id DESC
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
      scraped_at DESC,
      id DESC
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


-- ============================================================================
-- 9. ISOLATED MYANMAR VIDEOS SCHEMA & RPC BRIDGE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.myanmar_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,                     -- e.g. 'buumal'
    source_post_id TEXT NOT NULL,                  -- e.g. '6b027810'
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,                           -- Direct R2 MP4 stream URL or embed
    video_mime_type TEXT DEFAULT 'video/mp4',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,
    clean_thumbnail_url TEXT,
    duration_seconds INTEGER,
    duration_formatted VARCHAR(32),
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64),
    rating_score NUMERIC(5,2),
    rating_raw VARCHAR(32),
    categories TEXT[] DEFAULT '{"myanmar"}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_myanmar_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.myanmar_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_myanmar_videos ON scraper.myanmar_videos;
CREATE POLICY service_role_all_myanmar_videos ON scraper.myanmar_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_myanmar_videos ON scraper.myanmar_videos;
CREATE POLICY anon_select_myanmar_videos ON scraper.myanmar_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_myanmar_videos_source ON scraper.myanmar_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_myanmar_videos_published ON scraper.myanmar_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_myanmar_videos_views ON scraper.myanmar_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_myanmar_videos_tags ON scraper.myanmar_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_myanmar_videos_categories ON scraper.myanmar_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_myanmar_videos_actors ON scraper.myanmar_videos USING gin(actors);

-- UPSERT RPC
CREATE OR REPLACE FUNCTION public.scraper_upsert_myanmar_videos(items jsonb)
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
        INSERT INTO scraper.myanmar_videos (
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
            COALESCE(v->>'source_site', 'buumal'),
            v->>'source_post_id',
            COALESCE(v->>'title', 'Untitled Myanmar Video'),
            v->>'slug',
            COALESCE(v->>'detail_url', ''),
            v->>'full_video_url',
            COALESCE(v->>'video_mime_type', 'video/mp4'),
            v->>'trailer_preview_url',
            v->>'thumbnail_url',
            v->>'clean_thumbnail_url',
            (v->>'duration_seconds')::int,
            (v->>'duration_formatted'),
            COALESCE((v->>'views_count')::bigint, 0),
            (v->>'views_raw'),
            (v->>'rating_score')::numeric,
            (v->>'rating_raw'),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(v->'categories', '["myanmar"]'::jsonb))),
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
            full_video_url = COALESCE(EXCLUDED.full_video_url, scraper.myanmar_videos.full_video_url),
            trailer_preview_url = COALESCE(EXCLUDED.trailer_preview_url, scraper.myanmar_videos.trailer_preview_url),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.myanmar_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.myanmar_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.myanmar_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.myanmar_videos.duration_formatted),
            views_count = GREATEST(EXCLUDED.views_count, scraper.myanmar_videos.views_count),
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.myanmar_videos.rating_score),
            rating_raw = EXCLUDED.rating_raw,
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            published_at = COALESCE(EXCLUDED.published_at, scraper.myanmar_videos.published_at),
            raw_payload = EXCLUDED.raw_payload,
            updated_at = now();

        inserted_count := inserted_count + 1;
    END LOOP;

    RETURN jsonb_build_object('success', true, 'count', inserted_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_upsert_myanmar_videos(jsonb) TO service_role;

-- QUERY VIDEOS RPC
CREATE OR REPLACE FUNCTION public.scraper_get_myanmar_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.myanmar_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.myanmar_videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'myanmar')
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
      )
      -- Search Filter
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR (
              SELECT bool_and(
                lower(title) LIKE '%' || word || '%'
                OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
              )
              FROM unnest(string_to_array(lower(trim(p_search)), ' ')) AS word
              WHERE length(word) > 0
          )
      )
      AND status = 'active'
    ORDER BY
      CASE WHEN p_sort = 'views' THEN views_count END DESC NULLS LAST,
      CASE WHEN p_sort = 'rating' THEN rating_score END DESC NULLS LAST,
      CASE WHEN p_sort = 'longest' THEN duration_seconds END DESC NULLS LAST,
      CASE WHEN p_sort = 'oldest' THEN published_at END ASC NULLS LAST,
      published_at DESC NULLS LAST,
      scraped_at DESC,
      id DESC
    LIMIT p_limit OFFSET p_offset;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_myanmar_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT VIDEOS RPC
CREATE OR REPLACE FUNCTION public.scraper_count_myanmar_videos(
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
    SELECT count(*) FROM scraper.myanmar_videos
    WHERE (p_source_site IS NULL OR source_site = p_source_site)
      -- Category Filter
      AND (
          p_category IS NULL 
          OR lower(trim(p_category)) IN ('all', 'myanmar')
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
      )
      -- Search Filter
      AND (
          p_search IS NULL 
          OR trim(p_search) = ''
          OR (
              SELECT bool_and(
                lower(title) LIKE '%' || word || '%'
                OR lower(replace(array_to_string(tags, ' '), '-', ' ')) LIKE '%' || word || '%'
                OR lower(array_to_string(tags, ' ')) LIKE '%' || word || '%'
                OR lower(array_to_string(categories, ' ')) LIKE '%' || word || '%'
              )
              FROM unnest(string_to_array(lower(trim(p_search)), ' ')) AS word
              WHERE length(word) > 0
          )
      )
      AND status = 'active';
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_myanmar_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET SINGLE VIDEO BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_myanmar_video_by_id(p_id uuid)
RETURNS SETOF scraper.myanmar_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT * FROM scraper.myanmar_videos WHERE id = p_id LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_myanmar_video_by_id(uuid) TO anon, authenticated, service_role;

-- GET RELATED VIDEOS RPC
CREATE OR REPLACE FUNCTION public.scraper_get_myanmar_related_videos(
    p_video_id uuid,
    p_limit int DEFAULT 8
)
RETURNS SETOF scraper.myanmar_videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    WITH current_video AS (
        SELECT tags, categories FROM scraper.myanmar_videos WHERE id = p_video_id LIMIT 1
    )
    SELECT mv.*
    FROM scraper.myanmar_videos mv, current_video cv
    WHERE mv.id <> p_video_id
      AND (
        mv.tags && cv.tags
        OR mv.categories && cv.categories
      )
    ORDER BY mv.published_at DESC NULLS LAST, mv.views_count DESC
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_myanmar_related_videos(uuid, int) TO anon, authenticated, service_role;

-- ============================================================================
-- 10. ISOLATED ONLYFANS VIDEOS SCHEMA & RPC BRIDGE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.onlyfans_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_fingerprint TEXT,                    -- SHA256 semantic fingerprint for cross-site deduplication
    source_site TEXT NOT NULL,                     -- e.g. 'internetchicks', 'influencersgonewild', 'sheeshfans', 'pimpbunny'
    source_post_id TEXT NOT NULL,                  -- Original post ID on target site (e.g. '65140')
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,                           -- Default highest-quality stream (1080p MP4 / VOE / KVS)
    video_mime_type TEXT DEFAULT 'embed/iframe',
    trailer_preview_url TEXT,
    thumbnail_url TEXT,                            -- Standard poster/thumbnail
    clean_thumbnail_url TEXT,                      -- Highest resolution / watermark-free thumbnail
    quality_label VARCHAR(32) DEFAULT '1080p Full HD',
    quality_score INTEGER DEFAULT 1080,
    duration_seconds INTEGER DEFAULT 600,
    duration_formatted VARCHAR(32) DEFAULT '10:00',
    views_count BIGINT DEFAULT 0,
    views_raw VARCHAR(64) DEFAULT '0',
    rating_score NUMERIC(5,2) DEFAULT 95.0,
    rating_raw VARCHAR(32) DEFAULT '95%',
    categories TEXT[] DEFAULT '{"onlyfans"}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_onlyfans_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.onlyfans_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_onlyfans_videos ON scraper.onlyfans_videos;
CREATE POLICY service_role_all_onlyfans_videos ON scraper.onlyfans_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_onlyfans_videos ON scraper.onlyfans_videos;
CREATE POLICY anon_select_onlyfans_videos ON scraper.onlyfans_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_onlyfans_canonical_fingerprint ON scraper.onlyfans_videos(canonical_fingerprint);
CREATE INDEX IF NOT EXISTS idx_onlyfans_quality_score ON scraper.onlyfans_videos(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_source ON scraper.onlyfans_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_published ON scraper.onlyfans_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_views ON scraper.onlyfans_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_tags ON scraper.onlyfans_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_categories ON scraper.onlyfans_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_onlyfans_videos_actors ON scraper.onlyfans_videos USING gin(actors);

-- BATCH UPSERT RPC (Quality-First Default Promotion)
CREATE OR REPLACE FUNCTION public.scraper_upsert_onlyfans_videos(items jsonb)
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
            (x->>'canonical_fingerprint') AS canonical_fingerprint,
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
            COALESCE(x->>'quality_label', '1080p Full HD') AS quality_label,
            COALESCE((x->>'quality_score')::int, 1080) AS quality_score,
            COALESCE((x->>'duration_seconds')::int, 600) AS duration_seconds,
            COALESCE(x->>'duration_formatted', '10:00') AS duration_formatted,
            COALESCE((x->>'views_count')::bigint, 0) AS views_count,
            COALESCE(x->>'views_raw', '0') AS views_raw,
            COALESCE((x->>'rating_score')::numeric(5,2), 95.0) AS rating_score,
            COALESCE(x->>'rating_raw', '95%') AS rating_raw,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['onlyfans']::text[]) AS categories,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'tags') elem), ARRAY[]::text[]) AS tags,
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'actors') elem), ARRAY[]::text[]) AS actors,
            COALESCE(x->>'status', 'active') AS status,
            (x->>'published_at')::timestamptz AS published_at,
            COALESCE(x->'raw_payload', '{}'::jsonb) AS raw_payload
        FROM jsonb_array_elements(items) AS x
    ),
    upserted AS (
        INSERT INTO scraper.onlyfans_videos (
            canonical_fingerprint, source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, views_count, views_raw, rating_score,
            rating_raw, categories, tags, actors, status, published_at,
            raw_payload, updated_at
        )
        SELECT
            canonical_fingerprint, source_site, source_post_id, title, slug, detail_url,
            full_video_url, video_mime_type, trailer_preview_url,
            thumbnail_url, clean_thumbnail_url, quality_label, quality_score, duration_seconds,
            duration_formatted, views_count, views_raw, rating_score,
            rating_raw, categories, tags, actors, status, published_at,
            raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            canonical_fingerprint = COALESCE(EXCLUDED.canonical_fingerprint, scraper.onlyfans_videos.canonical_fingerprint),
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            detail_url = EXCLUDED.detail_url,
            full_video_url = CASE 
                WHEN COALESCE(EXCLUDED.quality_score, 0) >= COALESCE(scraper.onlyfans_videos.quality_score, 0)
                THEN EXCLUDED.full_video_url 
                ELSE scraper.onlyfans_videos.full_video_url 
            END,
            video_mime_type = CASE 
                WHEN COALESCE(EXCLUDED.quality_score, 0) >= COALESCE(scraper.onlyfans_videos.quality_score, 0)
                THEN EXCLUDED.video_mime_type 
                ELSE scraper.onlyfans_videos.video_mime_type 
            END,
            quality_label = CASE 
                WHEN COALESCE(EXCLUDED.quality_score, 0) >= COALESCE(scraper.onlyfans_videos.quality_score, 0)
                THEN EXCLUDED.quality_label 
                ELSE scraper.onlyfans_videos.quality_label 
            END,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.onlyfans_videos.quality_score, 0)),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.onlyfans_videos.clean_thumbnail_url),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
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

GRANT EXECUTE ON FUNCTION public.scraper_upsert_onlyfans_videos(jsonb) TO service_role;

-- QUERY RPC
CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.onlyfans_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT ov.*
    FROM scraper.onlyfans_videos ov
    WHERE (p_source_site IS NULL OR ov.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR ov.tags @> ARRAY[lower(trim(p_tag))] OR ov.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR ov.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR ov.title ILIKE '%' || trim(p_search) || '%')
      AND ov.status = 'active'
      AND (ov.full_video_url IS NOT NULL AND ov.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'views' THEN ov.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN ov.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN ov.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN ov.quality_score END DESC NULLS LAST,
        ov.published_at DESC NULLS LAST,
        ov.scraped_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_onlyfans_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_onlyfans_videos(
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
AS $$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM scraper.onlyfans_videos ov
    WHERE (p_source_site IS NULL OR ov.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR ov.tags @> ARRAY[lower(trim(p_tag))] OR ov.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR ov.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR ov.title ILIKE '%' || trim(p_search) || '%')
      AND ov.status = 'active'
      AND (ov.full_video_url IS NOT NULL AND ov.full_video_url <> '');
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_onlyfans_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_onlyfans_video_by_id(p_id text)
RETURNS SETOF scraper.onlyfans_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT ov.*
    FROM scraper.onlyfans_videos ov
    WHERE (ov.id::text = p_id OR ov.source_post_id = p_id OR ov.slug = p_id)
      AND ov.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_onlyfans_video_by_id(text) TO anon, authenticated, service_role;


