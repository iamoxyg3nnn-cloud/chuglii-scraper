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
        CASE WHEN p_sort = 'newest' THEN scraped_at END DESC NULLS LAST,
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
    p_limit int DEFAULT 8
)
RETURNS SETOF scraper.videos
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    WITH target AS (
        SELECT
            tags, categories, actors, source_site,
            lower(btrim(regexp_replace(coalesce(title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.videos
        WHERE id = p_video_id
        LIMIT 1
    ),
    tag_freq AS (
        SELECT t_tag AS tag, count(*)::numeric AS n
        FROM scraper.videos v, target t, unnest(t.tags) t_tag
        WHERE t_tag = ANY (v.tags)
        GROUP BY t_tag
    ),
    category_freq AS (
        SELECT t_category AS category, count(*)::numeric AS n
        FROM scraper.videos v, target t, unnest(t.categories) t_category
        WHERE t_category = ANY (v.categories)
        GROUP BY t_category
    ),
    scored AS (
        SELECT
            v AS video,
            LEAST(12.0,
                (SELECT coalesce(sum(LEAST(4.0, GREATEST(1.0, 300.0 / f.n))), 0)
                 FROM unnest(v.tags) x JOIN tag_freq f ON f.tag = x)
              + (SELECT coalesce(sum(LEAST(3.0, GREATEST(1.0, 150.0 / f.n))), 0)
                 FROM unnest(v.categories) x JOIN category_freq f ON f.category = x)
              + (CASE WHEN v.actors && (SELECT actors FROM target) THEN 3.0 ELSE 0 END)
              + (CASE WHEN v.source_site = (SELECT source_site FROM target) THEN 1.0 ELSE 0 END)
            ) AS relevance,
            lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.videos v, target t
        WHERE v.id <> p_video_id
          AND (t.title_key = '' OR lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) <> t.title_key)
    ),
    ranked AS (
        SELECT s.*,
               row_number() OVER (
                   PARTITION BY coalesce(nullif(s.title_key, ''), (s.video).id::text)
                   ORDER BY s.relevance DESC, (s.video).scraped_at DESC,
                            (s.video).views_count DESC, (s.video).id DESC
               ) AS dedupe_rank
        FROM scored s
    )
    SELECT (r.video).*
    FROM ranked r
    WHERE r.dedupe_rank = 1
    ORDER BY r.relevance DESC, (r.video).scraped_at DESC,
             (r.video).views_count DESC, (r.video).id DESC
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
      CASE WHEN p_sort = 'newest' THEN scraped_at END DESC NULLS LAST,
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
    WITH target AS (
        SELECT
            tags, categories, actors, source_site,
            lower(btrim(regexp_replace(coalesce(title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.japanese_videos
        WHERE id = p_video_id
        LIMIT 1
    ),
    tag_freq AS (
        SELECT t_tag AS tag, count(*)::numeric AS n
        FROM scraper.japanese_videos v, target t, unnest(t.tags) t_tag
        WHERE t_tag = ANY (v.tags)
        GROUP BY t_tag
    ),
    category_freq AS (
        SELECT t_category AS category, count(*)::numeric AS n
        FROM scraper.japanese_videos v, target t, unnest(t.categories) t_category
        WHERE t_category = ANY (v.categories)
        GROUP BY t_category
    ),
    scored AS (
        SELECT
            v AS video,
            LEAST(12.0,
                (SELECT coalesce(sum(LEAST(4.0, GREATEST(1.0, 300.0 / f.n))), 0)
                 FROM unnest(v.tags) x JOIN tag_freq f ON f.tag = x)
              + (SELECT coalesce(sum(LEAST(3.0, GREATEST(1.0, 150.0 / f.n))), 0)
                 FROM unnest(v.categories) x JOIN category_freq f ON f.category = x)
              + (CASE WHEN v.actors && (SELECT actors FROM target) THEN 3.0 ELSE 0 END)
              + (CASE WHEN v.source_site = (SELECT source_site FROM target) THEN 1.0 ELSE 0 END)
            ) AS relevance,
            lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.japanese_videos v, target t
        WHERE v.id <> p_video_id
          AND (t.title_key = '' OR lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) <> t.title_key)
    ),
    ranked AS (
        SELECT s.*,
               row_number() OVER (
                   PARTITION BY coalesce(nullif(s.title_key, ''), (s.video).id::text)
                   ORDER BY s.relevance DESC, (s.video).scraped_at DESC,
                            (s.video).views_count DESC, (s.video).id DESC
               ) AS dedupe_rank
        FROM scored s
    )
    SELECT (r.video).*
    FROM ranked r
    WHERE r.dedupe_rank = 1
    ORDER BY r.relevance DESC, (r.video).scraped_at DESC,
             (r.video).views_count DESC, (r.video).id DESC
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
      CASE WHEN p_sort = 'newest' THEN scraped_at END DESC NULLS LAST,
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
    WITH target AS (
        SELECT
            tags, categories, actors, source_site,
            lower(btrim(regexp_replace(coalesce(title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.myanmar_videos
        WHERE id = p_video_id
        LIMIT 1
    ),
    tag_freq AS (
        SELECT t_tag AS tag, count(*)::numeric AS n
        FROM scraper.myanmar_videos v, target t, unnest(t.tags) t_tag
        WHERE t_tag = ANY (v.tags)
        GROUP BY t_tag
    ),
    category_freq AS (
        SELECT t_category AS category, count(*)::numeric AS n
        FROM scraper.myanmar_videos v, target t, unnest(t.categories) t_category
        WHERE t_category = ANY (v.categories)
        GROUP BY t_category
    ),
    scored AS (
        SELECT
            v AS video,
            LEAST(12.0,
                (SELECT coalesce(sum(LEAST(4.0, GREATEST(1.0, 300.0 / f.n))), 0)
                 FROM unnest(v.tags) x JOIN tag_freq f ON f.tag = x)
              + (SELECT coalesce(sum(LEAST(3.0, GREATEST(1.0, 150.0 / f.n))), 0)
                 FROM unnest(v.categories) x JOIN category_freq f ON f.category = x)
              + (CASE WHEN v.actors && (SELECT actors FROM target) THEN 3.0 ELSE 0 END)
              + (CASE WHEN v.source_site = (SELECT source_site FROM target) THEN 1.0 ELSE 0 END)
            ) AS relevance,
            lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) AS title_key
        FROM scraper.myanmar_videos v, target t
        WHERE v.id <> p_video_id
          AND (t.title_key = '' OR lower(btrim(regexp_replace(coalesce(v.title, ''), '[^[:alnum:]]+', ' ', 'g'))) <> t.title_key)
    ),
    ranked AS (
        SELECT s.*,
               row_number() OVER (
                   PARTITION BY coalesce(nullif(s.title_key, ''), (s.video).id::text)
                   ORDER BY s.relevance DESC, (s.video).scraped_at DESC,
                            (s.video).views_count DESC, (s.video).id DESC
               ) AS dedupe_rank
        FROM scored s
    )
    SELECT (r.video).*
    FROM ranked r
    WHERE r.dedupe_rank = 1
    ORDER BY r.relevance DESC, (r.video).scraped_at DESC,
             (r.video).views_count DESC, (r.video).id DESC
    LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_myanmar_related_videos(uuid, int) TO anon, authenticated, service_role;

-- ============================================================================
-- 10. ISOLATED ONLYFANS VIDEOS SCHEMA & RPC BRIDGE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.onlyfans_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_fingerprint TEXT,                    -- SHA256 semantic fingerprint for cross-site deduplication
    source_site TEXT NOT NULL,                     -- e.g. 'internetchicks', 'influencersgonewild', 'sheeshfans'
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
        CASE WHEN p_sort = 'newest' THEN ov.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN ov.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN ov.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN ov.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN ov.quality_score END DESC NULLS LAST,
        ov.published_at DESC NULLS LAST,
        ov.scraped_at DESC,
        ov.id DESC
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



-- ============================================================================
-- 11. ISOLATED VOYEUR HOUSE VIDEOS SCHEMA & RPC BRIDGE (multisite-ready)
-- ============================================================================

-- ============================================================================
-- VOYEUR HOUSE — ISOLATED DATABASE + MULTISITE RPC BRIDGE
-- Applies the same per-site isolation model used for OnlyFans.
-- No cross-category dedupe. No fallback to scraper.videos or any other table.
-- Source sites are represented by `source_site` (first: 'voyeurhouseorg').
-- Dead / unavailable videos are stored but hidden by the public read RPCs via
-- `status` and `dead_reason`, with `last_checked_at` for slow retry cadence.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.voyeurhouse_videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_site TEXT NOT NULL,                     -- e.g. 'voyeurhouseorg'
    source_post_id TEXT NOT NULL,                  -- trailing code from video_<id>.html
    title TEXT NOT NULL,
    slug TEXT,
    detail_url TEXT NOT NULL,
    full_video_url TEXT,                           -- direct MP4 if resolved, else embed URL
    video_mime_type TEXT DEFAULT 'embed/iframe',   -- 'video/mp4' or 'embed/iframe'
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
    categories TEXT[] DEFAULT '{"voyeurhouse"}',
    tags TEXT[] DEFAULT '{}',
    actors TEXT[] DEFAULT '{}',
    status TEXT DEFAULT 'active',                  -- active | dead | quarantined
    dead_reason TEXT,
    last_checked_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    raw_payload JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT uq_voyeurhouse_videos UNIQUE (source_site, source_post_id)
);

ALTER TABLE scraper.voyeurhouse_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS service_role_all_voyeurhouse_videos ON scraper.voyeurhouse_videos;
CREATE POLICY service_role_all_voyeurhouse_videos ON scraper.voyeurhouse_videos
    FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS anon_select_voyeurhouse_videos ON scraper.voyeurhouse_videos;
CREATE POLICY anon_select_voyeurhouse_videos ON scraper.voyeurhouse_videos
    FOR SELECT TO anon, authenticated USING (status = 'active');

CREATE INDEX IF NOT EXISTS idx_vh_videos_source ON scraper.voyeurhouse_videos(source_site, source_post_id);
CREATE INDEX IF NOT EXISTS idx_vh_videos_status ON scraper.voyeurhouse_videos(status);
CREATE INDEX IF NOT EXISTS idx_vh_videos_published ON scraper.voyeurhouse_videos(published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_vh_videos_views ON scraper.voyeurhouse_videos(views_count DESC);
CREATE INDEX IF NOT EXISTS idx_vh_videos_tags ON scraper.voyeurhouse_videos USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_vh_videos_categories ON scraper.voyeurhouse_videos USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_vh_videos_actors ON scraper.voyeurhouse_videos USING gin(actors);

-- BATCH UPSERT RPC (per-site isolation; revive dead rows when a stream is back)
CREATE OR REPLACE FUNCTION public.scraper_upsert_voyeurhouse_videos(items jsonb)
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
            COALESCE((SELECT array_agg(elem::text) FROM jsonb_array_elements_text(x->'categories') elem), ARRAY['voyeurhouse']::text[]) AS categories,
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
        INSERT INTO scraper.voyeurhouse_videos (
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
                ELSE scraper.voyeurhouse_videos.full_video_url
            END,
            video_mime_type = EXCLUDED.video_mime_type,
            quality_label = EXCLUDED.quality_label,
            quality_score = GREATEST(COALESCE(EXCLUDED.quality_score, 0), COALESCE(scraper.voyeurhouse_videos.quality_score, 0)),
            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, scraper.voyeurhouse_videos.thumbnail_url),
            clean_thumbnail_url = COALESCE(EXCLUDED.clean_thumbnail_url, scraper.voyeurhouse_videos.clean_thumbnail_url),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, scraper.voyeurhouse_videos.duration_seconds),
            duration_formatted = COALESCE(EXCLUDED.duration_formatted, scraper.voyeurhouse_videos.duration_formatted),
            views_count = EXCLUDED.views_count,
            views_raw = EXCLUDED.views_raw,
            rating_score = COALESCE(EXCLUDED.rating_score, scraper.voyeurhouse_videos.rating_score),
            rating_raw = COALESCE(EXCLUDED.rating_raw, scraper.voyeurhouse_videos.rating_raw),
            categories = EXCLUDED.categories,
            tags = EXCLUDED.tags,
            actors = EXCLUDED.actors,
            status = EXCLUDED.status,
            dead_reason = EXCLUDED.dead_reason,
            last_checked_at = EXCLUDED.last_checked_at,
            published_at = COALESCE(EXCLUDED.published_at, scraper.voyeurhouse_videos.published_at),
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

GRANT EXECUTE ON FUNCTION public.scraper_upsert_voyeurhouse_videos(jsonb) TO service_role;

-- QUERY RPC
CREATE OR REPLACE FUNCTION public.scraper_get_voyeurhouse_videos(
    p_source_site text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0
)
RETURNS SETOF scraper.voyeurhouse_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.voyeurhouse_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '')
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN vv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC,
        vv.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_voyeurhouse_videos(text, text, text, text, text, int, int) TO anon, authenticated, service_role;

-- COUNT RPC
CREATE OR REPLACE FUNCTION public.scraper_count_voyeurhouse_videos(
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
    FROM scraper.voyeurhouse_videos vv
    WHERE (p_source_site IS NULL OR lower(trim(p_source_site)) = 'all' OR vv.source_site = p_source_site)
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR vv.tags @> ARRAY[lower(trim(p_tag))] OR vv.actors @> ARRAY[trim(p_tag)])
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR vv.categories @> ARRAY[lower(trim(p_category))])
      AND (p_search IS NULL OR vv.title ILIKE '%' || trim(p_search) || '%')
      AND vv.status = 'active'
      AND (vv.full_video_url IS NOT NULL AND vv.full_video_url <> '');
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_voyeurhouse_videos(text, text, text, text) TO anon, authenticated, service_role;

-- GET BY ID RPC
CREATE OR REPLACE FUNCTION public.scraper_get_voyeurhouse_video_by_id(p_id text)
RETURNS SETOF scraper.voyeurhouse_videos
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    SELECT vv.*
    FROM scraper.voyeurhouse_videos vv
    WHERE (vv.id::text = p_id OR vv.source_post_id = p_id OR vv.slug = p_id)
      AND vv.status = 'active'
    LIMIT 1;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_voyeurhouse_video_by_id(text) TO anon, authenticated, service_role;

-- OPERATIONAL RPC: quarantine dead videos discovered by re-checks
CREATE OR REPLACE FUNCTION public.scraper_mark_voyeurhouse_dead(p_site text, p_post_id text, p_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    UPDATE scraper.voyeurhouse_videos
       SET status = 'dead',
           dead_reason = COALESCE(NULLIF(p_reason, ''), 'dead'),
           last_checked_at = now(),
           updated_at = now()
     WHERE source_site = p_site AND source_post_id = p_post_id;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_mark_voyeurhouse_dead(text, text, text) TO service_role;

-- ============================================================================
-- END OF VOYEUR HOUSE ISOLATED SCHEMA
-- ============================================================================

-- ============================================================================
-- VOYEUR HOUSE (AVS) — FOUR PHYSICALLY ISOLATED SOURCE DATABASES
-- hotscopes.net/videos/voyeur-house, reallifecam.to/videos,
-- hotscopes.net/videos/spygasm?o=mr, hotscopes.net/videos/reallifecam?o=mr
-- Each source has its own table + RPC family. No cross-source data sharing.
-- ============================================================================


-- ============================================================================
-- VOYEUR HOUSE (AVS) — hotscopesvoyeurhouse — ISOLATED DATABASE + RPC BRIDGE
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
        CASE WHEN p_sort = 'newest' THEN vv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC,
        vv.id DESC
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
-- VOYEUR HOUSE (AVS) — reallifecamto — ISOLATED DATABASE + RPC BRIDGE
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
        CASE WHEN p_sort = 'newest' THEN vv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC,
        vv.id DESC
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
-- VOYEUR HOUSE (AVS) — hotscopesspygasm — ISOLATED DATABASE + RPC BRIDGE
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
        CASE WHEN p_sort = 'newest' THEN vv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC,
        vv.id DESC
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
-- VOYEUR HOUSE (AVS) — hotscopesreallifecam — ISOLATED DATABASE + RPC BRIDGE
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
        CASE WHEN p_sort = 'newest' THEN vv.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC,
        vv.id DESC
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

-- ============================================================================
-- SHARED SCRAPER PROGRESS & DEDUPE LEDGER
-- Canonical mirror of supabase migrations-app/014 and 015.  A fresh schema
-- apply must create these tables/functions: every scraper pipeline reads and
-- writes its cursor and known IDs through these RPCs.
-- ============================================================================

CREATE TABLE IF NOT EXISTS scraper.checkpoints (
    key TEXT PRIMARY KEY,
    state JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc'::text, now())
);

CREATE TABLE IF NOT EXISTS scraper.known_post_ids (
    site          TEXT        NOT NULL,
    post_id       TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'seen',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc'::text, now()),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT timezone('utc'::text, now()),
    PRIMARY KEY (site, post_id)
);

CREATE INDEX IF NOT EXISTS known_post_ids_site_last_seen_idx
    ON scraper.known_post_ids (site, last_seen_at DESC);

GRANT USAGE ON SCHEMA scraper TO anon, authenticated, service_role;
GRANT ALL ON scraper.checkpoints TO anon, authenticated, service_role;
GRANT ALL ON scraper.known_post_ids TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.scraper_get_checkpoint(p_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_state JSONB;
BEGIN
    SELECT state INTO v_state FROM scraper.checkpoints WHERE key = p_key;
    RETURN COALESCE(v_state, '{}'::jsonb);
END;
$func$;

CREATE OR REPLACE FUNCTION public.scraper_set_checkpoint(p_key TEXT, p_state JSONB)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
BEGIN
    INSERT INTO scraper.checkpoints (key, state, updated_at)
    VALUES (p_key, p_state, timezone('utc'::text, now()))
    ON CONFLICT (key) DO UPDATE
       SET state = EXCLUDED.state,
           updated_at = timezone('utc'::text, now());
    RETURN jsonb_build_object('success', true);
END;
$func$;

CREATE OR REPLACE FUNCTION public.scraper_add_known_post_ids(
    p_site   TEXT,
    p_ids    JSONB,
    p_status TEXT DEFAULT 'seen'
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_site   TEXT;
    v_status TEXT;
    v_ids    TEXT[];
BEGIN
    v_site := lower(btrim(COALESCE(p_site, '')));
    v_status := lower(btrim(COALESCE(p_status, 'seen')));
    IF v_site = '' THEN
        RETURN jsonb_build_object('success', false, 'error', 'missing site');
    END IF;

    SELECT COALESCE(array_agg(DISTINCT btrim(x)), '{}'::TEXT[])
      INTO v_ids
      FROM jsonb_array_elements_text(COALESCE(p_ids, '[]'::JSONB)) AS x
     WHERE btrim(x) <> '';

    INSERT INTO scraper.known_post_ids (site, post_id, status, last_seen_at)
    SELECT v_site, u, v_status, timezone('utc'::text, now())
      FROM unnest(v_ids) AS u
    ON CONFLICT (site, post_id) DO UPDATE
       SET status = EXCLUDED.status,
           last_seen_at = timezone('utc'::text, now());

    RETURN jsonb_build_object('success', true, 'count', cardinality(v_ids));
END;
$func$;

CREATE OR REPLACE FUNCTION public.scraper_get_known_post_ids(
    p_site  TEXT,
    p_limit INT DEFAULT 100000
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $func$
DECLARE
    v_site    TEXT := lower(btrim(COALESCE(p_site, '')));
    v_limit   INT := GREATEST(1, LEAST(COALESCE(p_limit, 100000), 200000));
    v_table   TEXT;
    v_filter  TEXT := '';
    v_legacy  JSONB := '[]'::JSONB;
    v_ledger  JSONB := '[]'::JSONB;
    v_result  JSONB;
BEGIN
    v_table := CASE v_site
        WHEN 'viral-mms'              THEN 'videos'
        WHEN 'mmsvibex'               THEN 'videos'
        WHEN 'japanese'               THEN 'japanese_videos'
        WHEN 'supjav'                 THEN 'japanese_videos'
        WHEN 'jav'                    THEN 'japanese_videos'
        WHEN 'myanmar'                THEN 'myanmar_videos'
        WHEN 'buumal'                 THEN 'myanmar_videos'
        WHEN 'internetchicks'         THEN 'onlyfans_videos'
        WHEN 'influencersgonewild'    THEN 'onlyfans_videos'
        WHEN 'sheeshfans'             THEN 'onlyfans_videos'
        WHEN 'voyeurhouseorg'         THEN 'voyeurhouse_videos'
        WHEN 'hotscopesvoyeurhouse'   THEN 'hotscopesvoyeurhouse_videos'
        WHEN 'reallifecamto'          THEN 'reallifecamto_videos'
        WHEN 'hotscopesspygasm'       THEN 'hotscopesspygasm_videos'
        WHEN 'hotscopesreallifecam'   THEN 'hotscopesreallifecam_videos'
        ELSE NULL
    END;

    IF v_table IS NOT NULL AND to_regclass('scraper.' || v_table) IS NOT NULL THEN
        IF v_site IN ('internetchicks', 'influencersgonewild', 'sheeshfans') THEN
            v_filter := format('AND source_site = %L', v_site);
        END IF;
        EXECUTE format(
            'SELECT COALESCE(jsonb_agg(source_post_id), ''[]''::JSONB) '
            'FROM (SELECT source_post_id FROM scraper.%I '
            'WHERE source_post_id IS NOT NULL %s '
            'ORDER BY scraped_at DESC LIMIT $1) t',
            v_table, v_filter
        ) INTO v_legacy USING v_limit;
    END IF;

    SELECT COALESCE(jsonb_agg(post_id), '[]'::JSONB)
      INTO v_ledger
      FROM (
          SELECT post_id
            FROM scraper.known_post_ids
           WHERE site = v_site
           ORDER BY last_seen_at DESC
           LIMIT v_limit
      ) s;

    SELECT COALESCE(jsonb_agg(DISTINCT x), '[]'::JSONB)
      INTO v_result
      FROM jsonb_array_elements_text(v_legacy || v_ledger) AS x;

    RETURN COALESCE(v_result, '[]'::JSONB);
END;
$func$;

GRANT EXECUTE ON FUNCTION public.scraper_get_checkpoint(TEXT) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.scraper_set_checkpoint(TEXT, JSONB) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.scraper_add_known_post_ids(TEXT, JSONB, TEXT) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.scraper_get_known_post_ids(TEXT, INT) TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';



-- ============================================================================
-- 12. ISOLATED CHINESE VIDEOS SCHEMA & RPC BRIDGE (multisite-ready)
-- ============================================================================
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
    category_group TEXT,                           -- subcategory group: chinese | jav | livestream | western
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

-- Idempotent column add + backfill for pre-existing installs (must precede the group index)
ALTER TABLE scraper.chinese_videos ADD COLUMN IF NOT EXISTS category_group TEXT;
UPDATE scraper.chinese_videos
SET category_group = CASE
    WHEN category_id IN ('20130496', '19930497', '19940497') THEN 'jav'
    WHEN category_id IN ('20070496','20280496','20290496','20300496','20310496','20320496','20330496','19970497') THEN 'livestream'
    WHEN category_id IN ('19950497') THEN 'western'
    ELSE 'chinese'
END
WHERE category_group IS NULL;

CREATE INDEX IF NOT EXISTS idx_chinese_videos_group ON scraper.chinese_videos(category_group);
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
            COALESCE(x->>'category_group', 'chinese') AS category_group,
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
            category_id, category_name_cn, category_name_en, category_group,
            magnet_uri, magnet_hash, torrent_file_url, size_label, resolution,
            cdn_endpoints, screenshots, categories, tags, status, published_at,
            raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, content_type, title, slug, detail_url,
            full_video_url, video_mime_type, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            category_id, category_name_cn, category_name_en, category_group,
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
            category_group = COALESCE(EXCLUDED.category_group, scraper.chinese_videos.category_group),
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

-- QUERY RPC (content_type + category_group aware; playable-URL filter applies to videos only)
DROP FUNCTION IF EXISTS public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int);
CREATE OR REPLACE FUNCTION public.scraper_get_chinese_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0,
    p_group text DEFAULT NULL
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
      AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR cv.category_group = lower(trim(p_group)))
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

GRANT EXECUTE ON FUNCTION public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int, text) TO anon, authenticated, service_role;

-- COUNT RPC (category_group aware)
DROP FUNCTION IF EXISTS public.scraper_count_chinese_videos(text, text, text, text, text);
CREATE OR REPLACE FUNCTION public.scraper_count_chinese_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_group text DEFAULT NULL
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
      AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR cv.category_group = lower(trim(p_group)))
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

GRANT EXECUTE ON FUNCTION public.scraper_count_chinese_videos(text, text, text, text, text, text) TO anon, authenticated, service_role;

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

-- ─── Chinese title_en upgrade (idempotent; mirrors 20261125_chinese_title_en.sql) ───
-- English title column for the isolated Chinese category.
-- Original CJK titles are kept (source-of-truth for search integrity and
-- re-translation); title_en holds the Google-translated English title used by
-- the frontend. All changes idempotent.

ALTER TABLE scraper.chinese_videos ADD COLUMN IF NOT EXISTS title_en TEXT;
CREATE INDEX IF NOT EXISTS idx_chinese_title_en ON scraper.chinese_videos(title_en);

-- ─── BATCH UPSERT RPC (now title_en aware) ─────────────────────────────────
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
            (x->>'title_en') AS title_en,
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
            COALESCE(x->>'category_group', 'chinese') AS category_group,
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
            source_site, source_post_id, content_type, title, title_en, slug, detail_url,
            full_video_url, video_mime_type, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            category_id, category_name_cn, category_name_en, category_group,
            magnet_uri, magnet_hash, torrent_file_url, size_label, resolution,
            cdn_endpoints, screenshots, categories, tags, status, published_at,
            raw_payload, updated_at
        )
        SELECT
            source_site, source_post_id, content_type, title, title_en, slug, detail_url,
            full_video_url, video_mime_type, thumbnail_url, clean_thumbnail_url,
            quality_label, quality_score, duration_seconds, duration_formatted,
            category_id, category_name_cn, category_name_en, category_group,
            magnet_uri, magnet_hash, torrent_file_url, size_label, resolution,
            cdn_endpoints, screenshots, categories, tags, status, published_at,
            raw_payload, now()
        FROM raw_items
        ON CONFLICT (source_site, source_post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            title_en = COALESCE(EXCLUDED.title_en, scraper.chinese_videos.title_en),
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
            category_group = COALESCE(EXCLUDED.category_group, scraper.chinese_videos.category_group),
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

-- ─── SEARCH NOW MATCHES ENGLISH TITLES TOO ─────────────────────────────────
-- (get RPC: drop + recreate with widened search predicate)
DROP FUNCTION IF EXISTS public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int);
CREATE OR REPLACE FUNCTION public.scraper_get_chinese_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_sort text DEFAULT 'newest',
    p_limit int DEFAULT 24,
    p_offset int DEFAULT 0,
    p_group text DEFAULT NULL
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
      AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR cv.category_group = lower(trim(p_group)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR cv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = cv.category_name_en
           OR cv.category_id = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR cv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR cv.title ILIKE '%' || trim(p_search) || '%'
           OR COALESCE(cv.title_en, '') ILIKE '%' || trim(p_search) || '%')
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

GRANT EXECUTE ON FUNCTION public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int, text) TO anon, authenticated, service_role;

DROP FUNCTION IF EXISTS public.scraper_count_chinese_videos(text, text, text, text, text);
CREATE OR REPLACE FUNCTION public.scraper_count_chinese_videos(
    p_source_site text DEFAULT NULL,
    p_content_type text DEFAULT NULL,
    p_category text DEFAULT NULL,
    p_tag text DEFAULT NULL,
    p_search text DEFAULT NULL,
    p_group text DEFAULT NULL
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
      AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR cv.category_group = lower(trim(p_group)))
      AND (p_category IS NULL OR lower(trim(p_category)) = 'all' OR cv.categories @> ARRAY[lower(trim(p_category))]
           OR lower(trim(p_category)) = cv.category_name_en
           OR cv.category_id = trim(p_category))
      AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR cv.tags @> ARRAY[lower(trim(p_tag))])
      AND (p_search IS NULL OR cv.title ILIKE '%' || trim(p_search) || '%'
           OR COALESCE(cv.title_en, '') ILIKE '%' || trim(p_search) || '%')
      AND cv.status = 'active'
      AND (cv.content_type <> 'video' OR (cv.full_video_url IS NOT NULL AND cv.full_video_url <> ''));
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_chinese_videos(text, text, text, text, text, text) TO anon, authenticated, service_role;

-- ─── TITLE_EN BATCH UPDATE RPC (backfill + re-translation) ─────────────────
CREATE OR REPLACE FUNCTION public.scraper_update_chinese_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH batch AS (
        SELECT (x->>'source_site') AS source_site,
               (x->>'source_post_id') AS source_post_id,
               (x->>'title_en') AS title_en
        FROM jsonb_array_elements(items) AS x
        WHERE COALESCE(x->>'source_site', '') <> ''
          AND COALESCE(x->>'source_post_id', '') <> ''
    ),
    upd AS (
        UPDATE scraper.chinese_videos cv
        SET title_en = NULLIF(b.title_en, '')
        FROM batch b
        WHERE cv.source_site = b.source_site
          AND cv.source_post_id = b.source_post_id
        RETURNING 1
    )
    SELECT COUNT(*) INTO updated_count FROM upd;

    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_chinese_title_en(jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';
