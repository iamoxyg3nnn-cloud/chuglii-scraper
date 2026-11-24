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
        CASE WHEN p_sort = 'views' THEN vv.views_count END DESC NULLS LAST,
        CASE WHEN p_sort = 'rating' THEN vv.rating_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN vv.published_at END ASC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN vv.quality_score END DESC NULLS LAST,
        vv.published_at DESC NULLS LAST,
        vv.scraped_at DESC
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
