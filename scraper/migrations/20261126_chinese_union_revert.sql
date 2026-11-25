-- ============================================================================
-- MIGRATION: Revert the chinese<->studios union (20261126_chinese_studios_union)
-- Date: 2026-11-26
-- Purpose: The Chinese category goes back to plain per-site isolation: each
--   website keeps its own isolated table and RPC family, and the Chinese
--   category page lists them as a site picker (fangsung pattern). The union
--   RPCs are replaced with the original SETOF scraper.chinese_videos versions.
-- ============================================================================

DROP FUNCTION IF EXISTS public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int, text);
DROP FUNCTION IF EXISTS public.scraper_count_chinese_videos(text, text, text, text, text, text);

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

-- 6. COUNT RPC (now group-aware)
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

NOTIFY pgrst, 'reload schema';
