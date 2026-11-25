-- ============================================================================
-- MIGRATION: Chinese category unions the Studios rows (madouer + dasj91)
-- Date: 2026-11-26
-- Purpose: The user wants the Studios sites (madouer.cc, 91dasj.com) visible
--   inside the Chinese parent category as well. Rather than double-storing
--   the rows, scraper_get_chinese_videos / scraper_count_chinese_videos are
--   rewritten as UNION ALL over scraper.chinese_videos and
--   scraper.studios_videos (source_site IN ('madouer','dasj91')), with the
--   studios rows mapped into the chinese_videos column shape:
--     region      -> category_name_cn
--     region_en   -> category_name_en
--     raw_payload->>'category_id' -> category_id  (zone filter support)
--     'studios'   -> category_group               (new browse group chip)
-- Return type changes from SETOF scraper.chinese_videos to SETOF jsonb —
-- PostgREST serves identical JSON objects, so the frontend is unaffected.
-- ============================================================================

-- 0. Backfill studios raw_payload.category_id from region_en (rows crawled
--    before the pipeline started injecting it).
UPDATE scraper.studios_videos sv
SET raw_payload = jsonb_build_object('category_id', CASE sv.region_en
    WHEN 'Madou Videos' THEN '1'
    WHEN 'Japanese Videos' THEN '2'
    WHEN 'Western Videos' THEN '3'
    WHEN 'Anime Videos' THEN '4'
    WHEN 'Chinese Amateur' THEN '5'
    WHEN 'Madou Original' THEN '6'
    WHEN '91 Studio' THEN '7'
    WHEN 'Tianmei Media' THEN '8'
    WHEN 'Peach Images' THEN '9'
    WHEN 'Starlight Media' THEN '10'
    WHEN 'Royal Chinese' THEN '11'
    WHEN 'Jingdong Studio' THEN '12'
    WHEN 'Lebo Media' THEN '13'
    WHEN 'Adult Headlines' THEN '14'
    WHEN 'Mr. Rabbit' THEN '15'
    WHEN 'Xingba Original' THEN '16'
    WHEN 'HongKongDoll' THEN '17'
    WHEN 'Tangxin Vlog' THEN '18'
    WHEN 'Chinese Original' THEN '22'
    WHEN 'Twitter Girls' THEN '31'
    WHEN 'Chinese Series' THEN '32'
    WHEN 'AI Dramas' THEN 'aifl'
    WHEN 'Contrast Goddess' THEN 'rmbg'
    WHEN 'Tanhua Masters' THEN 'jxth'
    WHEN 'Cuckold Stories' THEN 'ldcz'
    WHEN 'Taboo Drama' THEN 'qwys'
    ELSE NULL END)
WHERE sv.raw_payload->>'category_id' IS NULL;

-- 1. Drop old-signature RPCs (return type changes, so REPLACE is not enough)
DROP FUNCTION IF EXISTS public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int, text);
DROP FUNCTION IF EXISTS public.scraper_count_chinese_videos(text, text, text, text, text, text);

-- 2. QUERY RPC (union over chinese + studios)
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
RETURNS SETOF jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    RETURN QUERY
    WITH base AS (
        SELECT cv.id, cv.source_site, cv.source_post_id, cv.content_type,
               cv.title, cv.title_en, cv.slug, cv.detail_url, cv.full_video_url,
               cv.video_mime_type, cv.thumbnail_url, cv.clean_thumbnail_url,
               cv.quality_label, cv.quality_score, cv.duration_seconds,
               cv.duration_formatted, cv.category_id, cv.category_name_cn,
               cv.category_name_en, cv.magnet_uri, cv.magnet_hash,
               cv.torrent_file_url, cv.size_label, cv.resolution,
               cv.cdn_endpoints, cv.screenshots, cv.categories, cv.tags,
               cv.status, cv.published_at, cv.scraped_at, cv.updated_at,
               cv.raw_payload, cv.category_group
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
        UNION ALL
        SELECT sv.id, sv.source_site, sv.source_post_id, sv.content_type,
               sv.title, sv.title_en, sv.slug, sv.detail_url, sv.full_video_url,
               sv.video_mime_type, sv.thumbnail_url, sv.clean_thumbnail_url,
               sv.quality_label, sv.quality_score, sv.duration_seconds,
               sv.duration_formatted,
               sv.raw_payload->>'category_id', sv.region, sv.region_en,
               NULL::text, NULL::text, NULL::text, NULL::text,
               sv.resolution, '[]'::jsonb, '[]'::jsonb,
               sv.categories, sv.tags, sv.status, sv.published_at,
               sv.scraped_at, sv.updated_at, sv.raw_payload,
               'studios'::text
        FROM scraper.studios_videos sv
        WHERE sv.source_site IN ('madouer', 'dasj91')
          AND (p_source_site IS NULL OR sv.source_site = p_source_site)
          AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
          AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR 'studios' = lower(trim(p_group)))
          AND (p_category IS NULL OR lower(trim(p_category)) = 'all'
               OR sv.raw_payload->>'category_id' = trim(p_category)
               OR lower(trim(p_category)) = sv.region_en)
          AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
          AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%')
          AND sv.status = 'active'
          AND (sv.content_type <> 'video' OR (sv.full_video_url IS NOT NULL AND sv.full_video_url <> ''))
    )
    SELECT to_jsonb(b)
    FROM base b
    ORDER BY
        CASE WHEN p_sort = 'newest' THEN b.scraped_at END DESC NULLS LAST,
        CASE WHEN p_sort = 'views' THEN b.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'quality' THEN b.quality_score END DESC NULLS LAST,
        CASE WHEN p_sort = 'oldest' THEN b.scraped_at END ASC NULLS LAST,
        b.scraped_at DESC,
        b.id DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_get_chinese_videos(text, text, text, text, text, text, int, int, text) TO anon, authenticated, service_role;

-- 3. COUNT RPC (same union, count only)
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
    FROM (
        SELECT cv.id
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
        UNION ALL
        SELECT sv.id
        FROM scraper.studios_videos sv
        WHERE sv.source_site IN ('madouer', 'dasj91')
          AND (p_source_site IS NULL OR sv.source_site = p_source_site)
          AND (p_content_type IS NULL OR lower(trim(p_content_type)) = 'all' OR sv.content_type = lower(trim(p_content_type)))
          AND (p_group IS NULL OR lower(trim(p_group)) = 'all' OR 'studios' = lower(trim(p_group)))
          AND (p_category IS NULL OR lower(trim(p_category)) = 'all'
               OR sv.raw_payload->>'category_id' = trim(p_category)
               OR lower(trim(p_category)) = sv.region_en)
          AND (p_tag IS NULL OR lower(trim(p_tag)) = 'all' OR sv.tags @> ARRAY[lower(trim(p_tag))])
          AND (p_search IS NULL OR sv.title ILIKE '%' || trim(p_search) || '%')
          AND sv.status = 'active'
          AND (sv.content_type <> 'video' OR (sv.full_video_url IS NOT NULL AND sv.full_video_url <> ''))
    ) u;
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_count_chinese_videos(text, text, text, text, text, text) TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';
