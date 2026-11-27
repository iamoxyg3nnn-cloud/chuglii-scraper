-- Fix the broken per-site title_en update RPCs.
--
-- Symptom in production (run 45904626049 and every run since the split):
--     [!] Supabase RPC scraper_update_dasj91_title_en failed after 3 attempts:
--         HTTP 400 ... column x.title_en does not exist
--     hint: Perhaps you meant to reference the column "sv.title_en"
--
-- Cause: the functions were written as
--     SET title_en = x.title_en
--     FROM jsonb_array_elements(items) AS x
-- but `x` there is the *jsonb value*, whose only column is `value`; jsonb
-- fields must be read with `x->>'title_en'`. Every call raised 42703, so
-- English titles for fangsung / madouer / dasj91 / madouhd were never
-- backfilled (the "English-only UI safety net" was a no-op for four tables).
--
-- The callers (translate_backfill.py) send {source_site, source_post_id,
-- title_en} — no `id` — so the functions now match on source_post_id as well,
-- and still accept `id` for direct repairs.

-- ─── fangsung ───────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_update_fangsung_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH batch AS (
        SELECT NULLIF(e->>'id', '')                    AS id,
               NULLIF(e->>'source_post_id', '')        AS source_post_id,
               NULLIF(left(e->>'title_en', 255), '')   AS title_en
        FROM jsonb_array_elements(items) AS e
    )
    UPDATE scraper.fangsung_videos fv
    SET title_en = b.title_en, updated_at = now()
    FROM batch b
    WHERE b.title_en IS NOT NULL
      AND (fv.id::text = b.id OR fv.source_post_id = b.source_post_id);
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_fangsung_title_en(jsonb) TO service_role;

-- ─── madouer ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_update_madouer_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH batch AS (
        SELECT NULLIF(e->>'id', '')                    AS id,
               NULLIF(e->>'source_post_id', '')        AS source_post_id,
               NULLIF(left(e->>'title_en', 255), '')   AS title_en
        FROM jsonb_array_elements(items) AS e
    )
    UPDATE scraper.madouer_videos sv
    SET title_en = b.title_en, updated_at = now()
    FROM batch b
    WHERE b.title_en IS NOT NULL
      AND (sv.id::text = b.id OR sv.source_post_id = b.source_post_id);
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_madouer_title_en(jsonb) TO service_role;

-- ─── dasj91 ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_update_dasj91_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH batch AS (
        SELECT NULLIF(e->>'id', '')                    AS id,
               NULLIF(e->>'source_post_id', '')        AS source_post_id,
               NULLIF(left(e->>'title_en', 255), '')   AS title_en
        FROM jsonb_array_elements(items) AS e
    )
    UPDATE scraper.dasj91_videos sv
    SET title_en = b.title_en, updated_at = now()
    FROM batch b
    WHERE b.title_en IS NOT NULL
      AND (sv.id::text = b.id OR sv.source_post_id = b.source_post_id);
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_dasj91_title_en(jsonb) TO service_role;

-- ─── madouhd ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.scraper_update_madouhd_title_en(items jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
DECLARE
    updated_count int := 0;
BEGIN
    WITH batch AS (
        SELECT NULLIF(e->>'id', '')                    AS id,
               NULLIF(e->>'source_post_id', '')        AS source_post_id,
               NULLIF(left(e->>'title_en', 255), '')   AS title_en
        FROM jsonb_array_elements(items) AS e
    )
    UPDATE scraper.madouhd_videos sv
    SET title_en = b.title_en, updated_at = now()
    FROM batch b
    WHERE b.title_en IS NOT NULL
      AND (sv.id::text = b.id OR sv.source_post_id = b.source_post_id);
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN jsonb_build_object('success', true, 'updated', updated_count);
END;
$$;

GRANT EXECUTE ON FUNCTION public.scraper_update_madouhd_title_en(jsonb) TO service_role;
