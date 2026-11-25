-- Sprite-storyboard storage for zero-latency timeline hover previews.
--
-- Metadata lives on each video row (preview_thumbs jsonb):
--   {"interval": 12, "tileW": 160, "tileH": 90, "cols": 10, "rows": 10,
--    "count": 412, "sheets": 5}
-- Bytes live here, one row per sprite sheet JPEG, served by the frontend
-- edge worker via scraper_get_preview_sprite (base64) with immutable
-- edge/browser caching. No external storage required.

CREATE TABLE IF NOT EXISTS scraper.preview_sprite_blobs (
    table_name   TEXT NOT NULL,                   -- e.g. 'chinese_videos'
    video_id     TEXT NOT NULL,                   -- video row id (uuid text)
    sheet_idx    INT  NOT NULL DEFAULT 0,
    data         BYTEA NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
    bytes        INT  NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, video_id, sheet_idx)
);

-- Public (anon) read of one sheet as base64 text for the edge route.
CREATE OR REPLACE FUNCTION public.scraper_get_preview_sprite(
    p_table TEXT,
    p_id    TEXT,
    p_sheet INT DEFAULT 0
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = scraper, public
AS $$
    SELECT encode(b.data, 'base64')
    FROM scraper.preview_sprite_blobs b
    WHERE b.table_name = p_table
      AND b.video_id   = p_id
      AND b.sheet_idx  = p_sheet
    LIMIT 1;
$$;

REVOKE ALL ON FUNCTION public.scraper_get_preview_sprite(TEXT, TEXT, INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.scraper_get_preview_sprite(TEXT, TEXT, INT) TO anon, authenticated, service_role;

-- Writer helper (service role only). Lives in `public` so PostgREST can
-- resolve it via /rest/v1/rpc; SECURITY DEFINER with a scraper search_path.
CREATE OR REPLACE FUNCTION public.upsert_preview_sprite(
    p_table   TEXT,
    p_id      TEXT,
    p_sheet   INT,
    p_data    BYTEA,
    p_meta    JSONB
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = scraper, public
AS $$
BEGIN
    INSERT INTO scraper.preview_sprite_blobs (table_name, video_id, sheet_idx, data, bytes)
    VALUES (p_table, p_id, p_sheet, p_data, octet_length(p_data))
    ON CONFLICT (table_name, video_id, sheet_idx)
    DO UPDATE SET data = EXCLUDED.data, bytes = EXCLUDED.bytes, created_at = now();

    IF p_table = 'chinese_videos' THEN
        UPDATE scraper.chinese_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'videos' THEN
        UPDATE scraper.videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'japanese_videos' THEN
        UPDATE scraper.japanese_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'myanmar_videos' THEN
        UPDATE scraper.myanmar_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'onlyfans_videos' THEN
        UPDATE scraper.onlyfans_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'voyeurhouse_videos' THEN
        UPDATE scraper.voyeurhouse_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'hotscopesvoyeurhouse_videos' THEN
        UPDATE scraper.hotscopesvoyeurhouse_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'reallifecamto_videos' THEN
        UPDATE scraper.reallifecamto_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'hotscopesspygasm_videos' THEN
        UPDATE scraper.hotscopesspygasm_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    ELSIF p_table = 'hotscopesreallifecam_videos' THEN
        UPDATE scraper.hotscopesreallifecam_videos SET preview_thumbs = p_meta WHERE id::text = p_id;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.upsert_preview_sprite(TEXT, TEXT, INT, BYTEA, JSONB) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.upsert_preview_sprite(TEXT, TEXT, INT, BYTEA, JSONB) TO service_role;
