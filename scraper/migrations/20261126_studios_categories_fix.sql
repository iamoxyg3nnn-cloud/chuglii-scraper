-- Studios: repair the categories array after the per-site split.
-- Rows written during the (reverted) union era carry categories=['studios'],
-- which never matches the section-id chip filters (p_category = '1' / 'rmbg').
-- Rebuild categories as [site_key, zone_id] from raw_payload.category_id.

UPDATE scraper.madouer_videos
SET categories = CASE
        WHEN raw_payload ? 'category_id' THEN
            ARRAY['madouer', raw_payload->>'category_id']
        ELSE ARRAY['madouer']::text[]
    END
WHERE categories = ARRAY['studios']::text[]
   OR NOT categories @> ARRAY['madouer']::text[];

UPDATE scraper.dasj91_videos
SET categories = CASE
        WHEN raw_payload ? 'category_id' THEN
            ARRAY['dasj91', raw_payload->>'category_id']
        ELSE ARRAY['dasj91']::text[]
    END
WHERE categories = ARRAY['studios']::text[]
   OR NOT categories @> ARRAY['dasj91']::text[];
