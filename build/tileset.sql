CREATE OR REPLACE FUNCTION slice_language_tags(tags hstore)
RETURNS hstore AS $$
    SELECT delete_empty_keys(slice(tags, ARRAY['int_name', 'loc_name', 'name', 'wikidata', 'wikipedia']))
$$ LANGUAGE SQL IMMUTABLE;
DO $$ BEGIN RAISE NOTICE 'Layer contour'; END$$;CREATE OR REPLACE FUNCTION layer_contour(bbox geometry, zoom_level int)
RETURNS TABLE(geometry geometry, ele int) AS $$
    SELECT geometry, ele
    FROM osm_contour
    WHERE geometry && bbox;
$$ LANGUAGE SQL IMMUTABLE;

