DO $$ BEGIN
    CREATE TYPE geo_query AS ( id varchar, lat varchar, lon varchar );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION get_geo_areas(data_type int2, items geo_query[])
RETURNS TABLE (osm_id text, ids varchar[])
as $$
DECLARE
    item record;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_results AS
    SELECT 1::int8 fid, '' osm_id, '' id WHERE FALSE;

    TRUNCATE tmp_results;

    FOREACH item IN ARRAY items LOOP
        IF data_type = 0 THEN
            -- country
            INSERT INTO tmp_results
            SELECT 
                c.fid,
                c.osm_id,
                item.id
            FROM countries c
            WHERE ST_Within(ST_SetSRID(ST_GeomFromText(format('POINT(%s %s)', item.lon, item.lat)), 4326), c.geom::GEOMETRY);
        ELSIF data_type = 1 THEN
            -- federal subpart
            INSERT INTO tmp_results
            SELECT 
                c.fid,
                c.osm_id,
                item.id
            FROM fed_districts c
            WHERE ST_Within(ST_SetSRID(ST_GeomFromText(format('POINT(%s %s)', item.lon, item.lat)), 4326), c.geom::GEOMETRY);
        ELSIF data_type = 2 THEN
            -- administrative division
            INSERT INTO tmp_results
            SELECT 
                c.fid,
                c.osm_id,
                item.id
            FROM adm_districts c
            WHERE ST_Within(ST_SetSRID(ST_GeomFromText(format('POINT(%s %s)', item.lon, item.lat)), 4326), c.geom::GEOMETRY);
        END IF;
    END LOOP;

    RETURN QUERY
    SELECT 
        t.osm_id AS osm_id,
        array_agg(t.id)::varchar[] AS ids
    FROM tmp_results t
    GROUP BY t.fid, t.osm_id;

    DROP TABLE IF EXISTS tmp_results;
END;
$$ LANGUAGE plpgsql;
