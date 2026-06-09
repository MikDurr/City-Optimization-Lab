-- City-Optimization-Lab cleaning pipeline (SQLite).
--
-- Executed by src/pipeline/clean.py. The file is split into named sections
-- (marked "-- @section NAME"); clean.py runs them in order, with a Python
-- spatial-join step in between that produces node_bldg / node_zone.
--
-- ${SQM_PER_PERSON} and ${DENSITY_REF_SQM} are substituted by clean.py from
-- the matching Python constants (single source of truth).
--
-- Registered SQL function: meters(lat1, lon1, lat2, lon2) -> metres.


-- @section nodes_edges
-- Collapse duplicated street vertices into unique intersection nodes, then
-- build a deduplicated, undirected edge table with metres, travel time and
-- multi-modal flags derived from the OSM highway class.

DROP TABLE IF EXISTS node_xy;
CREATE TABLE node_xy AS
WITH pts AS (
    SELECT from_node AS osm_id, from_lat AS lat, from_lon AS lon FROM raw_streets
    UNION ALL
    SELECT to_node   AS osm_id, to_lat   AS lat, to_lon   AS lon FROM raw_streets
)
SELECT osm_id,
       AVG(lat) AS lat,
       AVG(lon) AS lon
FROM pts
WHERE osm_id IS NOT NULL AND osm_id <> ''
GROUP BY osm_id;

-- Normalize each raw directed segment, then collapse undirected duplicates.
DROP TABLE IF EXISTS edge_norm;
CREATE TABLE edge_norm AS
WITH seg AS (
    SELECT
        MIN(from_node, to_node) AS u,
        MAX(from_node, to_node) AS v,
        LOWER(highway)          AS hw,
        meters(from_lat, from_lon, to_lat, to_lon) AS length_m,
        CASE LOWER(highway)
            WHEN 'motorway'      THEN 100
            WHEN 'trunk'         THEN 80
            WHEN 'primary'       THEN 60
            WHEN 'secondary'     THEN 50
            WHEN 'tertiary'      THEN 40
            WHEN 'residential'   THEN 30
            WHEN 'unclassified'  THEN 30
            WHEN 'living_street' THEN 15
            ELSE 30
        END AS speed_kph,
        CASE LOWER(highway)
            WHEN 'motorway'      THEN 2000
            WHEN 'trunk'         THEN 1800
            WHEN 'primary'       THEN 1200
            WHEN 'secondary'     THEN 1000
            WHEN 'tertiary'      THEN 800
            WHEN 'residential'   THEN 600
            WHEN 'unclassified'  THEN 600
            WHEN 'living_street' THEN 300
            ELSE 600
        END AS capacity
    FROM raw_streets
    WHERE from_node <> to_node
)
SELECT
    u, v,
    -- one representative highway class (the busiest among duplicates)
    MIN(hw)            AS highway,
    SUM(length_m)      AS length_m,   -- full polyline length across merged segs
    MAX(speed_kph)     AS speed_kph,
    MAX(capacity)      AS capacity
FROM seg
GROUP BY u, v;

-- Final edge table: travel time (s) + multi-modal flags.
DROP TABLE IF EXISTS clean_edges;
CREATE TABLE clean_edges AS
SELECT
    ROW_NUMBER() OVER (ORDER BY u, v) AS edge_id,
    u, v,
    ROUND(length_m, 2)                              AS length_m,
    ROUND(length_m / (speed_kph / 3.6), 3)          AS travel_time,  -- seconds
    capacity,
    highway,
    'car'
      || CASE WHEN highway NOT IN ('motorway','trunk') THEN ',ped' ELSE '' END
      || CASE WHEN highway IN ('primary','secondary','trunk') THEN ',transit' ELSE '' END
        AS modes
FROM edge_norm
WHERE length_m > 0;


-- @section buildings
-- Classify each building footprint into a zone class and impute missing
-- storey counts, then derive floor area (footprint x levels).

DROP TABLE IF EXISTS bldg;
CREATE TABLE bldg AS
SELECT
    way_id,
    centroid_lat AS lat,
    centroid_lon AS lon,
    footprint_m2,
    -- map raw building tag -> one of our 3 zone classes
    CASE
        WHEN building IN ('industrial','warehouse','factory','manufacture') THEN 'industrial'
        WHEN building IN ('retail','commercial','office','supermarket','hotel') THEN 'commercial'
        WHEN amenity IN ('university','college','hospital','school') THEN 'commercial'
        WHEN building IN ('apartments','house','detached','semidetached_house',
                          'terrace','residential','dormitory','bungalow') THEN 'residential'
        ELSE 'residential'   -- 'yes' and unknowns default to residential
    END AS bclass,
    -- impute levels when missing, by (mapped) class
    CASE
        WHEN levels GLOB '[0-9]*' THEN CAST(levels AS REAL)
        WHEN building = 'apartments'        THEN 4
        WHEN building IN ('office','commercial','retail','hotel') THEN 3
        WHEN amenity IN ('university','hospital') THEN 4
        WHEN building IN ('industrial','warehouse') THEN 1
        ELSE 2
    END AS levels_use
FROM raw_buildings
WHERE footprint_m2 > 0;

DROP TABLE IF EXISTS bldg_floor;
CREATE TABLE bldg_floor AS
SELECT way_id, lat, lon, bclass,
       footprint_m2 * levels_use AS floor_area
FROM bldg;


-- @section node_attrs
-- Aggregate building floor area onto nodes (population + density) and attach
-- the zone assigned by the Python spatial join. Runs AFTER node_bldg /
-- node_zone have been populated.

DROP TABLE IF EXISTS clean_nodes;
CREATE TABLE clean_nodes AS
WITH agg AS (
    SELECT
        n.osm_id,
        n.lat, n.lon,
        COALESCE(SUM(b.floor_area), 0)                                   AS floor_area,
        COALESCE(SUM(CASE WHEN b.bclass='residential' THEN b.floor_area END), 0) AS res_floor
    FROM node_xy n
    LEFT JOIN node_bldg nb ON nb.node_osm = n.osm_id
    LEFT JOIN bldg_floor b  ON b.way_id   = nb.way_id
    GROUP BY n.osm_id, n.lat, n.lon
)
SELECT
    a.osm_id                                              AS osm_id,
    a.lat, a.lon,
    CAST(ROUND(a.res_floor / ${SQM_PER_PERSON}) AS INT)   AS population,
    ROUND(MIN(1.0, a.floor_area / ${DENSITY_REF_SQM}), 4) AS density,
    COALESCE(z.zoning, 'residential')                     AS zoning
FROM agg a
LEFT JOIN node_zone z ON z.node_osm = a.osm_id;
