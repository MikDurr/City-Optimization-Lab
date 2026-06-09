"""
SQL cleaning pipeline: raw OSM extracts  ->  tidy, connected city graph.

Engine is SQLite. The relational work (dedupe, unit conversion, highway/
building normalization, aggregation, filtering) is done in SQL. Two steps
are not relational, so a thin Python layer handles them:
  - nearest-node spatial join (grid index)        -> assign zones & buildings
  - largest connected component (graph traversal)  -> drop disconnected bits

Outputs data/clean/city.db with two clean tables the simulator can load:
  clean_nodes(node_id, lat, lon, grid_row, grid_col,
              zoning, population, density, baseline_energy)
  clean_edges(edge_id, u, v, length_m, travel_time, capacity, highway, modes)

Run:  python src/pipeline/clean.py
"""
from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd

ROOT = Path(__file__).resolve().parents[1].parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "clean"
DB = OUT / "city.db"

# Floor area (m^2) per resident — standard occupancy assumption.
SQM_PER_PERSON = 40.0
# Floor area (m^2) that counts as fully built-up for density = 1.0.
DENSITY_REF_SQM = 5000.0


# ---------------------------------------------------------------------------
# Registered SQL helper: great-circle-ish distance in meters (equirectangular,
# fine at city scale). Lets us compute edge lengths inside SQL.
# ---------------------------------------------------------------------------
def _meters(lat1, lon1, lat2, lon2):
    lat0 = math.radians((lat1 + lat2) / 2.0)
    x = math.radians(lon2 - lon1) * math.cos(lat0) * 6_371_000.0
    y = math.radians(lat2 - lat1) * 6_371_000.0
    return math.hypot(x, y)


# ---------------------------------------------------------------------------
# Load raw CSVs into SQLite
# ---------------------------------------------------------------------------
def load_raw(conn: sqlite3.Connection) -> None:
    for name in ("raw_streets", "raw_nodes", "raw_landuse", "raw_buildings"):
        df = pd.read_csv(RAW / f"{name}.csv")
        df.to_sql(name, conn, if_exists="replace", index=False)


# ---------------------------------------------------------------------------
# SQL: nodes + edges (dedupe, meters, highway normalization, modes)
# ---------------------------------------------------------------------------
SQL_NODES_EDGES = """
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
"""

# ---------------------------------------------------------------------------
# SQL: classify buildings + impute missing levels  -> floor area
# ---------------------------------------------------------------------------
SQL_BUILDINGS = """
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
"""

# ---------------------------------------------------------------------------
# SQL: aggregate per-node attributes once spatial joins are written back
#   (node_bldg / node_zone are produced by the Python grid index step)
# ---------------------------------------------------------------------------
SQL_NODE_ATTRS = f"""
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
    CAST(ROUND(a.res_floor / {SQM_PER_PERSON}) AS INT)    AS population,
    ROUND(MIN(1.0, a.floor_area / {DENSITY_REF_SQM}), 4)  AS density,
    COALESCE(z.zoning, 'residential')                     AS zoning
FROM agg a
LEFT JOIN node_zone z ON z.node_osm = a.osm_id;
"""


# ---------------------------------------------------------------------------
# Python: grid-index nearest-node spatial join
# ---------------------------------------------------------------------------
class GridIndex:
    """Tiny uniform-grid nearest-neighbour index over node lat/lon."""

    def __init__(self, nodes, cell_deg=0.0009):
        self.cell = cell_deg
        self.cells = defaultdict(list)
        for osm_id, lat, lon in nodes:
            self.cells[self._key(lat, lon)].append((osm_id, lat, lon))

    def _key(self, lat, lon):
        return (int(lat / self.cell), int(lon / self.cell))

    def nearest(self, lat, lon):
        ci, cj = self._key(lat, lon)
        best, best_d = None, float("inf")
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for osm_id, nlat, nlon in self.cells.get((ci + di, cj + dj), ()):
                    d = (nlat - lat) ** 2 + (nlon - lon) ** 2
                    if d < best_d:
                        best, best_d = osm_id, d
        return best


def spatial_join(conn: sqlite3.Connection) -> None:
    nodes = conn.execute("SELECT osm_id, lat, lon FROM node_xy").fetchall()
    idx = GridIndex(nodes)

    # buildings -> nearest node
    bldgs = conn.execute("SELECT way_id, lat, lon FROM bldg_floor").fetchall()
    nb = [(idx.nearest(lat, lon), wid) for wid, lat, lon in bldgs]
    nb = [(n, w) for n, w in nb if n is not None]
    conn.execute("DROP TABLE IF EXISTS node_bldg")
    conn.execute("CREATE TABLE node_bldg (node_osm INTEGER, way_id INTEGER)")
    conn.executemany("INSERT INTO node_bldg VALUES (?, ?)", nb)

    # Each NODE takes the zone of its nearest land-use polygon, so every node
    # gets a real zone (not just the ~185 nodes a polygon happens to land on).
    lu = conn.execute(
        """SELECT centroid_lat, centroid_lon,
                  CASE
                    WHEN landuse IN ('industrial') THEN 'industrial'
                    WHEN landuse IN ('commercial','retail') THEN 'commercial'
                    WHEN amenity = 'university' THEN 'commercial'
                    ELSE 'residential'
                  END AS zoning
           FROM raw_landuse"""
    ).fetchall()
    zone_idx = GridIndex([(z, lat, lon) for lat, lon, z in lu], cell_deg=0.004)
    nz = []
    for osm_id, lat, lon in nodes:
        z = zone_idx.nearest(lat, lon)        # returns the zone label payload
        if z is not None:
            nz.append((osm_id, z))
    conn.execute("DROP TABLE IF EXISTS node_zone")
    conn.execute("CREATE TABLE node_zone (node_osm INTEGER, zoning TEXT)")
    conn.executemany("INSERT INTO node_zone VALUES (?, ?)", nz)
    conn.commit()


# ---------------------------------------------------------------------------
# Python: keep largest connected component, add grid_row/col + baseline_energy
# ---------------------------------------------------------------------------
def finalize(conn: sqlite3.Connection) -> dict:
    edges = conn.execute("SELECT u, v FROM clean_edges").fetchall()
    G = nx.Graph()
    G.add_edges_from(edges)
    if G.number_of_nodes() == 0:
        raise RuntimeError("no edges produced — check raw inputs")
    keep = max(nx.connected_components(G), key=len)

    keep_tbl = [(n,) for n in keep]
    conn.execute("DROP TABLE IF EXISTS keep_nodes")
    conn.execute("CREATE TABLE keep_nodes (osm_id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO keep_nodes VALUES (?)", keep_tbl)

    conn.execute("DELETE FROM clean_edges WHERE u NOT IN (SELECT osm_id FROM keep_nodes) "
                 "OR v NOT IN (SELECT osm_id FROM keep_nodes)")
    conn.execute("DELETE FROM clean_nodes WHERE osm_id NOT IN (SELECT osm_id FROM keep_nodes)")

    # baseline_energy mirrors the model's expectation (population * density)
    conn.execute("ALTER TABLE clean_nodes ADD COLUMN baseline_energy REAL")
    conn.execute("UPDATE clean_nodes SET baseline_energy = "
                 "MAX(1.0, population * density)")

    # raster grid_row / grid_col for the energy heatmap (bin lat/lon)
    rows = conn.execute("SELECT osm_id, lat, lon FROM clean_nodes").fetchall()
    n = len(rows)
    side = max(1, int(round(math.sqrt(n))))
    lats = sorted(r[1] for r in rows)
    lons = sorted(r[2] for r in rows)
    lat_min, lat_max = lats[0], lats[-1]
    lon_min, lon_max = lons[0], lons[-1]

    def bucket(val, lo, hi):
        if hi <= lo:
            return 0
        return min(side - 1, int((val - lo) / (hi - lo) * side))

    conn.execute("ALTER TABLE clean_nodes ADD COLUMN grid_row INTEGER")
    conn.execute("ALTER TABLE clean_nodes ADD COLUMN grid_col INTEGER")
    conn.execute("ALTER TABLE clean_nodes ADD COLUMN node_id INTEGER")
    for i, (osm_id, lat, lon) in enumerate(rows):
        conn.execute(
            "UPDATE clean_nodes SET grid_row=?, grid_col=?, node_id=? WHERE osm_id=?",
            (bucket(lat, lat_min, lat_max), bucket(lon, lon_min, lon_max), i, osm_id),
        )
    conn.commit()

    stats = {
        "nodes": conn.execute("SELECT COUNT(*) FROM clean_nodes").fetchone()[0],
        "edges": conn.execute("SELECT COUNT(*) FROM clean_edges").fetchone()[0],
        "population": conn.execute("SELECT SUM(population) FROM clean_nodes").fetchone()[0],
        "grid_side": side,
    }
    return stats


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    if DB.exists():
        DB.unlink()
    conn = sqlite3.connect(DB)
    conn.create_function("meters", 4, _meters)

    print("1/5  loading raw CSVs into SQLite ...")
    load_raw(conn)

    print("2/5  SQL: nodes, edges, highway normalization, modes ...")
    conn.executescript(SQL_NODES_EDGES)

    print("3/5  SQL: classify buildings + impute levels -> floor area ...")
    conn.executescript(SQL_BUILDINGS)

    print("4/5  Python grid-index spatial join (zones + buildings -> nodes) ...")
    spatial_join(conn)
    conn.executescript(SQL_NODE_ATTRS)

    print("5/5  largest connected component + grid raster + baseline energy ...")
    stats = finalize(conn)

    # zone distribution for a quick sanity read
    zones = dict(conn.execute(
        "SELECT zoning, COUNT(*) FROM clean_nodes GROUP BY zoning").fetchall())
    conn.close()

    print("\nClean dataset written to", DB)
    print(f"  nodes        {stats['nodes']:>6}")
    print(f"  edges        {stats['edges']:>6}")
    print(f"  population    {stats['population']:>6}")
    print(f"  grid raster   {stats['grid_side']} x {stats['grid_side']}")
    print(f"  zoning        {zones}")


if __name__ == "__main__":
    main()
