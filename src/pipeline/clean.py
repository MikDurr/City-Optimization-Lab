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
SQL_FILE = Path(__file__).resolve().parent / "clean.sql"

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
# Load the SQL pipeline from clean.sql, split into named "-- @section NAME"
# blocks. Placeholders like ${SQM_PER_PERSON} are filled by render().
# ---------------------------------------------------------------------------
def load_sql_sections(path: Path) -> dict:
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for line in path.read_text().splitlines():
        marker = line.strip()
        if marker.startswith("-- @section "):
            current = marker[len("-- @section "):].strip()
            sections[current] = []
        elif current is not None:
            sections[current].append(line)
    return {name: "\n".join(lines).strip() for name, lines in sections.items()}


def render(sql: str, **subs) -> str:
    for key, val in subs.items():
        sql = sql.replace(f"${{{key}}}", str(val))
    return sql


# ---------------------------------------------------------------------------
# Load raw CSVs into SQLite
# ---------------------------------------------------------------------------
def load_raw(conn: sqlite3.Connection) -> None:
    for name in ("raw_streets", "raw_nodes", "raw_landuse", "raw_buildings"):
        df = pd.read_csv(RAW / f"{name}.csv")
        df.to_sql(name, conn, if_exists="replace", index=False)


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

    sql = load_sql_sections(SQL_FILE)

    print("1/5  loading raw CSVs into SQLite ...")
    load_raw(conn)

    print("2/5  SQL: nodes, edges, highway normalization, modes ...")
    conn.executescript(sql["nodes_edges"])

    print("3/5  SQL: classify buildings + impute levels -> floor area ...")
    conn.executescript(sql["buildings"])

    print("4/5  Python grid-index spatial join (zones + buildings -> nodes) ...")
    spatial_join(conn)
    conn.executescript(render(
        sql["node_attrs"],
        SQM_PER_PERSON=SQM_PER_PERSON,
        DENSITY_REF_SQM=DENSITY_REF_SQM,
    ))

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
