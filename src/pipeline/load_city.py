"""
Adapter: load the cleaned SQLite city (data/clean/city.db) into an object that
is drop-in compatible with the parts of `CityGrid` the simulator/optimizer use.

The simulator only touches:  city.graph  (node + edge attributes),
city.width, city.height, city.zones, city.zone_colors.  We build a real
networkx graph from the clean tables and expose exactly those.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import networkx as nx

DEFAULT_DB = Path(__file__).resolve().parents[1].parent / "data" / "clean" / "city.db"

# pedestrian / transit speed assumptions (m/s) for deriving mode travel times
PED_SPEED = 1.4
TRANSIT_SPEEDUP = 0.6      # transit is ~40% faster than car on the same edge
TRANSIT_CAPACITY = 600


class RealCity:
    """Minimal CityGrid-compatible wrapper around a cleaned OSM city."""

    def __init__(self, graph: nx.Graph, width: int, height: int):
        self.graph = graph
        self.width = width
        self.height = height
        self.zones = ("residential", "commercial", "industrial")
        self.zone_colors = {
            "residential": "darkviolet",
            "commercial": "darkgreen",
            "industrial": "darkred",
        }

    def __repr__(self) -> str:
        return (f"RealCity(nodes={self.graph.number_of_nodes()}, "
                f"edges={self.graph.number_of_edges()}, "
                f"grid={self.width}x{self.height})")


def load_city(db_path: Optional[Path] = None) -> RealCity:
    db_path = Path(db_path) if db_path else DEFAULT_DB
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} not found — run the pipeline first:\n"
            f"  python src/pipeline/fetch_osm.py && python src/pipeline/clean.py"
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    G = nx.Graph()

    width = height = 1
    for r in conn.execute(
        "SELECT osm_id, lat, lon, grid_row, grid_col, zoning, "
        "population, density, baseline_energy FROM clean_nodes"
    ):
        G.add_node(
            r["osm_id"],
            row=r["grid_row"],
            col=r["grid_col"],
            pos=(r["lon"], r["lat"]),          # (x, y) for any plotting
            zoning=r["zoning"],
            population=int(r["population"]),
            density=float(r["density"]),
            baseline_energy=float(r["baseline_energy"]),
        )
        width = max(width, r["grid_col"] + 1)
        height = max(height, r["grid_row"] + 1)

    for r in conn.execute(
        "SELECT u, v, length_m, travel_time, capacity, highway, modes FROM clean_edges"
    ):
        modes = set(r["modes"].split(","))
        length = float(r["length_m"])
        travel_time = float(r["travel_time"])
        attrs = {
            "kind": "road",
            "highway": r["highway"],
            "length": length,
            "travel_time": travel_time,
            "capacity": float(r["capacity"]),
            "modes": modes,
        }
        if "ped" in modes:
            attrs["walk_time"] = length / PED_SPEED
        if "transit" in modes:
            attrs["transit_time"] = travel_time * TRANSIT_SPEEDUP
            attrs["transit_capacity"] = TRANSIT_CAPACITY
        G.add_edge(r["u"], r["v"], **attrs)

    conn.close()
    return RealCity(G, width=width, height=height)


if __name__ == "__main__":
    city = load_city()
    print(city)
    # quick attribute sanity check
    n0 = next(iter(city.graph.nodes(data=True)))
    e0 = next(iter(city.graph.edges(data=True)))
    print("sample node:", n0)
    print("sample edge:", e0)
    pop = sum(d["population"] for _, d in city.graph.nodes(data=True))
    print("total population:", pop)
