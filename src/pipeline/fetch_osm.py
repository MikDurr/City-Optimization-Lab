"""
Fetch a real city street network + land use from OpenStreetMap (Overpass API)
and write it to disk as *raw*, unprocessed CSVs.

These files are intentionally messy (the way real extracts arrive):
  - geometry points are duplicated across every way that touches them
  - lengths are stored in raw degrees, not meters
  - highway / landuse tags are kept verbatim (including ones we don't model)
  - names and oneway flags are frequently missing

The SQL pipeline (clean.py) is responsible for turning this into a tidy,
deduplicated, connected city graph.

Usage:
    python src/pipeline/fetch_osm.py            # default area (Cambridge, MA)
    python src/pipeline/fetch_osm.py --place somerville
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

RAW_DIR = Path(__file__).resolve().parents[1].parent / "data" / "raw"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {"User-Agent": "city-optimization-lab/1.0 (educational urban sim project)"}

# A few real, recognizable areas. (south, west, north, east) lat/lon bbox.
PLACES: Dict[str, Tuple[str, Tuple[float, float, float, float]]] = {
    # ~0.7 km^2 core around Central Square, Cambridge: dense mixed-use,
    # small enough to keep the optimizer fast (comparable to the old grid).
    "cambridge_core": ("Central Square, Cambridge, MA", (42.3620, -71.1070, 42.3700, -71.0970)),
    "cambridge": ("Cambridge, MA", (42.3600, -71.1200, 42.3800, -71.0850)),
    "somerville": ("Somerville, MA", (42.3780, -71.1100, 42.4000, -71.0800)),
    "downtown_sf": ("Downtown San Francisco, CA", (37.7840, -122.4150, 37.7990, -122.3950)),
}

# Drivable street classes we ask OSM for.
HIGHWAY_CLASSES = "motorway|trunk|primary|secondary|tertiary|residential|unclassified|living_street"

# Land-use polygon classes we ask OSM for (maps onto our 3 zone types later).
LANDUSE_CLASSES = (
    "residential|commercial|retail|industrial|"
    "civic|institutional|education|mixed"
)


def build_query(bbox: Tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    box = f"{s},{w},{n},{e}"
    return f"""
[out:json][timeout:90];
(
  way["highway"~"^({HIGHWAY_CLASSES})$"]({box});
  way["landuse"~"^({LANDUSE_CLASSES})$"]({box});
  way["amenity"="university"]({box});
);
out geom;
""".strip()


def build_buildings_query(bbox: Tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    box = f"{s},{w},{n},{e}"
    return f"""
[out:json][timeout:120];
(
  way["building"]({box});
);
out geom;
""".strip()


def polygon_area_m2(geom: List[dict]) -> float:
    """Approximate footprint area (m^2) via shoelace on a local equirectangular
    projection. Good enough for city-block-scale building footprints."""
    if len(geom) < 3:
        return 0.0
    lat0 = sum(p["lat"] for p in geom) / len(geom)
    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * abs(__import__("math").cos(__import__("math").radians(lat0)))
    xs = [p["lon"] * m_per_deg_lon for p in geom]
    ys = [p["lat"] * m_per_deg_lat for p in geom]
    area2 = 0.0
    for i in range(len(geom) - 1):
        area2 += xs[i] * ys[i + 1] - xs[i + 1] * ys[i]
    return abs(area2) / 2.0


def fetch(bbox: Tuple[float, float, float, float], retries: int = 3) -> dict:
    return _post(build_query(bbox), retries)


def fetch_buildings(bbox: Tuple[float, float, float, float], retries: int = 3) -> dict:
    return _post(build_buildings_query(bbox), retries)


def _post(query: str, retries: int = 3) -> dict:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=120
            )
            if resp.status_code == 200:
                return resp.json()
            last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            last_err = str(exc)
        wait = 5 * attempt
        print(f"  attempt {attempt} failed ({last_err}); retrying in {wait}s...")
        time.sleep(wait)
    raise RuntimeError(f"Overpass request failed after {retries} attempts: {last_err}")


def split(elements: List[dict]) -> Tuple[List[dict], List[dict]]:
    """Separate street ways from land-use / polygon ways."""
    streets, polys = [], []
    for el in elements:
        if el.get("type") != "way":
            continue
        tags = el.get("tags", {})
        if "highway" in tags:
            streets.append(el)
        elif "landuse" in tags or tags.get("amenity") == "university":
            polys.append(el)
    return streets, polys


def write_raw_nodes(streets: List[dict], path: Path) -> int:
    """One row per geometry vertex of every street way (duplicates kept on purpose)."""
    count = 0
    with path.open("w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["way_id", "seq", "osm_node_id", "lat", "lon"])
        for w in streets:
            wid = w["id"]
            geom = w.get("geometry", [])
            node_ids = w.get("nodes", [None] * len(geom))
            for seq, pt in enumerate(geom):
                nid = node_ids[seq] if seq < len(node_ids) else ""
                wr.writerow([wid, seq, nid, pt["lat"], pt["lon"]])
                count += 1
    return count


def write_raw_streets(streets: List[dict], path: Path) -> int:
    """One row per consecutive vertex-pair (raw segment). Length in raw degrees."""
    count = 0
    with path.open("w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(
            ["way_id", "seg_seq", "from_node", "to_node",
             "from_lat", "from_lon", "to_lat", "to_lon",
             "highway", "name", "oneway", "maxspeed", "lanes", "deg_length"]
        )
        for w in streets:
            wid = w["id"]
            tags = w.get("tags", {})
            geom = w.get("geometry", [])
            node_ids = w.get("nodes", [None] * len(geom))
            for i in range(len(geom) - 1):
                a, b = geom[i], geom[i + 1]
                na = node_ids[i] if i < len(node_ids) else ""
                nb = node_ids[i + 1] if i + 1 < len(node_ids) else ""
                deg_len = (
                    (a["lat"] - b["lat"]) ** 2 + (a["lon"] - b["lon"]) ** 2
                ) ** 0.5
                wr.writerow([
                    wid, i, na, nb,
                    a["lat"], a["lon"], b["lat"], b["lon"],
                    tags.get("highway", ""),
                    tags.get("name", ""),          # often missing
                    tags.get("oneway", ""),        # often missing
                    tags.get("maxspeed", ""),      # mixed units / missing
                    tags.get("lanes", ""),         # often missing
                    f"{deg_len:.8f}",
                ])
                count += 1
    return count


def write_raw_landuse(polys: List[dict], path: Path) -> int:
    """One row per land-use polygon with a naive centroid + bbox (raw)."""
    count = 0
    with path.open("w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(
            ["way_id", "landuse", "amenity", "name",
             "centroid_lat", "centroid_lon",
             "min_lat", "min_lon", "max_lat", "max_lon", "n_pts"]
        )
        for w in polys:
            tags = w.get("tags", {})
            geom = w.get("geometry", [])
            if not geom:
                continue
            lats = [p["lat"] for p in geom]
            lons = [p["lon"] for p in geom]
            wr.writerow([
                w["id"],
                tags.get("landuse", ""),
                tags.get("amenity", ""),
                tags.get("name", ""),
                f"{sum(lats)/len(lats):.7f}",
                f"{sum(lons)/len(lons):.7f}",
                f"{min(lats):.7f}", f"{min(lons):.7f}",
                f"{max(lats):.7f}", f"{max(lons):.7f}",
                len(geom),
            ])
            count += 1
    return count


def write_raw_buildings(buildings: List[dict], path: Path) -> int:
    """One row per building footprint. Raw tags kept verbatim (levels/height
    frequently missing). footprint_m2 is a computed convenience column; the
    population estimate itself is left to the SQL cleaning step."""
    count = 0
    with path.open("w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(
            ["way_id", "building", "amenity", "name",
             "levels", "height", "centroid_lat", "centroid_lon",
             "footprint_m2", "n_pts"]
        )
        for w in buildings:
            tags = w.get("tags", {})
            geom = w.get("geometry", [])
            if len(geom) < 3:
                continue
            lats = [p["lat"] for p in geom]
            lons = [p["lon"] for p in geom]
            wr.writerow([
                w["id"],
                tags.get("building", ""),                 # "yes", "house", "apartments", "commercial", ...
                tags.get("amenity", ""),
                tags.get("name", ""),
                tags.get("building:levels", ""),          # often missing
                tags.get("height", ""),                   # often missing / mixed units
                f"{sum(lats)/len(lats):.7f}",
                f"{sum(lons)/len(lons):.7f}",
                f"{polygon_area_m2(geom):.2f}",
                len(geom),
            ])
            count += 1
    return count


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch raw OSM city data → CSV")
    ap.add_argument("--place", default="cambridge_core", choices=list(PLACES),
                    help="Which real area to download")
    args = ap.parse_args()

    label, bbox = PLACES[args.place]
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching real OSM data for {label}  bbox={bbox}")
    data = fetch(bbox)
    elements = data.get("elements", [])
    print(f"  received {len(elements)} OSM elements")

    streets, polys = split(elements)
    print(f"  {len(streets)} street ways, {len(polys)} land-use polygons")

    n_nodes = write_raw_nodes(streets, RAW_DIR / "raw_nodes.csv")
    n_segs = write_raw_streets(streets, RAW_DIR / "raw_streets.csv")
    n_poly = write_raw_landuse(polys, RAW_DIR / "raw_landuse.csv")

    # Second dataset: building footprints -> drives population & density.
    print("Fetching building footprints (population/density source)...")
    b_data = fetch_buildings(bbox)
    buildings = [el for el in b_data.get("elements", []) if el.get("type") == "way"]
    print(f"  received {len(buildings)} building ways")
    n_bldg = write_raw_buildings(buildings, RAW_DIR / "raw_buildings.csv")

    # tiny provenance file
    (RAW_DIR / "source.txt").write_text(
        f"source: OpenStreetMap via Overpass API\n"
        f"place: {label}\n"
        f"bbox (s,w,n,e): {bbox}\n"
        f"street_ways: {len(streets)}\n"
        f"landuse_polys: {len(polys)}\n"
        f"buildings: {len(buildings)}\n"
        f"raw_node_rows: {n_nodes}\n"
        f"raw_street_segments: {n_segs}\n"
        f"license: ODbL (OpenStreetMap contributors)\n"
    )

    print("\nWrote raw dataset to", RAW_DIR)
    print(f"  raw_nodes.csv      {n_nodes:>7} rows (vertices, with duplicates)")
    print(f"  raw_streets.csv    {n_segs:>7} rows (raw segments, degree lengths)")
    print(f"  raw_landuse.csv    {n_poly:>7} rows (zone polygons)")
    print(f"  raw_buildings.csv  {n_bldg:>7} rows (footprints -> population/density)")
    print("  source.txt         provenance / license")


if __name__ == "__main__":
    sys.exit(main())
