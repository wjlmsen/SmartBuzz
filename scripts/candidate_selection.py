from pathlib import Path
import sys
import warnings

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

# Thresholds (edit as needed)
MIN_STALLS = 8
STALLS_TO_AREA_RATIO = 10  # assume 1 stall per 10 parking stalls → ~1 per 10 total? We'll approximate area
MIN_AREA_SQFT = 800 * MIN_STALLS  # quick proxy if we assume 100 sqft per stall => 800 sqft; but escalate. We'll set 30000 below.

MIN_AREA_SQFT = 30000
MIN_DISTANCE_M = 800  # parcel should be at least 800m from existing EV station
LANDUSE_ALLOWED = {"COM", "C", "COMM", "10", "11", "12", "13"}  # example codes for commercial/retail

WORKSPACE = Path(__file__).resolve().parents[1]
DATA_PROCESSED = WORKSPACE / "data" / "processed"


#########################
# Utility functions
#########################


def load_parcels() -> gpd.GeoDataFrame:
    files = [
        DATA_PROCESSED / "parcels_suffolk.gpkg",
        DATA_PROCESSED / "parcels_nassau.gpkg",
    ]
    gdfs = []
    for fp in files:
        if fp.exists():
            print(f"[load] parcels → {fp}")
            gdf = gpd.read_file(fp)
            gdfs.append(gdf)
        else:
            print(f"[skip] {fp} not found")
    if not gdfs:
        raise FileNotFoundError("No parcel datasets found. Run download scripts first.")
    parcels = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    return parcels


def validate_parcels(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Ensure geometries are polygons
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[~gdf.geometry.is_empty]
    gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]
    # Fix invalid geometries if any
    if ( ~gdf.geometry.is_valid ).any():
        warnings.warn("Some parcel geometries invalid – attempting buffer(0) fix")
        gdf.loc[~gdf.geometry.is_valid, "geometry"] = (
            gdf.loc[~gdf.geometry.is_valid, "geometry"].buffer(0)
        )
    # Ensure CRS is projected for area/distance calcs
    if gdf.crs is None or gdf.crs.is_geographic:
        gdf = gdf.to_crs(6539)  # NAD83(2011) / New York Long Island (ftUS)
    return gdf


def add_area(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # area in square feet (since CRS uses US feet)
    gdf["area_sqft"] = gdf.geometry.area
    return gdf


def landuse_filter(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    lu_col = next(
        (c for c in gdf.columns if c.upper() in {"LANDUSE", "LU", "LU_DESC", "PROP_CLASS", "PROPCL"}),
        None,
    )
    if lu_col is None:
        warnings.warn("Land-use column not detected. Skipping land-use filter.")
        return gdf
    filtered = gdf[gdf[lu_col].astype(str).str.upper().isin(LANDUSE_ALLOWED)]
    print(f"[filter] {len(filtered):,}/{len(gdf):,} parcels after land-use filter")
    return filtered


def distance_filter(parcels: gpd.GeoDataFrame, stations: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Ensure same CRS (project to meters crs EPSG:32618 – UTM zone 18N covers Long Island)
    parcels_m = parcels.to_crs(32618)
    stations_m = stations.to_crs(32618)

    # Spatial index join for nearest distance
    joined = gpd.sjoin_nearest(parcels_m, stations_m[["geometry"]], how="left", distance_col="dist_m")
    filtered = joined[joined["dist_m"] >= MIN_DISTANCE_M].copy()
    print(
        f"[filter] {len(filtered):,}/{len(parcels):,} parcels beyond {MIN_DISTANCE_M} m from existing chargers"
    )
    return filtered.to_crs(parcels.crs)


#########################
# Main analysis
#########################


def main():
    try:
        parcels = load_parcels()
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(1)

    parcels = validate_parcels(parcels)
    parcels = add_area(parcels)

    # Area filter
    parcels = parcels[parcels["area_sqft"] >= MIN_AREA_SQFT]
    print(f"[filter] {len(parcels):,} parcels >= {MIN_AREA_SQFT} sqft")

    parcels = landuse_filter(parcels)

    # Load EV stations
    stations_fp = DATA_PROCESSED / "ev_stations_li.gpkg"
    if not stations_fp.exists():
        print("[warn] EV station layer not found – skipping distance filter.")
        final = parcels
    else:
        stations = gpd.read_file(stations_fp)
        final = distance_filter(parcels, stations)

    # Save
    out_fp = DATA_PROCESSED / "candidate_lots.gpkg"
    final.to_file(out_fp, driver="GPKG")
    print(f"[write] Saved {len(final):,} candidate lots → {out_fp}")


if __name__ == "__main__":
    import pandas as pd

    main()