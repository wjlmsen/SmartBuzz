from pathlib import Path
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

API_KEY = "DEMO_KEY"  # replace with production key if available
STATE = "NY"
FUEL_TYPE = "ELEC"

# Bounding box for Long Island (approx): (min_lon, min_lat, max_lon, max_lat)
BBOX = (-74.1, 40.45, -71.7, 41.3)

OUTPUT_NAME = "ev_stations_li"


def fetch_nrel_csv(api_key: str = API_KEY, state: str = STATE, fuel_type: str = FUEL_TYPE) -> pd.DataFrame:
    """Fetch NREL Alternative Fuel Stations CSV for given state and fuel type."""
    url = (
        "https://developer.nrel.gov/api/alt-fuel-stations/v1.csv"
        f"?api_key={api_key}&fuel_type={fuel_type}&state={state}&country=US&download=true"
    )
    print(f"[download] NREL stations → {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    csv_text = r.text
    df = pd.read_csv(pd.compat.StringIO(csv_text))
    return df


def to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert DataFrame with latitude/longitude to GeoDataFrame (EPSG:4326)."""
    geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf


def filter_bounding_box(gdf: gpd.GeoDataFrame, bbox=BBOX) -> gpd.GeoDataFrame:
    min_lon, min_lat, max_lon, max_lat = bbox
    within = gdf.cx[min_lon:max_lon, min_lat:max_lat]
    print(f"[filter] {len(within):,} stations within Long Island bbox")
    return within


def main():
    workspace = Path(__file__).resolve().parents[1]
    processed_dir = workspace / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    df = fetch_nrel_csv()
    gdf = to_geodataframe(df)
    gdf_li = filter_bounding_box(gdf)

    # Save outputs
    gpkg_path = processed_dir / f"{OUTPUT_NAME}.gpkg"
    csv_path = processed_dir / f"{OUTPUT_NAME}.csv"
    print(f"[write] GeoPackage → {gpkg_path}")
    gdf_li.to_file(gpkg_path, driver="GPKG")

    print(f"[write] CSV → {csv_path}")
    gdf_li.drop(columns="geometry").to_csv(csv_path, index=False)


if __name__ == "__main__":
    main()