from pathlib import Path
import os
import sys
import requests
import geopandas as gpd
from shapely.geometry import shape

# Nassau County parcel service information (requires authentication).
PORTAL_URL = "https://gis.nassaucountyny.gov/portal"
PARCEL_ITEM_ID = os.getenv("NASSAU_PARCELS_ITEM_ID")  # set if you have access
API_USERNAME = os.getenv("NASSAU_PORTAL_USER")
API_PASSWORD = os.getenv("NASSAU_PORTAL_PASSWORD")


def get_token(username: str, password: str, referer: str = "https://www.arcgis.com"):
    """Generate a short-lived token for ArcGIS Enterprise portal."""
    url = f"{PORTAL_URL}/sharing/rest/generateToken"
    data = {
        "username": username,
        "password": password,
        "client": "referer",
        "referer": referer,
        "expiration": 60,
        "f": "json",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    resp = r.json()
    if "token" not in resp:
        raise RuntimeError(f"Token generation failed: {resp}")
    return resp["token"]


def download_feature_service(token: str, item_id: str, out_fp: Path):
    metadata_url = f"{PORTAL_URL}/sharing/rest/content/items/{item_id}?f=json&token={token}"
    r = requests.get(metadata_url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Unable to fetch item metadata (status {r.status_code}).")
    meta = r.json()
    if "url" not in meta:
        raise RuntimeError("Item metadata missing 'url'. Cannot proceed.")
    service_url = meta["url"]

    # Query all features in chunks.
    query_url = f"{service_url}/0/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "geojson",
        "token": token,
    }
    print(f"[download] Querying {query_url}")
    r2 = requests.get(query_url, params=params, timeout=120)
    r2.raise_for_status()
    gjson = r2.json()
    # Convert to GeoDataFrame
    features = gjson.get("features", [])
    if not features:
        raise RuntimeError("No features returned. Check access permissions or item id.")
    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_fp, driver="GPKG")
    print(f"[write] Saved Nassau parcels to {out_fp}")



def main():
    workspace = Path(__file__).resolve().parents[1]
    processed_dir = workspace / "data" / "processed"
    out_fp = processed_dir / "parcels_nassau.gpkg"

    if not all([PARCEL_ITEM_ID, API_USERNAME, API_PASSWORD]):
        print("[warn] Nassau parcel integration requires credentials.\n"
              "Set env vars NASSAU_PARCELS_ITEM_ID, NASSAU_PORTAL_USER, NASSAU_PORTAL_PASSWORD."
              "\nSkipping download.")
        sys.exit(1)

    token = get_token(API_USERNAME, API_PASSWORD)
    download_feature_service(token, PARCEL_ITEM_ID, out_fp)


if __name__ == "__main__":
    main()