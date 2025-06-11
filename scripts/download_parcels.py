import os
import shutil
import tempfile
from pathlib import Path

import requests
import geopandas as gpd

DATA_URL = (
    "https://gisdata.ny.gov/GISData/State/Parcels/NYS-Tax-Parcels.zip"
)
COUNTY_FILTERS = ["SUFFOLK"]  # Adjust list to include more counties when data becomes open.


def download_zip(url: str, dest: Path, chunk_size: int = 1 << 20):
    """Download a remote ZIP file to the destination path with streaming."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"[skip] {dest} already exists")
        return dest
    print(f"[download] {url} → {dest}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
    return dest


def extract_gdb_from_zip(zip_path: Path, extract_to: Path) -> Path:
    """Extract the .gdb directory from the ZIP into a temp folder and return path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Extract only the geodatabase directory
        shutil.unpack_archive(zip_path, tmpdir)
        gdb_dirs = list(Path(tmpdir).rglob("*.gdb"))
        if not gdb_dirs:
            raise FileNotFoundError("No .gdb found in downloaded ZIP")
        gdb_src = gdb_dirs[0]
        extract_to.mkdir(parents=True, exist_ok=True)
        target_gdb = extract_to / gdb_src.name
        if target_gdb.exists():
            shutil.rmtree(target_gdb)
        shutil.copytree(gdb_src, target_gdb)
        return target_gdb


def filter_county(gdb_path: Path, out_dir: Path, counties=COUNTY_FILTERS):
    """Read parcels from geodatabase, filter counties, save to GeoPackage."""
    print(f"[read] Loading parcels from {gdb_path}")
    layers = gpd.io.file.fiona.listlayers(gdb_path)
    # Standard dataset layer name is likely 'NYS_Tax_Parcels_Public'.
    layer_name = None
    for cand in ["NYS_Tax_Parcels_Public", "TaxParcels", "parcels"] + layers:
        if cand in layers:
            layer_name = cand
            break
    if layer_name is None:
        raise ValueError(f"Could not find parcel layer in {gdb_path}. Layers: {layers}")

    gdf = gpd.read_file(gdb_path, layer=layer_name)
    county_col = next((c for c in gdf.columns if c.upper().startswith("COUNTY")), None)
    if county_col is None:
        raise ValueError("County column not found in parcel dataset")

    counties_upper = [c.upper() for c in counties]
    filtered = gdf[gdf[county_col].str.upper().isin(counties_upper)].copy()
    print(f"[filter] {len(filtered):,} parcels match county filter {counties}")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / f"parcels_{'_'.join(counties_upper).lower()}.gpkg"
    print(f"[write] Saving to {out_fp}")
    filtered.to_file(out_fp, driver="GPKG")
    return out_fp


def main():
    workspace = Path(__file__).resolve().parents[1]
    raw_dir = workspace / "data" / "raw"
    processed_dir = workspace / "data" / "processed"

    zip_path = raw_dir / "NYS-Tax-Parcels.zip"
    download_zip(DATA_URL, zip_path)

    gdb_dir = raw_dir / "NYS_Tax_Parcels_Public.gdb"
    if not gdb_dir.exists():
        gdb_dir = extract_gdb_from_zip(zip_path, raw_dir)

    filter_county(gdb_dir, processed_dir, counties=COUNTY_FILTERS)


if __name__ == "__main__":
    main()