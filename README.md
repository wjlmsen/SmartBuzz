# SmartBuzz

# SmartBuzz EV Site Selection – Data Pipeline

This repo holds the first deliverable for identifying parking‐lot candidates in Long Island (Suffolk & Nassau Counties) that can host ~8 EV chargers.

## 1. Project layout

```
├── data
│   ├── raw/          # downloaded source files (zips, GDBs, etc.)
│   └── processed/    # cleaned & filtered GeoPackage layers
├── scripts/          # one-off ingestion & ETL helpers
│   └── download_parcels.py
├── requirements.txt  # Python dependencies
└── README.md         # you are here
```

## 2. Quick-start

1. Create & activate a virtual-env (optional but recommended):

   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the parcel ingestion script (downloads ~700 MB once):

   ```bash
   python scripts/download_parcels.py
   ```

   This will:
   • Download the NYS statewide parcel geodatabase
   • Extract only Suffolk County parcels
   • Save them to `data/processed/parcels_suffolk.gpkg`

## 3. Next steps

• Add Nassau County parcels (requires separate data source – TBD)
• Join SafeGraph / Placer foot-traffic stats
• Overlay NREL EV-charger station layer & utility feeders
• Implement scoring notebook and produce ranked candidates

Feel free to open issues or PRs ✌️
