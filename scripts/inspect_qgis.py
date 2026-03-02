import geopandas as gpd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def main():
    qgis_dir = Path("/home/mateus/fipezap-pipeline/GeoJSONs-QGIS")
    for f in qgis_dir.glob("*.geojson"):
        try:
            gdf = gpd.read_file(f)
            logging.info(f"File: {f.name} - Columns: {list(gdf.columns)}")
            # Show top 5 rows
            logging.info(f"Head:\n{gdf.head()[list(gdf.columns)]}")
        except Exception as e:
            logging.error(f"Error reading {f.name}: {e}")

if __name__ == "__main__":
    main()
