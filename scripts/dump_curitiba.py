import geopandas as gpd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def main():
    curitiba_file = Path("/home/mateus/fipezap-pipeline/data/geojsons_brutos/Curitiba .geojson")
    gdf = gpd.read_file(curitiba_file)
    names = gdf['name'].dropna().tolist()
    logging.info(f"All names in Curitiba:\n{sorted(names)}")

if __name__ == "__main__":
    main()
