import geopandas as gpd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def main():
    qgis_dir = Path("/home/mateus/fipezap-pipeline/data/geojsons_brutos")
    curitiba_file = qgis_dir / "Curitiba .geojson"
    
    if not curitiba_file.exists():
        logging.error(f"File not found: {curitiba_file}")
        return
        
    gdf = gpd.read_file(curitiba_file)
    names = gdf['name'].dropna().tolist()
    
    matches = [n for n in names if 'industrial' in n.lower() or 'cidade' in n.lower()]
    logging.info(f"Possible matches for 'Cidade Industrial' in Curitiba GeoJSON: {matches}")

if __name__ == "__main__":
    main()
