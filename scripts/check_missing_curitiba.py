import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def main():
    gdf = gpd.read_file("/home/mateus/fipezap-pipeline/data/geojsons_brutos/Curitiba .geojson")
    for idx, row in gdf.iterrows():
        if row['name'] and len(row['name']) <= 3:
            logging.info(f"Short name: {row['name']}")
    
    no_name = gdf[gdf['name'].isna() | (gdf['name'] == '')]
    if not no_name.empty:
        logging.info(f"Features sem nome: {len(no_name)}")
        logging.info(f"Colunas do sem nome: {no_name[['osm_id', 'name', 'alt_name']]}")

if __name__ == "__main__":
    main()
