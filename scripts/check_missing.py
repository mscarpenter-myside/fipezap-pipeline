import json
import logging
import re
import difflib
import unicodedata as ud
from pathlib import Path
import gspread
import geopandas as gpd
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
NEW_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"
QGIS_DIR = Path("/home/mateus/fipezap-pipeline/GeoJSONs-QGIS")

def get_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    return gspread.authorize(creds)

def process_capital(client, geojson_path, sheet_id, tab_title_query):
    gdf = gpd.read_file(geojson_path)
    geo_names = [str(x) for x in gdf['name'].dropna().tolist()]
    
    sheet = client.open_by_key(sheet_id)
    ws = next(w for w in sheet.worksheets() if tab_title_query.lower() in w.title.lower())
    data = ws.get_all_values()
    headers = data[0]
    bairro_idx = next(i for i, h in enumerate(headers) if h.lower() == 'bairro')
    
    planilha_bairros = [row[bairro_idx].strip() for row in data[1:] if len(row) > bairro_idx and row[bairro_idx].strip()]
    
    matches = []
    missing = []
    
    for b in planilha_bairros:
        if b in geo_names:
            matches.append(b)
        else:
            missing.append(b)
            
    logging.info(f"[{geojson_path.stem}] Exatos: {len(matches)} | Missing: {len(missing)}")
    if missing:
        logging.info(f"[{geojson_path.stem}] Ex Missing: {missing[:10]}")

def main():
    client = get_client()
    for filename, tab_query in [("SÃO PAULO.geojson", "SÃO PAULO"), ("Curitiba .geojson", "Curitiba")]:
        path = QGIS_DIR / filename
        process_capital(client, path, NEW_SHEET_ID, tab_query)

if __name__ == "__main__":
    main()
