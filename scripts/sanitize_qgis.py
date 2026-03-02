import gspread
import logging
from google.oauth2.service_account import Credentials
import geopandas as gpd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
NEW_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"
BRUTOS_DIR = Path("/home/mateus/fipezap-pipeline/data/geojsons_brutos")

def fix_spreadsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(NEW_SHEET_ID)
    ws = next(w for w in sheet.worksheets() if 'Curitiba' in w.title)
    data = ws.get_all_values()
    
    for i, row in enumerate(data):
        if row and row[0] == 'Curitiba':
            cell = f"A{i+1}"
            ws.update_acell(cell, "Cidade Industrial")
            logging.info(f"Updated cell {cell} to 'Cidade Industrial' in Google Sheets.")
            break

def sanitize_geojson(filename):
    file_path = BRUTOS_DIR / filename
    if not file_path.exists():
        logging.warning(f"File {filename} not found in {BRUTOS_DIR}")
        return
        
    logging.info(f"Sanitizando {filename}...")
    gdf = gpd.read_file(file_path)
    
    if 'name' in gdf.columns:
        gdf = gdf.rename(columns={'name': 'NM_BAIRRO'})
        
    if 'NM_BAIRRO' not in gdf.columns:
        logging.error(f"Não foi possível encontrar a coluna de nomes no {filename}.")
        return
        
    # Se for curitiba, trocar o bairro "Curitiba" para "Cidade Industrial"
    if 'Curitiba' in filename:
        gdf['NM_BAIRRO'] = gdf['NM_BAIRRO'].replace('Curitiba', 'Cidade Industrial')
        
    # Manter só NM_BAIRRO e geometry
    gdf = gdf[['NM_BAIRRO', 'geometry']]
    
    # Salvar sobrescrevendo
    gdf.to_file(file_path, driver="GeoJSON")
    logging.info(f"Salvo {filename} com {len(gdf)} features sanitizadas.")

def main():
    fix_spreadsheet()
    for fname in ["Curitiba .geojson", "Rio de Janeiro.geojson", "SÃO PAULO.geojson"]:
        sanitize_geojson(fname)

if __name__ == "__main__":
    main()
