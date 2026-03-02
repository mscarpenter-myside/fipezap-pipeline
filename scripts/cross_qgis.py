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

def normalize(s):
    if not isinstance(s, str): return ""
    s = s.strip().lower()
    return ''.join(c for c in ud.normalize('NFD', s) if ud.category(c) != 'Mn')

def process_capital(client, geojson_path, sheet_id, tab_title_query):
    logging.info(f"--- Processando Capital: {geojson_path.stem} ---")
    gdf = gpd.read_file(geojson_path)
    if 'name' not in gdf.columns:
        logging.error(f"GeoJSON sem coluna 'name': {geojson_path.name}")
        return
        
    geo_names = [str(x) for x in gdf['name'].dropna().tolist()]
    geo_norm_map = {normalize(n): n for n in geo_names}
    
    sheet = client.open_by_key(sheet_id)
    ws = None
    for w in sheet.worksheets():
        if tab_title_query.lower() in w.title.lower():
            ws = w
            break
            
    if not ws:
        logging.error(f"Aba para '{tab_title_query}' não encontrada.")
        return
        
    logging.info(f"Aba encontrada: '{ws.title}'")
    
    data = ws.get_all_values()
    if not data: return
    
    headers = data[0]
    bairro_idx = -1
    for i, h in enumerate(headers):
        if h.lower() == 'bairro':
            bairro_idx = i
            break
            
    if bairro_idx == -1:
        logging.error("Coluna 'Bairro' não encontrada.")
        return
        
    updates = []
    
    for row_idx, row in enumerate(data[1:], start=2): # 1-based, plus 1 for header
        if row_idx > len(data): break
        if len(row) <= bairro_idx: continue
        
        bairro_planilha = row[bairro_idx].strip()
        if not bairro_planilha: continue
        
        # Match exato
        if bairro_planilha in geo_names:
            continue
            
        bairro_norm = normalize(bairro_planilha)
        best_match = None
        
        # Match normalizado
        if bairro_norm in geo_norm_map:
            best_match = geo_norm_map[bairro_norm]
        else:
            # Fuzzy match
            geo_norm_keys = list(geo_norm_map.keys())
            close = difflib.get_close_matches(bairro_norm, geo_norm_keys, n=1, cutoff=0.7)
            if close:
                best_match = geo_norm_map[close[0]]
                
        if best_match and best_match != bairro_planilha:
            logging.info(f"Corrigindo: '{bairro_planilha}' -> '{best_match}'")
            # Update batch: {"range": f"A{row_idx}", "values": [[best_match]]}
            # Letter of column: chr(65 + bairro_idx)
            col_letter = chr(65 + bairro_idx)
            cell = f"{col_letter}{row_idx}"
            updates.append({"range": cell, "values": [[best_match]]})
            
    if updates:
        logging.info(f"Aplicando {len(updates)} atualizações na aba {ws.title}...")
        ws.batch_update(updates)
        logging.info("Atualizações concluídas.")
    else:
        logging.info("Nenhuma correção necessária ou possível.")

def main():
    client = get_client()
    mappings = {
        "SÃO PAULO.geojson": "SÃO PAULO",
        "Rio de Janeiro.geojson": "RIO DE JANEIRO",
        "Curitiba .geojson": "Curitiba"
    }
    
    for filename, tab_query in mappings.items():
        path = QGIS_DIR / filename
        if path.exists():
            process_capital(client, path, NEW_SHEET_ID, tab_query)
        else:
            logging.error(f"Arquivo não encontrado: {path}")

if __name__ == "__main__":
    main()
