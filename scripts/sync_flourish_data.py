import json
import logging
import gspread
import pandas as pd
import geopandas as gpd
from pathlib import Path
from google.oauth2.service_account import Credentials

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Caminhos e Constantes
CRED_FILE      = "/home/mateus/mapas_bairros_mais_valorizados/credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
OUR_SHEET_ID   = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"
FLOURISH_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"
DATA_MONTH     = "2026-01"  # Mês base para sincronização

base_dir       = Path("/home/mateus/mapas_bairros_mais_valorizados")
manifest_path  = base_dir / "data" / "geojson_manifest.json"

def get_fipezap_data(client, city_name):
    """Busca os dados do FipeZAP para uma capital específica no mês base."""
    try:
        ws = client.open_by_key(OUR_SHEET_ID).worksheet(DATA_MONTH)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        # Filtrar pela cidade (buscamos match parcial já que o GeoJSON diz 'São Paulo' e a planilha 'São Paulo (SP)')
        df_city = df[df['Cidade'].str.contains(city_name, case=False, na=False)].copy()
        return df_city
    except Exception as e:
        logging.error(f"Erro ao buscar dados FipeZAP para {city_name}: {e}")
        return pd.DataFrame()

def main():
    # Autenticação
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    
    # Carregar Manifesto
    if not manifest_path.exists():
        logging.error("Manifesto não encontrado. Rode process_geojsons.py primeiro.")
        return
        
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
        
    master_ss = client.open_by_key(FLOURISH_SHEET_ID)
    
    for item in manifest:
        if item["status"] != "OK": continue
        
        city_name = item["cidade"]
        geojson_path = base_dir / item["arquivo_final"]
        
        logging.info(f"Sincronizando: {city_name}...")
        
        # 1. Ler GeoJSON para pegar a ordem e coordenadas
        gdf = gpd.read_file(geojson_path)
        
        # Calcular centroides para Lat e Lon
        # Se for WGS84 (EPSG:4326), centroid.y é Latitude e centroid.x é Longitude
        gdf['centroid'] = gdf.centroid
        gdf['latitude'] = gdf['centroid'].y
        gdf['longitude'] = gdf['centroid'].x
        
        neighborhoods_data = gdf[['nome_bairro', 'latitude', 'longitude']].to_dict('records')
        
        # 2. Buscar dados FipeZAP
        fipe_df = get_fipezap_data(client, city_name)
        
        # 3. Montar tabela sincronizada
        rows = []
        for item_geo in neighborhoods_data:
            bairro = item_geo['nome_bairro']
            # Procurar nos dados do FipeZAP
            match = fipe_df[fipe_df['Bairro'] == bairro]
            
            if not match.empty:
                row_data = match.iloc[0].to_dict()
                rows.append({
                    "Bairro": bairro,
                    "Valor do m²": row_data.get("Valor do m²", ""),
                    "Variação (12 meses)": row_data.get("Variação (12 meses)", ""),
                    "Destaque": "Sim",
                    "Latitude": item_geo['latitude'],
                    "Longitude": item_geo['longitude']
                })
            else:
                rows.append({
                    "Bairro": bairro,
                    "Valor do m²": "",
                    "Variação (12 meses)": "",
                    "Destaque": "Não",
                    "Latitude": item_geo['latitude'],
                    "Longitude": item_geo['longitude']
                })
        
        # 4. Upload para a planilha Master Flourish
        final_df = pd.DataFrame(rows).fillna('')
        
        # Preparar aba (limpar se já existir, criar se não)
        tab_name = city_name[:100] # Limite de nome de aba
        try:
            ws = master_ss.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = master_ss.add_worksheet(title=tab_name, rows="100", cols="20")
            
        # Escrever dados
        ws.update([final_df.columns.values.tolist()] + final_df.values.tolist())
        logging.info(f"  Sucesso! {len(final_df)} linhas enviadas para a aba '{tab_name}'.")

    # 5. Reordenar abas por ordem crescente (alfabética de capitais)
    reorder_tabs(client, FLOURISH_SHEET_ID)
    logging.info("\nSincronização completa!")

def reorder_tabs(client, sheet_id):
    """Sort all worksheets in the spreadsheet by name descending."""
    try:
        logging.info("Reordenando abas por ordem DESCRESCENTE...")
        ss = client.open_by_key(sheet_id)
        worksheets = ss.worksheets()
        # Sort worksheets by title descending.
        sorted_worksheets = sorted(worksheets, key=lambda w: w.title, reverse=True)
        
        ss.reorder_worksheets(sorted_worksheets)
        logging.info("Abas reordenadas com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao reordenar abas: {e}")

if __name__ == "__main__":
    main()
