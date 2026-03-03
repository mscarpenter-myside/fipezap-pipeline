import os
import re
import json
import glob
import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.validation import make_valid

import gspread
from google.oauth2.service_account import Credentials
import unicodedata as _ud

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Caminhos e Constantes
CRED_FILE    = "/home/mateus/fipezap-pipeline/credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
OUR_SHEET_ID = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"
base_dir     = Path("/home/mateus/fipezap-pipeline")
input_dir    = base_dir / "GeoJSONs - Bairros - Municípios"
output_dir   = base_dir / "data" / "geojsons_simplificados"
manifest_path = base_dir / "data" / "geojson_manifest.json"

FIPEZAP_CAPITALS = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Brasília", "Salvador", "Fortaleza",
    "Recife", "Porto Alegre", "Curitiba", "Florianópolis", "Vitória", "Goiânia",
    "João Pessoa", "Campo Grande", "Maceió", "Manaus", "Belém", "Cuiabá", "São Luís",
    "Teresina", "Aracaju", "Natal"
]

# Cache para correções de bairros
_BAIRRO_CORRECTIONS_CACHE = None

def _load_bairro_corrections():
    """Load the canonical bairro name map from check_bairros_novos tab."""
    global _BAIRRO_CORRECTIONS_CACHE
    if _BAIRRO_CORRECTIONS_CACHE is not None:
        return _BAIRRO_CORRECTIONS_CACHE

    try:
        logging.info("Carregando 'fonte da verdade' (check_bairros_novos)...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        ws = client.open_by_key(OUR_SHEET_ID).worksheet("check_bairros_novos")
        data = ws.get_all_values()

        # Encontrar linha de cabeçalho
        header_row = 1
        for i, row in enumerate(data):
            if any(c.strip().lower() in ('cidade', 'bairro') for c in row):
                header_row = i
                break

        rows = data[header_row + 1:]
        corrections = {}

        for row in rows:
            if len(row) < 2: continue
            bairro_raw = row[1].strip()
            if not bairro_raw: continue
            
            # Key: lowercase and lowercase without accents
            key = re.sub(r'\s+', ' ', bairro_raw.lower().strip())
            key_no_accent = ''.join(c for c in _ud.normalize('NFD', key) if _ud.category(c) != 'Mn')
            
            corrections[key_no_accent] = bairro_raw
            corrections[key] = bairro_raw
            
        logging.info(f"Loaded {len(corrections)} canonical bairro names.")
        _BAIRRO_CORRECTIONS_CACHE = corrections
        return corrections
    except Exception as e:
        logging.error(f"Erro ao carregar fonte da verdade: {e}")
        return {}

def fix_bairro_name(name, corrections=None):
    """Align name with source of truth."""
    if not name: return name
    if corrections is None: corrections = _BAIRRO_CORRECTIONS_CACHE or {}
    
    key = re.sub(r'\s+', ' ', str(name).strip().lower())
    if key in corrections:
        return corrections[key]
        
    key_stripped = ''.join(c for c in _ud.normalize('NFD', key) if _ud.category(c) != 'Mn')
    return corrections.get(key_stripped, name)

# Mapeamento de possíveis nomes de colunas de bairro
BAIRRO_COL_CANDIDATES = [
    'NM_BAIRRO', 'nome_bairro', 'BAIRRO', 'bairro', 'NOME', 'nome', 'Nome', 'Bairro', 'NM_BAIRR',
    'name', 'Name', 'NAME', 'addr:suburb', 'suburb', 'neighbourhood', 'neighborhood'
]

def find_bairro_column(df):
    """Tenta encontrar qual coluna contém o nome do bairro."""
    for col in BAIRRO_COL_CANDIDATES:
        if col in df.columns:
            return col
    return None

def process_geojson(file_path):
    """Processa, limpa e simplifica um arquivo GeoJSON."""
    file_name = file_path.name
    logging.info(f"Processando: {file_name}")
    
    try:
        # Carregar GeoJSON
        gdf = gpd.read_file(file_path)
        original_size = file_path.stat().st_size
        
        # Identificar coluna de bairro
        bairro_col = find_bairro_column(gdf)
        if not bairro_col:
            logging.warning(f"  Colunda de bairro não encontrada em {file_name}. Colunas: {gdf.columns.tolist()}")
            return {
                "cidade": file_name,
                "arquivo_original": file_name,
                "status": "ERRO: Coluna bairro não encontrada",
                "colunas": gdf.columns.tolist()
            }
        
        # Limpar: manter apenas bairro e geometry
        # Garantir geometrias válidas
        gdf['geometry'] = gdf['geometry'].apply(lambda x: make_valid(x) if x is not None else None)
        gdf = gdf[[bairro_col, 'geometry']].copy()
        gdf = gdf.rename(columns={bairro_col: 'nome_bairro'})
        
        # Alinhar nomes com a fonte da verdade
        corrections = _load_bairro_corrections()
        gdf['nome_bairro'] = gdf['nome_bairro'].apply(lambda x: fix_bairro_name(x, corrections))
        
        # Edge case handler para São Paulo (Jardins e Paraíso)
        if "são paulo" in file_name.lower() or "sao paulo" in file_name.lower():
            # Jardins não é bairro oficial, mapeamos para a geometria do Jardim Paulista
            gdf.loc[gdf['nome_bairro'] == 'Jardim Paulista', 'nome_bairro'] = 'Jardins'
            
            # Paraíso faz parte da Vila Mariana, duplicamos a geometria para ter ambos no mapa
            vila_mariana = gdf[gdf['nome_bairro'] == 'Vila Mariana']
            if not vila_mariana.empty:
                paraiso = vila_mariana.copy()
                paraiso['nome_bairro'] = 'Paraíso'
                gdf = pd.concat([gdf, paraiso], ignore_index=True)
        
        # Identificar quais bairros são "Destaque" (estão no FipeZAP)
        # Usamos os próprios valores das correções como referência de nomes canônicos
        canonical_meta_names = set(corrections.values())
        gdf['is_destaque'] = gdf['nome_bairro'].apply(lambda x: x in canonical_meta_names)
        
        # Simplificação Seletiva
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
            
        destaque_mask = gdf['is_destaque']
        logging.info(f"  Destaques: {destaque_mask.sum()} | Outros: {(~destaque_mask).sum()}")
        
        # Simplificação Seletiva mantendo a ordem original
        def selective_simplify(row):
            tolerance = 0.0001 if row['is_destaque'] else 0.005
            return row['geometry'].simplify(tolerance=tolerance, preserve_topology=True)
            
        gdf['geometry'] = gdf.apply(selective_simplify, axis=1)
        
        # Remover coluna auxiliar
        gdf = gdf.drop(columns=['is_destaque'])
        
        # Salvar versão simplificada
        output_path = output_dir / file_name
        gdf.to_file(output_path, driver='GeoJSON')
        
        final_size = output_path.stat().st_size
        reduction = (1 - (final_size / original_size)) * 100
        
        logging.info(f"  Sucesso! Tamanho: {original_size/1024:.1f}KB -> {final_size/1024:.1f}KB ({reduction:.1f}% redução) | Features: {len(gdf)}")
        
        return {
            "cidade": file_name.replace(".geojson", ""),
            "arquivo_original": file_name,
            "arquivo_final": str(output_path.relative_to(base_dir)),
            "tamanho_original_kb": round(original_size / 1024, 1),
            "tamanho_final_kb": round(final_size / 1024, 1),
            "reducao_porcentagem": round(reduction, 1),
            "num_features": len(gdf),
            "coluna_bairro_detectada": bairro_col,
            "status": "OK"
        }
        
    except Exception as e:
        logging.error(f"  Erro ao processar {file_name}: {e}")
        return {
            "cidade": file_name,
            "arquivo_original": file_name,
            "status": f"ERRO: {str(e)}"
        }

def main():
    # Garantir que o diretório de saída existe
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Carregar correções antecipadamente
    _load_bairro_corrections()
    
    # Ler de múltiplas pastas para pegar tanto os GeoJSONs manuais quanto os extraídos do IBGE
    all_files = []
    for source_dir in [base_dir / "data" / "geojsons_brutos", base_dir / "GeoJSONs - Bairros - Municípios"]:
        if source_dir.exists():
            all_files.extend(list(source_dir.glob("*.geojson")))

    # Deduplicar arquivos usando o nome da capital (lower) para evitar processar a mesma cidade duas vezes.
    # Dado que "GeoJSONs - Bairros - Municípios" é processado por último no loop acima, 
    # as versões manuais vão sobrescrever as do IBGE no dicionário abaixo.
    unique_files = {}
    capital_names_lower = [c.lower() for c in FIPEZAP_CAPITALS]

    for f in all_files:
        name_lower = f.name.replace(".geojson", "").strip().lower()
        # Tratamento especial para o RIO DE JANEIRO (que às vezes tem espaço no final)
        name_lower_clean = name_lower.replace(" ", "")

        for cap in capital_names_lower:
            cap_clean = cap.replace(" ", "")
            if cap_clean == name_lower_clean or cap_clean in name_lower_clean:
                unique_files[cap] = f # Mantém a última (priorizando manuais)
                break

    geojson_files = list(unique_files.values())
    logging.info(f"Processando {len(geojson_files)} capitais identificadas (desduplicadas).")
            
    logging.info(f"Processando {len(geojson_files)} capitais identificadas.")
    
    manifest = []
    processed_output_files = []
    for f in geojson_files:
        result = process_geojson(f)
        manifest.append(result)
        if result.get("status") == "OK":
            processed_output_files.append(Path(result["arquivo_final"]).name)
            
    # Limpar arquivos que não são capitais do diretório de saída
    for f in output_dir.glob("*.geojson"):
        if f.name not in processed_output_files:
            logging.info(f"Limpando arquivo não-capital: {f.name}")
            f.unlink()
        
    # Salvar manifesto
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)
        
    logging.info(f"\nManifesto gerado em: {manifest_path}")

if __name__ == "__main__":
    main()
