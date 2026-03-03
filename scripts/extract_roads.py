import os
import osmnx as ox
import geopandas as gpd
from pathlib import Path

def extract_capital_roads(capital_name, state_abbr, output_dir):
    """
    Extrai a malha viária principal (rodovias, avenidas primárias e secundárias) de uma capital
    usando OSMnx e salva como GeoJSON otimizado para o Flourish.
    """
    print(f"Buscando malha viária para: {capital_name} - {state_abbr}")
    
    # Busca a cidade usando a formatação "NOME, ESTADO, Brasil" para melhor precisão do Nominatim
    place_query = f"{capital_name}, {state_abbr}, Brasil"
    
    # Filtro customizado para OpenStreetMap (Apenas avenidas/eixos principais)
    # Isso impede o download de milhares de estradinhas de terra, becos e vias residenciais pesadas.
    custom_filter = '["highway"~"motorway|trunk|primary|secondary|motorway_link|trunk_link|primary_link|secondary_link"]'
    
    try:
        # Baixa os grafos de estradas usando o filtro
        G = ox.graph_from_place(place_query, network_type="drive", custom_filter=custom_filter)
        
        # Converte o grafo para um GeoDataFrame do GeoPandas (apenas arestas/ruas)
        _, edges = ox.graph_to_gdfs(G)
        
        # Remove colunas complexas do OSMNX que podem dar erro na conversão pra JSON
        # (Ex: listas no campo 'osmid', 'highway')
        cols_to_keep = ['name', 'highway', 'geometry']
        edges = edges[[c for c in cols_to_keep if c in edges.columns]]
        
        # Simplifica colunas com listas para strings separadas por vírgula (se existir)
        for col in edges.columns:
            if col != 'geometry':
                edges[col] = edges[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        # Aplica uma simplificação geométrica levíssima para aliviar o peso (remove vértices redundantes em retas)
        # 0.0001 graus = ~11 metros
        edges['geometry'] = edges.simplify(0.0001, preserve_topology=True)
        
        # Cria/salva
        os.makedirs(output_dir, exist_ok=True)
        output_path = Path(output_dir) / f"{capital_name}_ruas.geojson"
        
        edges.to_file(output_path, driver="GeoJSON")
        
        # Calcula tamanho gerado
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Sucesso! Salvo em {output_path} ({size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Erro ao processar {capital_name}: {e}")

import time

if __name__ == "__main__":
    out_dir = "/home/mateus/fipezap-pipeline/data/geojsons_ruas"
    os.makedirs(out_dir, exist_ok=True)
    
    # As 22 Capitais presentes no Índice FipeZAP
    capitais = [
        ("São Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Belo Horizonte", "MG"),
        ("Salvador", "BA"),
        ("Fortaleza", "CE"),
        ("Recife", "PE"),
        ("Porto Alegre", "RS"),
        ("Curitiba", "PR"),
        ("Florianópolis", "SC"),
        ("Vitória", "ES"),
        ("Goiânia", "GO"),
        ("Brasília", "DF"),
        ("Campo Grande", "MS"),
        ("Cuiabá", "MT"),
        ("João Pessoa", "PB"),
        ("Maceió", "AL"),
        ("Natal", "RN"),
        ("Aracaju", "SE"),
        ("São Luís", "MA"),
        ("Teresina", "PI"),
        ("Belém", "PA"),
        ("Manaus", "AM")
    ]
    
    print(f"Iniciando a extração da malha viária para {len(capitais)} capitais.")
    
    for cidade, estado in capitais:
        # Pula as que já existem para poupar a API da Overpass
        arquivo = Path(out_dir) / f"{cidade}_ruas.geojson"
        if arquivo.exists():
            print(f"Já existe: {cidade}. Pulando.")
            continue
            
        print(f"\n{"="*40}")
        extract_capital_roads(cidade, estado, out_dir)
        
        # Pausa de 3 segundos entre requisições para respeitar os limites da Overpass API pública e evitar banimentos
        time.sleep(3)
        
    print("\nProcesso de extração de malhas viárias finalizado!")
