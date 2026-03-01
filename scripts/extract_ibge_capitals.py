#!/usr/bin/env python3
"""
extract_ibge_capitals.py
========================
Opção B do pipeline: baixa o Shapefile de bairros do IBGE (Censo 2022),
filtra as 22 capitais monitoradas pelo FipeZAP, e exporta cada uma como
um arquivo GeoJSON individual, pronto para consumo pelo
process_geojsons.py.

Uso:
    python scripts/extract_ibge_capitals.py            # usa cache se já baixou
    python scripts/extract_ibge_capitals.py --force     # re-baixa o Shapefile
"""

import os
import sys
import shutil
import logging
import argparse
import zipfile
from pathlib import Path

import requests
import geopandas as gpd

# ──────────────────────────────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

BASE_DIR = Path("/home/mateus/mapas_bairros_mais_valorizados")
RAW_DIR = BASE_DIR / "data" / "ibge_raw"
OUTPUT_DIR = BASE_DIR / "data" / "geojsons_brutos"

# URL oficial do IBGE – Arquivo geoespacial de Bairros (Brasil inteiro, Shapefile)
IBGE_BAIRROS_URL = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
    "censo_2022/bairros/shp/BR/BR_bairros_CD2022.zip"
)

# Fallback URLs para tentar caso a principal falhe
IBGE_FALLBACK_URLS = [
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/"
    "censo_2022/bairros/shp/BR_bairros_CD2022.zip",
    "https://geoftp.ibge.gov.br/downloads/BR_bairros_CD2022.zip",
]

# As 22 capitais monitoradas pelo FipeZAP, mapeadas à sua UF.
# Usadas para filtragem pelo campo NM_MUN no Shapefile do IBGE.
FIPEZAP_CAPITALS = {
    "São Paulo":      "SP",
    "Rio de Janeiro": "RJ",
    "Belo Horizonte": "MG",
    "Brasília":       "DF",
    "Salvador":       "BA",
    "Fortaleza":      "CE",
    "Recife":         "PE",
    "Porto Alegre":   "RS",
    "Curitiba":       "PR",
    "Florianópolis":  "SC",
    "Vitória":        "ES",
    "Goiânia":        "GO",
    "João Pessoa":    "PB",
    "Campo Grande":   "MS",
    "Maceió":         "AL",
    "Manaus":         "AM",
    "Belém":          "PA",
    "Cuiabá":         "MT",
    "São Luís":       "MA",
    "Teresina":       "PI",
    "Aracaju":        "SE",
    "Natal":          "RN",
}


# ──────────────────────────────────────────────────────────────────────
# Funções
# ──────────────────────────────────────────────────────────────────────

def download_ibge_shapefile(force: bool = False) -> Path:
    """Baixa o Shapefile nacional de bairros do IBGE e extrai o ZIP.

    Retorna o caminho para o diretório onde os .shp foram extraídos.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "BR_bairros_CD2022.zip"
    extract_dir = RAW_DIR / "BR_bairros_CD2022"

    if zip_path.exists() and not force:
        logging.info(f"Shapefile já em cache: {zip_path}")
    else:
        urls_to_try = [IBGE_BAIRROS_URL] + IBGE_FALLBACK_URLS
        downloaded = False

        for url in urls_to_try:
            logging.info(f"Baixando Shapefile de bairros do IBGE: {url} ...")
            try:
                resp = requests.get(url, stream=True, timeout=300)
                if resp.status_code == 200:
                    total = int(resp.headers.get("content-length", 0))
                    with open(zip_path, "wb") as f:
                        downloaded_bytes = 0
                        for chunk in resp.iter_content(chunk_size=1024 * 256):
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                            if total:
                                pct = downloaded_bytes / total * 100
                                print(
                                    f"\r  Progresso: {downloaded_bytes / 1024 / 1024:.1f} MB"
                                    f" / {total / 1024 / 1024:.1f} MB ({pct:.0f}%)",
                                    end="",
                                    flush=True,
                                )
                    print()  # newline after progress
                    logging.info(f"Download concluído ({downloaded_bytes / 1024 / 1024:.1f} MB)")
                    downloaded = True
                    break
                else:
                    logging.warning(f"  URL retornou status {resp.status_code}, tentando próxima...")
            except requests.RequestException as e:
                logging.warning(f"  Erro ao acessar URL: {e}, tentando próxima...")

        if not downloaded:
            logging.error(
                "Não foi possível baixar o Shapefile de nenhuma URL.\n"
                "Possíveis soluções:\n"
                "  1. Acesse https://www.ibge.gov.br/geociencias/organizacao-do-territorio/"
                "malhas-territoriais/26577-malha-de-bairros.html\n"
                "  2. Baixe 'Arquivo geoespacial de Bairros – Brasil (shp)'\n"
                f"  3. Salve o .zip em: {zip_path}\n"
                "  4. Re-execute este script."
            )
            sys.exit(1)

    # Extrair ZIP
    if extract_dir.exists() and not force:
        logging.info(f"Shapefile já extraído: {extract_dir}")
    else:
        logging.info("Extraindo ZIP...")
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        logging.info("Extração concluída.")

    return extract_dir


def find_shapefile(extract_dir: Path) -> Path:
    """Encontra o arquivo .shp principal dentro do diretório extraído."""
    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        logging.error(f"Nenhum .shp encontrado em {extract_dir}")
        sys.exit(1)

    # Prefere o arquivo que contém 'bairro' no nome
    for shp in shp_files:
        if "bairro" in shp.name.lower():
            return shp
    return shp_files[0]


def detect_municipality_column(gdf: gpd.GeoDataFrame) -> str:
    """Detecta a coluna que contém o nome do município."""
    candidates = ["NM_MUN", "NM_MUNICIP", "NOME_MUN", "nome_mun", "municipio", "MUNICIPIO"]
    for col in candidates:
        if col in gdf.columns:
            return col

    # Tentar encontrar por conteúdo: procurar uma coluna que contenha "São Paulo"
    for col in gdf.columns:
        if gdf[col].dtype == "object":
            sample = gdf[col].head(1000)
            if sample.str.contains("Paulo", case=False, na=False).any():
                return col

    logging.error(f"Coluna de município não encontrada. Colunas disponíveis: {gdf.columns.tolist()}")
    sys.exit(1)


def detect_bairro_column(gdf: gpd.GeoDataFrame) -> str:
    """Detecta a coluna que contém o nome do bairro."""
    candidates = ["NM_BAIRRO", "NM_BAIRR", "NOME_BAIRRO", "nome_bairro", "BAIRRO", "bairro"]
    for col in candidates:
        if col in gdf.columns:
            return col
    logging.warning(f"Coluna de bairro não encontrada. Colunas disponíveis: {gdf.columns.tolist()}")
    return None


def extract_capitals(extract_dir: Path) -> dict:
    """Lê o Shapefile, filtra as capitais FipeZAP e exporta GeoJSONs individuais.

    Retorna um dict com os resultados por capital.
    """
    shp_path = find_shapefile(extract_dir)
    logging.info(f"Lendo Shapefile: {shp_path}")
    logging.info("  (Isso pode levar alguns minutos para o arquivo do Brasil inteiro...)")

    gdf = gpd.read_file(shp_path)
    logging.info(f"  Shapefile carregado: {len(gdf)} features, colunas: {gdf.columns.tolist()}")

    # Detectar colunas
    mun_col = detect_municipality_column(gdf)
    bairro_col = detect_bairro_column(gdf)
    logging.info(f"  Coluna município: {mun_col} | Coluna bairro: {bairro_col or '(não encontrada)'}")

    # Reprojetar para EPSG:4326 se necessário
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        logging.info(f"  Reprojetando de {gdf.crs} para EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    # Criar diretório de saída
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    capital_names = list(FIPEZAP_CAPITALS.keys())

    # Normalizar nomes de municípios no DataFrame para comparação
    mun_values = gdf[mun_col].str.strip()

    for capital in capital_names:
        logging.info(f"  Extraindo: {capital}...")

        # Filtro por nome do município (case-insensitive match)
        mask = mun_values.str.lower() == capital.lower()
        capital_gdf = gdf[mask].copy()

        if capital_gdf.empty:
            # Tentar match parcial
            mask = mun_values.str.contains(capital, case=False, na=False)
            capital_gdf = gdf[mask].copy()

        if capital_gdf.empty:
            logging.warning(f"    ⚠ Nenhum bairro encontrado para '{capital}'")
            results[capital] = {"status": "NOT_FOUND", "bairros": 0}
            continue

        # Manter apenas coluna de bairro + geometria
        keep_cols = ["geometry"]
        if bairro_col and bairro_col in capital_gdf.columns:
            keep_cols.insert(0, bairro_col)
            capital_gdf = capital_gdf[keep_cols].copy()
            # Renomear para nome padronizado
            capital_gdf = capital_gdf.rename(columns={bairro_col: "NM_BAIRRO"})
        else:
            capital_gdf = capital_gdf[keep_cols].copy()

        # Salvar GeoJSON
        output_path = OUTPUT_DIR / f"{capital}.geojson"
        capital_gdf.to_file(output_path, driver="GeoJSON")

        size_kb = output_path.stat().st_size / 1024
        num_bairros = len(capital_gdf)
        logging.info(f"    ✓ {num_bairros} bairros | {size_kb:.1f} KB → {output_path.name}")

        results[capital] = {
            "status": "OK",
            "bairros": num_bairros,
            "size_kb": round(size_kb, 1),
            "path": str(output_path),
        }

    return results


def print_report(results: dict):
    """Imprime um relatório final com todas as capitais processadas."""
    print("\n" + "=" * 70)
    print("RELATÓRIO DE EXTRAÇÃO — Bairros IBGE → GeoJSON")
    print("=" * 70)
    print(f"{'Capital':<20} {'UF':>3} {'Bairros':>8} {'Tamanho':>10} {'Status'}")
    print("-" * 70)

    ok_count = 0
    for capital, uf in FIPEZAP_CAPITALS.items():
        r = results.get(capital, {"status": "SKIPPED", "bairros": 0})
        status = r["status"]
        bairros = r.get("bairros", 0)
        size = f"{r.get('size_kb', 0):.0f} KB" if r.get("size_kb") else "-"

        icon = "✓" if status == "OK" else "✗"
        print(f"  {icon} {capital:<18} {uf:>3} {bairros:>8} {size:>10} {status}")

        if status == "OK":
            ok_count += 1

    print("-" * 70)
    print(f"  Total: {ok_count}/{len(FIPEZAP_CAPITALS)} capitais extraídas com sucesso.")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 70)


# Capitais sabidamente AUSENTES do shapefile de bairros do IBGE Censo 2022.
# Nem todos os municípios do Brasil possuem bairros oficialmente delimitados
# no levantamento do IBGE. Brasília (DF), Goiânia (GO) e São Luís (MA) não
# constam na base. Os GeoJSONs dessas cidades deverão ser obtidos de outra
# fonte (ex.: prefeitura, OpenStreetMap, etc.).
IBGE_KNOWN_MISSING = {"Brasília", "Goiânia", "São Luís"}


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extrai GeoJSONs de bairros das capitais FipeZAP a partir do Shapefile do IBGE."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Força re-download e re-extração do Shapefile IBGE.",
    )
    args = parser.parse_args()

    logging.info("=== Extração de GeoJSONs de Bairros (IBGE → FipeZAP Capitals) ===")

    # 1. Download do Shapefile
    extract_dir = download_ibge_shapefile(force=args.force)

    # 2. Extrair e exportar cada capital
    results = extract_capitals(extract_dir)

    # 3. Relatório
    print_report(results)

    # Checar se todas foram extraídas (ignorando as sabidamente ausentes)
    failed = [c for c, r in results.items() if r["status"] != "OK"]
    unexpected_failures = [c for c in failed if c not in IBGE_KNOWN_MISSING]
    expected_missing = [c for c in failed if c in IBGE_KNOWN_MISSING]

    if expected_missing:
        logging.warning(
            f"Capitais ausentes do IBGE (esperado): {', '.join(expected_missing)}. "
            "Essas cidades não possuem bairros delimitados no Censo 2022 do IBGE. "
            "Utilize outra fonte (prefeitura, OSM) para obter os GeoJSONs dessas cidades."
        )

    if unexpected_failures:
        logging.error(f"Capitais com falha inesperada: {', '.join(unexpected_failures)}")
        logging.info(
            "Dica: verifique se os nomes no Shapefile do IBGE correspondem "
            "aos nomes esperados (NM_MUN)."
        )
        sys.exit(1)

    ok_count = sum(1 for r in results.values() if r["status"] == "OK")
    logging.info(f"Extração concluída! {ok_count}/{len(FIPEZAP_CAPITALS)} capitais extraídas do IBGE.")


if __name__ == "__main__":
    main()

