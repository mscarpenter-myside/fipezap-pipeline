import os
import re
import sys
import glob
import unicodedata
from pathlib import Path
import pandas as pd
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Dynamically loaded from 'check_bairros_novos' tab (source of truth for bairro names)
# Key: stripped lowercase name from PDF -> Value: correct accented name
_BAIRRO_CORRECTIONS_CACHE = None

def _load_bairro_corrections(cred_file, sheet_id):
    """Load the canonical bairro name map from check_bairros_novos tab."""
    global _BAIRRO_CORRECTIONS_CACHE
    if _BAIRRO_CORRECTIONS_CACHE is not None:
        return _BAIRRO_CORRECTIONS_CACHE

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(cred_file, scopes=scopes)
        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_id).worksheet("check_bairros_novos")
        data = ws.get_all_values()

        # Row 0 is blank, row 1 is the header (cidade, bairro)
        # Find the actual header row
        header_row = 1
        for i, row in enumerate(data):
            if any(c.strip().lower() in ('cidade', 'bairro') for c in row):
                header_row = i
                break

        rows = data[header_row + 1:]  # data rows after header
        corrections = {}

        for row in rows:
            if len(row) < 2:
                continue
            bairro_raw = row[1].strip() if len(row) > 1 else ""
            if not bairro_raw:
                continue
            key = re.sub(r'\s+', ' ', bairro_raw.lower().strip())
            # Also strip accents to create the unaccented key
            import unicodedata as _ud
            key_no_accent = ''.join(
                c for c in _ud.normalize('NFD', key) if _ud.category(c) != 'Mn'
            )
            corrections[key_no_accent] = bairro_raw
            corrections[key] = bairro_raw  # also map accented key to itself

        logging.info(f"[check_bairros_novos] Loaded {len(corrections)} bairro corrections.")
        _BAIRRO_CORRECTIONS_CACHE = corrections
        return corrections

    except Exception as e:
        logging.warning(f"[check_bairros_novos] Could not load — using empty corrections: {e}")
        _BAIRRO_CORRECTIONS_CACHE = {}
        return {}

def fix_bairro_name(name, corrections=None):
    """Fix accent-stripped bairro name using the canonical check_bairros_novos map."""
    if corrections is None:
        corrections = _BAIRRO_CORRECTIONS_CACHE or {}
    if not name or not corrections:
        return name
    key = re.sub(r'\s+', ' ', name.strip().lower())
    # Try exact lowercase match first
    if key in corrections:
        return corrections[key]
    # Try accent-stripped match
    import unicodedata as _ud
    key_stripped = ''.join(c for c in _ud.normalize('NFD', key) if _ud.category(c) != 'Mn')
    return corrections.get(key_stripped, name)

def standardize_bairro(bairro_name):
    """Auto-corrige nomes baseando-se na lista oficial (check_bairros_novos) para evitar duplicadas por digitação ou truncamento (...)"""
    corrections = _BAIRRO_CORRECTIONS_CACHE or {}
    if not corrections:
        return bairro_name
        
    all_official_bairros = list(set(corrections.values()))
    if bairro_name in all_official_bairros:
        return bairro_name
        
    clean_name = bairro_name.replace('...', '').replace('…', '').strip()
    
    # 1. Checa por truncamento (ex: Pedro Ludovico/Bela... -> Pedro Ludovico/Bela Vista)
    matches = [kb for kb in all_official_bairros if kb.lower().startswith(clean_name.lower())]
    if len(matches) == 1:
        logging.info(f"Auto-correção por truncamento: '{bairro_name}' -> '{matches[0]}'")
        return matches[0]
        
    # 2. Checa por similaridade de digitação (ex: pequenas diferenças de acento/espaço)
    import difflib
    close_matches = difflib.get_close_matches(clean_name, all_official_bairros, n=1, cutoff=0.88)
    if close_matches:
        logging.info(f"Auto-correção por similaridade: '{bairro_name}' -> '{close_matches[0]}'")
        return close_matches[0]
        
    return bairro_name


def format_variation(v_decimal):
    """Format variation: suppress trailing zeros, handle -0 and exact-zero edge cases."""
    if v_decimal == 0 or v_decimal == -0.0:
        return "0"
    s = f"{v_decimal:g}".replace(".", ",")
    if s.startswith(","):
        s = "0" + s
    elif s.startswith("-,"):
        s = "-0" + s[1:]
    return s

def parse_pdf_data(filepath):

    logging.info(f"Fazendo o parsing de: {filepath}")
    data = []
    # List of known cities from the document
    expected_cities = [
        "SÃO PAULO", "RIO DE JANEIRO", "BELO HORIZONTE", "BRASÍLIA", "SALVADOR", "FORTALEZA",
        "RECIFE", "PORTO ALEGRE", "CURITIBA", "FLORIANÓPOLIS", "VITÓRIA", "GOIÂNIA",
        "JOÃO PESSOA", "CAMPO GRANDE", "MACEIÓ", "MANAUS",
        "BELÉM", "CUIABÁ", "SÃO LUÍS", "TERESINA", "ARACAJU", "NATAL"
    ]
    
    uf_map = {
        "SÃO PAULO": "SP", "RIO DE JANEIRO": "RJ", "BELO HORIZONTE": "MG", "BRASÍLIA": "DF", 
        "SALVADOR": "BA", "FORTALEZA": "CE", "RECIFE": "PE", "PORTO ALEGRE": "RS", 
        "CURITIBA": "PR", "FLORIANÓPOLIS": "SC", "VITÓRIA": "ES", "GOIÂNIA": "GO",
        "JOÃO PESSOA": "PB", "CAMPO GRANDE": "MS", "MACEIÓ": "AL", "MANAUS": "AM",
        "BELÉM": "PA", "CUIABÁ": "MT", "SÃO LUÍS": "MA", "TERESINA": "PI", 
        "ARACAJU": "SE", "NATAL": "RN"
    }

    # Bairro regex: R$ 10.081 (optionally followed by /m²) +4,3%
    # We make the \s*/m² part optional by wrapping it in (\s*/m²)?
    bairro_re = re.compile(r'R\$\s+([\d\.]+)(?:\s+/m²)?\s+([\+\-]?\d+,\d+)%')
    
    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
                
            if "Zonas, distritos ou bairros mais representativos" in text:
                city_name = None
                
                # Check top words in the entire page using bounding boxes
                words = page.extract_words()
                words.sort(key=lambda w: w['top'])
                # Get the first 40 words just to be safe it hasn't skipped past logos
                top_text = " ".join([w['text'].upper() for w in words[:40]])
                
                for expected in expected_cities:
                    # Because we use ' ' as separator, 'SÃO PAULO' might be combined. 
                    # We just check if the substring is in the top_text string.
                    if expected in top_text:
                        city_formatted = expected.title().replace(" De ", " de ").replace(" Da ", " da ").replace(" Do ", " do ")
                        uf = uf_map[expected]
                        city_name = f"{city_formatted} ({uf})"
                        if city_name.upper().startswith("SÃO PAULO"):
                            city_name = "São Paulo (SP)"
                        break
                        
                if not city_name:
                    logging.warning(f"Could not find city name on page {i+1}. Top Text was: {top_text[:100]}...")
                    continue
                    
                in_bairro_section = False
                lines = text.split('\n')
                found_bairros = False
                for line in lines:
                    if "preço médio em" in line.lower() and "12 meses" in line.lower():
                        in_bairro_section = True
                        continue
                    
                    if in_bairro_section:
                        if "Fonte:" in line or "Fipe não divulga" in line:
                            break
                            
                        # Search for Price and Variation. Example: R$ 6.947 /m² +5,2% or R$ 4.298 -0,181%
                        # The price group captures digits, dots and commas. The /m² is optional. The var group captures +/- and digits/commas.
                        match = re.search(r'R\$\s*([\d\.,]+)(?:\s*/m²)?\s*([\+\-]?\d+,\d+)%', line)
                        
                        if match:
                            found_bairros = True
                            price_str = match.group(1)
                            var_str = match.group(2)
                            
                            prefix = line[:match.start()].strip()
                            prefix = re.sub(r'Preço médio\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'mais alto\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'mais baixo\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'\(R\$/m²\)\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'Sem informação\s*', '', prefix, flags=re.IGNORECASE)
                            
                            raw_name = prefix.strip()
                            # Only titlecase if the extracted text is all-uppercase (PDF artefact)
                            if raw_name == raw_name.upper():
                                bairro_name = raw_name.title()
                                bairro_name = bairro_name.replace(" De ", " de ").replace(" Da ", " da ").replace(" Do ", " do ")
                            else:
                                bairro_name = raw_name
                            # Fix accent-stripped names from PDF
                            bairro_name = fix_bairro_name(bairro_name)
                            # E padroniza de acordo com o histórico da planilha
                            bairro_name = standardize_bairro(bairro_name)
                            
                            v = float(var_str.replace(',', '.'))
                            v_decimal = v / 100
                            v_formatted = format_variation(v_decimal)
                            
                            price_formatted = f"R$ {price_str}"
                            
                            data.append([city_name, bairro_name, price_formatted, v_formatted])
                            
                # Fallback: if we didn't extract any neighborhoods from the text, they might be in a Table
                if not found_bairros:
                    # Let's inspect the tables on this page
                    tables = page.extract_tables()
                    for t in tables:
                        for row in t:
                            for c in row:
                                if not c:
                                    continue
                                # The cell might contain multiple lines if pdfplumber grouped them
                                lines_in_cell = str(c).split('\n')
                                for line in lines_in_cell:
                                    # Same regex
                                    match = re.search(r'R\$\s*([\d\.,]+)(?:\s*/m²)?\s*([\+\-]?\d+,\d+)%', line)
                                    if match:
                                        price_str = match.group(1)
                                        var_str = match.group(2)
                                        
                                        prefix = line[:match.start()].strip()
                                        prefix = re.sub(r'Preço médio\s*', '', prefix, flags=re.IGNORECASE)
                                        prefix = re.sub(r'mais alto\s*', '', prefix, flags=re.IGNORECASE)
                                        prefix = re.sub(r'mais baixo\s*', '', prefix, flags=re.IGNORECASE)
                                        prefix = re.sub(r'\(R\$/m²\)\s*', '', prefix, flags=re.IGNORECASE)
                                        prefix = re.sub(r'Sem informação\s*', '', prefix, flags=re.IGNORECASE)
                                        
                                        raw_name = prefix.strip()
                                        if raw_name == raw_name.upper():
                                            bairro_name = raw_name.title()
                                            bairro_name = bairro_name.replace(" De ", " de ").replace(" Da ", " da ").replace(" Do ", " do ")
                                        else:
                                            bairro_name = raw_name
                                        # Fix accent-stripped names from PDF
                                        bairro_name = fix_bairro_name(bairro_name)
                                        # E padroniza de acordo com o histórico da planilha
                                        bairro_name = standardize_bairro(bairro_name)
                                        
                                        v = float(var_str.replace(',', '.'))
                                        v_decimal = v / 100
                                        v_formatted = format_variation(v_decimal)
                                        
                                        price_formatted = f"R$ {price_str}"
                                        
                                        data.append([city_name, bairro_name, price_formatted, v_formatted])
                            
    df = pd.DataFrame(data, columns=["Cidade", "Bairro", "Valor do m²", "Variação (12 meses)"])
    # Preserve PDF order (same order as reference sheet — do NOT sort alphabetically)
    return df

CRED_FILE    = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
OUR_SHEET_ID = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"
REFERENCE_BQ_SHEET_ID = "1esOzR5cl1NboGfEBxZSHjG76_4DhHXXCGWLLJMsMRn8"

def update_google_sheet(client, sheet, df, tab_name):
    try:
        worksheet = sheet.worksheet(tab_name)
        logging.info(f"Tab {tab_name} exists, clearing it...")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        logging.info(f"Creating new tab: {tab_name}")
        worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="4")
        
    # Prepare data for upload
    upload_data = [df.columns.tolist()] + df.values.tolist()
    
    # Update spreadsheet
    worksheet.update(values=upload_data, range_name=f"A1:{gspread.utils.rowcol_to_a1(len(upload_data), len(df.columns))}")
    logging.info(f"Successfully updated tab {tab_name} with {len(df)} rows.")

def reorder_tabs(creds_file, sheet_id):
    """Sort all worksheets in the spreadsheet by name ascending."""
    try:
        logging.info("Reordenando abas por ordem DESCRESCENTE (recentes primeiro)...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        ss = client.open_by_key(sheet_id)
        
        worksheets = ss.worksheets()
        
        def sort_key(w):
            # Coloca datas (YYYY-MM) primeiro em ordem decrescente, 
            # e outras abas (atualizacao, etc) depois.
            title = w.title
            if re.match(r'^\d{4}-\d{2}', title):
                return (1, title) # Grupo 1: Datas
            return (0, title) # Grupo 0: Utilitários
            
        sorted_worksheets = sorted(worksheets, key=sort_key, reverse=True)
        
        ss.reorder_worksheets(sorted_worksheets)
        logging.info("Abas reordenadas com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao reordenar abas: {e}")

def main():
    # Load canonical bairro names from check_bairros_novos (source of truth) using the BigQuery Sheet
    _load_bairro_corrections(CRED_FILE, REFERENCE_BQ_SHEET_ID)

    pdf_files = glob.glob("data/raw/*.pdf")
    if not pdf_files:
        logging.warning("No PDF files found in data/raw/")
        return
        
    # Regex to find something like fipezap-202601-residencial
    date_regex = re.compile(r'fipezap-(\d{4})(\d{2})-')
    
    # Conecta no Google Sheets de uma vez
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(OUR_SHEET_ID)
    
    # Captura abas que já existem para não fazer parsing atoa
    existing_tabs = [w.title for w in sheet.worksheets()]
            
    for pdf in pdf_files:
        match = date_regex.search(pdf)
        if match:
            year = match.group(1)
            month = match.group(2)
            tab_name = f"{year}-{month}"
            
            # Se a aba já existe na nuvem, ele desconsidera (conforme solicitado pelo usuário)
            if tab_name in existing_tabs:
                logging.info(f"[{tab_name}] já existe no Google Sheets. Pulando parsing do PDF para ignorar processamento duplicado.")
                continue
            
            logging.info(f"\nProcessing {pdf} for tab: {tab_name}")
            df = parse_pdf_data(pdf)
            
            if not df.empty:
                update_google_sheet(client, sheet, df, tab_name)
            else:
                logging.warning(f"No data parsed for {pdf}")
                
    # Final step: sort tabs
    reorder_tabs(CRED_FILE, OUR_SHEET_ID)

if __name__ == "__main__":
    main()
