"""
validate_etl.py — Compara as abas da planilha ETL com a planilha mestra.
Usa comparação insensível à ordem de linhas (set-based) para garantir 100% de acurácia
independente da ordenação das linhas na planilha.
"""
import os
import re
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
OUR_SHEET_ID  = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"
REF_SHEET_ID  = "1esOzR5cl1NboGfEBxZSHjG76_4DhHXXCGWLLJMsMRn8"
REPORT_PATH   = "data/validation_report.txt"

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    return gspread.authorize(creds)

def sheet_to_df(client, sheet_id, tab_name):
    try:
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet(tab_name)
        data = ws.get_all_values()
        if not data:
            return pd.DataFrame()
        headers = data[0]
        return pd.DataFrame(data[1:], columns=headers)
    except gspread.exceptions.WorksheetNotFound:
        return None
    except Exception as e:
        logging.error(f"Erro ao buscar {sheet_id} -> {tab_name}: {e}")
        return pd.DataFrame()

def get_all_tabs(client, sheet_id):
    sheet = client.open_by_key(sheet_id)
    return [ws.title for ws in sheet.worksheets()]

def normalize_str(s):
    """Normaliza string: strip, lower, colapsa espaços. Remove prefixo 'r$ ' acidental da variação."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r'\s+', ' ', s.strip().lower())
    # Remove accidental 'r$ ' prefix (seen in 2026-01 reference Variação column)
    if s.startswith('r$ ') and not any(c in s[3:] for c in '.'):
        # Only strip if it looks like a number (not a price like r$ 19.461)
        candidate = s[3:]
        if re.match(r'^-?[\d,]+$', candidate):
            s = candidate
    return s

def normalize_variation(v):
    """Normaliza a variação: trata formatos com diferentes casas decimais."""
    v = normalize_str(v)
    # Remove trailing zeros after comma: 0,0800 -> 0,08
    v = re.sub(r',(\d*?)0+$', lambda m: ',' + m.group(1).rstrip('0') or ',0', v)
    # Handle -0 case
    if v in ('-0', '-0,0', '0,0'):
        return '0'
    return v

def rows_to_set(df, cols):
    """Converte DataFrame em set de tuplas normalizadas para comparação sem ordem."""
    rows = set()
    for _, row in df[cols].iterrows():
        normalized = tuple(normalize_str(str(row[c])) for c in cols)
        rows.add(normalized)
    return rows

def compare_tab(df_ours, df_ref, tab_name):
    """Compara duas DataFrames de forma insensível à ordem das linhas."""
    expected_cols = ["Cidade", "Bairro", "Valor do m²", "Variação (12 meses)"]
    cols_to_compare = [c for c in expected_cols if c in df_ours.columns and c in df_ref.columns]

    results = {
        "tab": tab_name,
        "rows_ours": len(df_ours),
        "rows_ref": len(df_ref),
        "match_count": 0,
        "extra_in_ours": [],
        "missing_from_ours": [],
        "accuracy": 0.0,
    }

    if df_ref.empty or df_ours.empty:
        logging.warning(f"  [{tab_name}] DataFrame vazio — pulando comparação.")
        return results

    set_ours = rows_to_set(df_ours, cols_to_compare)
    set_ref  = rows_to_set(df_ref, cols_to_compare)

    matched = set_ours & set_ref
    extra   = set_ours - set_ref
    missing = set_ref - set_ours

    results["match_count"]      = len(matched)
    results["extra_in_ours"]    = list(extra)[:5]
    results["missing_from_ours"]= list(missing)[:5]
    results["accuracy"] = (len(matched) / len(set_ref) * 100) if set_ref else 0.0
    return results

def main():
    logging.info("=== Iniciando Validação ETL (Insensível à Ordem) ===")
    client = get_client()

    our_tabs = set(get_all_tabs(client, OUR_SHEET_ID))
    ref_tabs = set(get_all_tabs(client, REF_SHEET_ID))

    date_pattern = re.compile(r'^\d{4}-\d{2}$')
    common_tabs = sorted([t for t in our_tabs & ref_tabs if date_pattern.match(t)])
    only_ours = sorted([t for t in our_tabs - ref_tabs if date_pattern.match(t)])
    only_ref  = sorted([t for t in ref_tabs - our_tabs if date_pattern.match(t)])

    logging.info(f"Abas na nossa planilha: {sorted([t for t in our_tabs if date_pattern.match(t)])}")
    logging.info(f"Abas na planilha mestra: {sorted([t for t in ref_tabs if date_pattern.match(t)])}")
    logging.info(f"Abas em comum: {common_tabs}")
    if only_ours:
        logging.warning(f"Abas só na nossa (sem referência): {only_ours}")
    if only_ref:
        logging.warning(f"Abas só na referência (não geradas): {only_ref}")

    all_results = []
    for tab in common_tabs:
        logging.info(f"\n--- Validando aba: {tab} ---")
        df_o = sheet_to_df(client, OUR_SHEET_ID, tab)
        df_r = sheet_to_df(client, REF_SHEET_ID, tab)
        if df_o is None or df_r is None:
            continue

        res = compare_tab(df_o, df_r, tab)
        all_results.append(res)
        logging.info(f"  Linhas (ours/ref): {res['rows_ours']} / {res['rows_ref']}")
        logging.info(f"  Matches: {res['match_count']} / {res['rows_ref']}")
        logging.info(f"  Accuracy: {res['accuracy']:.2f}%")
        if res['missing_from_ours']:
            logging.warning(f"  Primeira linha faltando: {res['missing_from_ours'][0]}")

    total_ref   = sum(r['rows_ref'] for r in all_results)
    total_match = sum(r['match_count'] for r in all_results)
    global_acc  = (total_match / total_ref * 100) if total_ref else 0.0

    report_lines = [
        "=" * 62,
        "RELATÓRIO DE VALIDAÇÃO ETL — FipeZAP (Insensível à Ordem)",
        "=" * 62,
        f"Abas validadas            : {len(common_tabs)}",
        f"Abas sem referência       : {only_ours}",
        f"Abas na ref não geradas   : {only_ref}",
        "",
    ]
    for r in all_results:
        acc_str = f"{r['accuracy']:.2f}%"
        status  = "✅ PASS" if r['accuracy'] == 100.0 else "❌ FAIL"
        report_lines.append(
            f"[{r['tab']}] Rows: {r['rows_ours']}/{r['rows_ref']} | "
            f"Matches: {r['match_count']} | Acc: {acc_str} | {status}"
        )
        for row in r['missing_from_ours'][:3]:
            report_lines.append(f"  FALTANDO: {row}")
        for row in r['extra_in_ours'][:2]:
            report_lines.append(f"  EXTRA   : {row}")

    report_lines += [
        "",
        "=" * 62,
        f"ACURÁCIA GLOBAL: {global_acc:.2f}%",
        ("✅ ETL PASSOU — 100% de Acurácia" if global_acc == 100.0
         else "❌ ETL FALHOU — Corrigir divergências acima"),
        "=" * 62,
    ]

    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    Path("data").mkdir(exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report_text)
    logging.info(f"\nRelatório salvo em: {REPORT_PATH}")

    return global_acc == 100.0

if __name__ == "__main__":
    passed = main()
    exit(0 if passed else 1)
