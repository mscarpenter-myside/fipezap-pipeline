import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def fetch_specific_tab(sheet_id, tab_name):
    CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet(tab_name)
        # using get_all_values() to manually build df, as get_all_records() fails on empty/duplicate headers
        data = ws.get_all_values()
        if not data:
            return pd.DataFrame()
        
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        return df
    except Exception as e:
        logging.error(f"Error fetching {sheet_id} -> {tab_name}: {e}")
        return pd.DataFrame()

def main():
    our_sheet_id = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"
    ref_sheet_id = "1esOzR5cl1NboGfEBxZSHjG76_4DhHXXCGWLLJMsMRn8"
    tab_name = "2026-01"
    
    logging.info(f"Fetching our generated data from tab {tab_name}...")
    df_ours = fetch_specific_tab(our_sheet_id, tab_name)
    
    logging.info(f"Fetching Reference data from tab {tab_name}...")
    df_ref = fetch_specific_tab(ref_sheet_id, tab_name)
    
    if df_ours.empty or df_ref.empty:
        logging.error("Failed to fetch data properly.")
        return
        
    print(f"\nTotal rows in Ours: {len(df_ours)}")
    print(f"Total rows in Reference: {len(df_ref)}")

    print("\n--- Our Top 5 ---")
    print(df_ours.head())
    
    print("\n--- Reference Top 5 ---")
    print(df_ref.head())
    
    # Check what cities are in one but not the other
    cities_ours = set(df_ours['Cidade'].unique())
    cities_ref = set(df_ref['Cidade'].unique()) if 'Cidade' in df_ref.columns else set()
    
    print("\n--- Rows per City in Ours ---")
    print(df_ours['Cidade'].value_counts())
    
    print("\n--- Rows per City in Reference ---")
    print(df_ref['Cidade'].value_counts())
    
    # Also check if there are empty rows in Reference
    print(f"\nEmpty 'Cidade' cells in Reference: {df_ref['Cidade'].isna().sum() + (df_ref['Cidade'] == '').sum()}")

if __name__ == "__main__":
    main()
