import json
import logging
import re
import gspread
import geopandas as gpd
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
NEW_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"
GID_TARGET = 993666513

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    return gspread.authorize(creds)

def main():
    client = get_client()
    sheet = client.open_by_key(NEW_SHEET_ID)
    
    target_ws = None
    for ws in sheet.worksheets():
        if ws.id == GID_TARGET:
            target_ws = ws
            break
            
    if not target_ws:
        logging.error(f"Worksheet with GID {GID_TARGET} not found.")
        return
        
    logging.info(f"Target worksheet found: {target_ws.title}")
    
    data = target_ws.get_all_values()
    if not data:
        logging.error("No data found in worksheet.")
        return
        
    headers = data[0]
    logging.info(f"Headers: {headers}")
    for i in range(1, min(5, len(data))):
        logging.info(f"Row {i}: {data[i]}")

if __name__ == "__main__":
    main()
