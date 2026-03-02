import gspread
import logging
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
NEW_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"

def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(NEW_SHEET_ID)
    ws = next(w for w in sheet.worksheets() if 'Curitiba' in w.title)
    data = ws.get_all_values()
    
    bairros = []
    for i, row in enumerate(data):
        if i == 0: continue
        if row: bairros.append(row[0])
        
    logging.info(f"Bairros na planilha (top 15): {bairros[:15]}")
    logging.info(f"Does 'Cidade Industrial' exist? {'Cidade Industrial' in bairros}")

if __name__ == "__main__":
    main()
