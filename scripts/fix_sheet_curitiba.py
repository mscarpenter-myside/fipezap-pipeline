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
    
    # Encontrar a linha onde Bairro == 'Curitiba'
    for i, row in enumerate(data):
        if row and row[0] == 'Curitiba':
            logging.info(f"Row {i} (0-indexed): {row}")
            # Vamos corrigir na planilha para "Cidade Industrial"
            cell = f"A{i+1}"
            ws.update(cell, "Cidade Industrial")
            logging.info(f"Updated cell {cell} to 'Cidade Industrial'")
            break

if __name__ == "__main__":
    main()
