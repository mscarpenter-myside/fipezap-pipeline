import gspread
import logging
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
CRED_FILE = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
NEW_SHEET_ID = "1I98Lt0W5etlohZbAnmtGSPl1bxiGAm_yf2MKOWHlocc"

def get_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=scopes)
    return gspread.authorize(creds)

def main():
    client = get_client()
    sheet = client.open_by_key(NEW_SHEET_ID)
    for ws in sheet.worksheets():
        logging.info(f"Tab: {ws.title} | GID: {ws.id}")

if __name__ == "__main__":
    main()
