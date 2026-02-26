"""
Debug specific extraction/normalization issues.
"""
import sys
sys.path.insert(0, '.')
from scripts.etl_pipeline import parse_pdf_data, _load_bairro_corrections, fix_bairro_name
import logging

logging.basicConfig(level=logging.INFO)

CRED_FILE    = "credentials/projeto-mkt-buyer-experience-ab8bb5499148.json"
OUR_SHEET_ID = "1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA"

corrections = _load_bairro_corrections(CRED_FILE, OUR_SHEET_ID)

# 1. Check 'Dos Araés' mapping
names_to_check = ["dos araes", "dos arães", "pedro ludovico/bela\u2026", "pedro ludovico/bela vista"]
print("\n=== Checking Bairro Name Corrections ===")
for name in names_to_check:
    fixed = fix_bairro_name(name, corrections)
    print(f"'{name}' -> '{fixed}'")

# 2. Check 2025-06 precision in PDF
print("\n=== Checking 2025-06 (June) PDF data sample ===")
df_06 = parse_pdf_data("data/raw/fipezap-202506-residencial-venda.pdf")
print(df_06.head(5).to_string())

# 3. Check 2026-01 PDF data sample
print("\n=== Checking 2026-01 (Jan) PDF data sample ===")
df_01 = parse_pdf_data("data/raw/fipezap-202601-residencial-venda-.pdf")
print(df_01.head(5).to_string())
