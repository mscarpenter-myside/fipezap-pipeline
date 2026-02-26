import pdfplumber
import re
import sys
import pandas as pd

def parse_pdf_data(filepath):
    print(f"Parsing: {filepath}")
    
    data = []
    
    # Regex to find City (UF) - usually the first line, like "SÃO PAULO (SP)"
    city_re = re.compile(r'^([A-Z\u00C0-\u00DC\s\.\-\']+)\s\(([A-Z]{2})\)')
    
    # Regex for neighborhood data: JARDIM CUIABA R$ 10.081 /m² +4,3%
    # But occasionally it has garbage prefix: "Preço médio Preço médio Sem informação DOS ARAES R$ 4.831 /m² +16,4%"
    # We match "R$ <value> /m² <variation>%" and capture everything uppercase before it.
    # The garbage text "Preço médio... " is mixed casing. So we can look for uppercase words.
    bairro_re = re.compile(r'R\$\s+([\d\.]+)\s+/m²\s+([\+\-]?\d+,\d+)%')
    
    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
                
            if "Zonas, distritos ou bairros mais representativos" in text:
                lines = text.split('\n')
                
                # City is usually in the first 3 lines
                city_name = None
                for line in lines[:5]:
                    match = city_re.search(line.strip())
                    if match:
                        # Clean city, title case it
                        raw_city = match.group(1).strip()
                        uf = match.group(2).strip()
                        city_name = f"{raw_city.title()} ({uf})"
                        if city_name.upper().startswith("SÃO PAULO"):
                            city_name = "São Paulo (SP)" # force correct titlecasing
                        break
                        
                if not city_name:
                    continue
                    
                # Now find the neighborhood section
                in_bairro_section = False
                for line in lines:
                    if "preço médio em" in line.lower() and "12 meses" in line.lower():
                        in_bairro_section = True
                        continue
                    
                    if in_bairro_section:
                        if "Fonte:" in line or "Fipe não divulga" in line:
                            break # End of section
                            
                        # Search for price and variation
                        match = bairro_re.search(line)
                        if match:
                            price_str = match.group(1)
                            var_str = match.group(2)
                            
                            # Extract neighborhood name (everything before "R$")
                            prefix = line[:match.start()].strip()
                            # Clean up the prefix from known map artifacts
                            prefix = re.sub(r'Preço médio\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'mais alto\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'mais baixo\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'\(R\$/m²\)\s*', '', prefix, flags=re.IGNORECASE)
                            prefix = re.sub(r'Sem informação\s*', '', prefix, flags=re.IGNORECASE)
                            
                            bairro_name = prefix.strip()
                            
                            # Apply desired format rules
                            # Variação em numeral modelo 0,000. E.g. +4,3% -> 0,043
                            v = float(var_str.replace(',', '.'))
                            v_formatted = f"{v/100:.3f}".replace('.', ',')
                            
                            # Preço do m2 formatado (can keep numerical or "R$ 10.081/m²")
                            # The user said "Valor do m² em uma planilha única". 
                            # If they want numerical, "10081" or formatted "R$ 10.081 /m2"
                            # We will save clean string for now: R$ 10.081 /m²
                            price_formatted = f"R$ {price_str} /m²"
                            
                            data.append({
                                "Cidade": city_name,
                                "Bairro": bairro_name,
                                "Valor do m²": price_formatted,
                                "Variação (12 meses)": v_formatted
                            })
                            
    df = pd.DataFrame(data)
    print(df.head(20))
    print(df.tail(10))
    print(f"Total extracted rows: {len(df)}")
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        parse_pdf_data(sys.argv[1])
    else:
        print("Provide path")
