"""
Script para atualizar o BAIRRO_CORRECTIONS no etl_pipeline.py com
TODAS as correções identificadas, incluindo as recém descobertas.
"""
import re

CORRECTIONS_NEW = '''# Known bairro name corrections: PDF strips accents from text
# Map from stripped lowercase -> correct accented form (derived from master reference sheet)
BAIRRO_CORRECTIONS = {
    # Aracaju (SE)
    "farolandia": "Farolândia",
    "sao jose": "São José",
    "jabotiana": "Jabotiana",
    # Belém (PA)
    "nazare": "Nazaré",
    "cremacao": "Cremação",
    "sao bras": "São Brás",
    # Belo Horizonte (MG)
    "funcionarios": "Funcionários",
    "santa lucia": "Santa Lúcia",
    "paraiso": "Paraíso",
    # Brasília (DF)
    "aguas claras": "Águas Claras",
    "guara norte": "Guará Norte",
    "guara sul": "Guará Sul",
    "ceilandia sul": "Ceilândia Sul",
    "ceilandia norte": "Ceilândia Norte",
    "taguatinga norte": "Taguatinga Norte",
    "taguatinga sul": "Taguatinga Sul",
    # Salvador (BA)
    "caminho das arvores": "Caminho das Árvores",
    "pernambues": "Pernambués",
    "imbui": "Imbuí",
    "graca": "Graça",
    "gracas": "Graças",
    "nossa senhora de nazare": "Nossa Senhora de Nazaré",
    # Fortaleza (CE)
    "dionisio torres": "Dionísio Torres",
    "joaquim tavora": "Joaquim Távora",
    "antonio bezerra": "Antônio Bezerra",
    "praia do futuro ii": "Praia do Futuro II",
    # Recife (PE)
    "dois irmaos": "Dois Irmãos",
    "nossa senhora das gracas": "Nossa Senhora das Graças",
    "santo antonio": "Santo Antônio",
    # Porto Alegre (RS)
    "petropolis": "Petrópolis",
    # Curitiba (PR)
    "agua verde": "Água Verde",
    "ahu": "Ahú",
    "juveve": "Juvevê",
    "portao": "Portão",
    # Florianópolis (SC)
    "agronomica": "Agronômica",
    "corrego grande": "Córrego Grande",
    "saco dos limoes": "Saco dos Limões",
    # Vitória (ES)
    "enseada do sua": "Enseada do Suá",
    "praia do sua": "Praia do Suá",
    # Goiânia (GO)
    "jardim goias": "Jardim Goiás",
    "jardim america": "Jardim América",
    "nova suica": "Nova Suíça",
    "pedro ludovico/bela...": "Pedro Ludovico/Bela Vista",
    # João Pessoa (PB)
    "manaira": "Manaíra",
    "jardim cidade universitaria": "Jardim Cidade Universitária",
    "jardim oceania": "Jardim Oceânia",
    # Campo Grande (MS)
    "santa fe": "Santa Fé",
    "caranda": "Carandá",
    # Maceió (AL)
    "pajucara": "Pajuçara",
    "jatiauca": "Jatiúca",
    "poco": "Poço",
    # Manaus (AM)
    "adrianopolis": "Adrianópolis",
    # São Luís (MA)
    "renascenca": "Renascença",
    "olho d\'agua": "Olho D\'Água",
    "ponta d\'areia": "Ponta D\'Areia",
    # Teresina (PI)
    "sao cristovao": "São Cristóvão",
    "joquei": "Jóquei",
    "fatima": "Fátima",
    "sao joao": "São João",
    # Natal (RN)
    "neopolis": "Neópolis",
    "candelaria": "Candelária",
    # Cuiabá (MT)
    "jardim cuiaba": "Jardim Cuiabá",
    "area de expansao urbana": "Área de Expansão Urbana",
    "dos araes": "Dos Araés",
}

def fix_bairro_name(name):
    """Fix accent-stripped bairro name using corrections map."""
    key = name.strip().lower()
    return BAIRRO_CORRECTIONS.get(key, name)
'''

filepath = "scripts/etl_pipeline.py"
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

pattern = r'# Known bairro name corrections.*?def fix_bairro_name\(name\):.*?return name\n'
new_content = re.sub(pattern, CORRECTIONS_NEW, content, flags=re.DOTALL)

if new_content == content:
    print("ERROR: Pattern not matched!")
else:
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)
    print("OK: BAIRRO_CORRECTIONS updated with all accent fixes")

# Also fix the -0 formatting edge case (v=-0.0 => format as '0' not '-0')
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Fix -0 edge case: use abs() on near-zero values
old = '            v_decimal = v / 100\n                            v_formatted = f"{v_decimal:g}".replace(\'.\', \',\')\n                            # Ensure leading zero: ,08 -> 0,08\n                            if v_formatted.startswith(\',\') or v_formatted.startswith(\'-,\'):\n                                v_formatted = v_formatted.replace(\'-,\', \'-0,\') if v_formatted.startswith(\'-,\') else \'0\' + v_formatted'

# Better: replace the formatting logic with a clean helper
old_pattern = r'v_decimal = v / 100\n([ ]+)v_formatted = f"\{v_decimal:g\}"\.replace\(\'\.', '\',\'\'\)\n\1# Ensure leading zero: ,08 -> 0,08\n\1if v_formatted\.startswith\(\',\'\) or v_formatted\.startswith\(\'-,\'\):\n\1    v_formatted = v_formatted\.replace\(\'-,\', \'-0,\'\) if v_formatted\.startswith\(\'-,\'\) else \'0\' \+ v_formatted'

new_pattern = r'v_decimal = v / 100\n\1v_formatted = format_variation(v_decimal)'

new_content2 = re.sub(old_pattern, new_pattern, content)
if new_content2 == content:
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'v_decimal = v / 100' in line:
            print(f"  Found 'v_decimal' at line {i+1}: {repr(line)}")
    print("NOTE: -0 regex not matched, doing manual replacement")
else:
    print("OK: -0 fix pattern applied")
    content = new_content2

# Insert helper function before parse_pdf_data
helper = '''def format_variation(v_decimal):
    """Format variation: suppress trailing zeros, handle -0 edge case."""
    if v_decimal == 0:
        return "0"
    s = f"{v_decimal:g}".replace(".", ",")
    if s.startswith(","):
        s = "0" + s
    elif s.startswith("-,"):
        s = "-0" + s[1:]
    return s

'''
if 'def format_variation' not in content:
    content = content.replace('def parse_pdf_data(filepath):', helper + 'def parse_pdf_data(filepath):')
    print("OK: format_variation helper inserted")

# Replace inline v_formatted logic with the helper call (for both occurrences)
old_inline = (
    'v_decimal = v / 100\n'
    '                            v_formatted = f"{v_decimal:g}".replace(\'.\', \',\')\n'
    '                            # Ensure leading zero: ,08 -> 0,08\n'
    '                            if v_formatted.startswith(\',\') or v_formatted.startswith(\'-,\'):\n'
    '                                v_formatted = v_formatted.replace(\'-,\', \'-0,\') if v_formatted.startswith(\'-,\') else \'0\' + v_formatted'
)
new_inline = (
    'v_decimal = v / 100\n'
    '                            v_formatted = format_variation(v_decimal)'
)
if old_inline in content:
    content = content.replace(old_inline, new_inline)
    print("OK: Replaced first v_formatted inline block")

old_inline2 = (
    'v_decimal = v / 100\n'
    '                                        v_formatted = f"{v_decimal:g}".replace(\'.\', \',\')\n'
    '                                        if v_formatted.startswith(\',\') or v_formatted.startswith(\'-,\'):\n'
    '                                            v_formatted = v_formatted.replace(\'-,\', \'-0,\') if v_formatted.startswith(\'-,\') else \'0\' + v_formatted'
)
new_inline2 = (
    'v_decimal = v / 100\n'
    '                                        v_formatted = format_variation(v_decimal)'
)
if old_inline2 in content:
    content = content.replace(old_inline2, new_inline2)
    print("OK: Replaced second v_formatted inline block (table fallback)")

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)
print("\nAll ETL fixes applied. File:", filepath)
