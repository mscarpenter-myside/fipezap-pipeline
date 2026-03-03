import importlib.metadata

packages = ['pandas', 'geopandas', 'shapely', 'pdfplumber', 'gspread', 'google-auth', 'beautifulsoup4', 'curl_cffi']
for pkg in packages:
    try:
        print(f"{pkg}=={importlib.metadata.version(pkg)}")
    except importlib.metadata.PackageNotFoundError:
        print(f"{pkg} v? (not installed in this env)")
