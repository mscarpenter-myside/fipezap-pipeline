#!/bin/bash
# run_pipeline.sh - Endpoint for the ETL pipeline, executed by GitHub Actions

# Navigate to the project directory if necessary, but GH Actions will run it from the root
echo "Starting FipeZAP ETL Pipeline..."

# Step 1: Download PDFs
echo "[Step 1] Running fipezap_scraper.py..."
./venv/bin/python3 scripts/fipezap_scraper.py

# Step 2: Parse and Upload data to Google Sheets
echo "[Step 2] Running etl_pipeline.py..."
./venv/bin/python3 scripts/etl_pipeline.py

# Step 3: Parse and Simplify GeoJSONs
echo "[Step 3] Running process_geojsons.py..."
./venv/bin/python3 scripts/process_geojsons.py

# Step 4: Sync data to Flourish Tables 
echo "[Step 4] Running sync_flourish_data.py..."
./venv/bin/python3 scripts/sync_flourish_data.py

echo "Pipeline completed successfully!"
