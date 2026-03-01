# 🏙️ FipeZAP Pipeline to Flourish Maps

Este projeto é um pipeline de dados (ETL) e processamento geoespacial construído em Python. O objetivo principal é automatizar a extração de dados do mercado imobiliário brasileiro (Índice FipeZAP Residencial), tratá-los de forma consistente e prepará-los milimetricamente para ingestão e renderização de mapas interativos (Choropleth/3D) no Flourish Studio.

## 🚀 Principais Features

*   📄 **PDF Scraping & Parsing:** Baixa automaticamente os relatórios mensais e extrai de forma estruturada dados de preços por m² e variações de bairros em diversas capitais brasileiras.
*   🧹 **Data Normalization:** Compara e limpa os nomes dos bairros com base em uma "fonte da verdade" (planilha mestre), driblando inconsistências de digitação nos PDFs oficias.
*   🗺️ **Processamento GeoJSON (Mapshaper/Geopandas):** Processa malhas de capitais brasileiras aplicando **Simplificação Seletiva** (preserva precisão geométrica em bairros de interesse e reduz drasticamente o peso de bairros periféricos para otimização de performance no carregamento web).
*   📍 **Geocoding Integrado:** Extrai coordenadas de Latitude e Longitude (centroides) de cada bairro para a plotagem exata de *labels* numéricos e *markers*.
*   ☁️ **Sincronização com Google Sheets API:** Sincroniza abas mes-a-mês e garante que os dados tabulares tenham uma ordenação restrita que combina exatamente 1:1 com o vetor (índice) do GeoJSON, mitigando peculiaridades do Flourish.

## 🛠️ Tecnologias e Bibliotecas

*   `Python` + `Pandas` (Processamento de dados tabulares)
*   `GeoPandas` + `Shapely` (Manipulação e simplificação de geometrias)
*   `pdfplumber` (Extração de texto de PDF)
*   `gspread` + `google-auth` (Integração Google Sheets)
*   `BeautifulSoup` + `curl_cffi` (Web scraping dinâmico)
*   `GitHub Actions` (Orquestração do ETL pipeline)

## ⏳ Automação e Deploy (GitHub Actions)

Todo o processo de extração dos relatórios FipeZAP, cruzamento com os GeoJSONs simplificados e sincronização com a planilha no Flourish é completamente automatizado através do **GitHub Actions**.

O workflow (`.github/workflows/monthly_pipeline.yml`) roda todo dia **06 de cada mês**, com as seguintes etapas e redundâncias:
1. **Primeira Tentativa (09:00 UTC):** Tenta baixar o relatório e rodar o pipeline através do `run_pipeline.sh`.
2. **Segunda Tentativa (12:00 UTC):** Caso a FipeZAP atrase a publicação na primeira tentativa, roda como fallback. Possui trava no script para evitar que rode o mesmo trabalho repetitivamente caso já exista um processamento desse mês.

**Engenharia de Conteúdo & Automação**
