import asyncio
import os
import re
from pathlib import Path
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class FipeZapScraper:
    def __init__(self, download_dir="data/raw"):
        self.url = "https://www.datazap.com.br/conteudos-fipezap/"
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
    async def fetch_pdf_links(self):
        """Fetches the page and extracts unique PDF links for 'Venda'."""
        logging.info(f"Acessando url: {self.url}")
        
        async with AsyncSession(impersonate="chrome110") as session:
            response = await session.get(self.url)
            
            if response.status_code != 200:
                logging.error(f"Erro ao acessar {self.url} (Status: {response.status_code})")
                return []
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = soup.find_all('a', href=True)
            report_links = []
            seen_urls = set()
            
            # The structure has links like "Fevereiro 2026" with href to PDF
            for a in links:
                href = a['href']
                text = a.get_text(strip=True)
                
                # Check if it's a PDF link for Venda
                if href.endswith('.pdf') and 'venda' in href.lower():
                    if href not in seen_urls:
                        seen_urls.add(href)
                        report_links.append({"text": text, "url": href})
                        
            return report_links

    async def download_file(self, session, url, filepath):
        """Downloads a single file asynchronously."""
        logging.info(f"Baixando: {url}")
        response = await session.get(url)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            logging.info(f"Salvo em: {filepath}")
        else:
            logging.error(f"Erro ao baixar {url} (Status: {response.status_code})")

    async def run(self, num_months=12):
        """Main execution to get the last N months."""
        urls = await self.fetch_pdf_links()
        
        if not urls:
            logging.warning("Nenhum link encontrado.")
            return

        # Take the top N (assuming they are ordered from newest to oldest)
        target_urls = urls[:num_months]
        logging.info(f"Encontrados links. Focando nos últimos {num_months} meses.")
        
        async with AsyncSession(impersonate="chrome110") as session:
            tasks = []
            for item in target_urls:
                # Create a filename
                filename = item['url'].split('/')[-1]
                filepath = self.download_dir / filename
                
                # Verify if already downloaded
                if not filepath.exists():
                    tasks.append(self.download_file(session, item['url'], filepath))
                else:
                    logging.info(f"Arquivo já existe, ignorando: {filepath}")
                    
            if tasks:
                await asyncio.gather(*tasks)
            else:
                logging.info("Todos os arquivos já estavam baixados.")
                
        return [self.download_dir / item['url'].split('/')[-1] for item in target_urls]

if __name__ == "__main__":
    scraper = FipeZapScraper()
    # Pega os últimos 12 meses
    downloaded_files = asyncio.run(scraper.run(num_months=12))
    logging.info(f"Processo concluído. Arquivos principais: {downloaded_files}")
