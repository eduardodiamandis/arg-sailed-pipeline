"""
downloader.py
-------------
Baixa os arquivos Excel via URL simulando um navegador real usando Playwright.
"""
from __future__ import annotations
from pathlib import Path
from playwright.sync_api import sync_playwright
from logger_config import logger

def download_file(
    url: str,
    file_name: str,
    destination_path: Path | str,
    timeout: int = 40,
) -> Path:
    
    destination_path = Path(destination_path)
    destination_path.mkdir(parents=True, exist_ok=True)
    output_path = destination_path / file_name

    logger.info(f"Iniciando download via navegador: {file_name}")
    logger.info(f"  URL: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        page = browser.new_page()

        try:
            # Playwright usa milissegundos (ex: 40s viram 40000ms)
            timeout_ms = timeout * 1000
            
            with page.expect_download(timeout=timeout_ms) as download_info:
                try:
                    # wait_until="commit" é o segredo. Ele avisa o Playwright para não esperar o HTML.
                    page.goto(url, timeout=timeout_ms, wait_until="commit")
                except Exception as goto_err:
                    # É comum o 'goto' reclamar que a navegação foi "abortada" 
                    # porque o navegador percebeu que é um download e não um site.
                    logger.debug(f"Aviso de navegação (normal em downloads): {goto_err}")
            
            # Pega o arquivo que foi baixado e salva
            download = download_info.value
            download.save_as(output_path)
            
            size_kb = output_path.stat().st_size / 1024
            logger.info(f"Download concluído: {file_name} ({size_kb:.1f} KB) -> {output_path}")
            
        except Exception as e:
            logger.error(f"Erro crítico durante o download via navegador: {e}")
            raise
        finally:
            browser.close()

    return output_path