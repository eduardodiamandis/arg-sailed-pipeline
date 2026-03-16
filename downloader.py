"""
downloader.py
-------------
Baixa os arquivos Excel via URL usando Selenium (Chrome headless).
Necessário porque as URLs do Nabsa retornam uma página HTML com redirect
via JavaScript — o requests simples não consegue seguir esse tipo de redirect.

Dependências:
    pip install selenium webdriver-manager

O nome final do arquivo inclui a data extraída do nome detectado pelo Chrome,
no formato:
    <stem>_<server_stem>.xlsx
Ex: vessels_sailed_update_Sailed Vessels_2026-01-01.xlsx
"""
from __future__ import annotations

import re
import shutil
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
from logger_config import logger

# Selenium — importado no topo para permitir mock em testes unitários
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DOWNLOAD_WAIT_SECONDS = 60
_MIN_VALID_SIZE_BYTES = 4_096


# ---------------------------------------------------------------------------
# Helpers de nome (mantidos para compatibilidade com os testes)
# ---------------------------------------------------------------------------

def _extract_server_filename(response) -> str | None:
    """
    Extrai nome do arquivo do header Content-Disposition (requests.Response).
    Mantido para compatibilidade com testes unitários.
    No fluxo Selenium o nome vem do filesystem.
    """
    cd = response.headers.get("Content-Disposition", "")
    if not cd:
        return None
    m = re.search(r"filename\*\s*=\s*[^']*''(.+)", cd, re.IGNORECASE)
    if m:
        return unquote(m.group(1).strip().strip('"'))
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _build_output_name(base_stem: str, server_name: str | None) -> str:
    """
    Monta o nome final do arquivo.
    Com data ISO no nome do servidor -> base_stem + _ + server_stem + .xlsx
    Sem data -> base_stem + .xlsx
    """
    if not server_name:
        return f"{base_stem}.xlsx"
    server_stem = Path(server_name).stem
    if _DATE_RE.search(server_stem):
        return f"{base_stem}_{server_stem}.xlsx"
    return f"{base_stem}.xlsx"


# ---------------------------------------------------------------------------
# Validação
# ---------------------------------------------------------------------------

def _validate_excel_file(path: Path) -> None:
    """
    Verifica que o arquivo é um ZIP/Excel válido (magic bytes PK).

    Raises
    ------
    ValueError : arquivo muito pequeno ou não é ZIP
    """
    size = path.stat().st_size
    if size < _MIN_VALID_SIZE_BYTES:
        try:
            preview = path.read_text(encoding="utf-8", errors="replace")[:300]
        except Exception:
            preview = "(não foi possível ler)"
        raise ValueError(
            f"Arquivo suspeito: apenas {size / 1024:.1f} KB — mínimo esperado "
            f"{_MIN_VALID_SIZE_BYTES / 1024:.0f} KB.\n"
            f"Conteúdo recebido:\n{preview}"
        )
    with open(path, "rb") as f:
        magic = f.read(2)
    if magic != b"PK":
        try:
            preview = path.read_text(encoding="utf-8", errors="replace")[:300]
        except Exception:
            preview = "(não foi possível ler)"
        raise ValueError(
            f"Não é um ZIP/Excel válido (magic bytes: {magic!r}).\n"
            f"Provável página HTML de erro.\nConteúdo:\n{preview}"
        )


# ---------------------------------------------------------------------------
# Extrai data máxima do Excel para usar no nome do arquivo
# ---------------------------------------------------------------------------

def _extract_max_date_from_excel(path: Path) -> str | None:
    """
    Lê a coluna 'Date' do Excel baixado e retorna a data máxima
    no formato YYYY-MM-DD para compor o nome do arquivo.
    Retorna None se não conseguir ler.
    """
    try:
        df = pd.read_excel(path, header=7, engine="openpyxl", usecols=["Date"])
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
        if df.empty:
            return None
        max_date = df["Date"].max().strftime("%Y-%m-%d")
        return max_date
    except Exception as exc:
        logger.warning(f"  Não foi possível extrair data do Excel: {exc}")
        return None


# ---------------------------------------------------------------------------
# Espera pelo arquivo
# ---------------------------------------------------------------------------

def _wait_for_download(download_dir: Path, timeout: int) -> Path:
    """
    Aguarda .xlsx completo (sem .crdownload) aparecer em download_dir.

    Raises
    ------
    TimeoutError
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        candidates = [
            f for f in download_dir.iterdir()
            if f.is_file()
            and f.suffix.lower() == ".xlsx"
            and not f.name.endswith(".crdownload")
        ]
        if candidates:
            latest = max(candidates, key=lambda f: f.stat().st_mtime)
            logger.info(f"  Arquivo detectado na pasta de downloads: {latest.name}")
            return latest
        time.sleep(1)
    raise TimeoutError(
        f"Download não completou em {timeout}s. "
        "Verifique a URL e a conexão do Chrome."
    )


# ---------------------------------------------------------------------------
# Download principal
# ---------------------------------------------------------------------------

def download_file(
    url: str,
    file_name: str,
    destination_path: Path,
    timeout: int = 60,
) -> Path:
    """
    Abre a URL no Chrome headless, aguarda o download, move e valida o arquivo.

    Parameters
    ----------
    url              : URL que dispara o download (redirect via JS)
    file_name        : Nome base do arquivo (ex: 'vessels_sailed_update.xlsx')
    destination_path : Diretório de destino final
    timeout          : Tempo máximo para aguardar o download (segundos)

    Returns
    -------
    Path completo do arquivo salvo

    Raises
    ------
    TimeoutError : download não completou no prazo
    ValueError   : arquivo baixado não é Excel válido
    """
    destination_path = Path(destination_path)
    destination_path.mkdir(parents=True, exist_ok=True)

    # Pasta temporária isolada — evita capturar arquivos pré-existentes no destino
    tmp_download_dir = Path(tempfile.mkdtemp(prefix="nabsa_dl_"))
    base_stem = Path(file_name).stem

    logger.info(f"Iniciando download via navegador: {file_name}")
    logger.info(f"  URL: {url}")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_experimental_option("prefs", {
        "download.default_directory": str(tmp_download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    try:
        driver.get(url)
        logger.info(f"  Página aberta — aguardando download (timeout={timeout}s)...")
        downloaded = _wait_for_download(tmp_download_dir, timeout)
    finally:
        driver.quit()

    final_name = _build_output_name(base_stem, downloaded.name)
    output_path = destination_path / final_name
    shutil.move(str(downloaded), str(output_path))
    logger.info(f"  Arquivo movido para: {output_path}")

    try:
        tmp_download_dir.rmdir()
    except Exception:
        pass

    size_kb = output_path.stat().st_size / 1024
    logger.info(f"Download concluído: {final_name} ({size_kb:.1f} KB) -> {output_path}")

    _validate_excel_file(output_path)
    logger.info("  Validação OK: arquivo é um Excel/ZIP válido.")

    # Tenta enriquecer o nome com a data máxima extraída do conteúdo do Excel
    # (usado quando o servidor não envia Content-Disposition com data)
    if not _DATE_RE.search(final_name):
        max_date = _extract_max_date_from_excel(output_path)
        if max_date:
            dated_name = f"{base_stem}_{max_date}.xlsx"
            dated_path = destination_path / dated_name
            output_path.rename(dated_path)
            output_path = dated_path
            logger.info(f"  Arquivo renomeado com data extraída: {dated_name}")

    return output_path