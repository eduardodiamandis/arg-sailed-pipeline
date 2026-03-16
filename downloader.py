"""
downloader.py
-------------
Baixa os arquivos Excel via URL e os salva no diretório de backup.
Não contém nenhuma lógica de negócio — responsabilidade única: download.

Novidade: o nome final do arquivo inclui a data extraída do header
Content-Disposition (quando disponível), no formato:
    <stem>_<YYYY-MM-DD>.xlsx
Ex: vessels_sailed_update_Sailed Vessels_2026-01-01.xlsx
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

import requests

from logger_config import logger

# Regex para capturar a data ISO no nome vindo do servidor
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _extract_server_filename(response: requests.Response) -> str | None:
    """
    Tenta extrair o nome do arquivo do header Content-Disposition.

    Suporta os formatos mais comuns:
        attachment; filename="Sailed Vessels_2026-01-01.xlsx"
        attachment; filename*=UTF-8''Sailed%20Vessels_2026-01-01.xlsx
    """
    cd = response.headers.get("Content-Disposition", "")
    if not cd:
        return None

    # RFC 5987 (filename*)
    m = re.search(r"filename\*\s*=\s*[^']*''(.+)", cd, re.IGNORECASE)
    if m:
        return unquote(m.group(1).strip().strip('"'))

    # Formato simples (filename=)
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return None


def _build_output_name(base_stem: str, server_name: str | None) -> str:
    """
    Monta o nome final do arquivo.

    Lógica:
    -------
    Se o servidor devolver um nome com data (ex: 'Sailed Vessels_2026-01-01.xlsx'):
        → 'vessels_sailed_update_Sailed Vessels_2026-01-01.xlsx'

    Se não houver header ou data no nome:
        → 'vessels_sailed_update.xlsx'  (comportamento original)
    """
    if not server_name:
        return f"{base_stem}.xlsx"

    # Remove extensão do nome do servidor para usar só o stem
    server_stem = Path(server_name).stem  # ex: "Sailed Vessels_2026-01-01"

    # Verifica se já há uma data ISO no nome do servidor
    if _DATE_RE.search(server_stem):
        return f"{base_stem}_{server_stem}.xlsx"

    # Nome sem data — retorna o padrão
    return f"{base_stem}.xlsx"


def download_file(
    url: str,
    file_name: str,
    destination_path: Path,
    timeout: int = 30,
) -> Path:
    """
    Baixa um arquivo e salva em destination_path com nome enriquecido.

    O nome final é montado como:
        <stem_de_file_name>_<nome_do_servidor_incluindo_data>.xlsx

    Parameters
    ----------
    url              : URL de download direto
    file_name        : Nome base do arquivo (ex: 'vessels_sailed_update.xlsx')
    destination_path : Diretório de destino (criado automaticamente se não existir)
    timeout          : Timeout HTTP em segundos

    Returns
    -------
    Path completo do arquivo salvo
    """
    destination_path = Path(destination_path)
    destination_path.mkdir(parents=True, exist_ok=True)

    base_stem = Path(file_name).stem  # ex: 'vessels_sailed_update'

    logger.info(f"Iniciando download: {file_name}")
    logger.info(f"  URL: {url}")

    response = requests.get(url, timeout=timeout, stream=True)
    response.raise_for_status()

    # Tenta obter o nome real do servidor
    server_name = _extract_server_filename(response)
    if server_name:
        logger.info(f"  Nome recebido do servidor: {server_name}")
    else:
        logger.info("  Header Content-Disposition não encontrado — usando nome padrão.")

    final_name = _build_output_name(base_stem, server_name)
    output_path = destination_path / final_name

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8_192):
            f.write(chunk)

    size_kb = output_path.stat().st_size / 1024
    logger.info(f"Download concluído: {final_name} ({size_kb:.1f} KB) -> {output_path}")

    return output_path