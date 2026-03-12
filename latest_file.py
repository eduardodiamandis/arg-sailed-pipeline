"""
latest_file.py
--------------
Utilitário para encontrar o arquivo mais recente em um diretório.
"""
from __future__ import annotations

import os
from pathlib import Path

from logger_config import logger


def get_latest_file(directory: Path) -> Path:
    """
    Retorna o arquivo mais recente (por data de criação) em um diretório.

    Parameters
    ----------
    directory : Diretório onde procurar

    Returns
    -------
    Path do arquivo mais recente

    Raises
    ------
    FileNotFoundError : Se o diretório estiver vazio ou não existir
    """
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {directory}")

    files = [f for f in directory.iterdir() if f.is_file()]

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {directory}")

    latest = max(files, key=os.path.getctime)
    logger.info(f"Arquivo mais recente encontrado: {latest.name}")

    return latest
