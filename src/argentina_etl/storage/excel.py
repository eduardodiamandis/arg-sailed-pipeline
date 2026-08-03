"""
storage/excel.py
----------------
Escrita do arquivo local. Recebe DataFrame pronto; nao transforma dados.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

def salvar_local(df: pd.DataFrame, path: Path) -> None:
    """Salva apenas a sheet 'data_base' no arquivo local."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="data_base", index=False)
    logger.info(f"Arquivo local salvo: {path}")

