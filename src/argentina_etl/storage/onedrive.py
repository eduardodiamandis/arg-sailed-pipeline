"""
storage/onedrive.py
-------------------
Escrita do arquivo consumido pelo Power BI, com as sheets derivadas, e o
empurrao no cliente do OneDrive para que a sincronizacao ocorra na hora.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

def salvar_onedrive(df: pd.DataFrame, path: Path) -> None:
    """
    Salva o arquivo no OneDrive com sheets extras:
      - data_base  : banco completo
      - 2025       : apenas dados de 2025
      - 2026       : apenas dados de 2026
      - Pivot_2025 : soma de Tons por Destination em 2025
      - Pivot_2026 : soma de Tons por Destination em 2026
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    df_2025 = df[df["Year"] == 2025].copy()
    df_2026 = df[df["Year"] == 2026].copy()

    pivot_2025 = (
        df_2025.groupby("Destination", dropna=False)["Tons"]
        .sum()
        .reset_index()
        .rename(columns={"Tons": "Sum of Tons"})
    )
    pivot_2026 = (
        df_2026.groupby("Destination", dropna=False)["Tons"]
        .sum()
        .reset_index()
        .rename(columns={"Tons": "Sum of Tons"})
    )

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="data_base", index=False)
        df_2025.to_excel(writer, sheet_name="2025", index=False)
        df_2026.to_excel(writer, sheet_name="2026", index=False)
        pivot_2025.to_excel(writer, sheet_name="Pivot_2025", index=False)
        pivot_2026.to_excel(writer, sheet_name="Pivot_2026", index=False)

    logger.info(f"Arquivo OneDrive salvo com sheets extras: {path}")
    _forcar_sync_onedrive(path)


def _forcar_sync_onedrive(path: Path) -> None:
    """Garante que o OneDrive processe o arquivo imediatamente após o salvamento."""
    import os, subprocess, sys

    # Atualiza o timestamp para o OneDrive detectar a mudança
    os.utime(path, None)

    if sys.platform != "win32":
        return

    onedrive_exe = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "OneDrive" / "OneDrive.exe"
    if not onedrive_exe.exists():
        logger.warning("OneDrive.exe não encontrado — sync automático indisponível.")
        return

    try:
        # /start acorda o cliente se estiver pausado; não abre janela
        subprocess.Popen(
            [str(onedrive_exe), "/start"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info("OneDrive: cliente notificado para sincronizar.")
    except Exception as e:
        logger.warning(f"Não foi possível notificar o OneDrive: {e}")


