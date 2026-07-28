"""
storage/onedrive.py
-------------------
Escrita do arquivo consumido pelo Power BI, com as sheets derivadas, e o
empurrao no cliente do OneDrive para que a sincronizacao ocorra na hora.
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd

from argentina_etl.logging_setup import logger

# ---------------------------------------------------------------------------
# Pivots
# ---------------------------------------------------------------------------
# Filtros de negocio das pivots. Ficam explicitos aqui, no codigo, em vez de
# escondidos como "page fields" de um objeto PivotTable dentro do .xlsx.
#
# Ate 2026-07-28 estas sheets eram geradas duas vezes: primeiro aqui (sem
# filtro nenhum) e depois sobrescritas por uma automacao COM do Excel, que
# aplicava os filtros abaixo. A rota COM foi removida — ela dependia de o Excel
# estar instalado, deixava processos EXCEL.EXE orfaos segurando o arquivo,
# travava no timeout de 120s e, o pior, era o Excel quem resolvia o caminho
# local para a URL do SharePoint, construindo a pivot sobre a copia do servidor
# em vez do arquivo recem-gravado.
PIVOT_ORIGIN = "ARGENTINA"
PIVOT_CARGO = "CORN"

# Mes fixo da pivot do ano anterior; o ano corrente usa o mes de hoje.
PIVOT_MES_ANO_ANTERIOR = 12


def montar_pivot(
    df: pd.DataFrame,
    *,
    year: int,
    month: int,
    origin: str = PIVOT_ORIGIN,
    cargo: str = PIVOT_CARGO,
) -> pd.DataFrame:
    """
    Soma de Tons por Destination, com os quatro filtros aplicados.

    Devolve um DataFrame de duas colunas sem cabecalho, reproduzindo o layout
    que a versao COM gerava, para nao quebrar quem ja consome as sheets:

        Month        7
        Cargo        CORN
        Origin       ARGENTINA
        Year         2026
        (vazio)
        Row Labels   Sum of Tons
        ALGERIA      203118.47
        ...
        Grand Total  3906772.43
    """
    filtrado = df[
        (df["Year"] == year)
        & (df["Month"] == month)
        & (df["Origin"].astype(str).str.strip().str.upper() == origin.upper())
        & (df["Cargo"].astype(str).str.strip().str.upper() == cargo.upper())
    ]

    somas = (
        filtrado.groupby("Destination", dropna=False)["Tons"]
        .sum()
        .sort_index()
    )

    linhas: list[tuple] = [
        ("Month", month),
        ("Cargo", cargo),
        ("Origin", origin),
        ("Year", year),
        (None, None),
        ("Row Labels", "Sum of Tons"),
    ]
    linhas += [(str(dest), float(tons)) for dest, tons in somas.items()]
    linhas.append(("Grand Total", float(somas.sum())))

    logger.info(
        f"  Pivot {year}/{month:02d} ({origin}, {cargo}): "
        f"{len(somas)} destino(s), {somas.sum():,.2f} tons"
    )
    return pd.DataFrame(linhas)


def salvar_onedrive(df: pd.DataFrame, path: Path) -> None:
    """
    Salva o arquivo no OneDrive com sheets extras:
      - data_base  : banco completo
      - 2025 / 2026: recorte por ano
      - Pivot_2025 : Tons por Destination — dezembro/2025, ARGENTINA, CORN
      - Pivot_2026 : Tons por Destination — mes corrente/2026, ARGENTINA, CORN
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    df_2025 = df[df["Year"] == 2025].copy()
    df_2026 = df[df["Year"] == 2026].copy()

    pivot_2025 = montar_pivot(df, year=2025, month=PIVOT_MES_ANO_ANTERIOR)
    pivot_2026 = montar_pivot(df, year=2026, month=datetime.date.today().month)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="data_base", index=False)
        df_2025.to_excel(writer, sheet_name="2025", index=False)
        df_2026.to_excel(writer, sheet_name="2026", index=False)
        pivot_2025.to_excel(writer, sheet_name="Pivot_2025", index=False, header=False)
        pivot_2026.to_excel(writer, sheet_name="Pivot_2026", index=False, header=False)

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


