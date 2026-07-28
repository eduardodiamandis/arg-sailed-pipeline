"""
lineup_processor.py
-------------------
Lê o arquivo diário de Lineup do NABSA, transforma em snapshot
estruturado e persiste no SQL Server sem apagar histórico anterior.

Diferença fundamental em relação ao Sailed:
  - Arg_Sailed: DELETE + INSERT (substitui o estado do mundo)
  - Arg_Lineup: apenas INSERT (acumula snapshots diários)

Filtragem importante:
  - Status = 'SAILED' é removido antes da inserção
  - O Lineup nunca duplica dados do Sailed — é puramente forward-looking
"""
from __future__ import annotations

import datetime
import re
from pathlib import Path

import pandas as pd
import pyodbc

from argentina_etl.logging_setup import logger

# Colunas que esperamos encontrar após limpar o cabeçalho do arquivo NABSA
COLUNAS_LINEUP = [
    "Port", "Terminal", "Vessel", "ETA", "ETB", "ETF",
    "Ops", "Tons", "Commodity", "Destination", "Origin", "Charterer",
]

# Colunas que vão para o banco
COLUNAS_SQL = [
    "SnapshotDate", "Port", "Terminal", "Vessel",
    "ETA_Raw", "ETA_Date", "ETB_Date", "ETF_Date",
    "Status", "Tons", "Commodity", "Destination", "Origin", "Charterer",
]


# ---------------------------------------------------------------------------
# Leitura e limpeza do arquivo
# ---------------------------------------------------------------------------

def _encontrar_linha_header(path: Path) -> int:
    """
    O arquivo NABSA tem metadados nas primeiras linhas antes da tabela real.
    Procura pela linha que contém 'Port' na primeira coluna — esse é o header.
    Retorna o índice (0-based) dessa linha para passar ao read_excel como header=N.
    """
    df_raw = pd.read_excel(path, header=None, nrows=15, engine="openpyxl")
    for i, row in df_raw.iterrows():
        primeiro = str(row.dropna().iloc[0]).strip() if row.dropna().any() else ""
        if primeiro == "Port":
            logger.info(f"  Header do Lineup encontrado na linha {i}")
            return i
    logger.warning("  Header do Lineup não encontrado dinamicamente — usando fallback linha 5")
    return 5


def _parsear_data(texto: str | None) -> datetime.date | None:
    """
    Converte strings como 'ETA REC 19/05', 'AT REC 11/05', 'ETF 19/05'
    para um objeto date usando o ano corrente.

    Retorna None quando não há data válida (ex: 'ETB', '', NaN).
    """
    if not isinstance(texto, str) or not texto.strip():
        return None

    match = re.search(r"(\d{1,2})/(\d{1,2})(?:/(\d{4}))?", texto)
    if not match:
        return None

    dia = int(match.group(1))
    mes = int(match.group(2))
    ano = int(match.group(3)) if match.group(3) else datetime.date.today().year

    try:
        return datetime.date(ano, mes, dia)
    except ValueError:
        return None


def _e_data(valor) -> bool:
    """
    True apenas para datas reais e comparaveis.

    Nao basta `isinstance(valor, datetime.date)`: pd.NaT herda de
    datetime.datetime, que herda de datetime.date, entao passa no isinstance e
    depois estoura `TypeError: Cannot compare NaT with datetime.date object` na
    comparacao. Um unico NaT em ETA/ETB/ETF derrubava a classificacao inteira.
    """
    return isinstance(valor, datetime.date) and not pd.isna(valor)


def _classificar_status(row: pd.Series) -> str:
    """
    Classifica o estágio operacional do navio com base nas datas disponíveis.

    Categorias (em ordem de prioridade):
      SAILED   — ETF já passou → carregamento concluído, navio partiu
      LOADING  — ETB já passou, ETF ainda não → atracado e carregando agora
      WAITING — ETA já passou, ETB ainda não → ancoragem, aguardando berço
      EXPECTED — ETA no futuro → a caminho
      TBC      — sem datas confirmadas → slot reservado, datas a confirmar
    """
    hoje = datetime.date.today()

    etf = row["ETF_Date"]
    etb = row["ETB_Date"]
    eta = row["ETA_Date"]

    if _e_data(etf) and etf <= hoje:
        return "SAILED"
    if _e_data(etb) and etb <= hoje:
        return "LOADING"
    if _e_data(eta) and eta <= hoje:
        return "WAITING"
    if _e_data(eta) and eta > hoje:
        return "EXPECTED"
    return "TBC"


def ler_arquivo_lineup(path: Path, snapshot_date: datetime.date | None = None) -> pd.DataFrame:
    """
    Lê o arquivo de Lineup, limpa e retorna DataFrame estruturado.

    Parameters
    ----------
    path          : Caminho do arquivo .xlsx do NABSA
    snapshot_date : Data do snapshot (default: hoje)
    """
    if snapshot_date is None:
        snapshot_date = datetime.date.today()

    logger.info(f"Lendo arquivo de Lineup: {path.name} | SnapshotDate: {snapshot_date}")

    header_row = _encontrar_linha_header(path)
    df = pd.read_excel(path, header=header_row, engine="openpyxl")

    # Normaliza nomes de colunas (remove espaços extras)
    df.columns = [str(c).strip() for c in df.columns]

    # Normaliza: o arquivo pode chamar 'Cargo' em vez de 'Commodity'
    if "Cargo" in df.columns and "Commodity" not in df.columns:
        df = df.rename(columns={"Cargo": "Commodity"})

    # Mantém apenas as colunas conhecidas; ignora extras silenciosamente
    colunas_presentes = [c for c in COLUNAS_LINEUP if c in df.columns]
    df = df[colunas_presentes].copy()

    # Remove linhas completamente vazias e linhas sem Vessel definido
    df = df.dropna(how="all")
    df = df[df["Vessel"].notna() & (df["Vessel"].astype(str).str.strip() != "NIL")]

    linhas_antes = len(df)

    # Filtra apenas embarques de origem Argentina com operação de carga
    if "Origin" in df.columns:
        df = df[df["Origin"].astype(str).str.upper().str.strip() == "ARGENTINA"]
    if "Ops" in df.columns:
        df = df[df["Ops"].astype(str).str.lower().str.strip() == "load"]
        df = df.drop(columns=["Ops"])

    logger.info(f"  {linhas_antes} linhas brutas → {len(df)} após filtros (Origin=ARG, Ops=load)")

    # Parseia ETA — guarda texto original em ETA_Raw
    if "ETA" in df.columns:
        df["ETA_Raw"] = df["ETA"].astype(str).str.strip()
        df["ETA_Date"] = df["ETA_Raw"].apply(_parsear_data)
        df = df.drop(columns=["ETA"])
    else:
        df["ETA_Raw"] = None
        df["ETA_Date"] = None

    # Parseia ETB e ETF
    for col_raw, col_parsed in [("ETB", "ETB_Date"), ("ETF", "ETF_Date")]:
        if col_raw in df.columns:
            df[col_parsed] = df[col_raw].astype(str).str.strip().apply(_parsear_data)
            df = df.drop(columns=[col_raw])
        else:
            df[col_parsed] = None

    # Classifica status operacional
    df["Status"] = df.apply(_classificar_status, axis=1)

    # ⚠️ IMPORTANTE: Remove navios com Status=SAILED
    # O Lineup não deve duplicar dados do Sailed — é puramente forward-looking
    sailed_count = (df["Status"] == "SAILED").sum()
    if sailed_count > 0:
        logger.info(
            f"  ⚠️ Removendo {sailed_count} navio(s) com Status=SAILED "
            f"— evita duplicação com Arg_Sailed"
        )
        df = df[df["Status"] != "SAILED"].copy()

    # Normaliza tipos
    df["Tons"] = pd.to_numeric(df["Tons"], errors="coerce")
    df["SnapshotDate"] = snapshot_date

    # Logs de cobertura
    total = len(df)
    eta_ok = df["ETA_Date"].notna().sum()
    etb_ok = df["ETB_Date"].notna().sum()
    etf_ok = df["ETF_Date"].notna().sum()

    logger.info(f"  Lineup processado: {total} registros válidos para SnapshotDate {snapshot_date}")
    logger.info(f"  Datas parseadas — ETA: {eta_ok}/{total} | ETB: {etb_ok}/{total} | ETF: {etf_ok}/{total}")
    logger.info(f"  Cargos presentes: {sorted(df['Commodity'].dropna().unique())}")
    logger.info(f"  Destinos únicos: {df['Destination'].nunique()}")

    # Distribuição de status (SAILED já foi removido)
    status_counts = df["Status"].value_counts().to_dict()
    logger.info(f"  Status (sem SAILED): {status_counts}")

    return df[COLUNAS_SQL]


