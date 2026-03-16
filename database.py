"""
database.py
-----------
Toda a lógica de transformação de dados e persistência:
  - Limpeza do arquivo novo (remove linhas de rodapé)
  - Merge com o banco existente (sem duplicatas)
  - Salvamento local, OneDrive e SQL Server
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd
import pyodbc

from logger_config import logger

# Colunas canônicas esperadas no banco e no arquivo novo
COLUNAS = ["Date", "Destination", "Origin", "Cargo", "Tons", "Month", "Year"]


# ---------------------------------------------------------------------------
# Limpeza do arquivo bruto
# ---------------------------------------------------------------------------

def _cortar_apos_duas_linhas_vazias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove todas as linhas a partir de duas linhas consecutivas completamente vazias.
    Isso elimina rodapés e notas de rodapé presentes nos arquivos originais.
    """
    empty = df.isna().all(axis=1)
    for i in range(len(empty) - 1):
        if empty.iloc[i] and empty.iloc[i + 1]:
            logger.info(f"Rodapé detectado na linha {i} — descartando o restante.")
            return df.iloc[:i].copy()
    return df


def ler_arquivo_novo(path: Path) -> pd.DataFrame:
    """
    Lê o arquivo Excel baixado, remove o rodapé e garante os tipos corretos.

    O arquivo original tem 7 linhas de cabeçalho antes dos dados,
    por isso usamos header=7.
    """
    logger.info(f"Lendo arquivo novo: {path.name}")
    df = pd.read_excel(path, header=7, engine="openpyxl")
    df = _cortar_apos_duas_linhas_vazias(df)

    df["Date"] = pd.to_datetime(df["Date"])
    df["Month"] = df["Date"].dt.month
    df["Year"] = df["Date"].dt.year

    logger.info(f"  {len(df)} linhas carregadas | "
                f"períodos: {sorted(df['Date'].dt.to_period('M').unique().astype(str))}")
    return df


# ---------------------------------------------------------------------------
# Merge com o banco
# ---------------------------------------------------------------------------

def merge_com_banco(df_novo: pd.DataFrame, db: pd.DataFrame) -> pd.DataFrame:
    """
    Atualiza o banco removendo os períodos (mês/ano) presentes no arquivo novo
    e inserindo os dados novos no lugar.

    Lógica:
    -------
    O arquivo novo pode conter um ou mais meses (ex: jan + fev + mar parcial).
    Deletamos do banco TODOS esses períodos antes de inserir — evitando
    duplicatas independentemente de quantas vezes o processo rodar.

    Exemplo:
        Banco tem: 2025 completo + jan/2026 + fev/2026
        Arquivo novo tem: jan/2026 + fev/2026 + mar/2026 (parcial)
        → Deleta jan, fev e mar de 2026 do banco
        → Insere tudo do arquivo novo
        → Resultado: 2025 completo + jan/2026 + fev/2026 + mar/2026 (atualizado)
    """
    df_novo["Date"] = pd.to_datetime(df_novo["Date"])
    db["Date"] = pd.to_datetime(db["Date"])

    # Identifica todos os períodos (mês/ano) presentes no arquivo novo
    periodos_novos = df_novo["Date"].dt.to_period("M").unique()
    logger.info(f"Períodos do arquivo novo: {sorted(periodos_novos.astype(str))}")

    # Remove esses períodos do banco
    mascara_remover = db["Date"].dt.to_period("M").isin(periodos_novos)
    linhas_removidas = mascara_remover.sum()
    db_limpo = db[~mascara_remover].copy()

    logger.info(f"Linhas removidas do banco (períodos sobrepostos): {linhas_removidas}")

    # Concatena banco limpo com dados novos
    db_atualizado = pd.concat([db_limpo, df_novo], ignore_index=True)
    db_atualizado = db_atualizado.sort_values("Date").reset_index(drop=True)

    # Garante Month e Year consistentes
    db_atualizado["Month"] = db_atualizado["Date"].dt.month
    db_atualizado["Year"] = db_atualizado["Date"].dt.year

    linhas_adicionadas = len(db_atualizado) - len(db)
    logger.info(f"Linhas líquidas adicionadas ao banco: {linhas_adicionadas}")
    logger.info(f"Total de linhas no banco atualizado: {len(db_atualizado)}")

    return db_atualizado


# ---------------------------------------------------------------------------
# Salvamento
# ---------------------------------------------------------------------------

def salvar_local(df: pd.DataFrame, path: Path) -> None:
    """Salva apenas a sheet 'data_base' no arquivo local."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name="data_base", index=False)
    logger.info(f"Arquivo local salvo: {path}")


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


def salvar_sql_server(df: pd.DataFrame, server: str, database: str, table: str) -> None:
    """
    Substitui toda a tabela no SQL Server pelo DataFrame atualizado.
    Usa DELETE + INSERT em vez de TRUNCATE para manter log de transações.
    """
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

    logger.info(f"Conectando ao SQL Server: {server}/{database}")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    try:
        logger.info(f"Limpando tabela [dbo].[{table}]...")
        cursor.execute(f"DELETE FROM [dbo].[{table}]")

        # Prepara tipos nativos (evita erros de tipo 'object' do pandas)
        df_sql = df[COLUNAS].copy()
        df_sql["Date"] = pd.to_datetime(df_sql["Date"]).dt.date
        df_sql["Tons"] = pd.to_numeric(df_sql["Tons"], errors="coerce").fillna(0).astype(float)
        df_sql["Month"] = pd.to_numeric(df_sql["Month"], errors="coerce").fillna(0).astype(int)
        df_sql["Year"] = pd.to_numeric(df_sql["Year"], errors="coerce").fillna(0).astype(int)

        valores = df_sql.values.tolist()

        query = f"""
            INSERT INTO [dbo].[{table}]
                (Date, Destination, Origin, Cargo, Tons, Month, Year)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        logger.info(f"Inserindo {len(valores)} linhas no SQL Server...")
        cursor.executemany(query, valores)
        conn.commit()
        logger.info("SQL Server atualizado com sucesso.")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pivot Tables via Excel COM (win32)
# ---------------------------------------------------------------------------

def criar_pivot_tables(path_excel: Path) -> None:
    """
    Cria Pivot Tables reais no Excel usando win32com.
    Sheets Pivot_2025 e Pivot_2026 são limpas e recriadas.

    Filtros padrão aplicados:
      - Origin   = ARGENTINA
      - Cargo    = CORN
      - Year     = ano da pivot (2025 ou 2026)
      - Month    = mês atual (para 2026) / dezembro (para 2025)
    """

    import win32com.client as win32

    mes_atual = str(datetime.datetime.now().month)
    path_str = str(path_excel)

    logger.info(f"Criando Pivot Tables no Excel: {path_excel.name}")

    excel = win32.Dispatch("Excel.Application")
    excel.Visible = False

    try:
        wb = excel.Workbooks.Open(path_str)
        ws_data = wb.Worksheets("data_base")

        # Determina o range de dados
        last_row = ws_data.Cells(ws_data.Rows.Count, 1).End(-4162).Row  # xlUp
        last_col = ws_data.Cells(1, ws_data.Columns.Count).End(-4159).Column  # xlToLeft
        data_range = ws_data.Range(
            ws_data.Cells(1, 1), ws_data.Cells(last_row, last_col)
        )

        # Cache compartilhado entre as duas pivots
        pcache = wb.PivotCaches().Create(SourceType=1, SourceData=data_range)

        def _build_pivot(ws_name: str, pivot_name: str, year: str, month_filter: str):
            ws = wb.Worksheets(ws_name)
            ws.Cells.Clear()
            pt = pcache.CreatePivotTable(ws.Range("A3"), pivot_name)

            pt.PivotFields("Destination").Orientation = 1   # xlRowField
            pt.AddDataField(pt.PivotFields("Tons"), "Sum of Tons", -4157)  # xlSum

            for field in ("Year", "Origin", "Cargo", "Month"):
                pt.PivotFields(field).Orientation = 3  # xlPageField

            pt.PivotFields("Year").CurrentPage = year
            pt.PivotFields("Origin").CurrentPage = "ARGENTINA"
            pt.PivotFields("Cargo").CurrentPage = "CORN"
            pt.PivotFields("Month").CurrentPage = month_filter

            logger.info(f"  Pivot '{pivot_name}' criada (Year={year}, Month={month_filter})")

        _build_pivot("Pivot_2026", "Pivot_2026", year="2026", month_filter=mes_atual)
        _build_pivot("Pivot_2025", "Pivot_2025", year="2025", month_filter="12")

        wb.Save()
        logger.info("Pivot Tables salvas com sucesso.")

    finally:
        wb.Close()
        excel.Quit()
