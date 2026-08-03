"""
storage/sql_server.py
---------------------
Toda a persistencia em SQL Server, para os dois fluxos.

Diferenca fundamental entre eles:
  - Arg_Sailed: DELETE + INSERT total, substitui o estado do mundo
  - Arg_Lineup: apenas INSERT, acumula um snapshot por dia e nunca apaga
    historico. Dentro do mesmo dia, force=True substitui o snapshot.

Conexao por autenticacao Windows (Trusted_Connection); nunca senha em codigo.
"""
from __future__ import annotations

import datetime

import pandas as pd
import pyodbc

from argentina_etl.logging_setup import logger
from argentina_etl.pipelines.lineup import COLUNAS_SQL, _e_data
from argentina_etl.pipelines.sailed import COLUNAS


def _conn_str(server: str, database: str) -> str:
    """String de conexao unica para os dois fluxos."""
    return (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )


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

    try:
        logger.info(f"Limpando tabela [dbo].[{table}]...")
        cursor.execute(f"DELETE FROM [dbo].[{table}]")

        df_sql = df.reindex(columns=COLUNAS).copy()
        df_sql["Date"]  = pd.to_datetime(df_sql["Date"]).dt.date
        df_sql["Tons"]  = pd.to_numeric(df_sql["Tons"],  errors="coerce").fillna(0).astype(float)
        df_sql["Month"] = pd.to_numeric(df_sql["Month"], errors="coerce").fillna(0).astype(int)
        df_sql["Year"]  = pd.to_numeric(df_sql["Year"],  errors="coerce").fillna(0).astype(int)

        valores = df_sql.values.tolist()

        query = f"""
            INSERT INTO [dbo].[{table}]
                (Date, Destination, Origin, Cargo, Tons, Month, Year)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        cursor.fast_executemany = True
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
# Verificação de snapshot duplicado
# ---------------------------------------------------------------------------

def snapshot_ja_existe(conn_str: str, table: str, snapshot_date: datetime.date) -> bool:
    """
    Verifica se já existe registro para essa data no banco.
    Evita duplicatas caso o pipeline rode mais de uma vez no mesmo dia.
    """
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COUNT(1) FROM [dbo].[{table}] WHERE SnapshotDate = ?",
        snapshot_date.strftime("%Y-%m-%d"),
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


# ---------------------------------------------------------------------------
# Persistência no SQL Server
# ---------------------------------------------------------------------------

def salvar_lineup_sql(
        df: pd.DataFrame,
        server: str,
        database: str,
        table: str,
        force: bool = False,
) -> None:
    """
    Insere o snapshot do dia na tabela Arg_Lineup.

    Ao contrário do Sailed, aqui NUNCA fazemos DELETE global.
    Se o snapshot do dia já existir, o comportamento padrão é pular
    (evita duplicatas em re-execuções). Passe force=True para sobrescrever
    o snapshot do dia atual caso necessário.

    Parameters
    ----------
    df       : DataFrame retornado por ler_arquivo_lineup()
    server   : Nome do servidor SQL
    database : Nome do banco
    table    : Nome da tabela (ex: Arg_Lineup)
    force    : Se True, deleta o snapshot do dia antes de inserir
    """
    if df.empty:
        logger.warning("DataFrame de Lineup vazio — nada a inserir no SQL Server.")
        return

    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

    snapshot_date = df["SnapshotDate"].iloc[0]

    if snapshot_ja_existe(conn_str, table, snapshot_date) and not force:
        logger.info(
            f"Snapshot de {snapshot_date} já existe em [{table}] — pulando inserção. "
            "Use force=True para sobrescrever."
        )
        return

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # fast_executemany incompatível com None values no driver legado "SQL Server"
    cursor.fast_executemany = False

    try:
        if force and snapshot_ja_existe(conn_str, table, snapshot_date):
            cursor.execute(
                f"DELETE FROM [dbo].[{table}] WHERE SnapshotDate = ?",
                snapshot_date.strftime("%Y-%m-%d") if isinstance(snapshot_date, datetime.date) else str(snapshot_date),
            )
            logger.info(f"Snapshot anterior de {snapshot_date} removido (force=True).")

        query = f"""
            INSERT INTO [dbo].[{table}]
                (SnapshotDate, Port, Terminal, Vessel, ETA_Raw, ETA_Date, ETB_Date, ETF_Date,
                 Status, Tons, Commodity, Destination, Origin, Charterer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        df_sql = df.copy()
        df_sql["Tons"] = df_sql["Tons"].fillna(0).astype(float)

        # Converte datas para string ISO — driver legado não aceita datetime.date + None na mesma coluna.
        # _e_data (e não isinstance) porque pd.NaT herda de datetime.date e viraria a string "NaT".
        for col in ["SnapshotDate", "ETA_Date", "ETB_Date", "ETF_Date"]:
            df_sql[col] = df_sql[col].apply(
                lambda x: x.strftime("%Y-%m-%d") if _e_data(x) else None
            )

        # NaN → None, linha a linha.
        #
        # NAO usar df.where(pd.notna(df), other=None): em colunas object o pandas
        # devolve NaN de volta em vez de None, e o NaN sobrevive ate o pyodbc.
        # Isso quebrava a insercao de forma intermitente: o pyodbc infere o tipo
        # de cada parametro pela PRIMEIRA linha, entao um NaN em ETA/ETB/ETF na
        # linha 1 fazia o parametro ser ligado como float, e a primeira linha
        # seguinte com data em texto era rejeitada com
        # "o valor fornecido nao e uma instancia valida do tipo de dados float".
        # Dependia dos dados do dia, por isso passou despercebido ate 2026-07-28.
        valores = [
            [None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in linha]
            for linha in df_sql[COLUNAS_SQL].values.tolist()
        ]

        logger.info(f"Inserindo {len(valores)} registros de Lineup (SnapshotDate={snapshot_date})...")
        cursor.executemany(query, valores)
        conn.commit()
        logger.info("Lineup salvo no SQL Server com sucesso.")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
