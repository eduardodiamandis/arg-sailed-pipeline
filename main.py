"""
main.py
-------
Orquestrador do pipeline de atualização do banco Arg_sailed_database.

Fluxo:
  1. Download do arquivo Sailed e Line-Up
  2. Lê o arquivo mais recente do Sailed
  3. Lê o banco de dados existente
  4. Merge inteligente (remove períodos sobrepostos, insere novos)
  5. Salva localmente, no OneDrive e no SQL Server
  6. Cria Pivot Tables no arquivo OneDrive
"""
from __future__ import annotations

import sys
import time

import pandas as pd

# Garante que src/ está no path quando executado da raiz
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent / "src"))

from config import (
    DIR_LINEUP_BACKUP,
    DIR_SAILED_BACKUP,
    PATH_DATABASE,
    PATH_DATABASE_OUTPUT,
    PATH_ONEDRIVE,
    SQL_DATABASE,
    SQL_SERVER,
    SQL_TABLE,
    TIMEOUT_LINEUP,
    TIMEOUT_SAILED,
    URL_LINEUP,
    URL_SAILED,
)
from database import (
    criar_pivot_tables,
    ler_arquivo_novo,
    merge_com_banco,
    salvar_local,
    salvar_onedrive,
    salvar_sql_server,
)
from downloader import download_file
from latest_file import get_latest_file
from logger_config import logger


def main() -> None:
    logger.info("=" * 60)
    logger.info("INÍCIO DO PIPELINE — Arg Sailed Database")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 1. Downloads
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 1: Downloads ---")

    try:
        download_file(
            url=URL_SAILED,
            file_name="vessels_sailed_update.xlsx",
            destination_path=DIR_SAILED_BACKUP,
            timeout=TIMEOUT_SAILED,
        )
    except Exception as e:
        logger.error(f"Falha no download do Sailed: {e}")
        logger.error("Pipeline interrompido — não é possível continuar sem o arquivo.")
        sys.exit(1)

    time.sleep(3)  # Pequena pausa entre downloads

    try:
        download_file(
            url=URL_LINEUP,
            file_name="vessel_update.xlsx",
            destination_path=DIR_LINEUP_BACKUP,
            timeout=TIMEOUT_LINEUP,
        )
    except Exception as e:
        # Line-Up não é crítico para o banco — apenas loga e continua
        logger.warning(f"Falha no download do Line-Up (não crítico): {e}")

    # ------------------------------------------------------------------
    # 2. Leitura do arquivo mais recente
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 2: Leitura do arquivo ---")

    latest = get_latest_file(DIR_SAILED_BACKUP)
    df_novo = ler_arquivo_novo(latest)

    # ------------------------------------------------------------------
    # 3. Leitura do banco existente
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 3: Leitura do banco ---")

    logger.info(f"Lendo banco: {PATH_DATABASE}")
    db = pd.read_excel(PATH_DATABASE)
    db["Date"] = pd.to_datetime(db["Date"])
    logger.info(f"Banco carregado: {len(db)} linhas")

    # ------------------------------------------------------------------
    # 4. Merge
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 4: Merge ---")

    db_atualizado = merge_com_banco(df_novo, db)

    # Log das últimas 15 datas para conferência
    ultimas = (
        db_atualizado
        .sort_values("Date", ascending=False)
        .head(15)
        .sort_values("Date")
    )
    datas_str = ultimas["Date"].dt.strftime("%d/%m/%Y").to_string(index=False)
    logger.info(f"Últimas 15 datas no banco atualizado:\n{datas_str}")

    # ------------------------------------------------------------------
    # 5. Persistência
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 5: Salvamento ---")

    try:
        salvar_local(db_atualizado, PATH_DATABASE_OUTPUT)
    except Exception as e:
        logger.error(f"Falha ao salvar arquivo local: {e}")

    try:
        salvar_onedrive(db_atualizado, PATH_ONEDRIVE)
    except Exception as e:
        logger.error(f"Falha ao salvar no OneDrive: {e}")

    try:
        salvar_sql_server(db_atualizado, SQL_SERVER, SQL_DATABASE, SQL_TABLE)
    except Exception as e:
        logger.error(f"Falha ao salvar no SQL Server: {e}")

    # ------------------------------------------------------------------
    # 6. Pivot Tables
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 6: Pivot Tables ---")

    try:
        criar_pivot_tables(PATH_ONEDRIVE)
    except Exception as e:
        logger.error(f"Falha ao criar Pivot Tables: {e}")

    logger.info("=" * 60)
    logger.info("PIPELINE FINALIZADO COM SUCESSO")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
