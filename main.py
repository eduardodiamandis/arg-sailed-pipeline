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
  7. Envia resumo do log por e-mail
"""
from __future__ import annotations

import sys
import time

import pandas as pd

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
    ler_arquivo_novo,
    merge_com_banco,
    salvar_local,
    salvar_onedrive,
    salvar_sql_server,
    _forcar_sync_onedrive,
)
from downloader import download_file
from email_report import send_log_report
from latest_file import get_latest_file
from logger_config import logger, _DEFAULT_LOG_FILE
from pivot_tables import criar_pivot_tables   # módulo separado com timeout


def main() -> None:
    start_time = time.time()
    pipeline_ok = True

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
        pipeline_ok = False
        send_log_report(_DEFAULT_LOG_FILE, success=False,
                        duration_seconds=time.time() - start_time)
        sys.exit(1)

    time.sleep(3)

    try:
        download_file(
            url=URL_LINEUP,
            file_name="vessel_update.xlsx",
            destination_path=DIR_LINEUP_BACKUP,
            timeout=TIMEOUT_LINEUP,
        )
    except Exception as e:
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

    ultimas = (
        db_atualizado
        .sort_values("Date", ascending=False)
        .head(15)
        .sort_values("Date")
    )
    datas_str = ultimas["Date"].dt.strftime("%d/%m/%Y").to_string(index=False)
    logger.info(f"Últimas 15 datas no banco atualizado:\n{datas_str}")

    # Estatísticas para o e-mail de sucesso
    periodos_novos = df_novo["Date"].dt.to_period("M").unique()
    rows_added     = len(db_atualizado) - (len(db) - db["Date"].dt.to_period("M").isin(periodos_novos).sum())
    db_stats = {
        "total_rows": len(db_atualizado),
        "last_date":  db_atualizado["Date"].max().strftime("%d/%m/%Y"),
        "rows_added": int(rows_added),
        "periods":    ", ".join(sorted(periodos_novos.astype(str))),
    }

    # ------------------------------------------------------------------
    # 5. Persistência
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 5: Salvamento ---")

    ts = time.strftime("%Y-%m-%d_%H%M")
    path_local = PATH_DATABASE_OUTPUT.with_stem(PATH_DATABASE_OUTPUT.stem + f"_{ts}")
    try:
        salvar_local(db_atualizado, path_local)
    except Exception as e:
        logger.error(f"Falha ao salvar arquivo local: {e}")
        pipeline_ok = False

    # Atualiza a base para que a próxima execução parta do estado atual.
    # Sem isto a base congela: enquanto o NABSA entrega o mês corrente o merge
    # recompõe o mês, mas na virada os dias do fim do mês anterior caem num vão
    # que ninguém preenche (foi o que apagou 26–30/06/2026 do Power BI por 26
    # dias). Depende da trava de segurança de merge_com_banco — reescrever a base
    # com um merge que substitui períodos cegamente corromperia o histórico.
    # Ver ESTRUTURA.md, decisão 9.1.
    try:
        salvar_local(db_atualizado, PATH_DATABASE)
        logger.info(f"Base principal atualizada: {PATH_DATABASE}")
    except Exception as e:
        logger.error(f"Falha ao atualizar a base principal: {e}")
        pipeline_ok = False

    try:
        salvar_onedrive(db_atualizado, PATH_ONEDRIVE)
    except Exception as e:
        logger.error(f"Falha ao salvar no OneDrive: {e}")
        pipeline_ok = False

    try:
        salvar_sql_server(db_atualizado, SQL_SERVER, SQL_DATABASE, SQL_TABLE)
    except Exception as e:
        logger.error(f"Falha ao salvar no SQL Server: {e}")
        pipeline_ok = False

    # ------------------------------------------------------------------
    # 6. Pivot Tables (com timeout — não trava o Task Scheduler)
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 6: Pivot Tables ---")

    try:
        criar_pivot_tables(PATH_ONEDRIVE)
    except TimeoutError as e:
        logger.error(f"Timeout nas Pivot Tables: {e}")
        pipeline_ok = False
    except Exception as e:
        logger.error(f"Falha ao criar Pivot Tables: {e}")
        pipeline_ok = False
    finally:
        # Notifica o OneDrive após o Excel soltar o arquivo
        _forcar_sync_onedrive(PATH_ONEDRIVE)

    # ------------------------------------------------------------------
    # 7. E-mail com resumo do log
    # ------------------------------------------------------------------
    duration = time.time() - start_time
    logger.info("--- ETAPA 7: Envio de e-mail ---")

    if pipeline_ok:
        logger.info("=" * 60)
        logger.info("PIPELINE FINALIZADO COM SUCESSO")
        logger.info(f"Duração total: {duration:.1f}s")
        logger.info("=" * 60)
    else:
        logger.warning("=" * 60)
        logger.warning("PIPELINE FINALIZADO COM ERROS — verifique o log")
        logger.warning(f"Duração total: {duration:.1f}s")
        logger.warning("=" * 60)

    send_log_report(
        _DEFAULT_LOG_FILE,
        success=pipeline_ok,
        duration_seconds=duration,
        db_stats=db_stats,
    )

    


if __name__ == "__main__":
    main()