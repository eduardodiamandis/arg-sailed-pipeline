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

from argentina_etl.config import (
    DIR_LINEUP_BACKUP,
    DIR_SAILED_BACKUP,
    LINEUP_FORCE_SNAPSHOT,
    PATH_DATABASE,
    PATH_DATABASE_OUTPUT,
    PATH_ONEDRIVE,
    SQL_DATABASE,
    SQL_SERVER,
    SQL_TABLE,
    SQL_TABLE_LINEUP,
    TIMEOUT_LINEUP,
    TIMEOUT_SAILED,
    URL_LINEUP,
    URL_SAILED,
)
from argentina_etl.pipelines.sailed import ler_arquivo_novo, merge_com_banco
from argentina_etl.storage.excel import salvar_local
from argentina_etl.storage.onedrive import salvar_onedrive, _forcar_sync_onedrive
from argentina_etl.storage.sql_server import salvar_sql_server
from argentina_etl.downloader import download_file
from argentina_etl.reporting.report import send_log_report
from argentina_etl.utils.files import get_latest_file
from argentina_etl.pipelines.lineup import ler_arquivo_lineup
from argentina_etl.storage.sql_server import salvar_lineup_sql
from argentina_etl.logging_setup import logger, _DEFAULT_LOG_FILE
from argentina_etl.storage.pivot import criar_pivot_tables   # módulo separado com timeout
from argentina_etl.validation import detectar_gaps, validar_continuidade, validar_corte_rodape


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
    # 1b. Snapshot diario do Line-Up
    # ------------------------------------------------------------------
    # Arg_Lineup e append-only: acumula um snapshot por dia e nunca apaga
    # historico. Nao-critico — o Sailed e o produto principal e nao depende
    # disto. Navios com Status=SAILED sao filtrados dentro de
    # ler_arquivo_lineup para nao duplicar o que ja esta em Arg_Sailed.
    logger.info("--- ETAPA 1b: Snapshot do Line-Up ---")

    try:
        latest_lineup = get_latest_file(DIR_LINEUP_BACKUP)
        df_lineup = ler_arquivo_lineup(latest_lineup)
        salvar_lineup_sql(
            df_lineup,
            SQL_SERVER,
            SQL_DATABASE,
            SQL_TABLE_LINEUP,
            force=LINEUP_FORCE_SNAPSHOT,
        )
    except FileNotFoundError:
        logger.warning("Nenhum arquivo de Line-Up encontrado — etapa ignorada.")
    except Exception as e:
        logger.error(f"Falha no processamento do Line-Up (nao-critico): {e}")

    # ------------------------------------------------------------------
    # 2. Leitura do arquivo mais recente
    # ------------------------------------------------------------------
    logger.info("--- ETAPA 2: Leitura do arquivo ---")

    latest = get_latest_file(DIR_SAILED_BACKUP)
    df_novo = ler_arquivo_novo(latest)

    # Alerta se o detector de rodapé pode ter cortado dados cedo demais.
    # As ETAPAS 2-4 nao sao protegidas por try/except, e uma validacao que so
    # informa nunca deve derrubar o pipeline — dai a guarda local.
    try:
        validar_corte_rodape(df_novo, path_original=str(latest))
    except Exception as e:
        logger.error(f"Falha na validacao de corte de rodape (ignorada): {e}")

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

    # Continuidade entre o fim da base e o inicio do arquivo novo. E ESTA que
    # detecta a classe de perda ocorrida em 26-30/06/2026: base parada em 25/06
    # e arquivo trazendo so julho. O detectar_gaps abaixo NAO pega esse caso,
    # porque so examina os periodos presentes no arquivo novo.
    # Detecta dias que existiam no banco e sumiram apos o merge — na pratica,
    # o caso em que a trava de seguranca rejeitou um periodo. Os gaps sao
    # registrados como WARNING e o _extract_warnings do email_report os coleta
    # do log, entao aparecem na secao "Avisos" do e-mail sem acoplamento extra.
    #
    # 'db' ainda e o banco PRE-merge: merge_com_banco converte a coluna Date
    # in-place mas nao remove linhas.
    try:
        validar_continuidade(db, df_novo)
        gaps = detectar_gaps(df_novo, db_atualizado)
        if gaps:
            logger.warning(
                f"⚠️ {len(gaps)} periodo(s) com gaps — verifique o arquivo-fonte "
                f"antes de confiar nos dados."
            )
    except Exception as e:
        logger.error(f"Falha nas validacoes pos-merge (ignorada): {e}")

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