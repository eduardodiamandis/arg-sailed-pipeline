"""
validacao.py
------------
Validações pós-merge para detectar problemas antes de persistir.

Principal função: detectar gaps (dias faltando) nos períodos
que acabaram de ser atualizados.
"""
from __future__ import annotations

import pandas as pd

from argentina_etl.logging_setup import logger


def detectar_gaps(
    df_novo: pd.DataFrame,
    db_atualizado: pd.DataFrame,
) -> list[dict]:
    """
    Dias que existem no banco JÁ ATUALIZADO mas não vieram no arquivo novo.

    Para cada período (mês/ano) presente em `df_novo`, compara o conjunto de
    dias do arquivo com o conjunto de dias que o período tem em
    `db_atualizado`. Sobrando dias no banco, eles viram um gap.

    Na prática isso pega o caso em que a trava de segurança de
    `merge_com_banco` rejeitou o período: o banco manteve os dias antigos e o
    arquivo novo trouxe menos.

    NÃO compara com o calendário — nem todo dia tem embarque.

    ⚠️ **Limite de escopo: só examina períodos presentes no arquivo novo.**
    Um mês que sumiu por inteiro, ou que nunca aparece no arquivo, é invisível
    aqui. Foi exatamente o caso de 26–30/06/2026: o arquivo trazia apenas
    julho, junho jamais era examinado, e a função reportava "nenhum gap". Quem
    cobre esse ângulo é `validar_continuidade`, comparando o fim da base com o
    início do arquivo novo. Ver `test_gaps_e_cego_para_periodo_fora_do_arquivo_novo`.

    Até 2026-07-29 esta docstring dizia comparar "com o banco ANTES da
    atualização" — o código sempre comparou com o banco DEPOIS. Foi essa
    descrição que sustentou a suposição errada da Fase B (ESTRUTURA.md).

    Parameters
    ----------
    df_novo       : arquivo recém-lido do NABSA
    db_atualizado : banco DEPOIS do merge

    Returns
    -------
    Lista de dicts com:
        - periodo       : str  (ex: "2026-03")
        - dias_no_banco : int  (quantos dias únicos ficaram no banco)
        - dias_no_novo  : int  (quantos dias únicos vieram no arquivo novo)
        - dias_faltando : int  (quantos dias sobraram só no banco)
        - dias_ausentes : list[int]  (quais são esses dias)
    """
    gaps = []

    periodos_novos = df_novo["Date"].dt.to_period("M").unique()

    for periodo in sorted(periodos_novos):
        mes = periodo.month
        ano = periodo.year

        # Dias no arquivo novo para este período
        dias_novo = set(
            df_novo.loc[
                (df_novo["Date"].dt.month == mes) & (df_novo["Date"].dt.year == ano),
                "Date",
            ].dt.day.unique()
        )

        # Dias no banco atualizado para este período
        dias_banco = set(
            db_atualizado.loc[
                (db_atualizado["Date"].dt.month == mes)
                & (db_atualizado["Date"].dt.year == ano),
                "Date",
            ].dt.day.unique()
        )

        logger.info(
            f"Validação {periodo}: "
            f"{len(dias_novo)} dias no arquivo novo, "
            f"{len(dias_banco)} dias no banco atualizado"
        )

        if dias_novo != dias_banco:
            faltando = sorted(dias_banco - dias_novo)
            if faltando:
                gap_info = {
                    "periodo": str(periodo),
                    "dias_no_banco": len(dias_banco),
                    "dias_no_novo": len(dias_novo),
                    "dias_faltando": len(faltando),
                    "dias_ausentes": faltando,
                }
                gaps.append(gap_info)
                logger.warning(
                    f"⚠️ GAP DETECTADO em {periodo}: "
                    f"{len(faltando)} dia(s) ausentes no arquivo novo: {faltando}"
                )

    if not gaps:
        logger.info("✅ Nenhum gap detectado — todos os períodos estão consistentes.")

    return gaps


def validar_continuidade(
    db_antes: pd.DataFrame,
    df_novo: pd.DataFrame,
    tolerancia_dias: int = 3,
) -> dict | None:
    """
    Detecta descontinuidade entre o fim da base e o início do arquivo novo.

    Complementa `detectar_gaps`, que só examina os períodos presentes no arquivo
    novo e por isso é cego para o vão que se abre na virada de mês quando a base
    está parada. Foi assim que 26–30/06/2026 sumiram do Power BI por 26 dias:
    base congelada em 25/06, arquivo do NABSA trazendo apenas julho, e nenhuma
    das duas pontas reclamando.

    Sobreposição é o caso normal — o arquivo do NABSA cobre o mês corrente
    inteiro, então a primeira data dele costuma ser anterior à última da base e
    o vão fica negativo. Só um vão positivo indica base defasada.

    Parameters
    ----------
    db_antes        : Banco ANTES do merge
    df_novo         : Arquivo novo já lido
    tolerancia_dias : Vão tolerado sem alerta. Nem todo dia tem embarque, então
                      um mês pode legitimamente começar no dia 2 ou 3.

    Returns
    -------
    dict com 'ultima_base', 'primeira_nova' e 'dias_no_vao', ou None se está tudo bem.
    """
    if db_antes.empty or df_novo.empty:
        return None

    ultima_base = pd.to_datetime(db_antes["Date"], errors="coerce").max()
    primeira_nova = pd.to_datetime(df_novo["Date"], errors="coerce").min()

    if pd.isna(ultima_base) or pd.isna(primeira_nova):
        return None

    dias_no_vao = (primeira_nova - ultima_base).days - 1

    if dias_no_vao <= tolerancia_dias:
        logger.info(
            f"Continuidade OK: base até {ultima_base.strftime('%d/%m/%Y')}, "
            f"arquivo novo começa em {primeira_nova.strftime('%d/%m/%Y')}"
        )
        return None

    logger.warning(
        f"⚠️ DESCONTINUIDADE: a base termina em {ultima_base.strftime('%d/%m/%Y')} e o "
        f"arquivo novo só começa em {primeira_nova.strftime('%d/%m/%Y')} — {dias_no_vao} dia(s) "
        f"sem cobertura de nenhuma das duas fontes. Esses dias NÃO entrarão no resultado. "
        f"Verifique se a base parou de ser atualizada."
    )
    return {
        "ultima_base": ultima_base.strftime("%Y-%m-%d"),
        "primeira_nova": primeira_nova.strftime("%Y-%m-%d"),
        "dias_no_vao": int(dias_no_vao),
    }


def validar_corte_rodape(df: pd.DataFrame, path_original: str = "") -> None:
    """
    Validação extra: verifica se o DataFrame resultante do corte de rodapé
    tem pelo menos dados até o penúltimo dia do mês mais recente.
    Loga um warning se parecer que dados foram cortados prematuramente.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio após leitura de {path_original}")
        return

    ultima_data = df["Date"].max()
    if pd.isna(ultima_data):
        logger.warning("Coluna 'Date' não contém datas válidas após a leitura.")
        return

    # Se a última data é antes do dia 15 do mês, pode ser corte prematuro
    if ultima_data.day < 15:
        logger.warning(
            f"⚠️ Última data no arquivo é {ultima_data.strftime('%d/%m/%Y')} — "
            f"isso pode indicar corte prematuro pelo detector de rodapé. "
            f"Verifique o arquivo-fonte manualmente."
        )