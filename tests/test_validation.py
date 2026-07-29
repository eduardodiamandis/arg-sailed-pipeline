"""
test_validation.py
------------------
Testes para validation.py, portado do Desktop\\Argentina na Fase B.

Cobre em especial validar_continuidade, escrita a partir do incidente de
26-30/06/2026: a base congelou em 25/06, o arquivo do NABSA passou a trazer
apenas julho, e 5 dias sumiram do SQL Server e do Power BI por 26 dias sem
que nada reclamasse.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pandas as pd
import pytest

from datetime import date

from argentina_etl.validation import (
    detectar_gaps,
    resumo_mes_corrente,
    validar_continuidade,
    validar_corte_rodape,
)


def _df(dates: list[str]) -> pd.DataFrame:
    df = pd.DataFrame({"Date": pd.to_datetime(dates)})
    df["Destination"] = "CHINA"
    df["Origin"] = "ARGENTINA"
    df["Cargo"] = "CORN"
    df["Tons"] = 1000.0
    df["Month"] = df["Date"].dt.month
    df["Year"] = df["Date"].dt.year
    return df


# ---------------------------------------------------------------------------
# validar_continuidade
# ---------------------------------------------------------------------------

def test_continuidade_detecta_o_incidente_de_junho_2026():
    """O caso real: base parada em 25/06, arquivo novo começando em 01/07."""
    base = _df(["2026-06-20", "2026-06-25"])
    novo = _df(["2026-07-01", "2026-07-02"])

    r = validar_continuidade(base, novo)

    assert r is not None, "deveria alertar — 26 a 30/06 ficam sem cobertura"
    assert r["dias_no_vao"] == 5
    assert r["ultima_base"] == "2026-06-25"
    assert r["primeira_nova"] == "2026-07-01"


def test_continuidade_ok_quando_arquivo_sobrepoe_a_base():
    """Caso normal: o arquivo do NABSA cobre o mês inteiro, então sobrepõe."""
    base = _df(["2026-07-01", "2026-07-26"])
    novo = _df(["2026-07-01", "2026-07-27"])

    assert validar_continuidade(base, novo) is None


def test_continuidade_ok_na_virada_de_mes_com_base_atualizada():
    """Base até o último dia do mês, arquivo começando no primeiro do seguinte."""
    base = _df(["2026-06-29", "2026-06-30"])
    novo = _df(["2026-07-01"])

    assert validar_continuidade(base, novo) is None


def test_continuidade_tolera_dias_sem_embarque():
    """Nem todo dia tem embarque — um vão curto não deve alarmar."""
    base = _df(["2026-06-30"])
    novo = _df(["2026-07-03"])  # vão de 2 dias, dentro da tolerância padrão (3)

    assert validar_continuidade(base, novo) is None


def test_continuidade_respeita_tolerancia_customizada():
    base = _df(["2026-06-30"])
    novo = _df(["2026-07-03"])

    assert validar_continuidade(base, novo, tolerancia_dias=1) is not None


def test_continuidade_ignora_dataframes_vazios():
    vazio = pd.DataFrame({"Date": pd.to_datetime([])})
    assert validar_continuidade(vazio, _df(["2026-07-01"])) is None
    assert validar_continuidade(_df(["2026-07-01"]), vazio) is None


# ---------------------------------------------------------------------------
# detectar_gaps — documenta o alcance real, incluindo o ponto cego
# ---------------------------------------------------------------------------

def test_gaps_detecta_dia_que_sumiu_no_periodo_do_arquivo_novo():
    """Banco tem 3 dias de julho, arquivo novo só traz 2 — o dia 15 sumiu."""
    novo = _df(["2026-07-10", "2026-07-20"])
    banco = _df(["2026-07-10", "2026-07-15", "2026-07-20"])

    gaps = detectar_gaps(novo, banco)

    assert len(gaps) == 1
    assert gaps[0]["periodo"] == "2026-07"
    assert gaps[0]["dias_ausentes"] == [15]


def test_gaps_e_cego_para_periodo_fora_do_arquivo_novo():
    """
    Ponto cego documentado: detectar_gaps só examina os períodos presentes no
    arquivo novo. O buraco de junho passa despercebido — é por isso que
    validar_continuidade existe. Não é bug, é limite de escopo.
    """
    novo = _df(["2026-07-01", "2026-07-02"])
    banco = _df(["2026-06-25", "2026-07-01", "2026-07-02"])

    assert detectar_gaps(novo, banco) == []


def test_gaps_vazio_quando_tudo_consistente():
    novo = _df(["2026-07-01", "2026-07-02"])
    banco = _df(["2026-07-01", "2026-07-02"])

    assert detectar_gaps(novo, banco) == []


# ---------------------------------------------------------------------------
# validar_corte_rodape — só loga, nunca levanta
# ---------------------------------------------------------------------------

def test_corte_rodape_nao_levanta_com_df_vazio():
    validar_corte_rodape(pd.DataFrame({"Date": pd.to_datetime([])}), "teste.xlsx")


def test_corte_rodape_nao_levanta_com_data_recente():
    validar_corte_rodape(_df(["2026-07-27"]), "teste.xlsx")


def test_corte_rodape_nao_levanta_com_data_suspeita():
    """Última data antes do dia 15 é suspeita, mas só gera warning."""
    validar_corte_rodape(_df(["2026-07-03"]), "teste.xlsx")


# ---------------------------------------------------------------------------
# resumo_mes_corrente
# ---------------------------------------------------------------------------
# Substituiu o despejo das "ultimas 15 datas" no log: o que interessa e ate
# onde a base chegou e quantos dias faltam para o mes fechar.

def _base_do_mes(dias, ano=2026, mes=7):
    return pd.DataFrame({"Date": pd.to_datetime(
        [f"{ano}-{mes:02d}-{d:02d}" for d in dias]
    )})


def test_resumo_conta_os_dias_que_faltam_para_fechar():
    # Julho tem 31 dias; a base vai ate o dia 28 -> faltam 3
    r = resumo_mes_corrente(_base_do_mes([1, 15, 28]), hoje=date(2026, 7, 29))
    assert r["ultimo_dia"] == 28
    assert r["dias_no_mes"] == 31
    assert r["dias_para_fechar"] == 3
    assert r["dias_com_dados"] == 3


def test_resumo_reconhece_mes_fechado():
    r = resumo_mes_corrente(_base_do_mes([1, 31]), hoje=date(2026, 7, 31))
    assert r["dias_para_fechar"] == 0


def test_resumo_devolve_none_sem_dados_do_mes():
    """Normal na virada do mes, antes do primeiro embarque."""
    base = _base_do_mes([10, 20], mes=6)
    assert resumo_mes_corrente(base, hoje=date(2026, 7, 2)) is None


def test_resumo_usa_o_comprimento_real_do_mes():
    # Fevereiro de 2026 tem 28 dias
    r = resumo_mes_corrente(_base_do_mes([25], mes=2), hoje=date(2026, 2, 26))
    assert r["dias_no_mes"] == 28
    assert r["dias_para_fechar"] == 3


def test_resumo_ignora_outros_meses_e_anos():
    base = pd.DataFrame({"Date": pd.to_datetime(
        ["2026-07-10", "2026-08-31", "2025-07-31"]
    )})
    r = resumo_mes_corrente(base, hoje=date(2026, 7, 20))
    assert r["ultimo_dia"] == 10
    assert r["dias_com_dados"] == 1
