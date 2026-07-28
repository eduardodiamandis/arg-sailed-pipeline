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

from argentina_etl.validation import detectar_gaps, validar_continuidade, validar_corte_rodape


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
