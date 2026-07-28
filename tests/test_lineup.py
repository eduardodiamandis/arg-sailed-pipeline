"""
test_lineup.py
--------------
Testes para lineup.py, portado do Desktop\\Argentina na Fase C.

O parsing de datas e a classificacao de status nunca tiveram cobertura nos
dois repositorios, apesar de serem a parte mais fragil do modulo: dependem de
regex sobre texto livre do NABSA e de comparacoes com a data de hoje.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import datetime

import pandas as pd
import pytest

from argentina_etl.pipelines.lineup import COLUNAS_SQL, _classificar_status, _parsear_data


# ---------------------------------------------------------------------------
# _parsear_data
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("texto,esperado_dia,esperado_mes", [
    ("ETA REC 19/05", 19, 5),
    ("AT REC 11/05", 11, 5),
    ("ETF 19/05", 19, 5),
    ("  ETB 01/12  ", 1, 12),
    ("7/3", 7, 3),
])
def test_parsear_data_extrai_do_texto_livre(texto, esperado_dia, esperado_mes):
    r = _parsear_data(texto)
    assert r is not None
    assert (r.day, r.month) == (esperado_dia, esperado_mes)
    assert r.year == datetime.date.today().year, "sem ano no texto, usa o ano corrente"


def test_parsear_data_respeita_ano_explicito():
    assert _parsear_data("ETA 15/03/2024") == datetime.date(2024, 3, 15)


@pytest.mark.parametrize("texto", ["ETB", "", "   ", None, "sem data", "nan", 42, float("nan")])
def test_parsear_data_retorna_none_sem_data_valida(texto):
    assert _parsear_data(texto) is None


@pytest.mark.parametrize("texto", ["ETA 32/01", "ETA 15/13", "ETA 30/02"])
def test_parsear_data_rejeita_data_impossivel(texto):
    """Dia 32, mes 13 e 30/02 casam com a regex mas nao sao datas."""
    assert _parsear_data(texto) is None


# ---------------------------------------------------------------------------
# _classificar_status
# ---------------------------------------------------------------------------

def _row(eta=None, etb=None, etf=None) -> pd.Series:
    return pd.Series({"ETA_Date": eta, "ETB_Date": etb, "ETF_Date": etf})


ONTEM = datetime.date.today() - datetime.timedelta(days=1)
AMANHA = datetime.date.today() + datetime.timedelta(days=1)
HOJE = datetime.date.today()


def test_status_sailed_quando_etf_passou():
    assert _classificar_status(_row(eta=ONTEM, etb=ONTEM, etf=ONTEM)) == "SAILED"


def test_status_loading_quando_etb_passou_e_etf_nao():
    assert _classificar_status(_row(eta=ONTEM, etb=ONTEM, etf=AMANHA)) == "LOADING"


def test_status_waiting_quando_eta_passou_e_etb_nao():
    assert _classificar_status(_row(eta=ONTEM, etb=AMANHA)) == "WAITING"


def test_status_expected_quando_eta_no_futuro():
    assert _classificar_status(_row(eta=AMANHA)) == "EXPECTED"


def test_status_tbc_sem_nenhuma_data():
    assert _classificar_status(_row()) == "TBC"


def test_status_hoje_conta_como_passado():
    """As comparacoes usam <=, entao a data de hoje ja conta como ocorrida."""
    assert _classificar_status(_row(etf=HOJE)) == "SAILED"
    assert _classificar_status(_row(etb=HOJE)) == "LOADING"
    assert _classificar_status(_row(eta=HOJE)) == "WAITING"


def test_status_prioridade_etf_sobre_etb_sobre_eta():
    """ETF vence ETB, que vence ETA, mesmo com todas no passado."""
    assert _classificar_status(_row(eta=ONTEM, etb=ONTEM, etf=ONTEM)) == "SAILED"
    assert _classificar_status(_row(eta=ONTEM, etb=ONTEM)) == "LOADING"


def test_status_ignora_valores_nao_date():
    """NaT/None/strings nao devem ser tratados como data."""
    assert _classificar_status(_row(eta=None, etb=pd.NaT, etf="19/05")) == "TBC"


# ---------------------------------------------------------------------------
# Contrato com a tabela SQL
# ---------------------------------------------------------------------------

def test_colunas_sql_batem_com_a_tabela_arg_lineup():
    """
    As 14 colunas de COLUNAS_SQL devem casar com dbo.Arg_Lineup. Se alguem
    alterar a lista sem alterar a tabela, o INSERT quebra em producao.
    """
    esperado = [
        "SnapshotDate", "Port", "Terminal", "Vessel",
        "ETA_Raw", "ETA_Date", "ETB_Date", "ETF_Date",
        "Status", "Tons", "Commodity", "Destination", "Origin", "Charterer",
    ]
    assert COLUNAS_SQL == esperado
    assert len(COLUNAS_SQL) == 14
