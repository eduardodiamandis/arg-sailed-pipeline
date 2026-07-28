"""
test_pivot.py
-------------
Testes para montar_pivot, que substituiu a automacao COM do Excel em
2026-07-28.

Com o COM, os filtros viviam como "page fields" dentro do .xlsx e nao havia
como testa-los sem Excel instalado. Agora sao codigo Python comum.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl.storage.onedrive import PIVOT_CARGO, PIVOT_ORIGIN, montar_pivot


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _linha(rows: list[dict] = None, **kw) -> dict:
    base = {
        "Date": "2026-07-10", "Destination": "CHINA", "Origin": "ARGENTINA",
        "Cargo": "CORN", "Tons": 1000.0, "Month": 7, "Year": 2026,
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# Layout — o contrato visual herdado da versao COM
# ---------------------------------------------------------------------------

def test_cabecalho_declara_os_quatro_filtros():
    pv = montar_pivot(_df([_linha()]), year=2026, month=7)
    assert pv.iloc[0].tolist() == ["Month", 7]
    assert pv.iloc[1].tolist() == ["Cargo", "CORN"]
    assert pv.iloc[2].tolist() == ["Origin", "ARGENTINA"]
    assert pv.iloc[3].tolist() == ["Year", 2026]
    assert pv.iloc[5].tolist() == ["Row Labels", "Sum of Tons"]


def test_termina_com_grand_total():
    pv = montar_pivot(
        _df([_linha(Destination="CHINA", Tons=1000.0),
             _linha(Destination="PERU", Tons=500.0)]),
        year=2026, month=7,
    )
    assert pv.iloc[-1, 0] == "Grand Total"
    assert pv.iloc[-1, 1] == 1500.0


def test_destinos_em_ordem_alfabetica():
    pv = montar_pivot(
        _df([_linha(Destination="VIETNAM"), _linha(Destination="ALGERIA"),
             _linha(Destination="PERU")]),
        year=2026, month=7,
    )
    destinos = pv.iloc[6:-1, 0].tolist()
    assert destinos == sorted(destinos)


# ---------------------------------------------------------------------------
# Filtros — o que a versao COM aplicava como page fields
# ---------------------------------------------------------------------------

def test_filtra_por_ano():
    pv = montar_pivot(
        _df([_linha(Year=2026, Tons=100.0), _linha(Year=2025, Month=12, Tons=999.0)]),
        year=2026, month=7,
    )
    assert pv.iloc[-1, 1] == 100.0


def test_filtra_por_mes():
    pv = montar_pivot(
        _df([_linha(Month=7, Tons=100.0), _linha(Month=6, Tons=999.0)]),
        year=2026, month=7,
    )
    assert pv.iloc[-1, 1] == 100.0


def test_filtra_por_origin():
    pv = montar_pivot(
        _df([_linha(Origin="ARGENTINA", Tons=100.0), _linha(Origin="BRAZIL", Tons=999.0)]),
        year=2026, month=7,
    )
    assert pv.iloc[-1, 1] == 100.0


def test_filtra_por_cargo():
    pv = montar_pivot(
        _df([_linha(Cargo="CORN", Tons=100.0), _linha(Cargo="WHEAT", Tons=999.0)]),
        year=2026, month=7,
    )
    assert pv.iloc[-1, 1] == 100.0


@pytest.mark.parametrize("valor", ["argentina", "  ARGENTINA  ", "Argentina"])
def test_origin_e_insensivel_a_caixa_e_espacos(valor):
    pv = montar_pivot(_df([_linha(Origin=valor, Tons=100.0)]), year=2026, month=7)
    assert pv.iloc[-1, 1] == 100.0


@pytest.mark.parametrize("valor", ["corn", " CORN ", "Corn"])
def test_cargo_e_insensivel_a_caixa_e_espacos(valor):
    pv = montar_pivot(_df([_linha(Cargo=valor, Tons=100.0)]), year=2026, month=7)
    assert pv.iloc[-1, 1] == 100.0


def test_filtros_sao_customizaveis():
    pv = montar_pivot(
        _df([_linha(Origin="BRAZIL", Cargo="SOYA", Tons=42.0)]),
        year=2026, month=7, origin="BRAZIL", cargo="SOYA",
    )
    assert pv.iloc[1].tolist() == ["Cargo", "SOYA"]
    assert pv.iloc[2].tolist() == ["Origin", "BRAZIL"]
    assert pv.iloc[-1, 1] == 42.0


# ---------------------------------------------------------------------------
# Agregacao e bordas
# ---------------------------------------------------------------------------

def test_soma_tons_do_mesmo_destino():
    pv = montar_pivot(
        _df([_linha(Destination="CHINA", Tons=1000.0),
             _linha(Destination="CHINA", Tons=250.5)]),
        year=2026, month=7,
    )
    linhas = pv.iloc[6:-1]
    assert len(linhas) == 1
    assert linhas.iloc[0].tolist() == ["CHINA", 1250.5]


def test_sem_dados_no_filtro_gera_pivot_vazia_mas_valida():
    """
    Nenhuma linha casando nao e erro — e uma pivot com zero destinos.
    O cabecalho de filtros continua la, deixando claro o que foi consultado.
    """
    pv = montar_pivot(_df([_linha(Year=2025, Month=12)]), year=2026, month=7)
    assert pv.iloc[0].tolist() == ["Month", 7]
    assert pv.iloc[-1, 0] == "Grand Total"
    assert pv.iloc[-1, 1] == 0.0
    assert len(pv) == 7  # 4 filtros + vazia + cabecalho + Grand Total


def test_constantes_de_filtro_sao_as_do_negocio():
    assert PIVOT_ORIGIN == "ARGENTINA"
    assert PIVOT_CARGO == "CORN"
