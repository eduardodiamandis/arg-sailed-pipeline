"""
tests/test_database.py
----------------------
Testes unitários para a lógica de merge do banco de dados.
Execute com: pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import _cortar_apos_duas_linhas_vazias, merge_com_banco


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(dates: list[str], tons: list[float] | None = None) -> pd.DataFrame:
    df = pd.DataFrame({
        "Date": pd.to_datetime(dates),
        "Destination": "CHINA",
        "Origin": "ARGENTINA",
        "Cargo": "CORN",
        "Tons": tons or [1000.0] * len(dates),
    })
    df["Month"] = df["Date"].dt.month
    df["Year"] = df["Date"].dt.year
    return df


# ---------------------------------------------------------------------------
# Testes: _cortar_apos_duas_linhas_vazias
# ---------------------------------------------------------------------------

def test_cortar_sem_vazias():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    resultado = _cortar_apos_duas_linhas_vazias(df)
    assert len(resultado) == 3


def test_cortar_com_duas_vazias_consecutivas():
    df = pd.DataFrame({
        "A": [1, None, None, 99],
        "B": [2, None, None, 99],
    })
    resultado = _cortar_apos_duas_linhas_vazias(df)
    assert len(resultado) == 1
    assert resultado.iloc[0]["A"] == 1


# ---------------------------------------------------------------------------
# Testes: merge_com_banco
# ---------------------------------------------------------------------------

def test_merge_caso_normal():
    """Arquivo novo tem só o mês atual — deve substituir apenas esse mês."""
    banco = _make_df(["2025-12-01", "2025-12-15", "2026-01-10", "2026-02-05"])
    novo = _make_df(["2026-03-01", "2026-03-15"])

    resultado = merge_com_banco(novo, banco)

    periodos = resultado["Date"].dt.to_period("M").unique().astype(str)
    assert "2025-12" in periodos
    assert "2026-01" in periodos
    assert "2026-02" in periodos
    assert "2026-03" in periodos
    assert len(resultado) == 6  # 4 do banco + 2 novos


def test_merge_sem_duplicatas():
    """Rodar o merge duas vezes não deve duplicar linhas."""
    banco = _make_df(["2025-12-01"])
    novo = _make_df(["2026-01-10"])

    resultado_1 = merge_com_banco(novo, banco)
    resultado_2 = merge_com_banco(novo, resultado_1)

    assert len(resultado_1) == len(resultado_2)


def test_merge_substitui_mes_existente():
    """Arquivo novo com jan/2026 deve substituir jan/2026 que já estava no banco."""
    banco = _make_df(["2026-01-05", "2026-01-20"], tons=[500.0, 500.0])
    novo = _make_df(["2026-01-05", "2026-01-20", "2026-01-25"], tons=[600.0, 600.0, 600.0])

    resultado = merge_com_banco(novo, banco)

    jan_2026 = resultado[resultado["Date"].dt.to_period("M").astype(str) == "2026-01"]
    assert len(jan_2026) == 3
    assert all(jan_2026["Tons"] == 600.0)


def test_merge_multiplos_meses_novos():
    """Arquivo montado manualmente com jan+fev+mar deve inserir todos os três."""
    banco = _make_df(["2025-11-01", "2025-12-01"])
    novo = _make_df(["2026-01-10", "2026-02-15", "2026-03-05"])

    resultado = merge_com_banco(novo, banco)

    periodos = set(resultado["Date"].dt.to_period("M").astype(str))
    assert periodos == {"2025-11", "2025-12", "2026-01", "2026-02", "2026-03"}
    assert len(resultado) == 5


def test_merge_banco_vazio():
    """Banco vazio deve simplesmente receber os dados novos."""
    banco = pd.DataFrame(columns=["Date", "Destination", "Origin", "Cargo", "Tons", "Month", "Year"])
    novo = _make_df(["2026-03-01"])

    resultado = merge_com_banco(novo, banco)
    assert len(resultado) == 1


def test_merge_preserva_2025_completo():
    """2025 completo no banco nunca deve ser apagado pelo arquivo novo de 2026."""
    datas_2025 = [f"2025-{m:02d}-01" for m in range(1, 13)]
    banco = _make_df(datas_2025)
    novo = _make_df(["2026-03-01", "2026-03-15"])

    resultado = merge_com_banco(novo, banco)

    anos = resultado["Year"].unique()
    assert 2025 in anos
    assert 2026 in anos

    meses_2025 = resultado[resultado["Year"] == 2025]["Month"].unique()
    assert len(meses_2025) == 12
