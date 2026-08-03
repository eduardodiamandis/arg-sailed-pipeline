"""
test_dashboard.py
-----------------
Testes do gerador de dashboard HTML.

Tres coisas que precisam ficar presas aqui:

1. A **quarentena de tonelagem**. Ela existe porque a base tem uma linha de 49,8
   milhoes de toneladas num unico navio, que sozinha vale 5% do historico e
   achata a escala de todos os graficos. Se alguem afrouxar o limite sem
   perceber, o pico volta calado. E o contrario tambem importa: a linha nao pode
   sumir — tem que aparecer em `anomalias`, com navio e data.

2. O **escape do payload**. Os rotulos vem do arquivo do NABSA, ou seja, sao
   dados nao confiaveis dentro de uma tag <script>. Um '</script>' num nome de
   navio quebraria a pagina inteira.

3. O **grao das tabelas-fato**. Os filtros de ano, mes e carga so alcancam todos
   os graficos porque as duas tabelas comecam com o mesmo prefixo de chaves
   (ano, mes, carga). Perder isso tira um grafico do filtro sem nenhum sinal — a
   tela passa a mostrar dois recortes diferentes ao mesmo tempo.
"""
from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl.reporting import dashboard


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _sailed(linhas: list[dict]) -> pd.DataFrame:
    base = {
        "Port": "SAN LORENZO",
        "Terminal": "ACA San Lorenzo",
        "Vessel": "NAVIO",
        "Status": "SAILED",
        "Tons": 30_000.0,
        "Cargo": "CORN",
        "Origin": "ARGENTINA",
        "Destination": "CHINA",
        "Coordinator": None,
        "Charterer": "CARGILL",
    }
    return pd.DataFrame([{**base, **linha} for linha in linhas])


@pytest.fixture
def sailed_simples() -> pd.DataFrame:
    return _sailed([
        {"Date": datetime.datetime(2024, 1, 10), "Tons": 10_000.0, "Cargo": "CORN", "Destination": "CHINA"},
        {"Date": datetime.datetime(2024, 1, 20), "Tons": 20_000.0, "Cargo": "CORN", "Destination": "INDIA"},
        {"Date": datetime.datetime(2024, 2, 5), "Tons": 30_000.0, "Cargo": "WHEAT", "Destination": "CHINA"},
        {"Date": datetime.datetime(2025, 3, 1), "Tons": 40_000.0, "Cargo": "WHEAT", "Destination": "PERU"},
    ])


# ---------------------------------------------------------------------------
# _colapsar_cauda
# ---------------------------------------------------------------------------

def test_colapsar_cauda_mantem_os_mais_frequentes():
    serie = pd.Series(["A", "A", "A", "B", "B", "C", "D"])
    out = dashboard._colapsar_cauda(serie, 2)
    assert set(out.unique()) == {"A", "B", dashboard.ROTULO_OUTROS}
    assert (out == "A").sum() == 3
    assert (out == dashboard.ROTULO_OUTROS).sum() == 2  # C e D


def test_colapsar_cauda_trata_nulo_e_vazio_como_outros():
    serie = pd.Series(["A", "A", None, "   ", "B"])
    out = dashboard._colapsar_cauda(serie, 1)
    assert out.iloc[2] == dashboard.ROTULO_OUTROS
    assert out.iloc[3] == dashboard.ROTULO_OUTROS


def test_indexar_joga_outros_para_o_fim():
    """A pagina pinta o resíduo de cinza pela posicao, nao pela ordem alfabetica."""
    serie = pd.Series([dashboard.ROTULO_OUTROS, "ZINCO", "ALHO"])
    rotulos, mapa = dashboard._indexar(serie)
    assert rotulos == ["ALHO", "ZINCO", dashboard.ROTULO_OUTROS]
    assert mapa[dashboard.ROTULO_OUTROS] == len(rotulos) - 1


# ---------------------------------------------------------------------------
# agregar_sailed — grao e totais
# ---------------------------------------------------------------------------

def test_agregar_sailed_soma_confere_com_a_origem(sailed_simples):
    out = dashboard.agregar_sailed(sailed_simples)
    assert out["totais"]["tons"] == pytest.approx(100_000.0)
    assert out["totais"]["embarques"] == 4
    assert sum(l[-2] for l in out["fatoMes"]) == pytest.approx(100_000.0)


def test_agregar_sailed_as_duas_tabelas_batem_no_mesmo_total(sailed_simples):
    """
    As duas agregam a mesma base por recortes diferentes; se uma divergir, os
    graficos passam a contar historias diferentes sobre o mesmo filtro.
    """
    out = dashboard.agregar_sailed(sailed_simples)
    for chave in ("fatoMes", "fatoTerminal"):
        assert sum(l[-2] for l in out[chave]) == pytest.approx(100_000.0), chave


def test_agregar_sailed_todas_as_tabelas_carregam_ano_mes_e_carga(sailed_simples):
    """
    Os filtros de ano, mes e carga so alcancam todos os graficos porque as duas
    tabelas comecam com o mesmo prefixo de chaves (ano, mes, carga). Perder isso
    tira algum grafico do filtro sem nenhum sinal.
    """
    out = dashboard.agregar_sailed(sailed_simples)
    # fatoMes:      [ano, mes, carga, destino, tons, n]
    # fatoTerminal: [ano, mes, carga, terminal, tons, n]
    assert all(len(l) == 6 for l in out["fatoMes"])
    assert all(len(l) == 6 for l in out["fatoTerminal"])

    for chave in ("fatoMes", "fatoTerminal"):
        assert {l[0] for l in out[chave]} == {2024, 2025}, chave
        assert {l[1] for l in out[chave]} == {1, 2, 3}, chave


def test_agregar_sailed_usa_date_e_nao_month_year(sailed_simples):
    """
    Month/Year existem na planilha mas sao derivados e podem estar
    dessincronizados. Date manda.
    """
    df = sailed_simples.copy()
    df["Month"] = 99
    df["Year"] = 1900
    out = dashboard.agregar_sailed(df)
    assert out["anos"] == [2024, 2025]
    assert {l[1] for l in out["fatoMes"]} == {1, 2, 3}


def test_agregar_sailed_ignora_linhas_sem_data_ou_tonelagem():
    df = _sailed([
        {"Date": datetime.datetime(2024, 1, 10), "Tons": 10_000.0},
        {"Date": None, "Tons": 50_000.0},
        {"Date": datetime.datetime(2024, 1, 11), "Tons": None},
    ])
    out = dashboard.agregar_sailed(df)
    assert out["totais"]["embarques"] == 1
    assert out["totais"]["tons"] == pytest.approx(10_000.0)


def test_agregar_sailed_conta_distintos_antes_de_colapsar():
    """
    'Destinos distintos' precisa vir da base, nao das dimensoes: la sobrariam
    apenas os mantidos mais o balde OUTROS.
    """
    linhas = [
        {"Date": datetime.datetime(2024, 1, d + 1), "Destination": f"PAIS_{d}", "Tons": 1_000.0}
        for d in range(25)
    ]
    out = dashboard.agregar_sailed(_sailed(linhas))
    assert out["totais"]["destinos"] == 25
    assert len(out["dims"]["destinos"]) == dashboard.TOPO_DESTINOS + 1  # + OUTROS


# ---------------------------------------------------------------------------
# agregar_sailed — quarentena de tonelagem
# ---------------------------------------------------------------------------

def test_tonelagem_impossivel_sai_das_agregacoes():
    df = _sailed([
        {"Date": datetime.datetime(2025, 11, 20), "Tons": 49_806_070.5, "Vessel": "EPIC RADIANCE"},
        {"Date": datetime.datetime(2025, 11, 21), "Tons": 30_000.0, "Vessel": "NORMAL"},
    ])
    out = dashboard.agregar_sailed(df)
    assert out["totais"]["tons"] == pytest.approx(30_000.0)
    assert sum(l[-2] for l in out["fatoMes"]) == pytest.approx(30_000.0)


def test_tonelagem_impossivel_e_exibida_e_nao_descartada():
    """Sai do grafico, mas vai inteira para o aviso — com navio, data e valor."""
    df = _sailed([
        {"Date": datetime.datetime(2025, 11, 20), "Tons": 49_806_070.5,
         "Vessel": "EPIC RADIANCE", "Cargo": "SOYBEANMEAL", "Destination": "TURKEY"},
        {"Date": datetime.datetime(2025, 11, 21), "Tons": 30_000.0},
    ])
    out = dashboard.agregar_sailed(df)
    assert len(out["anomalias"]) == 1
    anomalia = out["anomalias"][0]
    assert anomalia["navio"] == "EPIC RADIANCE"
    assert anomalia["data"] == "2025-11-20"
    assert anomalia["destino"] == "TURKEY"
    assert anomalia["tons"] == pytest.approx(49_806_070.5)


def test_carga_plausivel_no_limite_permanece():
    """A quarentena e para o impossivel, nao para o navio grande."""
    df = _sailed([
        {"Date": datetime.datetime(2025, 9, 22), "Tons": 445_202.0, "Cargo": "IRON ORE"},
    ])
    out = dashboard.agregar_sailed(df)
    assert out["anomalias"] == []
    assert out["totais"]["tons"] == pytest.approx(445_202.0)


def test_base_sem_linhas_utilizaveis_levanta():
    df = _sailed([{"Date": None, "Tons": None}])
    with pytest.raises(ValueError):
        dashboard.agregar_sailed(df)


# ---------------------------------------------------------------------------
# agregar_lineup
# ---------------------------------------------------------------------------

def _lineup(linhas: list[dict]) -> pd.DataFrame:
    base = {
        "Port": "SAN LORENZO",
        "Terminal": "RENOVA NORTH",
        "Vessel": "NAVIO",
        "ETA_Raw": "ETA REC 22/05",
        "ETA_Date": datetime.date(2026, 5, 22),
        "ETB_Date": None,
        "ETF_Date": None,
        "Tons": 29_500.0,
        "Commodity": "SOYBEAN OIL",
        "Destination": "INDIA",
        "Origin": "ARGENTINA",
        "Charterer": "BUNGE",
        "Status": "WAITING",
    }
    return pd.DataFrame([{**base, **linha} for linha in linhas])


def test_agregar_lineup_usa_o_snapshot_mais_recente():
    df = _lineup([
        {"SnapshotDate": "2026-05-26", "Vessel": "ANTIGO"},
        {"SnapshotDate": "2026-08-03", "Vessel": "ATUAL"},
        {"SnapshotDate": "2026-08-03", "Vessel": "ATUAL_2"},
    ])
    out = dashboard.agregar_lineup(df)
    assert out["snapshot"] == "2026-08-03"
    assert {n["navio"] for n in out["navios"]} == {"ATUAL", "ATUAL_2"}


def test_agregar_lineup_mantem_o_historico_de_todos_os_snapshots():
    df = _lineup([
        {"SnapshotDate": "2026-05-26", "Tons": 1_000.0},
        {"SnapshotDate": "2026-08-03", "Tons": 2_000.0},
    ])
    out = dashboard.agregar_lineup(df)
    assert out["datasHistorico"] == ["2026-05-26", "2026-08-03"]
    # [data, porto, status, carga, tons, navios]
    assert sum(linha[4] for linha in out["historico"]) == pytest.approx(3_000.0)


def test_historico_do_lineup_e_quebrado_para_os_filtros_alcancarem():
    """
    O historico vem por (data, porto, status, carga) e nao como total pronto:
    sem essa quebra, o grafico de evolucao da fila discordaria dos KPIs assim
    que qualquer filtro da pagina fosse ligado.
    """
    df = _lineup([
        {"SnapshotDate": "2026-08-03", "Port": "ROSARIO", "Commodity": "CORN", "Tons": 500.0},
        {"SnapshotDate": "2026-08-03", "Port": "ROSARIO", "Commodity": "WHEAT", "Tons": 700.0},
    ])
    out = dashboard.agregar_lineup(df)
    cargas = {linha[3] for linha in out["historico"]}
    assert cargas == {"CORN", "WHEAT"}
    assert all(len(linha) == 6 for linha in out["historico"])


def test_agregar_lineup_nat_nao_vira_data():
    """pd.NaT passa em isinstance(x, datetime.date) — o helper tem que barrar."""
    df = _lineup([{"SnapshotDate": "2026-08-03", "ETB_Date": pd.NaT, "ETF_Date": pd.NaT}])
    out = dashboard.agregar_lineup(df)
    assert out["navios"][0]["etb"] is None
    assert out["navios"][0]["etf"] is None


def test_agregar_lineup_vazio_nao_quebra():
    out = dashboard.agregar_lineup(pd.DataFrame())
    assert out["snapshot"] is None
    assert out["navios"] == []


# ---------------------------------------------------------------------------
# renderizar
# ---------------------------------------------------------------------------

def test_renderizar_escapa_fechamento_de_script():
    """Um '</script>' num rotulo do NABSA fecharia a tag e mataria a pagina."""
    payload = {"sailed": {"dims": {"cargas": ["</script><img src=x onerror=alert(1)>"]}}}
    html = dashboard.renderizar(payload, "<script>const D = /*__PAYLOAD__*/;</script>")
    assert "</script><img" not in html
    assert "\\u003c" in html
    assert html.count("</script>") == 1  # so o fechamento real do template


def test_renderizar_produz_json_valido_apos_o_escape():
    payload = {"nome": "a<b", "n": 1}
    html = dashboard.renderizar(payload, "/*__PAYLOAD__*/")
    assert json.loads(html.replace("\\u003c", "<")) == payload


def test_renderizar_sem_marcador_levanta():
    with pytest.raises(ValueError):
        dashboard.renderizar({}, "<html>sem marcador</html>")


def test_template_existe_e_tem_o_marcador():
    """O template e um arquivo do pacote; se sumir do build, isso pega."""
    assert dashboard._TEMPLATE.exists()
    assert "/*__PAYLOAD__*/" in dashboard._TEMPLATE.read_text(encoding="utf-8")


def test_pagina_gerada_nao_faz_requisicao_externa():
    """
    O arquivo precisa ser autocontido: ele vai ser embutido em outra aplicacao,
    possivelmente atras de um CSP restritivo ou sem rede.
    """
    template = dashboard._TEMPLATE.read_text(encoding="utf-8")
    for proibido in ("src=\"http", "href=\"http", "@import", "fetch(", "XMLHttpRequest"):
        assert proibido not in template, proibido
