"""
test_config_graph.py
--------------------
Testes da configuracao da publicacao via Microsoft Graph (Fase H).

O ponto sensivel: as variaveis do Graph NAO usam _require, porque a publicacao
e opcional e uma variavel faltando nao pode impedir o pipeline de subir. Quem
verifica e validar_config_graph(), e so quando a flag esta ligada. Estes testes
prendem esse comportamento.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl import config


# ---------------------------------------------------------------------------
# _flag — leitura de booleanos do .env
# ---------------------------------------------------------------------------

def test_flag_aceita_grafias_verdadeiras():
    for valor in ("1", "true", "TRUE", "True", "yes", "sim", "  true  "):
        with patch.dict("os.environ", {"X_FLAG": valor}):
            assert config._flag("X_FLAG", "false") is True, valor


def test_flag_aceita_grafias_falsas():
    for valor in ("0", "false", "no", "nao", "", "qualquer coisa"):
        with patch.dict("os.environ", {"X_FLAG": valor}):
            assert config._flag("X_FLAG", "true") is False, valor


def test_flag_usa_o_padrao_quando_variavel_ausente():
    with patch.dict("os.environ", {}, clear=True):
        assert config._flag("X_AUSENTE", "true") is True
        assert config._flag("X_AUSENTE", "false") is False


# ---------------------------------------------------------------------------
# validar_config_graph
# ---------------------------------------------------------------------------

def test_configuracao_completa_nao_reporta_nada():
    completos = {nome: "valor" for nome in config._GRAPH_OBRIGATORIAS}
    with patch.multiple(config, **completos):
        assert config.validar_config_graph() == []


def test_reporta_apenas_as_variaveis_que_faltam():
    completos = {nome: "valor" for nome in config._GRAPH_OBRIGATORIAS}
    completos["GRAPH_CLIENT_SECRET"] = ""
    completos["GRAPH_FOLDER"] = ""
    with patch.multiple(config, **completos):
        assert sorted(config.validar_config_graph()) == [
            "GRAPH_CLIENT_SECRET",
            "GRAPH_FOLDER",
        ]


def test_devolve_lista_em_vez_de_levantar():
    """
    Faltar configuracao do Graph nao pode derrubar o import do config: com a
    publicacao desligada, a ausencia e irrelevante.
    """
    vazios = {nome: "" for nome in config._GRAPH_OBRIGATORIAS}
    with patch.multiple(config, **vazios):
        faltando = config.validar_config_graph()  # nao levanta
    assert len(faltando) == len(config._GRAPH_OBRIGATORIAS)


def test_o_secret_esta_entre_as_obrigatorias():
    """Prende a lista: um segredo ausente e a causa mais provavel de 401 mudo."""
    assert "GRAPH_CLIENT_SECRET" in config._GRAPH_OBRIGATORIAS
