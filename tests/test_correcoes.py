"""
test_correcoes.py
-----------------
Testes da camada de correcoes conhecidas do Sailed.

O comportamento mais importante daqui nao e "corrigir" — e **parar de corrigir
sozinho**. O valor errado faz parte da chave de casamento, entao quando a origem
consertar o dado a regra deixa de casar e nao faz nada, em vez de sobrescrever o
valor correto com o nosso palpite. Se alguem simplificar a chave, esses testes
caem.
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl.pipelines import correcoes


CABECALHO = ",".join(correcoes.COLUNAS_CORRECAO)


def _base(linhas: list[dict]) -> pd.DataFrame:
    padrao = {
        "Date": datetime.datetime(2025, 11, 20),
        "Vessel": "EPIC RADIANCE",
        "Cargo": "SOYBEANMEAL",
        "Destination": "TURKEY",
        "Tons": 49_806_067.0,
        "Port": "ROSARIO",
    }
    return pd.DataFrame([{**padrao, **linha} for linha in linhas])


def _regras(*linhas: str) -> pd.DataFrame:
    return pd.DataFrame(
        [dict(zip(correcoes.COLUNAS_CORRECAO, linha.split("|"))) for linha in linhas]
    )


REGRA_EPIC = (
    "2025-11-20|EPIC RADIANCE|SOYBEANMEAL|TURKEY|49806067.0|corrigir|49806.07|teste|2026-08-03"
)


# ---------------------------------------------------------------------------
# Aplicacao
# ---------------------------------------------------------------------------

def test_corrige_a_linha_que_casa():
    db = _base([{}])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)


def test_nao_toca_nas_demais_linhas():
    db = _base([
        {},
        {"Vessel": "OUTRO NAVIO", "Tons": 30_000.0},
        {"Date": datetime.datetime(2025, 11, 21), "Tons": 42_000.0},
    ])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)
    assert out.loc[1, "Tons"] == pytest.approx(30_000.0)
    assert out.loc[2, "Tons"] == pytest.approx(42_000.0)


def test_acao_remover_apaga_a_linha():
    regra = REGRA_EPIC.replace("|corrigir|49806.07|", "|remover||")
    db = _base([{}, {"Vessel": "OUTRO", "Tons": 30_000.0}])
    out = correcoes.aplicar_correcoes(db, _regras(regra))
    assert len(out) == 1
    assert out.loc[0, "Vessel"] == "OUTRO"


def test_e_idempotente():
    """Aplicar duas vezes nao pode dobrar a correcao."""
    db = _base([{}])
    regras = _regras(REGRA_EPIC)
    uma = correcoes.aplicar_correcoes(db, regras)
    duas = correcoes.aplicar_correcoes(uma, regras)
    assert duas.loc[0, "Tons"] == pytest.approx(49_806.07)


# ---------------------------------------------------------------------------
# O comportamento que protege o dado
# ---------------------------------------------------------------------------

def test_nao_age_quando_a_origem_ja_corrigiu():
    """
    O nucleo da seguranca desta camada. Se o NABSA publicar o valor certo, a
    regra nao pode sobrescrever com o nosso palpite — ela simplesmente nao casa.
    """
    db = _base([{"Tons": 48_500.0}])  # origem corrigiu, com valor diferente do nosso
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(48_500.0)


def test_nao_age_quando_o_navio_e_outro():
    db = _base([{"Vessel": "NAVIO DIFERENTE"}])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806_067.0)


def test_nao_age_quando_a_data_e_outra():
    db = _base([{"Date": datetime.datetime(2025, 11, 21)}])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806_067.0)


def test_tolerancia_absorve_ruido_de_arredondamento():
    """O valor faz Excel -> pandas -> CSV -> float; igualdade binaria falharia."""
    db = _base([{"Tons": 49_806_067.004}])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)


def test_base_sem_coluna_vessel_nao_e_tocada():
    """
    O Arg_Sailed do SQL nao tem Vessel. Sem ele o casamento cairia para
    data+carga+destino e poderia atingir a linha errada — melhor nao agir.
    """
    db = _base([{}]).drop(columns=["Vessel"])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert out.loc[0, "Tons"] == pytest.approx(49_806_067.0)


def test_regra_sem_alvo_nao_derruba_o_pipeline():
    db = _base([{"Vessel": "OUTRO", "Tons": 1_000.0}])
    out = correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))  # nao levanta
    assert len(out) == 1


def test_sem_correcoes_devolve_a_base_intacta():
    db = _base([{}])
    out = correcoes.aplicar_correcoes(db, pd.DataFrame(columns=correcoes.COLUNAS_CORRECAO))
    assert out.loc[0, "Tons"] == pytest.approx(49_806_067.0)


# ---------------------------------------------------------------------------
# Carregamento do arquivo
# ---------------------------------------------------------------------------

def test_arquivo_ausente_devolve_tabela_vazia(tmp_path):
    """Nao ter correcao nenhuma e o estado normal e saudavel do projeto."""
    assert carregar_vazio(tmp_path / "nao_existe.csv").empty


def carregar_vazio(path: Path) -> pd.DataFrame:
    return correcoes.carregar_correcoes(path)


def test_comentarios_sao_ignorados(tmp_path):
    arquivo = tmp_path / "c.csv"
    arquivo.write_text(
        "# um comentario\n" + CABECALHO + "\n" + REGRA_EPIC.replace("|", ",") + "\n",
        encoding="utf-8",
    )
    assert len(correcoes.carregar_correcoes(arquivo)) == 1


def test_acao_invalida_levanta(tmp_path):
    arquivo = tmp_path / "c.csv"
    arquivo.write_text(
        CABECALHO + "\n" + REGRA_EPIC.replace("|corrigir|", "|destruir|").replace("|", ",") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Acao invalida"):
        correcoes.carregar_correcoes(arquivo)


def test_corrigir_sem_valor_levanta(tmp_path):
    arquivo = tmp_path / "c.csv"
    arquivo.write_text(
        CABECALHO + "\n" + REGRA_EPIC.replace("|corrigir|49806.07|", "|corrigir||").replace("|", ",") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="tons_correto"):
        correcoes.carregar_correcoes(arquivo)


def test_coluna_faltando_levanta(tmp_path):
    arquivo = tmp_path / "c.csv"
    arquivo.write_text("data,navio\n2025-11-20,EPIC RADIANCE\n", encoding="utf-8")
    with pytest.raises(ValueError, match="colunas"):
        correcoes.carregar_correcoes(arquivo)


# ---------------------------------------------------------------------------
# O arquivo de verdade, versionado no repositorio
# ---------------------------------------------------------------------------

def test_arquivo_versionado_carrega_e_corrige_o_epic_radiance():
    """
    Prende o arquivo real: se alguem quebrar o CSV, isso pega antes da rodada
    noturna — que e quando ninguem esta olhando.
    """
    from argentina_etl import config

    regras = correcoes.carregar_correcoes(config.PATH_CORRECOES)
    assert not regras.empty

    db = _base([{}])
    out = correcoes.aplicar_correcoes(db, regras)
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)


# ---------------------------------------------------------------------------
# Modos: aplicar vs guarda
# ---------------------------------------------------------------------------
#
# A distincao existe porque uma correcao ja resolvida na origem gritaria toda
# noite para sempre. Num projeto onde os avisos vao para o e-mail, isso treina
# quem le a ignorar avisos — que e o oposto do que eles servem.

REGRA_GUARDA = REGRA_EPIC.replace("|corrigir|49806.07|", "|corrigir|49806.07|guarda|")


def _com_modo(*linhas: str) -> pd.DataFrame:
    colunas = correcoes.COLUNAS_CORRECAO[:7] + ["modo"] + correcoes.COLUNAS_CORRECAO[7:]
    return pd.DataFrame([dict(zip(colunas, l.split("|"))) for l in linhas])


def test_guarda_em_silencio_quando_nao_ha_regressao(caplog):
    """O dado ja esta certo: a sentinela nao tem o que fazer e nao pode alarmar."""
    db = _base([{"Tons": 49_806.07}])
    with caplog.at_level("WARNING"):
        out = correcoes.aplicar_correcoes(db, _com_modo(REGRA_GUARDA))
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)
    assert caplog.text == ""


def test_guarda_avisa_alto_quando_a_regressao_acontece(caplog):
    """Se o valor errado voltar, a sentinela corrige E denuncia."""
    db = _base([{}])  # valor errado de volta
    with caplog.at_level("WARNING"):
        out = correcoes.aplicar_correcoes(db, _com_modo(REGRA_GUARDA))
    assert out.loc[0, "Tons"] == pytest.approx(49_806.07)
    assert "Regressao detectada" in caplog.text


def test_aplicar_avisa_quando_nao_encontra(caplog):
    """Modo aplicar espera casar; nao casar merece revisao da regra."""
    db = _base([{"Tons": 49_806.07}])
    with caplog.at_level("WARNING"):
        correcoes.aplicar_correcoes(db, _regras(REGRA_EPIC))
    assert "sem efeito" in caplog.text


def test_modo_ausente_vale_como_aplicar(tmp_path):
    """Arquivo escrito antes da coluna existir continua valendo."""
    arquivo = tmp_path / "c.csv"
    arquivo.write_text(CABECALHO + "\n" + REGRA_EPIC.replace("|", ",") + "\n", encoding="utf-8")
    regras = correcoes.carregar_correcoes(arquivo)
    assert regras.loc[0, "modo"] == correcoes.MODO_PADRAO


def test_modo_invalido_levanta(tmp_path):
    arquivo = tmp_path / "c.csv"
    cab = ",".join(correcoes.COLUNAS_CORRECAO + ["modo"])
    arquivo.write_text(cab + "\n" + REGRA_EPIC.replace("|", ",") + ",vigiar\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Modo invalido"):
        correcoes.carregar_correcoes(arquivo)


def test_arquivo_versionado_esta_em_modo_guarda():
    """
    A base foi corrigida a mao em 03/08/2026. Se alguem devolver esta regra para
    modo=aplicar sem que o erro tenha voltado, o pipeline volta a avisar todo dia.
    """
    from argentina_etl import config

    regras = correcoes.carregar_correcoes(config.PATH_CORRECOES)
    assert regras.loc[0, "modo"] == "guarda"
