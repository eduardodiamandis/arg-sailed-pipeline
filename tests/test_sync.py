"""
test_sync.py
------------
Testes para a verificacao de sincronizacao do OneDrive.

Escritos a partir do incidente de 2026-07-28/29: o arquivo consumido pelo
Power BI ficou "Sincronizacao pendente" por mais de 9 horas enquanto todo o
resto da biblioteca sincronizava. A copia local estava correta e a do servidor,
velha — sem nenhum sinal para quem abrisse pela web.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl.storage import onedrive


def _com_status(*valores):
    """Faz status_sincronizacao devolver os valores em sequencia."""
    return patch.object(onedrive, "status_sincronizacao", side_effect=list(valores))


# ---------------------------------------------------------------------------
# verificar_sincronizacao
# ---------------------------------------------------------------------------

def test_confirma_quando_ja_esta_sincronizado():
    with _com_status("Disponível neste dispositivo"):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx")) is True


def test_confirma_quando_sincroniza_durante_a_espera():
    """Pendente na primeira leitura, disponivel na segunda."""
    with _com_status("Sincronização pendente", "Disponível neste dispositivo"), \
         patch("time.sleep"):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx"), espera_segundos=30) is True


def test_avisa_quando_continua_pendente():
    """O caso real: nunca sai de pendente dentro da espera."""
    with patch.object(onedrive, "status_sincronizacao", return_value="Sincronização pendente"), \
         patch("time.sleep"):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx"), espera_segundos=0) is False


@pytest.mark.parametrize("texto", [
    "Sincronização pendente", "Sync pending", "Sincronizando", "Syncing",
    "SINCRONIZAÇÃO PENDENTE",
])
def test_reconhece_os_textos_de_pendencia(texto):
    """O shell e localizado; os dois idiomas precisam ser reconhecidos."""
    with patch.object(onedrive, "status_sincronizacao", return_value=texto), \
         patch("time.sleep"):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx"), espera_segundos=0) is False


@pytest.mark.parametrize("texto", [
    "Disponível neste dispositivo", "Sempre disponível neste dispositivo",
    "Available on this device",
])
def test_reconhece_os_textos_de_sucesso(texto):
    with patch.object(onedrive, "status_sincronizacao", return_value=texto):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx")) is True


def test_status_indisponivel_nao_gera_alarme_falso():
    """
    Sem conseguir ler o status (outro SO, shell indisponivel), devolve True.
    Um aviso errado toda noite treinaria as pessoas a ignorar o e-mail.
    """
    with _com_status(None):
        assert onedrive.verificar_sincronizacao(Path("x.xlsx")) is True


# ---------------------------------------------------------------------------
# status_sincronizacao — nunca pode derrubar o pipeline
# ---------------------------------------------------------------------------

# create=True porque test_pipeline.py instala um win32com falso em sys.modules
# para rodar sem pywin32; sem isso o patch falha dependendo da ordem dos testes.

def test_status_devolve_none_em_qualquer_falha():
    with patch("win32com.client.Dispatch", side_effect=RuntimeError("COM indisponível"), create=True):
        assert onedrive.status_sincronizacao(Path("x.xlsx")) is None


def test_status_devolve_none_quando_a_pasta_nao_resolve():
    class ShellFake:
        def Namespace(self, _):
            return None

    with patch("win32com.client.Dispatch", return_value=ShellFake(), create=True):
        assert onedrive.status_sincronizacao(Path("x.xlsx")) is None


# ---------------------------------------------------------------------------
# Localizacao do cliente OneDrive
# ---------------------------------------------------------------------------

def test_localiza_onedrive_na_instalacao_por_maquina():
    """
    O bug original: so olhava %LOCALAPPDATA%, entao numa instalacao por maquina
    nunca encontrava o executavel e avisava "sync indisponivel" toda execucao.
    """
    def existe_so_program_files(self):
        return "Program Files" in str(self)

    with patch.dict("os.environ", {
        "LOCALAPPDATA": r"C:\Users\x\AppData\Local",
        "PROGRAMFILES": r"C:\Program Files",
    }), patch.object(Path, "exists", existe_so_program_files):
        achado = onedrive._localizar_onedrive()

    assert achado is not None
    assert "Program Files" in str(achado)


def test_localiza_onedrive_devolve_none_quando_nao_ha_instalacao():
    with patch.object(Path, "exists", lambda self: False):
        assert onedrive._localizar_onedrive() is None
