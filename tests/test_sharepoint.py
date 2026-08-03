"""
test_sharepoint.py
------------------
Testes da publicacao via Microsoft Graph, com a API inteira mockada.

Nao exigem credenciais, rede nem permissao concedida — a ideia e que a logica
esteja verificada antes de o chamado de permissao ser atendido.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from argentina_etl.storage import sharepoint
from argentina_etl.storage.sharepoint import ErroSharePoint


def _resposta(status: int, json_data: dict | None = None, texto: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_data if json_data is not None else {}
    r.text = texto
    return r


# ---------------------------------------------------------------------------
# obter_token
# ---------------------------------------------------------------------------

def test_token_devolve_access_token():
    with patch("requests.post", return_value=_resposta(200, {"access_token": "abc123"})):
        assert sharepoint.obter_token("t", "c", "s") == "abc123"


def test_token_envia_client_credentials_e_escopo_certo():
    with patch("requests.post", return_value=_resposta(200, {"access_token": "x"})) as post:
        sharepoint.obter_token("meu-tenant", "meu-client", "meu-segredo")

    url, kwargs = post.call_args[0][0], post.call_args[1]
    assert "meu-tenant" in url
    dados = kwargs["data"]
    assert dados["grant_type"] == "client_credentials"
    assert dados["scope"] == "https://graph.microsoft.com/.default"
    assert dados["client_secret"] == "meu-segredo"


def test_token_expirado_levanta_com_a_causa():
    erro = {"error": {"code": "invalid_client", "message": "secret expired"}}
    with patch("requests.post", return_value=_resposta(401, erro)):
        with pytest.raises(ErroSharePoint, match="invalid_client"):
            sharepoint.obter_token("t", "c", "s")


def test_token_ausente_na_resposta_levanta():
    with patch("requests.post", return_value=_resposta(200, {})):
        with pytest.raises(ErroSharePoint, match="Token ausente"):
            sharepoint.obter_token("t", "c", "s")


# ---------------------------------------------------------------------------
# Descoberta de site e biblioteca
# ---------------------------------------------------------------------------

def test_site_id_monta_a_url_no_formato_do_graph():
    with patch("requests.get", return_value=_resposta(200, {"id": "site-1"})) as get:
        assert sharepoint.descobrir_site_id("tk", "cgbent.sharepoint.com", "/sites/ZGC") == "site-1"

    assert get.call_args[0][0].endswith("/sites/cgbent.sharepoint.com:/sites/ZGC")


def test_site_inexistente_levanta():
    with patch("requests.get", return_value=_resposta(404, {"error": {"code": "itemNotFound"}})):
        with pytest.raises(ErroSharePoint, match="itemNotFound"):
            sharepoint.descobrir_site_id("tk", "h", "/sites/x")


def test_drive_id_encontra_pelo_nome():
    drives = {"value": [{"id": "d1", "name": "Outra"}, {"id": "d2", "name": "Documents"}]}
    with patch("requests.get", return_value=_resposta(200, drives)):
        assert sharepoint.descobrir_drive_id("tk", "s") == "d2"


def test_drive_id_e_insensivel_a_caixa():
    """O nome varia com o idioma do tenant."""
    drives = {"value": [{"id": "d1", "name": "documentos"}]}
    with patch("requests.get", return_value=_resposta(200, drives)):
        assert sharepoint.descobrir_drive_id("tk", "s", "Documentos") == "d1"


def test_drive_id_cai_no_primeiro_quando_nome_nao_bate():
    drives = {"value": [{"id": "d1", "name": "Documentos"}]}
    with patch("requests.get", return_value=_resposta(200, drives)):
        assert sharepoint.descobrir_drive_id("tk", "s", "Documents") == "d1"


def test_site_sem_biblioteca_levanta():
    with patch("requests.get", return_value=_resposta(200, {"value": []})):
        with pytest.raises(ErroSharePoint, match="nenhuma biblioteca"):
            sharepoint.descobrir_drive_id("tk", "s")


# ---------------------------------------------------------------------------
# enviar_arquivo
# ---------------------------------------------------------------------------

def test_upload_em_bloco_unico_devolve_o_item(tmp_path):
    arq = tmp_path / "teste.xlsx"
    arq.write_bytes(b"x" * 1024)
    item = {"id": "i1", "eTag": "\"abc\"", "lastModifiedDateTime": "2026-07-29T12:00:00Z"}

    with patch("requests.post", return_value=_resposta(200, {"uploadUrl": "https://u"})), \
         patch("requests.put", return_value=_resposta(201, item)) as put:
        assert sharepoint.enviar_arquivo("tk", "d1", "pasta/teste.xlsx", arq) == item

    cab = put.call_args[1]["headers"]
    assert cab["Content-Range"] == "bytes 0-1023/1024"


def test_upload_divide_em_blocos_contiguos(tmp_path):
    """Arquivo maior que um bloco: os ranges precisam cobrir tudo sem buraco."""
    arq = tmp_path / "grande.xlsx"
    arq.write_bytes(b"y" * (sharepoint._TAMANHO_BLOCO + 500))
    ranges = []

    def put_falso(url, headers=None, data=None, timeout=None):
        ranges.append(headers["Content-Range"])
        return _resposta(202 if len(ranges) == 1 else 201, {"id": "i1"})

    with patch("requests.post", return_value=_resposta(200, {"uploadUrl": "https://u"})), \
         patch("requests.put", side_effect=put_falso):
        sharepoint.enviar_arquivo("tk", "d1", "p/grande.xlsx", arq)

    total = sharepoint._TAMANHO_BLOCO + 500
    assert ranges == [
        f"bytes 0-{sharepoint._TAMANHO_BLOCO - 1}/{total}",
        f"bytes {sharepoint._TAMANHO_BLOCO}-{total - 1}/{total}",
    ]


def test_upload_pede_substituicao_em_vez_de_renomear(tmp_path):
    """Sem conflictBehavior=replace o Graph criaria 'arquivo 1.xlsx'."""
    arq = tmp_path / "a.xlsx"
    arq.write_bytes(b"z")

    with patch("requests.post", return_value=_resposta(200, {"uploadUrl": "https://u"})) as post, \
         patch("requests.put", return_value=_resposta(201, {"id": "i"})):
        sharepoint.enviar_arquivo("tk", "d1", "p/a.xlsx", arq)

    corpo = post.call_args[1]["json"]
    assert corpo["item"]["@microsoft.graph.conflictBehavior"] == "replace"


def test_upload_cancela_a_sessao_quando_um_bloco_falha(tmp_path):
    arq = tmp_path / "a.xlsx"
    arq.write_bytes(b"z" * 100)

    with patch("requests.post", return_value=_resposta(200, {"uploadUrl": "https://u"})), \
         patch("requests.put", return_value=_resposta(500, {"error": {"code": "serverError"}})), \
         patch("requests.delete") as delete:
        with pytest.raises(ErroSharePoint, match="serverError"):
            sharepoint.enviar_arquivo("tk", "d1", "p/a.xlsx", arq)

    delete.assert_called_once()


def test_upload_de_arquivo_inexistente_levanta_antes_de_chamar_a_api(tmp_path):
    with patch("requests.post") as post:
        with pytest.raises(ErroSharePoint, match="não encontrado"):
            sharepoint.enviar_arquivo("tk", "d1", "p/x.xlsx", tmp_path / "nao_existe.xlsx")
    post.assert_not_called()


# ---------------------------------------------------------------------------
# publicar
# ---------------------------------------------------------------------------

def test_publicar_encadeia_token_site_drive_e_upload(tmp_path):
    arq = tmp_path / "Arg.xlsx"
    arq.write_bytes(b"dados")
    item = {"id": "i1", "eTag": "\"e\""}

    with patch.object(sharepoint, "obter_token", return_value="tk") as tok, \
         patch.object(sharepoint, "descobrir_site_id", return_value="s1") as site, \
         patch.object(sharepoint, "descobrir_drive_id", return_value="d1") as drive, \
         patch.object(sharepoint, "enviar_arquivo", return_value=item) as envio:
        r = sharepoint.publicar(
            arq, tenant_id="t", client_id="c", client_secret="s",
            host="h.sharepoint.com", caminho_site="/sites/X",
            pasta_remota="Dataset Data Files/Trade Flow/ARG",
        )

    assert r == item
    tok.assert_called_once_with("t", "c", "s")
    site.assert_called_once_with("tk", "h.sharepoint.com", "/sites/X")
    drive.assert_called_once_with("tk", "s1", "Documents")
    # O nome do arquivo entra no caminho remoto
    assert envio.call_args[0][2] == "Dataset Data Files/Trade Flow/ARG/Arg.xlsx"
