"""
storage/sharepoint.py
---------------------
Publica arquivos numa biblioteca do SharePoint via Microsoft Graph API.

Por que existe
--------------
Até 2026-07-29 a publicação dependia de o cliente do OneDrive sincronizar uma
pasta local. Isso não é automação: o cliente foi desenhado para o computador de
uma pessoa e não oferece confirmação de entrega, código de retorno nem log
acessível ao pipeline. Em 2026-07-28 o arquivo ficou "Sincronização pendente"
por mais de 9 horas — a cópia local correta, a do servidor velha, e ninguém
sabendo. Não havia check-out, o cliente estava rodando, e um arquivo novo criado
na mesma pasta subiu na hora.

Aqui o upload retorna 201 com o eTag do arquivo no servidor: ou chegou, ou
sabemos exatamente por que não chegou.

Autenticação
------------
Fluxo client credentials: o pipeline se identifica como aplicativo, sem usuário,
sem sessão logada, sem MFA. Requer um App Registration no Entra ID com permissão
de Aplicativo sobre a biblioteca (ver README / chamado de permissões).

Usa apenas `requests`, já presente. A biblioteca `msal` traria cache e renovação
de token, que não fazem falta para um processo que pega um token por execução.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import requests

from argentina_etl.logging_setup import logger

_GRAPH = "https://graph.microsoft.com/v1.0"
_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

# A API exige blocos múltiplos de 320 KiB. 5 MiB = 327.680 x 16.
_TAMANHO_BLOCO = 5 * 1024 * 1024

_TIMEOUT = 120


class ErroSharePoint(RuntimeError):
    """Falha ao falar com o Graph. A mensagem carrega o status e o corpo."""


def _erro(resposta: requests.Response, contexto: str) -> ErroSharePoint:
    """Monta um erro com o suficiente para diagnosticar sem reproduzir."""
    try:
        corpo = resposta.json().get("error", {})
        detalhe = f"{corpo.get('code', '?')}: {corpo.get('message', '')}"
    except Exception:
        detalhe = (resposta.text or "")[:300]
    return ErroSharePoint(f"{contexto} — HTTP {resposta.status_code} — {detalhe}")


# ---------------------------------------------------------------------------
# Autenticação
# ---------------------------------------------------------------------------

def obter_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Token de aplicativo (client credentials)."""
    resposta = requests.post(
        _TOKEN_URL.format(tenant=tenant_id),
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=_TIMEOUT,
    )
    if resposta.status_code != 200:
        # Causas comuns: segredo expirado, client_id errado, tenant errado.
        raise _erro(resposta, "Falha ao obter token")

    token = resposta.json().get("access_token")
    if not token:
        raise ErroSharePoint("Token ausente na resposta do Entra ID.")
    logger.info("SharePoint: token obtido.")
    return token


def _cabecalhos(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Descoberta de site e biblioteca
# ---------------------------------------------------------------------------

def descobrir_site_id(token: str, host: str, caminho_site: str) -> str:
    """
    ID do site a partir do host e do caminho.

    Ex.: host='cgbent.sharepoint.com', caminho_site='/sites/ZGC-PBIResearch'
    """
    caminho = caminho_site.strip("/")
    url = f"{_GRAPH}/sites/{host}:/{caminho}"
    resposta = requests.get(url, headers=_cabecalhos(token), timeout=_TIMEOUT)
    if resposta.status_code != 200:
        raise _erro(resposta, f"Falha ao localizar o site {host}/{caminho}")

    site_id = resposta.json()["id"]
    logger.info(f"SharePoint: site localizado ({site_id.split(',')[0]}...).")
    return site_id


def descobrir_drive_id(token: str, site_id: str, nome_biblioteca: str = "Documents") -> str:
    """
    ID da biblioteca de documentos do site.

    O nome varia com o idioma do tenant ('Documents', 'Documentos'). Se não
    encontrar pelo nome, cai na biblioteca padrão do site.
    """
    resposta = requests.get(
        f"{_GRAPH}/sites/{site_id}/drives", headers=_cabecalhos(token), timeout=_TIMEOUT
    )
    if resposta.status_code != 200:
        raise _erro(resposta, "Falha ao listar as bibliotecas do site")

    drives = resposta.json().get("value", [])
    for drive in drives:
        if drive.get("name", "").lower() == nome_biblioteca.lower():
            logger.info(f"SharePoint: biblioteca '{drive['name']}' localizada.")
            return drive["id"]

    if not drives:
        raise ErroSharePoint("O site não tem nenhuma biblioteca de documentos.")

    nomes = ", ".join(d.get("name", "?") for d in drives)
    logger.warning(
        f"SharePoint: biblioteca '{nome_biblioteca}' não encontrada entre [{nomes}] "
        f"— usando '{drives[0].get('name')}'."
    )
    return drives[0]["id"]


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def enviar_arquivo(token: str, drive_id: str, caminho_remoto: str, arquivo: Path) -> dict[str, Any]:
    """
    Envia o arquivo por sessão de upload, em blocos.

    Sessão em vez de PUT direto porque a Microsoft recomenda blocos acima de
    4 MB e porque a sessão permite retomar um envio interrompido.

    Parameters
    ----------
    caminho_remoto : caminho na biblioteca, incluindo o nome do arquivo.
                     Ex.: 'Dataset Data Files/Trade Flow/ARG/Arg.xlsx'

    Returns
    -------
    O item criado, com 'id', 'eTag' e 'lastModifiedDateTime' — a confirmação de
    que chegou ao servidor.
    """
    if not arquivo.exists():
        raise ErroSharePoint(f"Arquivo não encontrado: {arquivo}")

    tamanho = arquivo.stat().st_size
    caminho = caminho_remoto.strip("/")

    sessao = requests.post(
        f"{_GRAPH}/drives/{drive_id}/root:/{caminho}:/createUploadSession",
        headers=_cabecalhos(token),
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
        timeout=_TIMEOUT,
    )
    if sessao.status_code not in (200, 201):
        raise _erro(sessao, f"Falha ao abrir sessão de upload para {caminho}")

    url_upload = sessao.json()["uploadUrl"]
    logger.info(f"SharePoint: enviando {arquivo.name} ({tamanho / 1024 / 1024:.1f} MB)...")

    with arquivo.open("rb") as fh:
        enviado = 0
        while enviado < tamanho:
            bloco = fh.read(_TAMANHO_BLOCO)
            fim = enviado + len(bloco) - 1
            resposta = requests.put(
                url_upload,
                headers={
                    "Content-Length": str(len(bloco)),
                    "Content-Range": f"bytes {enviado}-{fim}/{tamanho}",
                },
                data=bloco,
                timeout=_TIMEOUT,
            )
            # 202 = bloco aceito, faltam outros. 200/201 = último bloco, item criado.
            if resposta.status_code not in (200, 201, 202):
                _cancelar_sessao(url_upload)
                raise _erro(resposta, f"Falha ao enviar bloco {enviado}-{fim}")
            enviado = fim + 1

    item = resposta.json()
    logger.info(
        f"SharePoint: {arquivo.name} publicado — eTag {item.get('eTag', '?')}, "
        f"servidor registrou em {item.get('lastModifiedDateTime', '?')}."
    )
    return item


def _cancelar_sessao(url_upload: str) -> None:
    """Cancela a sessão para não deixar upload pela metade ocupando a biblioteca."""
    try:
        requests.delete(url_upload, timeout=30)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------

def publicar(
    arquivo: Path,
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    host: str,
    caminho_site: str,
    pasta_remota: str,
    nome_biblioteca: str = "Documents",
) -> dict[str, Any]:
    """
    Publica o arquivo na biblioteca, do token ao eTag.

    Levanta ErroSharePoint em qualquer falha — quem chama decide se é fatal.
    """
    token = obter_token(tenant_id, client_id, client_secret)
    site_id = descobrir_site_id(token, host, caminho_site)
    drive_id = descobrir_drive_id(token, site_id, nome_biblioteca)
    caminho_remoto = f"{pasta_remota.strip('/')}/{arquivo.name}"
    return enviar_arquivo(token, drive_id, caminho_remoto, arquivo)
