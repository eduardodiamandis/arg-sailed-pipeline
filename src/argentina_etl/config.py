"""
config.py
---------
Carrega todas as configurações a partir do arquivo .env na raiz do projeto.
Nenhum outro módulo deve ter paths ou URLs hardcoded.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Raiz do repositório: src/argentina_etl/config.py -> parents[2]
#   parents[0] = src/argentina_etl
#   parents[1] = src
#   parents[2] = raiz, onde vive o .env
_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variável '{key}' não encontrada. "
            "Copie .env.example para .env e preencha os valores."
        )
    return value


def _flag(key: str, padrao: str) -> bool:
    """Le uma variavel booleana. Aceita as grafias em portugues e ingles."""
    return os.getenv(key, padrao).strip().lower() in ("1", "true", "yes", "sim")


# --- URLs ---
URL_SAILED: str = _require("URL_SAILED")
URL_LINEUP: str = _require("URL_LINEUP")

# --- Paths locais ---
DIR_SAILED_BACKUP: Path = Path(_require("DIR_SAILED_BACKUP"))
DIR_LINEUP_BACKUP: Path = Path(_require("DIR_LINEUP_BACKUP"))
PATH_DATABASE: Path = Path(_require("PATH_DATABASE"))
PATH_DATABASE_OUTPUT: Path = Path(_require("PATH_DATABASE_OUTPUT"))

# --- OneDrive ---
DIR_ONEDRIVE: Path = Path(_require("DIR_ONEDRIVE"))
FILENAME_ONEDRIVE: str = _require("FILENAME_ONEDRIVE")
PATH_ONEDRIVE: Path = DIR_ONEDRIVE / FILENAME_ONEDRIVE

# --- SQL Server ---
SQL_SERVER: str = _require("SQL_SERVER")
SQL_DATABASE: str = _require("SQL_DATABASE")
SQL_TABLE: str = _require("SQL_TABLE")
SQL_TABLE_LINEUP: str = _require("SQL_TABLE_LINEUP")

# --- Lineup ---
# Arg_Lineup e append-only entre dias. Dentro do MESMO dia, force=True apaga o
# snapshot anterior antes de inserir, para que uma reexecucao substitua os dados
# em vez de manter os da primeira rodada. Com False, reexecucoes sao ignoradas.
# Nao afeta o historico de outros dias em nenhum dos casos. Ver ESTRUTURA.md 9.2.
LINEUP_FORCE_SNAPSHOT: bool = _flag("LINEUP_FORCE_SNAPSHOT", "true")

# --- Timeouts ---
TIMEOUT_SAILED: int = int(os.getenv("TIMEOUT_SAILED", "40"))
TIMEOUT_LINEUP: int = int(os.getenv("TIMEOUT_LINEUP", "18"))

# --- Correcoes conhecidas do Sailed ---
# Arquivo versionado no repositorio (nao e dado gerado, e regra revisavel).
# Sem _require: nao ter correcao nenhuma e o estado normal, e o modulo devolve
# tabela vazia quando o arquivo nao existe.
PATH_CORRECOES: Path = Path(
    os.getenv("PATH_CORRECOES", "").strip()
    or _ROOT / "config" / "correcoes_sailed.csv"
)

# --- Dashboard HTML ---
# Arquivo unico e autocontido gerado por reporting/dashboard.py, feito para ser
# embutido em outra aplicacao via iframe.
#
# Nao usa _require de proposito, pela mesma razao das variaveis do Graph: o
# dashboard e opcional e nao faz parte do pipeline noturno. Uma variavel ausente
# nao pode impedir o pipeline de subir, entao cai no padrao sob data/.
PATH_DASHBOARD_OUTPUT: Path = Path(
    os.getenv("PATH_DASHBOARD_OUTPUT", "").strip()
    or _ROOT / "data" / "dashboard" / "arg_dashboard.html"
)

# --- SharePoint via Microsoft Graph (Fase H) ---
# Publicacao com confirmacao de entrega, em substituicao a pasta sincronizada
# pelo cliente do OneDrive. Ver ESTRUTURA.md, Fase H.
#
# Estas NAO usam _require de proposito: a publicacao e opcional e esta desligada
# ate a permissao Sites.Selected ser concedida. Uma variavel faltando aqui nao
# pode impedir o pipeline inteiro de subir — o que ela impede e a publicacao, e
# quem verifica isso e validar_config_graph(), chamada so quando a flag esta on.
GRAPH_UPLOAD_ENABLED: bool = _flag("GRAPH_UPLOAD_ENABLED", "false")
GRAPH_TENANT_ID: str = os.getenv("GRAPH_TENANT_ID", "").strip()
GRAPH_CLIENT_ID: str = os.getenv("GRAPH_CLIENT_ID", "").strip()
GRAPH_CLIENT_SECRET: str = os.getenv("GRAPH_CLIENT_SECRET", "").strip()
GRAPH_HOST: str = os.getenv("GRAPH_HOST", "").strip()
GRAPH_SITE_PATH: str = os.getenv("GRAPH_SITE_PATH", "").strip()
GRAPH_FOLDER: str = os.getenv("GRAPH_FOLDER", "").strip()

_GRAPH_OBRIGATORIAS = (
    "GRAPH_TENANT_ID",
    "GRAPH_CLIENT_ID",
    "GRAPH_CLIENT_SECRET",
    "GRAPH_HOST",
    "GRAPH_SITE_PATH",
    "GRAPH_FOLDER",
)


def validar_config_graph() -> list[str]:
    """
    Nomes das variaveis do Graph que faltam. Lista vazia = configuracao completa.

    Devolve em vez de levantar para que quem chama decida: com a publicacao
    ligada, faltar variavel e erro; com ela desligada, nao interessa.
    """
    return [nome for nome in _GRAPH_OBRIGATORIAS if not globals().get(nome)]
