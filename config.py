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

# Raiz do repositório (dois níveis acima de src/)
_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variável '{key}' não encontrada. "
            "Copie .env.example para .env e preencha os valores."
        )
    return value


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

# --- Timeouts ---
TIMEOUT_SAILED: int = int(os.getenv("TIMEOUT_SAILED", "40"))
TIMEOUT_LINEUP: int = int(os.getenv("TIMEOUT_LINEUP", "18"))
