"""
logger_config.py
----------------
Logger centralizado do projeto. Todos os módulos importam `logger` daqui.
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOGGER_NAME = "argentina_logger"
_DEFAULT_LOG_FILE = Path.home() / "Desktop" / "Argentina" / "logs" / "argentina_updater.log"


def setup_logger(logfile: Path | None = None) -> logging.Logger:
    """
    Configura e retorna o logger do projeto.
    Idempotente: se já foi configurado, retorna o logger existente.
    """
    log = logging.getLogger(LOGGER_NAME)

    if log.handlers:
        return log

    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Handler de console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    log.addHandler(console_handler)

    # Handler de arquivo rotativo (5 MB, 3 backups)
    path = logfile or _DEFAULT_LOG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    log.addHandler(file_handler)

    return log


# Logger pronto para importação direta pelos outros módulos
logger = setup_logger()