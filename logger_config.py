"""
logger_config.py
----------------
Logger centralizado do projeto. Todos os módulos importam `logger` daqui.
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler, SMTPHandler
from pathlib import Path

LOGGER_NAME = "argentina_logger"
_DEFAULT_LOG_FILE = Path.home() / "Desktop" / "Argentina" / "logs" / "argentina_updater.log"

# --- Configurações de e-mail ---
SMTP_HOST = "smtp.gmail.com"        # ou smtp.office365.com, etc.
SMTP_PORT = 587
EMAIL_FROM = "seuemail@gmail.com"
EMAIL_TO   = ["eduardo.diamandis@zgbr.com.br", "eduardo.diamandis@aluno.faculdadeimpacta.com.br"]
EMAIL_USER = "seuemail@gmail.com"
EMAIL_PASS = "sua_senha_de_app"


def setup_logger(logfile: Path | None = None) -> logging.Logger:
    log = logging.getLogger(LOGGER_NAME)

    if log.handlers:
        return log

    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    log.addHandler(console_handler)

    # Arquivo rotativo
    path = logfile or _DEFAULT_LOG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    log.addHandler(file_handler)  # ← estava faltando

    # E-mail — só dispara em ERROR ou CRITICAL
    smtp_handler = SMTPHandler(
        mailhost=(SMTP_HOST, SMTP_PORT),
        fromaddr=EMAIL_FROM,
        toaddrs=EMAIL_TO,
        subject="❌ FALHA — Arg Sailed Pipeline",
        credentials=(EMAIL_USER, EMAIL_PASS),
        secure=(),  # necessário para TLS
    )
    smtp_handler.setLevel(logging.ERROR)
    smtp_handler.setFormatter(fmt)
    log.addHandler(smtp_handler)

    return log


logger = setup_logger()