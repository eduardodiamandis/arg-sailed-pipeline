"""
logging_setup.py
----------------
Logger centralizado do projeto. Todos os módulos importam `logger` daqui.
"""
from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler, SMTPHandler
from pathlib import Path

LOGGER_NAME = "argentina_logger"

# Raiz do projeto: src/argentina_etl/logging_setup.py -> parents[2]
_ROOT = Path(__file__).resolve().parents[2]

# O log vive junto do projeto, nao num caminho absoluto de outra pasta. Ate
# 2026-07-28 isto era Path.home()/"Desktop"/"Argentina"/"logs", o que amarrava
# o sailed_auto ao repositorio que ele veio substituir — mover ou arquivar
# aquela pasta levava o log junto.
#
# DIR_LOGS permite sobrescrever, mas so vale se o .env ja tiver sido carregado
# quando este modulo for importado; por isso a raiz do projeto e o padrao, e
# nao ha dependencia de config.py (que importaria o logger de volta).
_DEFAULT_LOG_FILE = Path(os.getenv("DIR_LOGS") or (_ROOT / "logs")) / "argentina_updater.log"

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

    # Arquivo rotativo — novo arquivo a cada dia, mantém 30 dias
    path = logfile or _DEFAULT_LOG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        path, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(fmt)
    log.addHandler(file_handler)

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