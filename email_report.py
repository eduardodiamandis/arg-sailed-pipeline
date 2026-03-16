"""
email_report.py
---------------
Envia um resumo diário do log por e-mail após o pipeline ser executado.

Configuração via .env:
    EMAIL_SMTP_HOST     = smtp.office365.com          (ou smtp.gmail.com)
    EMAIL_SMTP_PORT     = 587
    EMAIL_USER          = seu@email.com
    EMAIL_PASSWORD      = sua_senha_ou_app_password
    EMAIL_FROM          = seu@email.com
    EMAIL_TO            = destino@email.com           (separe múltiplos por vírgula)

Uso:
    from email_report import send_log_report
    send_log_report(log_path, success=True)
"""
from __future__ import annotations

import os
import re
import smtplib
import socket
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from logger_config import logger

# ---------------------------------------------------------------------------
# Configurações (lidas do ambiente — já carregado por config.py)
# ---------------------------------------------------------------------------

_SMTP_HOST: str = os.getenv("EMAIL_SMTP_HOST", "smtp.office365.com")
_SMTP_PORT: int = int(os.getenv("EMAIL_SMTP_PORT", "587"))
_EMAIL_USER: str = os.getenv("EMAIL_USER", "")
_EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "")
_EMAIL_FROM: str = os.getenv("EMAIL_FROM", _EMAIL_USER)
_EMAIL_TO_RAW: str = os.getenv("EMAIL_TO", "")

_MAX_LOG_LINES = 200   # Máximo de linhas do log para incluir no e-mail


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_recipients() -> list[str]:
    return [e.strip() for e in _EMAIL_TO_RAW.split(",") if e.strip()]


def _read_last_lines(log_path: Path, n: int) -> str:
    """Retorna as últimas n linhas do arquivo de log."""
    if not log_path.exists():
        return "(arquivo de log não encontrado)"
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])


def _count_errors(log_snippet: str) -> int:
    return len(re.findall(r"\b(ERROR|CRITICAL)\b", log_snippet))


def _count_warnings(log_snippet: str) -> int:
    return len(re.findall(r"\bWARNING\b", log_snippet))


def _build_html(log_snippet: str, success: bool, duration_seconds: float | None) -> str:
    status_color = "#2e7d32" if success else "#c62828"
    status_label = "✅ SUCESSO" if success else "❌ FALHA"
    errors = _count_errors(log_snippet)
    warnings = _count_warnings(log_snippet)
    hostname = socket.gethostname()
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    duration_str = f"{duration_seconds:.1f}s" if duration_seconds is not None else "—"

    # Coloriza linhas de erro/warning no log
    colored_lines = []
    for line in log_snippet.splitlines():
        if "ERROR" in line or "CRITICAL" in line:
            colored_lines.append(f'<span style="color:#c62828;font-weight:bold">{line}</span>')
        elif "WARNING" in line:
            colored_lines.append(f'<span style="color:#e65100">{line}</span>')
        else:
            colored_lines.append(line)
    log_html = "\n".join(colored_lines)

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Calibri, Arial, sans-serif; font-size: 14px; color: #333; }}
    .header {{ background: {status_color}; color: #fff; padding: 16px 24px; border-radius: 6px 6px 0 0; }}
    .header h1 {{ margin: 0; font-size: 20px; }}
    .header p  {{ margin: 4px 0 0; opacity: .85; font-size: 13px; }}
    .body {{ border: 1px solid #ddd; border-top: none; padding: 20px 24px; border-radius: 0 0 6px 6px; }}
    .metrics {{ display: flex; gap: 24px; margin-bottom: 20px; flex-wrap: wrap; }}
    .metric {{ background: #f5f5f5; border-radius: 6px; padding: 10px 18px; min-width: 120px; }}
    .metric .label {{ font-size: 11px; color: #777; text-transform: uppercase; letter-spacing: .5px; }}
    .metric .value {{ font-size: 22px; font-weight: bold; color: #333; margin-top: 2px; }}
    .metric.err .value {{ color: #c62828; }}
    .metric.warn .value {{ color: #e65100; }}
    pre {{ background: #1e1e1e; color: #d4d4d4; padding: 16px; border-radius: 6px;
           font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; }}
    .footer {{ margin-top: 16px; font-size: 12px; color: #aaa; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Arg Sailed Database — {status_label}</h1>
    <p>{now} &nbsp;|&nbsp; {hostname}</p>
  </div>
  <div class="body">
    <div class="metrics">
      <div class="metric">
        <div class="label">Duração</div>
        <div class="value">{duration_str}</div>
      </div>
      <div class="metric {'err' if errors else ''}">
        <div class="label">Erros</div>
        <div class="value">{errors}</div>
      </div>
      <div class="metric {'warn' if warnings else ''}">
        <div class="label">Avisos</div>
        <div class="value">{warnings}</div>
      </div>
    </div>
    <p><strong>Últimas {_MAX_LOG_LINES} linhas do log:</strong></p>
    <pre>{log_html}</pre>
    <div class="footer">Enviado automaticamente pelo pipeline Argentina Updater.</div>
  </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def send_log_report(
    log_path: Path,
    success: bool = True,
    duration_seconds: float | None = None,
) -> None:
    """
    Envia o relatório do log por e-mail.

    Parameters
    ----------
    log_path          : Caminho para o arquivo .log
    success           : True se o pipeline terminou sem erros críticos
    duration_seconds  : Duração total do pipeline (opcional)
    """
    recipients = _get_recipients()
    if not recipients:
        logger.warning("EMAIL_TO não configurado — e-mail de log não enviado.")
        return
    if not _EMAIL_USER or not _EMAIL_PASSWORD:
        logger.warning("EMAIL_USER / EMAIL_PASSWORD não configurados — e-mail ignorado.")
        return

    log_snippet = _read_last_lines(log_path, _MAX_LOG_LINES)
    status_label = "SUCESSO" if success else "FALHA"
    today = datetime.now().strftime("%d/%m/%Y")

    subject = f"[Argentina Updater] {status_label} — {today}"
    html_body = _build_html(log_snippet, success, duration_seconds)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = _EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        logger.info(f"Enviando e-mail de log para: {recipients}")
        with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(_EMAIL_USER, _EMAIL_PASSWORD)
            server.sendmail(_EMAIL_FROM, recipients, msg.as_string())
        logger.info("E-mail de log enviado com sucesso.")
    except Exception as exc:
        # Não deixa o e-mail travar o pipeline
        logger.error(f"Falha ao enviar e-mail de log: {exc}")