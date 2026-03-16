"""
email_report.py
---------------
Envia um resumo diário do log por e-mail após o pipeline ser executado.

Suporta dois backends, selecionado automaticamente via .env:

  1. Microsoft Graph API (Office 365 corporativo) — recomendado
     Configurar: EMAIL_BACKEND=graph
     Variáveis:  EMAIL_TENANT_ID, EMAIL_CLIENT_ID, EMAIL_CLIENT_SECRET, EMAIL_FROM, EMAIL_TO

  2. SMTP com STARTTLS (Gmail com App Password, etc.)
     Configurar: EMAIL_BACKEND=smtp  (ou deixar em branco — é o padrão)
     Variáveis:  EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, EMAIL_USER, EMAIL_PASSWORD, EMAIL_FROM, EMAIL_TO

Configuração no .env:
---------------------
# --- Backend Graph (Office 365) ---
EMAIL_BACKEND=graph
EMAIL_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
EMAIL_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
EMAIL_CLIENT_SECRET=sua_secret_aqui
EMAIL_FROM=seu@empresa.com
EMAIL_TO=dest1@empresa.com,dest2@empresa.com

# --- Backend SMTP (Gmail) ---
EMAIL_BACKEND=smtp
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_USER=seu@gmail.com
EMAIL_PASSWORD=abcd efgh ijkl mnop   (App Password de 16 chars)
EMAIL_FROM=seu@gmail.com
EMAIL_TO=dest1@email.com,dest2@email.com

Como obter credenciais do Graph API:
-------------------------------------
1. portal.azure.com → Azure Active Directory → App registrations → New registration
2. Certificates & secrets → New client secret → copie o valor
3. API permissions → Add → Microsoft Graph → Application → Mail.Send → Grant admin consent
4. Copie o Application (client) ID e o Directory (tenant) ID para o .env
"""
from __future__ import annotations

import json
import os
import re
import smtplib
import socket
import urllib.request
import urllib.parse
from base64 import b64encode
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from logger_config import logger

# ---------------------------------------------------------------------------
# Configurações lidas do ambiente
# ---------------------------------------------------------------------------

_BACKEND: str          = os.getenv("EMAIL_BACKEND", "smtp").lower()

# SMTP
_SMTP_HOST: str        = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
_SMTP_PORT: int        = int(os.getenv("EMAIL_SMTP_PORT", "587"))
_EMAIL_USER: str       = os.getenv("EMAIL_USER", "")
_EMAIL_PASSWORD: str   = os.getenv("EMAIL_PASSWORD", "")

# Graph API
_TENANT_ID: str        = os.getenv("EMAIL_TENANT_ID", "")
_CLIENT_ID: str        = os.getenv("EMAIL_CLIENT_ID", "")
_CLIENT_SECRET: str    = os.getenv("EMAIL_CLIENT_SECRET", "")

# Comuns
_EMAIL_FROM: str       = os.getenv("EMAIL_FROM", _EMAIL_USER)
_EMAIL_TO_RAW: str     = os.getenv("EMAIL_TO", "")
_MAX_LOG_LINES         = 200


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_recipients() -> list[str]:
    return [e.strip() for e in _EMAIL_TO_RAW.split(",") if e.strip()]


def _read_last_lines(log_path: Path, n: int) -> str:
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
    errors   = _count_errors(log_snippet)
    warnings = _count_warnings(log_snippet)
    hostname = socket.gethostname()
    now      = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    duration = f"{duration_seconds:.1f}s" if duration_seconds is not None else "—"

    colored_lines = []
    for line in log_snippet.splitlines():
        if "ERROR" in line or "CRITICAL" in line:
            colored_lines.append(f'<span style="color:#c62828;font-weight:bold">{line}</span>')
        elif "WARNING" in line:
            colored_lines.append(f'<span style="color:#e65100">{line}</span>')
        else:
            colored_lines.append(line)
    log_html = "\n".join(colored_lines)

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body{{font-family:Calibri,Arial,sans-serif;font-size:14px;color:#333}}
  .hdr{{background:{status_color};color:#fff;padding:16px 24px;border-radius:6px 6px 0 0}}
  .hdr h1{{margin:0;font-size:20px}}.hdr p{{margin:4px 0 0;opacity:.85;font-size:13px}}
  .body{{border:1px solid #ddd;border-top:none;padding:20px 24px;border-radius:0 0 6px 6px}}
  .metrics{{display:flex;gap:24px;margin-bottom:20px;flex-wrap:wrap}}
  .metric{{background:#f5f5f5;border-radius:6px;padding:10px 18px;min-width:120px}}
  .metric .label{{font-size:11px;color:#777;text-transform:uppercase;letter-spacing:.5px}}
  .metric .value{{font-size:22px;font-weight:bold;color:#333;margin-top:2px}}
  .metric.err .value{{color:#c62828}}.metric.warn .value{{color:#e65100}}
  pre{{background:#1e1e1e;color:#d4d4d4;padding:16px;border-radius:6px;font-size:12px;
       overflow-x:auto;white-space:pre-wrap;word-break:break-all}}
  .footer{{margin-top:16px;font-size:12px;color:#aaa}}
</style></head><body>
<div class="hdr"><h1>Arg Sailed Database — {status_label}</h1>
<p>{now} &nbsp;|&nbsp; {hostname}</p></div>
<div class="body">
<div class="metrics">
  <div class="metric"><div class="label">Duração</div><div class="value">{duration}</div></div>
  <div class="metric {'err' if errors else ''}"><div class="label">Erros</div><div class="value">{errors}</div></div>
  <div class="metric {'warn' if warnings else ''}"><div class="label">Avisos</div><div class="value">{warnings}</div></div>
</div>
<p><strong>Últimas {_MAX_LOG_LINES} linhas do log:</strong></p>
<pre>{log_html}</pre>
<div class="footer">Enviado automaticamente pelo pipeline Argentina Updater.</div>
</div></body></html>"""


# ---------------------------------------------------------------------------
# Backend SMTP
# ---------------------------------------------------------------------------

def _send_smtp(subject: str, html_body: str, recipients: list[str]) -> None:
    if not _EMAIL_USER or not _EMAIL_PASSWORD:
        raise ValueError("EMAIL_USER / EMAIL_PASSWORD não configurados.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = _EMAIL_FROM
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.login(_EMAIL_USER, _EMAIL_PASSWORD)
        server.sendmail(_EMAIL_FROM, recipients, msg.as_string())


# ---------------------------------------------------------------------------
# Backend Microsoft Graph API
# ---------------------------------------------------------------------------

def _graph_get_token() -> str:
    """Obtém access token via client credentials (app-only)."""
    if not all([_TENANT_ID, _CLIENT_ID, _CLIENT_SECRET]):
        raise ValueError(
            "EMAIL_TENANT_ID, EMAIL_CLIENT_ID e EMAIL_CLIENT_SECRET "
            "são obrigatórios para EMAIL_BACKEND=graph."
        )
    url = f"https://login.microsoftonline.com/{_TENANT_ID}/oauth2/v2.0/token"
    data = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "client_id":     _CLIENT_ID,
        "client_secret": _CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
    }).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["access_token"]


def _send_graph(subject: str, html_body: str, recipients: list[str]) -> None:
    """Envia e-mail via Microsoft Graph API (não usa SMTP — contorna auth básica)."""
    token = _graph_get_token()

    to_list = [{"emailAddress": {"address": r}} for r in recipients]
    payload = json.dumps({
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "from": {"emailAddress": {"address": _EMAIL_FROM}},
            "toRecipients": to_list,
        },
        "saveToSentItems": "false",
    }).encode("utf-8")

    url = f"https://graph.microsoft.com/v1.0/users/{_EMAIL_FROM}/sendMail"
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        # 202 Accepted = sucesso
        if resp.status not in (200, 202):
            raise RuntimeError(f"Graph API retornou status {resp.status}")


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def send_log_report(
    log_path: Path,
    success: bool = True,
    duration_seconds: float | None = None,
) -> None:
    """
    Envia o relatório do log por e-mail usando o backend configurado.

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

    log_snippet  = _read_last_lines(log_path, _MAX_LOG_LINES)
    status_label = "SUCESSO" if success else "FALHA"
    today        = datetime.now().strftime("%d/%m/%Y")
    subject      = f"[Argentina Updater] {status_label} — {today}"
    html_body    = _build_html(log_snippet, success, duration_seconds)

    try:
        logger.info(f"Enviando e-mail ({_BACKEND}) para: {recipients}")

        if _BACKEND == "graph":
            _send_graph(subject, html_body, recipients)
        else:
            _send_smtp(subject, html_body, recipients)

        logger.info("E-mail enviado com sucesso.")

    except Exception as exc:
        logger.error(f"Falha ao enviar e-mail de log: {exc}")
        if _BACKEND == "smtp":
            logger.error(
                "Dica: se usar Office 365 corporativo, troque para EMAIL_BACKEND=graph. "
                "Se usar Gmail, gere um App Password em myaccount.google.com → Segurança → Senhas de app."
            )