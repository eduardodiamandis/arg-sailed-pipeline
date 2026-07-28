"""
email_report.py
---------------
Envia relatório do pipeline por e-mail.

Sucesso  → resumo dos dados atualizados + link para o dashboard
Falha    → erros detalhados (o quê, onde, por quê) + log resumido

Backends suportados (configurado via .env):
  EMAIL_BACKEND=smtp   → Gmail / SMTP com STARTTLS
  EMAIL_BACKEND=graph  → Microsoft Graph API (Office 365 corporativo)
"""
from __future__ import annotations

import json
import os
import re
import smtplib
import socket
import urllib.request
import urllib.parse
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from logger_config import logger

# ---------------------------------------------------------------------------
# Configurações do .env
# ---------------------------------------------------------------------------

_BACKEND: str        = os.getenv("EMAIL_BACKEND", "smtp").lower()
_SMTP_HOST: str      = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
_SMTP_PORT: int      = int(os.getenv("EMAIL_SMTP_PORT", "587"))
_EMAIL_USER: str     = os.getenv("EMAIL_USER", "")
_EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "")
_TENANT_ID: str      = os.getenv("EMAIL_TENANT_ID", "")
_CLIENT_ID: str      = os.getenv("EMAIL_CLIENT_ID", "")
_CLIENT_SECRET: str  = os.getenv("EMAIL_CLIENT_SECRET", "")
_EMAIL_FROM: str     = os.getenv("EMAIL_FROM", _EMAIL_USER)
_EMAIL_TO_RAW: str   = os.getenv("EMAIL_TO", "")

DASHBOARD_URL = "http://192.168.16.180:5000"


# ---------------------------------------------------------------------------
# Helpers gerais
# ---------------------------------------------------------------------------

def _get_recipients() -> list[str]:
    return [e.strip() for e in _EMAIL_TO_RAW.split(",") if e.strip()]


def _read_current_run(log_path: Path) -> str:
    """Retorna apenas as linhas do run atual (desde o último INÍCIO DO PIPELINE)."""
    if not log_path.exists():
        return "(arquivo de log não encontrado)"
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    # Encontra o índice do último início de pipeline
    start = 0
    for i, line in enumerate(lines):
        if "INÍCIO DO PIPELINE" in line:
            start = i
    return "\n".join(lines[start:])


# ---------------------------------------------------------------------------
# Análise do log para erros
# ---------------------------------------------------------------------------

# Padrões para identificar a causa provável de um erro
_ERROR_CAUSES: list[tuple[str, str]] = [
    (r"UpdatedAt|coluna.*inválid|column.*invalid", "Coluna não existe na tabela SQL Server. Rode o pipeline novamente — ela será criada automaticamente."),
    (r"ODBC|SQL Server Driver",                    "Falha de conexão com o SQL Server. Verifique se o serviço está rodando e o servidor está acessível."),
    (r"timeout|Timeout",                           "O download demorou mais que o limite configurado. O site de origem pode estar lento."),
    (r"FileNotFoundError|não encontrado|not found","Arquivo não encontrado. Verifique se o download foi concluído e o caminho está correto."),
    (r"PermissionError|acesso negado|Access.*denied","Sem permissão para acessar o arquivo. Feche o Excel se o arquivo estiver aberto."),
    (r"OneDrive|onedrive",                         "Falha ao salvar no OneDrive. Verifique se o OneDrive está sincronizado e o caminho existe."),
    (r"Graph API|token|client_secret",             "Falha na autenticação do e-mail corporativo. Verifique as credenciais no .env."),
    (r"pivot|Pivot",                               "Erro ao criar Pivot Tables no Excel. Verifique se o Excel está fechado e o arquivo é válido."),
]

_STAGE_PATTERN = re.compile(r"--- (ETAPA \d+[^-]*) ---")


def _detect_cause(message: str) -> str:
    for pattern, cause in _ERROR_CAUSES:
        if re.search(pattern, message, re.IGNORECASE):
            return cause
    return "Verifique o log completo para mais detalhes."


def _extract_errors(log_text: str) -> list[dict[str, str]]:
    """Extrai erros do log com contexto de etapa e causa provável."""
    lines      = log_text.splitlines()
    errors     = []
    last_stage = "Inicialização"

    for i, line in enumerate(lines):
        stage_match = _STAGE_PATTERN.search(line)
        if stage_match:
            last_stage = stage_match.group(1).strip()

        if " - ERROR - " in line or " - CRITICAL - " in line:
            # Pega a mensagem depois do nível
            msg = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - \w+ - ", "", line)
            errors.append({
                "stage":   last_stage,
                "message": msg,
                "cause":   _detect_cause(msg),
            })

    return errors


_WARN_NOISE = re.compile(r"^={10,}|PIPELINE FINALIZADO|Duração total")

def _extract_warnings(log_text: str) -> list[str]:
    warnings = []
    for line in log_text.splitlines():
        if " - WARNING - " in line:
            msg = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - \w+ - ", "", line)
            if not _WARN_NOISE.search(msg):
                warnings.append(msg)
    return warnings


# ---------------------------------------------------------------------------
# Construção do HTML
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    return (text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;"))


_CSS = """
body{font-family:Calibri,Arial,sans-serif;font-size:14px;color:#333;margin:0;padding:0}
.hdr{padding:18px 24px;border-radius:6px 6px 0 0}
.hdr h1{margin:0;font-size:21px;color:#fff}
.hdr p{margin:5px 0 0;font-size:13px;color:rgba(255,255,255,.85)}
.body{border:1px solid #ddd;border-top:none;padding:22px 24px;border-radius:0 0 6px 6px}
.metrics{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:22px}
.metric{background:#f5f5f5;border-radius:8px;padding:12px 20px;min-width:130px}
.metric .lbl{font-size:11px;text-transform:uppercase;color:#888;letter-spacing:.5px}
.metric .val{font-size:22px;font-weight:bold;color:#333;margin-top:3px}
.metric.ok .val{color:#2e7d32}.metric.err .val{color:#c62828}.metric.warn .val{color:#e65100}
.section-title{font-size:15px;font-weight:bold;margin:20px 0 10px;padding-bottom:6px;
               border-bottom:2px solid #eee}
.error-card{background:#fff8f8;border-left:4px solid #c62828;border-radius:0 6px 6px 0;
            padding:12px 16px;margin-bottom:10px}
.error-card .where{font-size:11px;text-transform:uppercase;color:#888;margin-bottom:4px}
.error-card .what{font-size:13px;font-weight:bold;color:#c62828;margin-bottom:6px}
.error-card .why{font-size:12px;color:#555;background:#fff0f0;padding:6px 10px;border-radius:4px}
.warn-list{font-size:12px;color:#e65100;background:#fff8f0;border-left:3px solid #e65100;
           padding:10px 14px;border-radius:0 4px 4px 0;margin-bottom:16px}
.warn-list li{margin-bottom:3px}
.data-grid{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px}
.data-card{background:#f0f4ff;border-radius:8px;padding:12px 18px;min-width:140px;
           border:1px solid #d0daf5}
.data-card .lbl{font-size:11px;text-transform:uppercase;color:#556;letter-spacing:.4px}
.data-card .val{font-size:18px;font-weight:bold;color:#1a3a5c;margin-top:3px}
.btn-link{display:inline-block;background:#1a3a5c;color:#fff!important;text-decoration:none;
          padding:11px 24px;border-radius:6px;font-weight:bold;font-size:14px;margin-top:6px}
.btn-link:hover{background:#25527f}
pre{background:#1e1e1e;color:#d4d4d4;padding:14px;border-radius:6px;font-size:11px;
    overflow-x:auto;white-space:pre-wrap;word-break:break-all;max-height:300px;overflow-y:auto}
.footer{margin-top:18px;font-size:11px;color:#bbb;text-align:center}
"""


def _build_success_html(
    duration: str,
    db_stats: dict[str, Any],
    warnings: list[str],
    hostname: str,
    now: str,
) -> str:
    total     = db_stats.get("total_rows", "—")
    last_date = db_stats.get("last_date", "—")
    added     = db_stats.get("rows_added", "—")
    periods   = db_stats.get("periods", "—")

    warn_html = ""
    if warnings:
        items = "".join(f"<li>{_esc(w)}</li>" for w in warnings)
        warn_html = f'<div class="section-title">⚠️ Avisos ({len(warnings)})</div><ul class="warn-list">{items}</ul>'

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{_CSS}</style></head><body>
<div class="hdr" style="background:#1a3a5c">
  <h1>Arg Sailed Database — ✅ SUCESSO</h1>
  <p>{now} &nbsp;|&nbsp; {hostname}</p>
</div>
<div class="body">

<div class="metrics">
  <div class="metric"><div class="lbl">Duração</div><div class="val">{duration}</div></div>
  <div class="metric ok"><div class="lbl">Status</div><div class="val">OK</div></div>
  <div class="metric {'warn' if warnings else ''}"><div class="lbl">Avisos</div><div class="val">{len(warnings)}</div></div>
</div>

<div class="section-title">📊 Dados Atualizados</div>
<div class="data-grid">
  <div class="data-card"><div class="lbl">Total de linhas</div><div class="val">{total}</div></div>
  <div class="data-card"><div class="lbl">Última data</div><div class="val">{last_date}</div></div>
  <div class="data-card"><div class="lbl">Linhas adicionadas</div><div class="val">{added}</div></div>
  <div class="data-card"><div class="lbl">Período atualizado</div><div class="val">{periods}</div></div>
</div>

{warn_html}

<div class="section-title">🌐 Dashboard</div>
<p style="margin-bottom:10px;font-size:13px;color:#555">
  Clique para visualizar os dados completos ou disparar uma atualização manual:
</p>
<a class="btn-link" href="{DASHBOARD_URL}">Abrir Dashboard →</a>

<div class="footer">Enviado automaticamente pelo pipeline Argentina Updater.</div>
</div></body></html>"""


def _build_error_html(
    duration: str,
    errors: list[dict[str, str]],
    warnings: list[str],
    log_snippet: str,
    hostname: str,
    now: str,
) -> str:
    error_cards = ""
    for e in errors:
        error_cards += f"""
<div class="error-card">
  <div class="where">📍 {_esc(e['stage'])}</div>
  <div class="what">❌ {_esc(e['message'])}</div>
  <div class="why">💡 <strong>Causa provável:</strong> {_esc(e['cause'])}</div>
</div>"""

    warn_html = ""
    if warnings:
        items = "".join(f"<li>{_esc(w)}</li>" for w in warnings[:10])
        warn_html = f'<div class="section-title">⚠️ Avisos</div><ul class="warn-list">{items}</ul>'

    # Log resumido: só linhas de erro/warning + 1 linha de contexto anterior
    lines = log_snippet.splitlines()
    summary_lines = []
    for i, line in enumerate(lines):
        if " - ERROR - " in line or " - CRITICAL - " in line or " - WARNING - " in line:
            if i > 0 and lines[i - 1] not in summary_lines:
                summary_lines.append(lines[i - 1])
            summary_lines.append(line)
    log_summary = "\n".join(summary_lines) if summary_lines else log_snippet[-2000:]

    colored = []
    for line in log_summary.splitlines():
        esc = _esc(line)
        if "ERROR" in line or "CRITICAL" in line:
            colored.append(f'<span style="color:#ff8a80;font-weight:bold">{esc}</span>')
        elif "WARNING" in line:
            colored.append(f'<span style="color:#ffb74d">{esc}</span>')
        else:
            colored.append(esc)
    log_html = "\n".join(colored)

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{_CSS}</style></head><body>
<div class="hdr" style="background:#b71c1c">
  <h1>Arg Sailed Database — ❌ FALHA</h1>
  <p>{now} &nbsp;|&nbsp; {hostname} &nbsp;|&nbsp; Duração: {duration}</p>
</div>
<div class="body">

<div class="metrics">
  <div class="metric err"><div class="lbl">Erros</div><div class="val">{len(errors)}</div></div>
  <div class="metric {'warn' if warnings else ''}"><div class="lbl">Avisos</div><div class="val">{len(warnings)}</div></div>
  <div class="metric"><div class="lbl">Duração</div><div class="val">{duration}</div></div>
</div>

<div class="section-title">🔴 Erros Encontrados</div>
{error_cards}

{warn_html}

<div class="section-title">📋 Log Resumido</div>
<pre>{log_html}</pre>

<div class="section-title">🌐 Dashboard</div>
<p style="margin-bottom:10px;font-size:13px;color:#555">
  Após corrigir o problema, você pode disparar uma nova atualização pelo dashboard:
</p>
<a class="btn-link" href="{DASHBOARD_URL}">Abrir Dashboard →</a>

<div class="footer">Enviado automaticamente pelo pipeline Argentina Updater.</div>
</div></body></html>"""


# ---------------------------------------------------------------------------
# Backends de envio
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


def _graph_get_token() -> str:
    if not all([_TENANT_ID, _CLIENT_ID, _CLIENT_SECRET]):
        raise ValueError("EMAIL_TENANT_ID, EMAIL_CLIENT_ID e EMAIL_CLIENT_SECRET são obrigatórios.")
    url  = f"https://login.microsoftonline.com/{_TENANT_ID}/oauth2/v2.0/token"
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
    token   = _graph_get_token()
    to_list = [{"emailAddress": {"address": r}} for r in recipients]
    payload = json.dumps({
        "message": {
            "subject":      subject,
            "body":         {"contentType": "HTML", "content": html_body},
            "from":         {"emailAddress": {"address": _EMAIL_FROM}},
            "toRecipients": to_list,
        },
        "saveToSentItems": "false",
    }).encode("utf-8")
    url = f"https://graph.microsoft.com/v1.0/users/{_EMAIL_FROM}/sendMail"
    req = urllib.request.Request(url, data=payload, method="POST", headers={
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (200, 202):
            raise RuntimeError(f"Graph API retornou status {resp.status}")


def _dispatch(subject: str, html_body: str, recipients: list[str]) -> None:
    if _BACKEND == "graph":
        _send_graph(subject, html_body, recipients)
    else:
        _send_smtp(subject, html_body, recipients)


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def send_log_report(
    log_path: Path,
    success: bool = True,
    duration_seconds: float | None = None,
    db_stats: dict[str, Any] | None = None,
) -> None:
    """
    Envia o relatório por e-mail.

    Parameters
    ----------
    log_path         : Caminho do arquivo .log
    success          : True se o pipeline concluiu sem erros críticos
    duration_seconds : Duração total em segundos
    db_stats         : Dicionário com estatísticas do banco (total_rows, last_date, etc.)
                       Usado no e-mail de sucesso para mostrar os dados atualizados.
    """
    recipients = _get_recipients()
    if not recipients:
        logger.warning("EMAIL_TO não configurado — e-mail não enviado.")
        return

    log_text  = _read_current_run(log_path)
    errors    = _extract_errors(log_text)
    warnings  = _extract_warnings(log_text)
    hostname  = socket.gethostname()
    now       = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    duration  = f"{duration_seconds:.1f}s" if duration_seconds is not None else "—"
    today     = datetime.now().strftime("%d/%m/%Y")
    status    = "SUCESSO" if success else "FALHA"
    subject   = f"[Argentina Updater] {status} — {today}"

    if success:
        html_body = _build_success_html(duration, db_stats or {}, warnings, hostname, now)
    else:
        html_body = _build_error_html(duration, errors, warnings, log_text, hostname, now)

    try:
        logger.info(f"Enviando e-mail ({_BACKEND}) para: {recipients}")
        _dispatch(subject, html_body, recipients)
        logger.info("E-mail enviado com sucesso.")
    except Exception as exc:
        logger.error(f"Falha ao enviar e-mail: {exc}")
        if _BACKEND == "smtp":
            logger.error("Dica: para e-mail corporativo Office 365, configure EMAIL_BACKEND=graph no .env.")
