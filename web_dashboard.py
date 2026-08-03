"""
web_dashboard.py
----------------
Dashboard intranet para visualizar o banco Arg Sailed e disparar o pipeline
manualmente.

Acesso restrito à subnet 192.168.16.0/24.

Uso:
    python web_dashboard.py

Acesse em qualquer máquina da rede 16:
    http://192.168.16.180:5000
"""
from __future__ import annotations

import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, abort, jsonify, render_template_string, request

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from argentina_etl.config import PATH_DATABASE_OUTPUT  # reutiliza o .env já carregado

ALLOWED_SUBNET = "192.168.16."   # só IPs que começam com isso
MAIN_SCRIPT    = Path(__file__).parent / "main.py"
PYTHON_EXE     = sys.executable

app = Flask(__name__)

_pipeline_lock   = threading.Lock()
_pipeline_running = False


# ---------------------------------------------------------------------------
# Segurança: bloqueia IPs fora da intranet
# ---------------------------------------------------------------------------

def _get_client_ip() -> str:
    # Suporte a proxy reverso (X-Forwarded-For)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or ""


@app.before_request
def _restrict_to_intranet():
    ip = _get_client_ip()
    if not ip.startswith(ALLOWED_SUBNET) and ip not in ("127.0.0.1", "::1"):
        abort(403, description=f"Acesso negado. IP: {ip}")


# ---------------------------------------------------------------------------
# Template HTML
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Arg Sailed — Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: Calibri, Arial, sans-serif; background: #f4f6f9; color: #333; }
  header { background: #1a3a5c; color: #fff; padding: 16px 24px;
           display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 20px; }
  header small { font-size: 12px; opacity: .75; }
  .container { padding: 20px 24px; }
  .toolbar { display: flex; align-items: center; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
  .btn { padding: 10px 22px; border: none; border-radius: 6px; cursor: pointer;
         font-size: 14px; font-weight: bold; transition: background .2s; }
  .btn-primary { background: #1a3a5c; color: #fff; }
  .btn-primary:hover { background: #25527f; }
  .btn-primary:disabled { background: #999; cursor: not-allowed; }
  #status { font-size: 13px; color: #555; }
  #status.running { color: #e65100; font-weight: bold; }
  #status.ok      { color: #2e7d32; font-weight: bold; }
  #status.error   { color: #c62828; font-weight: bold; }
  .stats { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
  .stat { background: #fff; border-radius: 8px; padding: 12px 20px;
          box-shadow: 0 1px 4px rgba(0,0,0,.1); min-width: 160px; }
  .stat .label { font-size: 11px; text-transform: uppercase; color: #777; letter-spacing: .5px; }
  .stat .value { font-size: 24px; font-weight: bold; margin-top: 4px; }
  .tbl-wrap { overflow-x: auto; background: #fff; border-radius: 8px;
              box-shadow: 0 1px 4px rgba(0,0,0,.1); }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th { background: #1a3a5c; color: #fff; padding: 10px 12px; text-align: left;
       white-space: nowrap; position: sticky; top: 0; }
  td { padding: 8px 12px; border-bottom: 1px solid #eee; white-space: nowrap; }
  tr:hover td { background: #f0f4ff; }
  .updated { color: #1a3a5c; font-size: 11px; }
  input[type=text] { padding: 8px 12px; border: 1px solid #ccc; border-radius: 6px;
                     font-size: 13px; width: 240px; }
  .footer { margin-top: 16px; font-size: 12px; color: #aaa; text-align: center; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Arg Sailed — Database</h1>
    <small>Servidor: {{ server_ip }} &nbsp;|&nbsp; Rede intranet 192.168.16.x</small>
  </div>
  <div style="text-align:right">
    <div style="font-size:13px">Último arquivo</div>
    <div style="font-size:15px; font-weight:bold">{{ db_file }}</div>
  </div>
</header>

<div class="container">

  <div class="stats">
    <div class="stat">
      <div class="label">Total de linhas</div>
      <div class="value">{{ total_rows }}</div>
    </div>
    <div class="stat">
      <div class="label">Última data</div>
      <div class="value" style="font-size:18px">{{ last_date }}</div>
    </div>
    <div class="stat">
      <div class="label">Atualizado em</div>
      <div class="value" style="font-size:16px">{{ last_updated }}</div>
    </div>
  </div>

  <div class="toolbar">
    <button class="btn btn-primary" id="btnRun" onclick="runPipeline()">
      ▶ Atualizar Dados
    </button>
    <span id="status">{{ pipeline_status }}</span>
    <input type="text" id="search" placeholder="Filtrar tabela..." oninput="filterTable()" />
  </div>

  <div class="tbl-wrap">
    <table id="dataTable">
      <thead>
        <tr>{% for col in columns %}<th>{{ col }}</th>{% endfor %}</tr>
      </thead>
      <tbody>
        {% for row in rows %}
        <tr>
          {% for cell in row %}
          <td>{{ cell }}</td>
          {% endfor %}
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="footer">Pipeline: {{ main_script }}</div>
</div>

<script>
function runPipeline() {
  const btn = document.getElementById('btnRun');
  const st  = document.getElementById('status');
  btn.disabled = true;
  st.className = 'running';
  st.textContent = '⏳ Pipeline em execução…';

  fetch('/run', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      if (d.ok) {
        st.className = 'running';
        st.textContent = '⏳ Aguardando conclusão…';
        pollStatus();
      } else {
        st.className = 'error';
        st.textContent = '❌ ' + d.msg;
        btn.disabled = false;
      }
    })
    .catch(() => { st.className='error'; st.textContent='❌ Erro de rede'; btn.disabled=false; });
}

function pollStatus() {
  fetch('/status')
    .then(r => r.json())
    .then(d => {
      const st  = document.getElementById('status');
      const btn = document.getElementById('btnRun');
      if (d.running) {
        setTimeout(pollStatus, 3000);
      } else {
        st.className = d.last_ok ? 'ok' : 'error';
        st.textContent = d.last_ok ? '✅ Concluído com sucesso' : '❌ Pipeline terminou com erros';
        btn.disabled = false;
        setTimeout(() => location.reload(), 2000);
      }
    });
}

function filterTable() {
  const q = document.getElementById('search').value.toLowerCase();
  document.querySelectorAll('#dataTable tbody tr').forEach(tr => {
    tr.style.display = tr.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helpers de dados
# ---------------------------------------------------------------------------

def _load_db() -> pd.DataFrame | None:
    try:
        return pd.read_excel(PATH_DATABASE_OUTPUT, sheet_name="data_base", engine="openpyxl")
    except Exception:
        return None


def _format_cell(val) -> str:
    if pd.isna(val):
        return ""
    if isinstance(val, datetime):
        return val.strftime("%d/%m/%Y %H:%M")
    if hasattr(val, "strftime"):
        return val.strftime("%d/%m/%Y")
    return str(val)


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    df = _load_db()

    if df is None:
        columns, rows, total_rows, last_date, last_updated = [], [], 0, "—", "—"
        db_file = "não encontrado"
    else:
        columns    = list(df.columns)
        rows       = [[_format_cell(v) for v in row] for row in df.tail(500).itertuples(index=False)]
        total_rows = len(df)
        last_date  = df["Date"].max().strftime("%d/%m/%Y") if "Date" in df.columns else "—"

        if "UpdatedAt" in df.columns and df["UpdatedAt"].notna().any():
            last_updated = pd.to_datetime(df["UpdatedAt"]).max().strftime("%d/%m/%Y %H:%M")
        else:
            last_updated = "—"

        db_file = PATH_DATABASE_OUTPUT.name

    status_msg = "⏳ Em execução…" if _pipeline_running else "Pronto"

    return render_template_string(
        _HTML,
        columns=columns,
        rows=rows,
        total_rows=total_rows,
        last_date=last_date,
        last_updated=last_updated,
        db_file=db_file,
        server_ip="192.168.16.180",
        main_script=str(MAIN_SCRIPT),
        pipeline_status=status_msg,
    )


@app.route("/run", methods=["POST"])
def run_pipeline():
    global _pipeline_running

    with _pipeline_lock:
        if _pipeline_running:
            return jsonify(ok=False, msg="Pipeline já está rodando.")
        _pipeline_running = True

    def _execute():
        global _pipeline_running, _last_ok
        try:
            result = subprocess.run(
                [PYTHON_EXE, str(MAIN_SCRIPT)],
                cwd=str(MAIN_SCRIPT.parent),
                capture_output=True,
                text=True,
            )
            _last_ok = result.returncode == 0
        except Exception:
            _last_ok = False
        finally:
            _pipeline_running = False

    threading.Thread(target=_execute, daemon=True).start()
    return jsonify(ok=True, msg="Pipeline iniciado.")


_last_ok: bool | None = None


@app.route("/status")
def pipeline_status():
    return jsonify(running=_pipeline_running, last_ok=_last_ok)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Dashboard disponível em: http://192.168.16.180:5000")
    print("Acesso restrito à rede 192.168.16.*")
    # host='0.0.0.0' para escutar em todas as interfaces (necessário para acesso externo)
    app.run(host="0.0.0.0", port=5000, debug=False)
