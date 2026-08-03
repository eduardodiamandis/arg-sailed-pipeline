"""
main.py
-------
Ponto de entrada. O codigo vive em src/argentina_etl/; este arquivo apenas
coloca src/ no path e delega.

Existe para que a tarefa agendada do Windows (new_sailed_task, que roda
`python.exe main.py` com WorkingDirectory na raiz) continue funcionando sem
alteracao apos a reorganizacao em pacote. Equivale a `python -m argentina_etl`.

Uso:
    python main.py               # pipeline completo (o que a tarefa agendada roda)
    python main.py --dashboard   # so regera o dashboard HTML, sem tocar no ETL

A flag existe porque `python -m argentina_etl.reporting.dashboard` NAO funciona
da raiz: src/ so entra no sys.path por causa deste shim, e o -m resolve o modulo
antes de qualquer codigo nosso rodar. Sem a flag, restaria mandar quem gera o
dashboard exportar PYTHONPATH na mao.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from argentina_etl.__main__ import main

if __name__ == "__main__":
    if "--dashboard" in sys.argv[1:]:
        from argentina_etl.reporting.dashboard import main as gerar_dashboard

        gerar_dashboard()
    else:
        main()
