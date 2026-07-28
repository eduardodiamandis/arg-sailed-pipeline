"""
main.py
-------
Ponto de entrada. O codigo vive em src/argentina_etl/; este arquivo apenas
coloca src/ no path e delega.

Existe para que a tarefa agendada do Windows (new_sailed_task, que roda
`python.exe main.py` com WorkingDirectory na raiz) continue funcionando sem
alteracao apos a reorganizacao em pacote. Equivale a `python -m argentina_etl`.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from argentina_etl.__main__ import main

if __name__ == "__main__":
    main()
