"""
pivot_tables.py
---------------
Cria Pivot Tables reais no Excel usando win32com.
Separado de database.py para facilitar testes e isolamento de falhas.

Correções para rodar no Task Scheduler (sem desktop interativo):
  - excel.DisplayAlerts = False
  - excel.ScreenUpdating = False
  - excel.Interactive = False
  - Timeout via thread para evitar travamento indefinido
  - Cleanup garantido no finally (Quit + CoUninitialize)
"""
from __future__ import annotations

import datetime
import threading
from pathlib import Path

from logger_config import logger

# Tempo máximo (segundos) para toda a operação de Pivot Tables
_PIVOT_TIMEOUT_SECONDS = 120


def _criar_pivot_tables_interno(path_excel: Path, resultado: dict) -> None:
    """
    Executa a criação das Pivot Tables. Chamado em thread separada para
    permitir timeout controlado.
    """
    import pythoncom
    import win32com.client as win32

    # Necessário quando win32com é usado em threads secundárias
    pythoncom.CoInitialize()

    mes_atual = str(datetime.datetime.now().month)
    path_str = str(path_excel.resolve())

    excel = win32.DispatchEx("Excel.Application")  # DispatchEx = nova instância isolada
    excel.Visible = False
    excel.DisplayAlerts = False       # Evita diálogos que travam o processo
    excel.ScreenUpdating = False      # Desabilita redraw (mais rápido e sem precisar de tela)
    excel.Interactive = False         # Ignora qualquer interação de teclado/mouse

    try:
        wb = excel.Workbooks.Open(path_str)
        ws_data = wb.Worksheets("data_base")

        last_row = ws_data.Cells(ws_data.Rows.Count, 1).End(-4162).Row   # xlUp
        last_col = ws_data.Cells(1, ws_data.Columns.Count).End(-4159).Column  # xlToLeft
        data_range = ws_data.Range(
            ws_data.Cells(1, 1), ws_data.Cells(last_row, last_col)
        )

        pcache = wb.PivotCaches().Create(SourceType=1, SourceData=data_range)

        def _build_pivot(ws_name: str, pivot_name: str, year: str, month_filter: str):
            ws = wb.Worksheets(ws_name)
            ws.Cells.Clear()
            pt = pcache.CreatePivotTable(ws.Range("A3"), pivot_name)

            pt.PivotFields("Destination").Orientation = 1        # xlRowField
            pt.AddDataField(pt.PivotFields("Tons"), "Sum of Tons", -4157)  # xlSum

            for field in ("Year", "Origin", "Cargo", "Month"):
                pt.PivotFields(field).Orientation = 3            # xlPageField

            pt.PivotFields("Year").CurrentPage = year
            pt.PivotFields("Origin").CurrentPage = "ARGENTINA"
            pt.PivotFields("Cargo").CurrentPage = "CORN"
            pt.PivotFields("Month").CurrentPage = month_filter

            logger.info(f"  Pivot '{pivot_name}' criada (Year={year}, Month={month_filter})")

        _build_pivot("Pivot_2026", "Pivot_2026", year="2026", month_filter=mes_atual)
        _build_pivot("Pivot_2025", "Pivot_2025", year="2025", month_filter="12")
        
        if wb.ReadOnly:
            raise RuntimeError(
                f"Arquivo aberto em modo somente leitura — feche o Excel antes de rodar o pipeline: {path_str}"
                )

        
        wb.Save()
        wb.Close(False)
        logger.info("Pivot Tables salvas com sucesso.")
        resultado["ok"] = True

    except Exception as exc:
        resultado["error"] = exc
        try:
            wb.Close(False)
        except Exception:
            pass

    finally:
        try:
            excel.Quit()
        except Exception:
            pass
        pythoncom.CoUninitialize()


def criar_pivot_tables(path_excel: Path) -> None:
    """
    Cria Pivot Tables reais no Excel usando win32com, com timeout de
    _PIVOT_TIMEOUT_SECONDS segundos para evitar travamento no Task Scheduler.

    Raises
    ------
    TimeoutError  : Se a operação ultrapassar o timeout
    Exception     : Qualquer erro interno do win32com
    """
    logger.info(f"Criando Pivot Tables no Excel: {path_excel.name}")

    resultado: dict = {"ok": False, "error": None}

    t = threading.Thread(
        target=_criar_pivot_tables_interno,
        args=(path_excel, resultado),
        daemon=True,
    )
    t.start()
    t.join(timeout=_PIVOT_TIMEOUT_SECONDS)

    if t.is_alive():
        raise TimeoutError(
            f"Pivot Tables travaram após {_PIVOT_TIMEOUT_SECONDS}s. "
            "Processo Excel pode ter ficado aberto — verifique o Gerenciador de Tarefas."
        )

    if resultado.get("error"):
        raise resultado["error"]

    if not resultado.get("ok"):
        raise RuntimeError("Pivot Tables não foram criadas por motivo desconhecido.")