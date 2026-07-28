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

import ctypes
import datetime
import gc
import subprocess
import threading
import time
from pathlib import Path

from argentina_etl.logging_setup import logger

# Tempo máximo (segundos) para toda a operação de Pivot Tables
_PIVOT_TIMEOUT_SECONDS = 120

# Quanto esperar o EXCEL.EXE morrer sozinho antes de forçar
_QUIT_TIMEOUT_SECONDS = 10


# ---------------------------------------------------------------------------
# Encerramento garantido do processo Excel
# ---------------------------------------------------------------------------
# excel.Quit() sozinho nao basta: enquanto qualquer wrapper COM (wb, ws, pcache,
# pt) existir do lado do Python, o EXCEL.EXE sobrevive ao Quit e fica orfao,
# invisivel, segurando o arquivo. A execucao seguinte entao falha com
# "Microsoft Excel cannot access the file ... being used by another program",
# ou com OLE error 0x800a01a8 ao recriar as pivots.
#
# Todo o mecanismo abaixo opera sobre o PID da instancia criada por DispatchEx,
# que e sempre uma instancia nova e isolada. Nunca toca num Excel aberto pelo
# usuario.

def _pid_do_excel(excel) -> int | None:
    """PID da instância COM, para poder confirmar (e forçar) o encerramento."""
    try:
        import win32process
        _thread_id, pid = win32process.GetWindowThreadProcessId(excel.Hwnd)
        return pid or None
    except Exception as exc:
        logger.warning(f"  Não foi possível obter o PID do Excel: {exc}")
        return None


def _processo_vivo(pid: int) -> bool:
    """True se o PID ainda corresponde a um processo em execução."""
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259
    k32 = ctypes.windll.kernel32
    handle = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False
    try:
        codigo = ctypes.c_ulong()
        ok = k32.GetExitCodeProcess(handle, ctypes.byref(codigo))
        return bool(ok) and codigo.value == STILL_ACTIVE
    finally:
        k32.CloseHandle(handle)


def _matar_processo(pid: int) -> None:
    """Último recurso: encerra o PID à força."""
    try:
        subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            capture_output=True, timeout=15, check=False,
        )
    except Exception as exc:
        logger.error(f"  Falha ao forçar encerramento do Excel (PID {pid}): {exc}")


def _encerrar_excel(pid: int | None) -> None:
    """
    Confirma que o Excel morreu; força se necessário.

    Deve ser chamado DEPOIS de soltar as referências COM (ver `_soltar_com`),
    senão o processo continua vivo mesmo com o Quit() já chamado.
    """
    if pid is None:
        return
    limite = time.time() + _QUIT_TIMEOUT_SECONDS
    while time.time() < limite:
        if not _processo_vivo(pid):
            return
        time.sleep(0.3)

    logger.warning(
        f"  Excel (PID {pid}) não encerrou em {_QUIT_TIMEOUT_SECONDS}s — forçando. "
        "Isso costuma indicar diálogo modal invisível ou referência COM presa."
    )
    _matar_processo(pid)
    if _processo_vivo(pid):
        logger.error(f"  Excel (PID {pid}) sobreviveu ao taskkill — verifique manualmente.")


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

    # Publicado em `resultado` para que o caminho do TIMEOUT também consiga
    # encerrar o processo: quando a thread trava, é o chamador que precisa matar.
    pid = _pid_do_excel(excel)
    resultado["pid"] = pid

    wb = None
    ws_data = pcache = None

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
            if wb is not None:
                wb.Close(False)
        except Exception:
            pass

    finally:
        try:
            excel.Quit()
        except Exception:
            pass

        # Solta TODAS as referências COM antes de conferir se o processo morreu.
        # Enquanto qualquer uma sobreviver, o EXCEL.EXE ignora o Quit().
        wb = ws_data = pcache = excel = None
        gc.collect()

        _encerrar_excel(pid)
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
        # A thread e daemon: ela morre com o processo, mas o EXCEL.EXE que ela
        # criou NAO — foi por aqui que os orfaos apareceram, segurando o arquivo
        # e fazendo a execucao seguinte falhar. Encerramos o processo pelo PID
        # que a thread publicou antes de travar.
        pid = resultado.get("pid")
        if pid is not None:
            logger.warning(f"  Timeout — encerrando o Excel (PID {pid}) deixado pela thread travada.")
            _matar_processo(pid)
            if _processo_vivo(pid):
                logger.error(f"  Excel (PID {pid}) sobreviveu ao taskkill — verifique manualmente.")
            else:
                logger.info(f"  Excel (PID {pid}) encerrado.")
        else:
            logger.error(
                "  Timeout sem PID conhecido do Excel — pode haver processo orfao. "
                "Verifique o Gerenciador de Tarefas."
            )

        raise TimeoutError(
            f"Pivot Tables travaram após {_PIVOT_TIMEOUT_SECONDS}s. "
            f"O processo Excel (PID {pid}) foi encerrado."
        )

    if resultado.get("error"):
        raise resultado["error"]

    if not resultado.get("ok"):
        raise RuntimeError("Pivot Tables não foram criadas por motivo desconhecido.")