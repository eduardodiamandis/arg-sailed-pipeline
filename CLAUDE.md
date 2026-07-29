# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the pipeline

```bash
python main.py          # or: python -m argentina_etl
pytest                  # 123 tests
```

`main.py` at the root is a thin shim: it puts `src/` on the path and delegates to
`argentina_etl.__main__`. It exists so the Windows scheduled task `new_sailed_task`
(which runs `python.exe main.py` with `WorkingDirectory` at the root) keeps working
after the package reorganisation. Do not remove it.

Two tests in `tests/test_pipeline.py` fail for reasons predating the reorganisation
(`TestSalvarOnedrive::test_cria_cinco_sheets`,
`TestDownloadFile::test_salva_arquivo_com_nome_enriquecido`). Everything else passes.

## Documentation

| File | Subject |
|---|---|
| `README.md` | install and run |
| `ARQUITETURA.md` | how the pipeline works internally |
| `ESTRUTURA.md` | where things live and why — **read before moving or creating files** |
| `docs/` | operational documents |

## Architecture

`src/argentina_etl/__main__.py` orchestrates and nothing else. Business rules live in
`pipelines/`, persistence in `storage/`.

| Module | Purpose |
|---|---|
| `config.py` | Only module that reads `.env`. Required vars raise `EnvironmentError` at import. |
| `logging_setup.py` | Shared `logger` (`argentina_logger`), daily rotation into `logs/`. |
| `downloader.py` | Selenium headless Chrome. **Not `requests`**: NABSA URLs return HTML with a JavaScript redirect. |
| `validation.py` | `validar_continuidade`, `detectar_gaps`, `validar_corte_rodape`. |
| `pipelines/sailed.py` | `ler_arquivo_novo`, `merge_com_banco`. |
| `pipelines/lineup.py` | Reads the Line-Up snapshot, parses ETA/ETB/ETF, classifies status. |
| `storage/excel.py` | Local file. |
| `storage/onedrive.py` | Spreadsheet with derived sheets and pivots; OneDrive sync check. |
| `storage/sql_server.py` | Both flows: `salvar_sql_server` and `salvar_lineup_sql`. |
| `storage/sharepoint.py` | Microsoft Graph upload. Implemented, **not wired in yet** — awaiting permission. |
| `reporting/report.py` | HTML report over SMTP. |

**The `pipelines/` vs `storage/` split is the point.** `pipelines/` decides what the
data should be; `storage/` only writes what it received. If a `storage/` module needs
to consult a business rule, it is in the wrong place.

## Rules that must not be changed without understanding them

**Merge safety lock** (`pipelines/sailed.py`). A period from the new file replaces the
database only if it has ≥ as many rows. Without it, a truncated NABSA file would
destroy a whole month — permanently, since the base is rewritten on every run.

**The base is rewritten every run** (`storage/excel.py`, called from `__main__`). While
the NABSA file carries the current month the merge recomposes it and a frozen base
goes unnoticed. At the month boundary a gap opens and disappears from SQL and Power BI
with no signal. This caused a 5-day data loss in June 2026.

**`fast_executemany = False` for Line-Up** (`storage/sql_server.py`). The legacy
`SQL Server` ODBC driver rejects `None` values with fast mode on. `True` is fine for
Sailed.

**`pd.NaT` passes `isinstance(x, datetime.date)`** — it inherits from
`datetime.datetime`. Use the `_e_data` helper in `pipelines/lineup.py`, never a bare
`isinstance`.

**Pivots are computed with pandas**, not Excel COM. Do not reintroduce `win32com` for
this: it required Excel installed, left orphan `EXCEL.EXE` processes, and resolved the
local path to the SharePoint URL — building pivots from the server copy rather than
the file just written.

## Persistence: Sailed vs Line-Up

- **`Arg_Sailed`**: full `DELETE` + `INSERT` every run — reflects the latest state.
- **`Arg_Lineup`**: append-only, one snapshot per day, never deletes history.
  `LINEUP_FORCE_SNAPSHOT` only affects re-runs on the *same* day. Vessels with
  `Status=SAILED` are filtered out to avoid duplicating Sailed data.

## Code style conventions

- Docstrings and log messages in **Portuguese** — follow this in all new code.
- `from __future__ import annotations` at the top of every module.
- No paths, URLs or credentials outside `.env`. Every new variable goes into `.env`,
  `.env.example` and `config.py` in the same change.
- SQL Server uses Windows Authentication; never passwords in code.
- Validations must never bring the pipeline down: they inform, they do not raise.
