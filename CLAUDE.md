# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the pipeline

```bash
python main.py              # or: python -m argentina_etl
python main.py --dashboard  # regenerates the HTML dashboard only, no ETL
pytest                      # 200 tests, all passing
```

`python -m argentina_etl.reporting.dashboard` does **not** work from the root: `src/`
only reaches `sys.path` through the `main.py` shim, and `-m` resolves the module before
any of our code runs. Hence the flag.

`main.py` at the root is a thin shim: it puts `src/` on the path and delegates to
`argentina_etl.__main__`. It exists so the Windows scheduled task `new_sailed_task`
(which runs `python.exe main.py` with `WorkingDirectory` at the root) keeps working
after the package reorganisation. Do not remove it.

The suite is green. It is the main safety net for this project — a failing test is a
signal, never background noise. If one starts failing, fix it or explain it before
moving on.

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
| `validation.py` | `validar_continuidade`, `detectar_gaps`, `validar_corte_rodape`, `validar_tonelagem`. |
| `pipelines/sailed.py` | `ler_arquivo_novo`, `merge_com_banco`. |
| `pipelines/correcoes.py` | Known bad rows from `config/correcoes_sailed.csv`, re-applied every run. |
| `pipelines/lineup.py` | Reads the Line-Up snapshot, parses ETA/ETB/ETF, classifies status. |
| `storage/excel.py` | Local file. |
| `storage/onedrive.py` | Spreadsheet with derived sheets and pivots; OneDrive sync check. |
| `storage/sql_server.py` | Both flows: `salvar_sql_server` and `salvar_lineup_sql`. |
| `storage/sharepoint.py` | Microsoft Graph upload. Wired into `__main__` as step 5b, but gated by `GRAPH_UPLOAD_ENABLED`, which stays `false` until the `Sites.Selected` permission is granted. Until then every call returns `401`. |
| `reporting/report.py` | HTML report over SMTP. |
| `reporting/dashboard.py` | Generates the self-contained HTML dashboard. Not part of the nightly run — invoke with `python main.py --dashboard`. |

**The `pipelines/` vs `storage/` split is the point.** `pipelines/` decides what the
data should be; `storage/` only writes what it received. If a `storage/` module needs
to consult a business rule, it is in the wrong place.

## Rules that must not be changed without understanding them

**Merge safety lock** (`pipelines/sailed.py`). Replacement is *wholesale*: an accepted
period has its whole month deleted and reinserted from the new file. So a period is
only accepted if it passes **both** checks — (1) **volume**: ≥ as many rows as the
database, and (2) **coverage**: it carries every day the database already has for that
month. Without check 1, a truncated NABSA file would destroy a whole month; without
check 2, a file that is fat at the start and empty at the end passes silently — neither
`detectar_gaps` nor `validar_continuidade` catches that case. Equal row counts are
accepted on purpose: the source restates parcels without changing the count. See
ESTRUTURA.md, decisions 9.1 and 9.4.

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

**Known corrections are re-applied every run** (`pipelines/correcoes.py` +
`config/correcoes_sailed.csv`, step 4b in `__main__`). Fixing a bad row by hand does
not stick: `Arg_Sailed` gets a full DELETE+INSERT and the OneDrive sheet is
regenerated, both from the merged base, so a manual fix is undone the next night —
and if NABSA republishes the month, the wholesale replacement overwrites the base
too. **The wrong value is part of the match key**: a rule only fires when date,
vessel, cargo, destination *and* the wrong tonnage all match, so the day the source
corrects the data the rule stops matching on its own instead of overwriting the
correct value with our guess. A rule that matched nothing logs a WARNING — unless it is in `modo=guarda`, meaning the
data was already fixed upstream and the rule is a sentinel: silent when absent, loud when
it fires (a regression). Without that distinction a resolved correction would warn every
night forever, which trains readers to ignore warnings. See ESTRUTURA.md, decision 9.6.

**`validar_tonelagem` flags physically impossible shipments** (`validation.py`,
`LIMITE_TONELAGEM_NAVIO = 500_000`). NABSA published 49,806,067 t on one vessel
(EPIC RADIANCE, 20/11/2025) — 5.3% of the whole history — and it went unnoticed for
months because nothing checked the *magnitude* of values: `detectar_gaps` counts days
and `validar_continuidade` compares edges, and an absurd number inside a day that
exists violates neither. **Not a statistical outlier filter**: the cut is physical, so
the legitimate 445,000 t iron-ore shipment stays in. The dashboard keeps its own
quarantine as a second line of defence, showing any such row in full at the top of the
page. See ESTRUTURA.md, decisions 9.5 and 9.6.

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
