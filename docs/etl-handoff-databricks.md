# Argentina Sailed & Line-Up ETL — how it works today

**Purpose of this document.** We are moving these tables into Databricks. This is a plain
description of what the current pipeline does, written for whoever will build the
ingestion. It focuses on the **Line-Up (daily vessel line-up)** flow, because that is the
one with behaviour you cannot guess from looking at the table.

Nothing here is a proposal. It is what runs in production today.

---

## 1. In one paragraph

Every night a Windows scheduled task downloads two Excel bulletins from NABSA (the
Argentine port agency), parses them, and writes to two SQL Server tables:
`Arg_Sailed` (vessels that have already sailed — the historical record) and
`Arg_Lineup` (vessels currently queued at the ports — a daily photo). The two tables
look similar but have **opposite write semantics**, and that is the single most
important thing to understand before ingesting them.

---

## 2. Sources and schedule

| | |
|---|---|
| Source | NABSA bulletins, two URLs (in `.env`, not in code) |
| Download | Selenium + headless Chrome |
| Schedule | Windows task `new_sailed_task`, daily at **23:45** (America/Sao_Paulo) |
| Database | SQL Server, Windows Authentication |

**Why Selenium and not a plain HTTP request.** The NABSA URLs do not return the file.
They return an HTML page with a JavaScript redirect, so `requests` gets the HTML, not the
spreadsheet. A real browser is required. If you re-implement the download in Databricks,
this will be the first thing that surprises you.

---

## 3. The two flows write very differently

This matters more than anything else in this document.

| | `Arg_Sailed` | `Arg_Lineup` |
|---|---|---|
| Write mode | **full DELETE + INSERT every run** | **append only** |
| Meaning of a row | one shipment that happened | one vessel in the queue **on a given date** |
| Grain | shipment | (snapshot date × vessel × berth line) |
| History | the table *is* the current truth; no history of changes | every day adds a new snapshot; nothing is ever deleted |
| Rows today | ~47,200 | ~4,300 and growing daily |

So:

- **`Arg_Sailed` is a snapshot of the whole history, rewritten nightly.** If you do
  incremental ingestion by watching row counts or `UpdatedAt`, be aware that every row is
  rewritten every night, even rows from 2018. A full reload each day is the honest model.
- **`Arg_Lineup` is an append-only event log.** Never deduplicate across `SnapshotDate`.
  The same vessel appearing on 10 consecutive days is 10 legitimate rows, not duplicates.

There is one exception on the Line-Up side: if the pipeline runs **twice on the same
day**, the second run deletes that day's snapshot and reinserts it (controlled by
`LINEUP_FORCE_SNAPSHOT`, currently on). Other days are never touched.

---

## 4. Line-Up (daily vessel line-up) — the detailed part

### 4.1 What the file is

One Excel file per day (`vessel_update_<date>.xlsx`, ~330 KB, about 400 rows before
cleaning). Its columns are:

```
Port | Terminal | Vessel | ETA | ETB | ETF | Ops | Tons | Commodity | Destination | Origin | Charterer
```

`Ops` is read but not stored.

### 4.2 The header row moves

The data does not start on a fixed row. The parser scans the first rows looking for the
one that contains the expected column names, and uses that. **Do not hardcode a header
row** — NABSA has changed the number of preamble rows before.

### 4.3 ETA / ETB / ETF are free text, not dates

This is the part most likely to bite you. The source cells contain strings like:

```
"ETA REC 19/05"      "AT REC 11/05"      "ETF 19/05"      "ETB"      ""
```

The parser pulls the first `dd/mm` (or `dd/mm/yyyy`) it finds with a regular expression.
Anything with no readable date becomes `NULL`.

Both forms are stored: **`ETA_Raw`** keeps the original string, `ETA_Date` keeps the
parsed date. Keep `ETA_Raw` when you ingest — it is the only way to audit a bad parse.
(ETB and ETF keep only the parsed date today.)

**⚠️ Known issue: the year is guessed.** Most cells have no year, so the parser fills in
**the current year at the moment of parsing**. Near 1 January this produces wrong dates:

| Text in the file | Parsed on | Result | Should be |
|---|---|---|---|
| `ETA REC 28/12` | 2 Jan 2027 | **2027-12-28** | 2026-12-28 |

A date a few days in the past becomes a date almost a year in the future. This also
corrupts the derived `Status` (see below), because `Status` is computed from these dates.
The effect is limited to a few days around each year boundary, but it is real, it is in
the data already, and it is not corrected anywhere downstream.

### 4.4 Status is derived by us, not published by NABSA

**The bulletin has no status column.** We compute it by comparing the three dates against
**the date the pipeline runs**:

| Status | Rule | Meaning |
|---|---|---|
| `SAILED` | ETF has passed | finished loading and left |
| `LOADING` | ETB has passed, ETF has not | berthed, loading now |
| `WAITING` | ETA has passed, ETB has not | at anchorage, waiting for a berth |
| `EXPECTED` | ETA is in the future | still on its way |
| `TBC` | no readable date | slot booked, schedule unknown |

The rules are tested **in that order**, most advanced stage first, and the first match
wins. A vessel whose ETA and ETB have both passed is `LOADING`, not `WAITING`.

Two consequences you must design for:

1. **`SAILED` never reaches the table.** Those rows are dropped before the insert,
   because a vessel that has sailed belongs to `Arg_Sailed`. If you count tonnage across
   both tables, you will not double-count — by design.
2. **Status is a function of the run date, not of the file.** The same vessel with the
   same unchanged dates is `EXPECTED` today and `WAITING` tomorrow. Status is only
   meaningful when read together with its `SnapshotDate`. Never carry a status forward or
   compare statuses across snapshots as if the vessel had changed.

### 4.5 `SnapshotDate` is the run date

`SnapshotDate` is set to **the day the pipeline runs**, not to any date inside the file.
If a run is missed, that day simply does not exist in the table. Gaps are normal — the
table currently has ~15 snapshots, not one per calendar day, because the job has not run
every single day. **Do not interpolate across gaps.**

### 4.6 A technical note if you keep using the legacy ODBC driver

The Line-Up insert sets `fast_executemany = False`. The old `SQL Server` ODBC driver
raises on `None` values when fast mode is on, and Line-Up is full of nulls (ETB and ETF
are empty ~90% of the time). Sailed uses `True` because it has no nulls. Not relevant if
you rewrite the load in Spark, but it explains the asymmetry if you read the code.

---

## 5. Sailed — the shorter story

The Sailed bulletin carries the recent history (usually the current month). It is merged
into an Excel base file that holds the full history since 2018, and the whole base is
then written to `Arg_Sailed`.

The merge is **period by period, and replacement is wholesale**: an accepted month is
deleted from the base and reinserted from the new file. Because that is destructive, a
month is only accepted if it passes **both** checks:

1. **Volume** — the new file has at least as many rows as the base has for that month.
2. **Day coverage** — the new file contains every day the base already has for that month.

Months not present in the new file are never touched. Equal row counts are accepted on
purpose: NABSA restates parcels without changing the row count.

Two things worth knowing if numbers look odd:

- **There is a corrections layer.** A small versioned CSV (`config/correcoes_sailed.csv`)
  is applied on every run, after the merge. It exists because NABSA published one shipment
  of **49,806,067 tons** on a single vessel (EPIC RADIANCE, 20 Nov 2025) — 5% of the
  entire 8-year history, a typo for 49,806.07. Fixing it by hand did not stick, because
  the table is rewritten every night. If you ingest the raw bulletins instead of our
  tables, **this correction will not be there** and your totals will differ from ours by
  ~49.8 Mt.
- **A validation flags physically impossible tonnage** (over 500,000 t on one vessel — the
  largest bulk carrier in service loads about 400,000). It reports, it does not block.

---

## 6. Table schemas as they exist today

### `Arg_Sailed` — full reload nightly

| Column | Type | Note |
|---|---|---|
| `Date` | datetime | shipment date |
| `Destination`, `Origin`, `Cargo` | nvarchar(255) | |
| `Tons` | float | |
| `Month`, `Year` | int | derived from `Date`; redundant |
| `UpdatedAt` | datetime | mostly NULL in older rows |

Note that `Port`, `Terminal`, `Vessel` and `Charterer` **exist in the source spreadsheet
but are not in this table**. If you need them, ingest from the Excel base, not from SQL.

### `Arg_Lineup` — append only

| Column | Type | Note |
|---|---|---|
| `SnapshotDate` | date, NOT NULL | the run date — part of the grain |
| `Port` | nvarchar(100) | |
| `Terminal` | nvarchar(150) | |
| `Vessel` | nvarchar(100) | |
| `ETA_Raw` | nvarchar(50) | original text, e.g. `ETA REC 19/05` |
| `ETA_Date`, `ETB_Date`, `ETF_Date` | date | parsed; ETB/ETF null ~90% of the time |
| `Status` | nvarchar(20) | **derived by us** — see 4.4 |
| `Tons` | float | |
| `Commodity`, `Destination`, `Origin`, `Charterer` | nvarchar(100) | |

There is **no primary key and no surrogate id** on either table.

---

## 7. Short list of things that will bite you

1. `Arg_Sailed` is fully rewritten nightly — incremental logic based on it will mislead.
2. `Arg_Lineup` is append-only — deduplicating across `SnapshotDate` destroys the history.
3. `Status` does not come from the source; it is computed against the run date.
4. `SAILED` rows are deliberately absent from `Arg_Lineup`.
5. Dates with no year are assigned the current year — wrong near 1 January.
6. There is no primary key; the natural key for Line-Up is
   (`SnapshotDate`, `Vessel`, `Terminal`, `ETA_Raw`) and even that is not guaranteed unique.
7. Downloading requires a real browser (JavaScript redirect).
8. One known data correction is applied by us and is not in the source bulletins.
9. Missing days in `Arg_Lineup` are missing runs, not empty queues.

---

## 8. What we would suggest asking for

- Access to the NABSA bulletin URLs, so ingestion can read the source directly rather than
  our SQL tables.
- A decision on whether the corrections layer moves to Databricks or stays here. If it
  stays here and Databricks reads the source, the two will disagree.
- A decision on whether `Status` is recomputed in Databricks (and against which date) or
  ingested as we already computed it. Recomputing against ingestion time would silently
  change history.
