# ohlcv
<div align="center">

# `ohlcv-packing-pipeline`

**A single-file Yahoo Finance OHLCV bulk ingestion pipeline for a ~106,328+ ticker universe**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Format](https://img.shields.io/badge/Storage-Parquet-brightgreen.svg)](https://parquet.apache.org/)
[![CLI](https://img.shields.io/badge/Interface-CLI-lightgrey.svg)](#running-the-downloader)
[![Tests](https://img.shields.io/badge/Tests-Offline%20%2B%20Mocked-informational.svg)](#testing)

</div>

---

## Overview

`packing.py` is a single-script Python pipeline for downloading daily OHLCV history for a very large Yahoo Finance ticker universe and storing it in a clean, analysis-ready layout in Parquet files.

### Warning : YF's Data is not Clean

[Gmail - Incorrect Historical Price Data (around 31st july 2026 in ticker MPA.CR).pdf](https://github.com/user-attachments/files/31350981/Gmail.-.Incorrect.Historical.Price.Data.around.31st.july.2026.in.ticker.MPA.CR.pdf)


---

# Author
Rushidev Gajanan Ukalkar,  [email](rushidevjain@gmail.com) / [teleg](https://t.me/uku_jain/)

It is designed to:

* download OHLCV bars at scale,
* store one Parquet file per symbol,
* keep metadata for status and auditing,
* resume interrupted runs safely,
* update existing files incrementally,
* validate stored data,
* and remain easy to adapt to another provider later.

## Why this project exists

Working with a universe of 100,000+ symbols is very different from downloading a few tickers by hand.
This project is built to keep the process:

* reproducible,
* resumable,
* testable,
* and friendly to downstream tools like Python, Polars, DuckDB, PyArrow, and C++/Arrow.

> **Design choice:** the project stays in one file on purpose, but the code is organized into clean internal sections so it can be split into a package later if needed.

---

## Features

<table>
<tr>
<td valign="top">

### Download

* Bulk OHLCV ingestion
* Symbol filtering
* Threaded execution
* Retry handling
* Rate-limit awareness

</td>
<td valign="top">

### Storage

* One Parquet file per symbol
* Canonical schema
* Metadata tracking
* Fast reads from many tools

</td>
</tr>
<tr>
<td valign="top">

### Reliability

* Resumable downloads
* Incremental updates
* Offline test suite
* Validation report generation

</td>
<td valign="top">

### Extensibility

* Abstract `DataProvider`
* Easy vendor swap
* Clean CLI workflow
* Config-driven behavior

</td>
</tr>
</table>

---

## Repository layout

```text
.
├── packing.py                    # main pipeline script
├── config.yaml                   # all paths & tunables
├── requirements.txt
├── README.md
├── Yahoo-Finance-Ticker-Symbols.csv
├── tests/
│   └── test_packing.py
├── database/
│   ├── raw/
│   │   ├── AAPL.parquet
│   │   ├── MSFT.parquet
│   │   └── ...
│   └── metadata/
│       ├── symbols_metadata.parquet
│       └── validation_report.parquet
└── logs/
    ├── packing.log
    └── last_run_stats.json
```

---

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Tested with:** Python 3.10+

---

## Input file

Place `Yahoo-Finance-Ticker-Symbols.csv` next to `config.yaml`, or point `data.symbols_csv` to its location.

Expected columns:

```text
Ticker,Name,Exchange,Category Name,Country
```

Only `Ticker` is required.
The other columns are preserved in `symbols_metadata.parquet` so the universe can be filtered later without reopening the OHLCV files.

Blank tickers and duplicate tickers are skipped automatically and logged.

---

## Running the downloader

Always smoke-test on a small slice first.

```bash
python packing.py download --limit 20
```

Then run the full universe:

```bash
python packing.py download
```

### Useful commands

```bash
python packing.py download --overwrite
python packing.py download --workers 32
python packing.py download --symbols AAPL,MSFT,^GSPC
python packing.py download --config other.yaml
python packing.py update
python packing.py verify
```

> **Tip:** options are placed after the subcommand, like `git`, `docker`, or `kubectl`.

---

## Command reference

<details>
<summary><strong><code>download</code></strong></summary>

Downloads OHLCV data for the selected symbols.

This command is **resumable**:

* already successful symbols are skipped,
* interrupted runs can be restarted safely,
* `--overwrite` forces a full re-download.

</details>

<details>
<summary><strong><code>update</code></strong></summary>

Fetches only the bars since the last stored date for symbols that already exist.

If a symbol has no file yet, it bootstraps a full history fetch.

This makes `update` suitable for daily or scheduled maintenance.

</details>

<details>
<summary><strong><code>verify</code></strong></summary>

Re-reads every stored file and checks for:

* sorted, unique dates,
* `High >= Low`,
* positive prices,
* unexpectedly large gaps.

It writes `database/metadata/validation_report.parquet` and exits non-zero if any ERROR-level issue is found.

</details>

---

## Output schema

Every file in `database/raw/<SYMBOL>.parquet` uses the same schema:

| Column     |     Type | Notes                                              |
| ---------- | -------: | -------------------------------------------------- |
| `Date`     | `date32` | calendar date, daily bars                          |
| `Symbol`   | `string` | redundant with filename, useful for combined reads |
| `Open`     | `double` |                                                    |
| `High`     | `double` |                                                    |
| `Low`      | `double` |                                                    |
| `Close`    | `double` |                                                    |
| `AdjClose` | `double` | dividend/split-adjusted close                      |
| `Volume`   |  `int64` | nullable                                           |

### Why this schema

Two deliberate choices make the dataset easier to query:

* `AdjClose` has no spaces, so SQL and C++ code stay simple.
* `Date` is stored as `date32`, since these are daily bars and do not need time-of-day precision.

Because every file shares the same fixed schema, the full `database/raw/` directory can be treated as one logical dataset.

---

## Metadata output

`database/metadata/symbols_metadata.parquet` stores one row per symbol, including:

* `symbol`
* `name`
* `exchange`
* `category`
* `country`
* `status`
* `rows`
* `start_date`
* `end_date`
* `file_size_bytes`
* `attempts`
* `last_error`
* `download_seconds`
* `last_attempt_utc`
* `last_success_utc`

`status` is one of:

* `SUCCESS`
* `NO_DATA`
* `FAILED`

A symbol absent from this file means it has not been attempted yet.

---

## Reading the data

### Python + Polars

```python
import polars as pl

lf = pl.scan_parquet("database/raw/*.parquet")
aapl_2024 = (
    lf.filter((pl.col("Symbol") == "AAPL") & (pl.col("Date") >= pl.date(2024, 1, 1)))
      .collect()
)
```

### Python + pandas

```python
import pandas as pd

df = pd.read_parquet("database/raw/AAPL.parquet")
```

### Python + PyArrow dataset

```python
import pyarrow.dataset as ds

dataset = ds.dataset("database/raw", format="parquet")
table = dataset.to_table(filter=(ds.field("Symbol") == "AAPL"))
```

### DuckDB

```sql
SELECT Date, Close
FROM read_parquet('database/raw/*.parquet')
WHERE Symbol = 'AAPL' AND Date >= DATE '2024-01-01'
ORDER BY Date;
```

### C++ / Arrow

```cpp
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
arrow::dataset::FileSystemFactoryOptions options;
auto factory = arrow::dataset::FileSystemDatasetFactory::Make(
    fs, {"database/raw"}, std::make_shared<arrow::dataset::ParquetFileFormat>(), options
).ValueOrDie();
auto dataset = factory->Finish().ValueOrDie();
auto scanner_builder = dataset->NewScan().ValueOrDie();
scanner_builder->Filter(arrow::compute::equal(
    arrow::compute::field_ref("Symbol"), arrow::compute::literal("AAPL")));
auto table = scanner_builder->Finish().ValueOrDie()->ToTable().ValueOrDie();
```

---

## Extending to another data provider

The pipeline depends only on the abstract `DataProvider` interface:

```python
fetch_ohlcv(symbol, start, end, interval) -> DataFrame
```

To use another source, implement a provider class that returns the same OHLCV shape and raises:

* `NoDataError` when Yahoo confirms there is nothing for that symbol/range,
* `TransientProviderError` for retryable failures such as timeouts, HTTP 429, or connection issues.

Then pass your provider into `PackingEngine(cfg, provider, logger)` instead of `YahooFinanceProvider(...)`.

---

## Testing

Run the test suite with:

```bash
pytest -v
```

All tests run fully offline with a mocked HTTP layer.

Coverage includes:

* data cleaning and validation,
* retry and backoff logic,
* `NoDataError` vs `TransientProviderError`,
* Parquet round-tripping,
* config loading and path resolution,
* CSV universe loading,
* metadata persistence,
* download/update/verify flows,
* resumability,
* retry-queue recovery.

---

## Storage planning

Actual per-symbol file size depends on:

* how much history is available,
* the number of rows per symbol,
* and how well compression works.

A practical estimate method is:

1. run `download --limit 200`
2. check `du -sh database/raw`
3. divide by 200 to estimate average file size
4. multiply by the full universe size

`lookback_days` in `config.yaml` is the biggest lever on total disk usage and runtime.

---

## Provider notes

`YahooFinanceProvider` uses Yahoo’s public chart endpoint directly via `requests`, rather than the `yfinance` package.

That keeps the implementation aligned with the environment where direct requests are known to work reliably.

---

## Suggested repo name

If you want a polished public-facing name, use:

**`ohlcv-packing-pipeline`**

A few alternates:

* `yahoo-ohlcv-packer`
* `bulk-ohlcv-ingestion`
* `ticker-universe-pipeline`

---

## Author

**Rushidev Gajanan Ukalkar**
Email: [rushidevjain@gmail.com](mailto:rushidevjain@gmail.com)
Telegram: [@uku_jain](https://t.me/uku_jain)

---

## License

### MIT copyright 2026, Open Source.
