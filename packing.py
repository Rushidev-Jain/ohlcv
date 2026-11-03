"""
packing.py
================================================================================
Bulk historical OHLCV ingestion pipeline for a large Yahoo Finance ticker
universe (~106k symbols), producing one Apache Parquet file per symbol under
`database/raw/` plus a queryable Parquet metadata catalog.

Design goals
------------
  * Resumable  -- re-running `download` skips symbols already downloaded
                  successfully; interrupting with Ctrl+C leaves partial
                  progress intact and safely resumable.
  * Fast-failing on empty data -- a confirmed "no data for this symbol"
    response is never retried, so a universe containing many delisted /
    invalid tickers doesn't multiply the total run time.
  * Provider-agnostic -- all network access goes through the abstract
    `DataProvider` interface. Swapping Yahoo Finance for another vendor
    means writing one new class, nothing else changes.
  * Safe for concurrent, long-running batch jobs -- atomic file writes,
    thread-safe metadata, a rate limiter shared across worker threads, and
    a dedicated retry-queue pass for transient failures.
  * Built to be read quickly by other tools -- every per-symbol file shares
    one fixed schema (see OHLCV_SCHEMA) so the whole `database/raw/`
    directory can be opened as a single logical dataset by pandas, polars,
    DuckDB, or Arrow/C++ (see README.md for examples).

CLI usage
---------
    python packing.py download                  # fetch full history for new symbols
    python packing.py download --overwrite       # re-fetch everything, ignore cache
    python packing.py update                     # fetch only new bars since last run
    python packing.py verify                     # audit stored files, write a report

    # Useful on every subcommand:
    python packing.py download --limit 50         # smoke-test on the first 50 symbols
    python packing.py download --symbols AAPL,MSFT # restrict to specific tickers
    python packing.py download --workers 32        # override config.yaml's max_workers
    python packing.py download --config other.yaml # use a different config file

See README.md for the full walkthrough, folder layout, and cross-language
(Python / polars / DuckDB / C++ Arrow) read examples.

NOTE ON THE PROVIDER IMPLEMENTATION: this deliberately mirrors the exact
`requests`-based call pattern proven to work in this environment (see the
supplied `data_loader.py`) against Yahoo's public chart endpoint, rather
than the `yfinance` PyPI package -- per the brief, that package is not
usable here. See YahooFinanceProvider below.
"""

from __future__ import annotations

# ------------------------------------------------------------------------- #
# Standard library
# ------------------------------------------------------------------------- #
import argparse
import json
import logging
import logging.handlers
import os
import random
import re
import sys
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Literal

# ------------------------------------------------------------------------- #
# Third-party
# ------------------------------------------------------------------------- #
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml
from tqdm import tqdm

try:
    import polars as pl
    _HAS_POLARS = True
except ImportError:  # pragma: no cover - defensive fallback only
    _HAS_POLARS = False


 
# Exceptions
 

class ProviderError(Exception):
    """Base class for all data-provider errors."""


class TransientProviderError(ProviderError):
    """
    A retryable failure: network hiccup, timeout, HTTP 429/5xx, malformed
    response, etc. `fetch_with_retries` will back off and try again.
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        # If the server told us how long to wait (HTTP `Retry-After`), honor it.
        self.retry_after = retry_after


class NoDataError(ProviderError):
    """
    Terminal failure: the provider has *confirmed* there is nothing to fetch
    (delisted ticker, invalid symbol, empty result for the requested range).

    This is deliberately NEVER retried. Across a ~106k-symbol universe there
    will always be a meaningful fraction of invalid/delisted tickers, and
    retrying each one `max_retries` times with backoff would multiply total
    run time for zero benefit. Detecting this quickly (before doing any
    expensive parsing/cleaning) is a core requirement of this pipeline.
    """


 
# Status vocabulary
 

Status = Literal["SUCCESS", "NO_DATA", "FAILED", "SKIPPED"]


 
# Abstract provider interface (future extensibility point)
 

class DataProvider(ABC):
    """
    Abstract interface for any OHLCV data source.

    Everything downstream (retry logic, rate limiting, cleaning, storage,
    metadata, the CLI) depends only on this interface, never on Yahoo
    Finance specifics. To add a new data source (a paid vendor, an
    internal feed, a different free API) implement one class:

        class MyVendorProvider(DataProvider):
            def fetch_ohlcv(self, symbol, start, end, interval="1d"):
                ...

    and pass an instance of it into `PackingEngine(cfg, provider, logger)`.
    """

    @abstractmethod
    def fetch_ohlcv(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        Fetch raw OHLCV bars for `symbol` in [start, end] (tz-aware UTC
        datetimes). Must return a DataFrame indexed by a tz-naive
        DatetimeIndex named "Date" with columns:
        Open, High, Low, Close, Adj Close, Volume.

        Raises:
            NoDataError: confirmed empty / invalid symbol -- do not retry.
            TransientProviderError: retryable failure.
        """
        raise NotImplementedError


class YahooFinanceProvider(DataProvider):
    """
    Concrete provider that talks directly to Yahoo Finance's public chart
    endpoint using plain `requests` calls -- the same request shape as the
    reference `data_loader.py` -- rather than the `yfinance` package.

    A single `requests.Session` (with an enlarged connection pool) is
    reused across all calls a worker thread makes, since this pipeline can
    issue tens of thousands of requests to the same host.
    """

    CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    def __init__(
        self,
        timeout_seconds: float = 30.0,
        user_agent: str = "Mozilla/5.0",
        pool_size: int = 32,
    ) -> None:
        self._timeout = timeout_seconds
        self._headers = {
            "User-Agent": user_agent,
            "Accept": "application/json,text/plain,*/*",
        }
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            # We do our own retry/backoff (with NoData vs Transient
            # classification based on the response body) -- disable
            # urllib3's built-in retries so the two mechanisms don't fight.
            max_retries=0,
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def close(self) -> None:
        self._session.close()

    def fetch_ohlcv(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> pd.DataFrame:
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": interval,
            "includePrePost": "false",
            "includeAdjustedClose": "true",
            "events": "div,splits",
        }
        url = self.CHART_URL.format(symbol=symbol)

        try:
            r = self._session.get(url, params=params, headers=self._headers, timeout=self._timeout)
        except requests.exceptions.RequestException as exc:
            raise TransientProviderError(f"{symbol}: network error: {exc}") from exc

        if r.status_code == 404:
            raise NoDataError(f"{symbol}: HTTP 404 (symbol not found)")
        if r.status_code == 429:
            retry_after = None
            header_val = r.headers.get("Retry-After")
            if header_val:
                try:
                    retry_after = float(header_val)
                except ValueError:
                    retry_after = None
            raise TransientProviderError(f"{symbol}: HTTP 429 rate limited", retry_after=retry_after)
        if 500 <= r.status_code < 600:
            raise TransientProviderError(f"{symbol}: HTTP {r.status_code}")
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise TransientProviderError(f"{symbol}: HTTP error: {exc}") from exc

        try:
            data = r.json()
        except ValueError as exc:
            raise TransientProviderError(f"{symbol}: invalid JSON response: {exc}") from exc

        chart = data.get("chart", {})
        if chart.get("error"):
            # Yahoo returns HTTP 200 with an embedded error object for
            # invalid/delisted tickers -- this is a confirmed "no data",
            # not something worth retrying.
            raise NoDataError(f"{symbol}: {chart['error']}")

        result = chart.get("result")
        if not result:
            raise NoDataError(f"{symbol}: empty result payload")

        result = result[0]
        timestamps = result.get("timestamp", [])
        if not timestamps:
            # *** The "does this symbol actually have data?" fast check ***
            # An empty timestamp array means Yahoo has nothing at all for
            # this symbol/range. Fail fast here instead of retrying or
            # falling through into DataFrame construction.
            raise NoDataError(f"{symbol}: no timestamps in requested range")

        indicators = result.get("indicators", {})
        quote_list = indicators.get("quote", [{}])
        quote = quote_list[0] if quote_list else {}
        adjclose_list = indicators.get("adjclose", [{}])
        adjclose = (
            adjclose_list[0].get("adjclose", [None] * len(timestamps))
            if adjclose_list else [None] * len(timestamps)
        )

        closes = quote.get("close") or []
        if not closes or all(c is None for c in closes):
            # Timestamps exist but every close is null -- effectively no
            # usable data. Also terminal, not retryable.
            raise NoDataError(f"{symbol}: all close prices are null")

        df = pd.DataFrame({
            "Date": pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None),
            "Open": quote.get("open"),
            "High": quote.get("high"),
            "Low": quote.get("low"),
            "Close": quote.get("close"),
            "Adj Close": adjclose,
            "Volume": quote.get("volume"),
        })
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        return df


 
# Retry / backoff / rate limiting
 

def compute_backoff_seconds(attempt: int, base: float, cap: float) -> float:
    """
    Exponential backoff with full jitter (attempt is zero-indexed).
    See: AWS Architecture Blog, "Exponential Backoff And Jitter".
    """
    exp = min(cap, base * (2 ** attempt))
    return random.uniform(0, exp)


def fetch_with_retries(
    call: Callable[[], pd.DataFrame],
    symbol: str,
    max_retries: int,
    backoff_base: float,
    backoff_cap: float,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Invoke the zero-arg `call` (expected to perform exactly one rate-limited
    provider request), retrying with exponential backoff + jitter on
    TransientProviderError. NoDataError is propagated immediately without
    retrying -- see NoDataError's docstring.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return call()
        except NoDataError:
            raise
        except TransientProviderError as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = compute_backoff_seconds(attempt, backoff_base, backoff_cap)
                if exc.retry_after:
                    delay = max(delay, exc.retry_after)
                logger.debug(
                    "%s: transient error, retry %d/%d in %.1fs (%s)",
                    symbol, attempt + 1, max_retries, delay, exc,
                )
                time.sleep(delay)
    assert last_exc is not None
    raise last_exc


class RateLimiter:
    """
    Thread-safe token-bucket-style limiter shared by every worker thread,
    so the pipeline's total request rate against Yahoo stays under a
    configured ceiling regardless of how many threads are running.
    """

    def __init__(self, rate_per_sec: float) -> None:
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._interval
        if wait > 0:
            time.sleep(wait)


class RetryQueue:
    """
    Thread-safe holding area for symbols whose retries were exhausted
    during a pass with a *transient* error. These are worth attempting
    again in a later, lower-concurrency pass after a cooldown, since the
    most common cause is the whole batch being briefly rate-limited.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: list[str] = []

    def add(self, symbol: str) -> None:
        with self._lock:
            self._items.append(symbol)

    def drain(self) -> list[str]:
        with self._lock:
            items, self._items = self._items, []
        return items

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


 
# Canonical on-disk schemas
 
# Every per-symbol file shares this EXACT schema, which is what lets a
# backtesting engine open the whole `database/raw/` directory as a single
# logical dataset (pandas/polars/DuckDB/pyarrow.dataset) instead of reading
# files one at a time. Column names avoid spaces (unlike the raw Yahoo/
# pandas "Adj Close" convention) so they don't need quoting in SQL engines
# or C++ code. Date is stored as date32 (no time component) since these are
# daily bars -- smaller and semantically correct.

OHLCV_SCHEMA = pa.schema([
    pa.field("Date", pa.date32(), nullable=False),
    pa.field("Symbol", pa.string(), nullable=False),
    pa.field("Open", pa.float64()),
    pa.field("High", pa.float64()),
    pa.field("Low", pa.float64()),
    pa.field("Close", pa.float64()),
    pa.field("AdjClose", pa.float64()),
    pa.field("Volume", pa.int64()),
])

METADATA_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("name", pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("category", pa.string()),
    pa.field("country", pa.string()),
    pa.field("status", pa.string()),
    pa.field("rows", pa.int64()),
    pa.field("start_date", pa.date32()),
    pa.field("end_date", pa.date32()),
    pa.field("file_size_bytes", pa.int64()),
    pa.field("attempts", pa.int32()),
    pa.field("last_error", pa.string()),
    pa.field("download_seconds", pa.float64()),
    pa.field("last_attempt_utc", pa.timestamp("us")),
    pa.field("last_success_utc", pa.timestamp("us")),
])

VALIDATION_REPORT_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("severity", pa.string()),
    pa.field("message", pa.string()),
    pa.field("checked_at", pa.timestamp("us")),
])


 
# Small data types
 

@dataclass(frozen=True)
class SymbolInfo:
    """One row of the input ticker universe."""
    ticker: str
    name: str = ""
    exchange: str = ""
    category: str = ""
    country: str = ""


@dataclass
class SymbolResult:
    """Outcome of processing a single symbol, used to update metadata + stats."""
    symbol: str
    status: Status
    rows: int = 0
    start_date: date | None = None
    end_date: date | None = None
    file_size_bytes: int = 0
    attempts: int = 0
    error: str = ""
    duration_seconds: float = 0.0


@dataclass
class ValidationIssue:
    symbol: str
    severity: Literal["ERROR", "WARNING"]
    message: str


 
# Filesystem-safe naming
 

_UNSAFE_CHARS = re.compile(r'[\\/:*?"<>|\s]')


def sanitize_filename(symbol: str) -> str:
    """
    Yahoo tickers can contain characters that are unsafe or awkward in
    filenames across platforms (e.g. '^GSPC', 'EURUSD=X'). This mapping is
    deterministic, so a given symbol always resolves to the same file.
    """
    cleaned = _UNSAFE_CHARS.sub("_", symbol.strip())
    return cleaned or "_EMPTY_"


 
# Cleaning & validation
 

def is_effectively_empty(df: pd.DataFrame | None) -> bool:
    """
    Cheap check applied right after a provider call returns successfully:
    is there actually anything usable here, or are we about to persist (and
    later read back) a file with nothing in it?
    """
    if df is None or df.empty:
        return True
    price_cols = [c for c in ("Open", "High", "Low", "Close") if c in df.columns]
    if not price_cols:
        return True
    return bool(df[price_cols].isna().all(axis=None))


def clean_ohlcv(df: pd.DataFrame, symbol: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Normalize a raw provider DataFrame into the canonical on-disk shape:
      - Date becomes a plain column (python `date`), not an index
      - a Symbol column is added (needed once files are read together)
      - duplicate dates are removed (last occurrence wins)
      - sorted ascending by Date
      - rows where every price field is NaN are dropped
      - canonical dtypes are applied (float64 prices, nullable Int64 volume)
    """
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex):
        if "Date" in out.columns:
            # A "Date" column already exists (e.g. clean_ohlcv is called on
            # data that was already normalized) -- drop the redundant
            # DatetimeIndex instead of colliding with reset_index().
            out = out.reset_index(drop=True)
        else:
            out = out.reset_index()
            if "index" in out.columns and "Date" not in out.columns:
                out = out.rename(columns={"index": "Date"})

    if "Date" not in out.columns:
        raise ValueError(f"{symbol}: cleaned frame has no Date column")

    out["Date"] = pd.to_datetime(out["Date"]).dt.normalize()

    price_cols = [c for c in ("Open", "High", "Low", "Close") if c in out.columns]
    before = len(out)
    if price_cols:
        out = out.dropna(subset=price_cols, how="all")
    dropped = before - len(out)
    if dropped:
        logger.debug("%s: dropped %d all-NaN row(s)", symbol, dropped)

    dupes = int(out["Date"].duplicated(keep="last").sum())
    if dupes:
        logger.debug("%s: removed %d duplicate date row(s)", symbol, dupes)
        out = out.drop_duplicates(subset="Date", keep="last")

    out = out.sort_values("Date").reset_index(drop=True)
    out["Symbol"] = symbol

    for col in ("Open", "High", "Low", "Close"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64") if col in out.columns else np.nan

    if "Adj Close" in out.columns:
        out["AdjClose"] = pd.to_numeric(out["Adj Close"], errors="coerce").astype("float64")
    elif "AdjClose" in out.columns:
        out["AdjClose"] = pd.to_numeric(out["AdjClose"], errors="coerce").astype("float64")
    else:
        out["AdjClose"] = np.nan

    if "Volume" in out.columns:
        out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce").astype("Int64")
    else:
        out["Volume"] = pd.array([pd.NA] * len(out), dtype="Int64")

    out["Date"] = out["Date"].dt.date
    return out[["Date", "Symbol", "Open", "High", "Low", "Close", "AdjClose", "Volume"]]


def validate_ohlcv(df: pd.DataFrame, symbol: str, max_allowed_gap_days: int) -> list[ValidationIssue]:
    """
    Audit an already-cleaned OHLCV DataFrame (as read back from Parquet).
    Used by the `verify` CLI command. ERROR-level issues mark a symbol as
    failing verification; WARNING-level issues are informational.
    """
    issues: list[ValidationIssue] = []
    if df is None or df.empty:
        issues.append(ValidationIssue(symbol, "ERROR", "no rows"))
        return issues

    dates = pd.to_datetime(df["Date"])
    if not dates.is_monotonic_increasing:
        issues.append(ValidationIssue(symbol, "ERROR", "dates are not sorted ascending"))

    dup = int(dates.duplicated().sum())
    if dup:
        issues.append(ValidationIssue(symbol, "ERROR", f"{dup} duplicate date(s)"))

    price_cols = [c for c in ("Open", "High", "Low", "Close") if c in df.columns]
    if price_cols:
        non_positive = int((df[price_cols] <= 0).any(axis=1).sum())
        if non_positive:
            issues.append(ValidationIssue(symbol, "WARNING", f"{non_positive} row(s) with non-positive price"))

    if {"High", "Low"}.issubset(df.columns):
        bad_hl = int((df["High"] < df["Low"]).sum())
        if bad_hl:
            issues.append(ValidationIssue(symbol, "ERROR", f"{bad_hl} row(s) where High < Low"))

    gaps = dates.diff().dt.days.dropna()
    big_gaps = gaps[gaps > max_allowed_gap_days]
    if len(big_gaps):
        issues.append(ValidationIssue(
            symbol, "WARNING", f"{len(big_gaps)} gap(s) exceeding {max_allowed_gap_days} calendar days",
        ))

    return issues


 
# Parquet I/O
 

def write_parquet_atomic(df: pd.DataFrame, path: Path, compression: str, compression_level: int) -> int:
    """
    Write `df` to `path` using OHLCV_SCHEMA, via a temp-file-then-rename so
    a reader (or a crashed/interrupted run) never observes a half-written
    file. Returns the resulting file size in bytes.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=OHLCV_SCHEMA, preserve_index=False)
    tmp_path = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    pq.write_table(table, tmp_path, compression=compression, compression_level=compression_level)
    os.replace(tmp_path, path)  # atomic on POSIX and Windows
    return path.stat().st_size


def read_symbol_parquet(path: Path) -> pd.DataFrame:
    """
    Read a per-symbol OHLCV Parquet file back into the same in-memory shape
    `clean_ohlcv()` produces (Date as python `date`, Volume as nullable
    Int64), so callers can safely concat/compare without dtype surprises.
    """
    df = pq.read_table(path).to_pandas(date_as_object=True)
    if "Volume" in df.columns:
        df["Volume"] = df["Volume"].astype("Int64")
    return df


def write_validation_report(issues: list[ValidationIssue], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    table = pa.Table.from_pydict(
        {
            "symbol": [i.symbol for i in issues],
            "severity": [i.severity for i in issues],
            "message": [i.message for i in issues],
            "checked_at": [now] * len(issues),
        },
        schema=VALIDATION_REPORT_SCHEMA,
    )
    pq.write_table(table, path, compression="zstd")


 
# Configuration
 

DEFAULT_CONFIG: dict[str, Any] = {
    "data": {
        "symbols_csv": "Yahoo-Finance-Ticker-Symbols.csv",
        "raw_dir": "database/raw",
        "metadata_dir": "database/metadata",
        "log_dir": "logs",
    },
    "download": {
        "lookback_days": 7300,
        "interval": "1d",
        "request_timeout_seconds": 30,
        "max_retries": 5,
        "retry_backoff_base_seconds": 1.5,
        "retry_backoff_max_seconds": 60,
        "retry_queue_passes": 2,
        "retry_queue_cooldown_seconds": 30,
        "max_workers": 16,
        "rate_limit_per_sec": 8,
        "user_agent": "Mozilla/5.0",
        "metadata_flush_every": 250,
    },
    "storage": {
        "compression": "zstd",
        "compression_level": 9,
    },
    "logging": {
        "level": "INFO",
        "console": True,
        "file": True,
        "max_bytes": 10_485_760,
        "backup_count": 5,
    },
    "validation": {
        "max_allowed_gap_days": 10,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@dataclass
class AppConfig:
    config_path: Path
    symbols_csv: Path
    raw_dir: Path
    metadata_dir: Path
    log_dir: Path
    lookback_days: int
    interval: str
    request_timeout_seconds: float
    max_retries: int
    retry_backoff_base_seconds: float
    retry_backoff_max_seconds: float
    retry_queue_passes: int
    retry_queue_cooldown_seconds: float
    max_workers: int
    rate_limit_per_sec: float
    user_agent: str
    metadata_flush_every: int
    compression: str
    compression_level: int
    log_level: str
    log_console: bool
    log_file: bool
    log_max_bytes: int
    log_backup_count: int
    max_allowed_gap_days: int

    @property
    def metadata_parquet_path(self) -> Path:
        return self.metadata_dir / "symbols_metadata.parquet"


def load_config(config_path: str | Path) -> AppConfig:
    config_path = Path(config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}
    cfg = _deep_merge(DEFAULT_CONFIG, user_cfg)
    base_dir = config_path.parent

    def resolve(p: str) -> Path:
        pp = Path(p)
        return pp if pp.is_absolute() else (base_dir / pp).resolve()

    d, dl, st, lg, vl = cfg["data"], cfg["download"], cfg["storage"], cfg["logging"], cfg["validation"]
    return AppConfig(
        config_path=config_path,
        symbols_csv=resolve(d["symbols_csv"]),
        raw_dir=resolve(d["raw_dir"]),
        metadata_dir=resolve(d["metadata_dir"]),
        log_dir=resolve(d["log_dir"]),
        lookback_days=int(dl["lookback_days"]),
        interval=str(dl["interval"]),
        request_timeout_seconds=float(dl["request_timeout_seconds"]),
        max_retries=int(dl["max_retries"]),
        retry_backoff_base_seconds=float(dl["retry_backoff_base_seconds"]),
        retry_backoff_max_seconds=float(dl["retry_backoff_max_seconds"]),
        retry_queue_passes=int(dl["retry_queue_passes"]),
        retry_queue_cooldown_seconds=float(dl["retry_queue_cooldown_seconds"]),
        max_workers=int(dl["max_workers"]),
        rate_limit_per_sec=float(dl["rate_limit_per_sec"]),
        user_agent=str(dl["user_agent"]),
        metadata_flush_every=int(dl["metadata_flush_every"]),
        compression=str(st["compression"]),
        compression_level=int(st["compression_level"]),
        log_level=str(lg["level"]),
        log_console=bool(lg["console"]),
        log_file=bool(lg["file"]),
        log_max_bytes=int(lg["max_bytes"]),
        log_backup_count=int(lg["backup_count"]),
        max_allowed_gap_days=int(vl["max_allowed_gap_days"]),
    )


 
# Logging
 

def setup_logging(cfg: AppConfig, log_level_override: str | None = None) -> logging.Logger:
    logger = logging.getLogger("packing")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    level_name = log_level_override or cfg.log_level
    level = getattr(logging, level_name.upper(), logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(message)s")

    if cfg.log_console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    if cfg.log_file:
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            cfg.log_dir / "packing.log",
            maxBytes=cfg.log_max_bytes,
            backupCount=cfg.log_backup_count,
            encoding="utf-8",
        )
        fh.setLevel(logging.DEBUG)  # keep full detail on disk even if console is quieter
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


 
# Symbol universe loading
 

def load_symbol_universe(csv_path: Path, logger: logging.Logger) -> list[SymbolInfo]:
    """
    Read the ticker universe CSV (columns: Ticker,Name,Exchange,Category
    Name,Country). Uses polars when available (fast on 100k+ row CSVs),
    falling back to pandas so the pipeline still runs without it.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Symbols CSV not found at {csv_path}. Update `data.symbols_csv` in "
            f"your config.yaml, or place the file at this path."
        )

    if _HAS_POLARS:
        raw = pl.read_csv(csv_path, infer_schema_length=10_000)
        columns = raw.columns
        n = raw.height
    else:  # pragma: no cover - exercised only when polars is absent
        raw = pd.read_csv(csv_path)
        columns = list(raw.columns)
        n = len(raw)

    if "Ticker" not in columns:
        raise ValueError(f"Expected a 'Ticker' column in {csv_path}, found: {columns}")

    def col(name: str) -> list:
        if name not in columns:
            return [""] * n
        series = raw[name]
        return series.to_list() if _HAS_POLARS else series.tolist()

    tickers = col("Ticker")
    names = col("Name")
    exchanges = col("Exchange")
    categories = col("Category Name")
    countries = col("Country")

    seen: set[str] = set()
    universe: list[SymbolInfo] = []
    skipped = 0
    for t, nm, ex, cat, ctry in zip(tickers, names, exchanges, categories, countries):
        ticker = "" if t is None else str(t).strip()
        if not ticker or ticker.lower() == "nan":
            skipped += 1
            continue
        if ticker in seen:
            skipped += 1
            continue
        seen.add(ticker)
        universe.append(SymbolInfo(
            ticker=ticker,
            name="" if nm is None else str(nm).strip(),
            exchange="" if ex is None else str(ex).strip(),
            category="" if cat is None else str(cat).strip(),
            country="" if ctry is None else str(ctry).strip(),
        ))

    logger.info(
        "Loaded %d unique symbols from %s (%d blank/duplicate row(s) skipped)",
        len(universe), csv_path, skipped,
    )
    return universe


 
# Metadata store
 

class MetadataStore:
    """
    One record per symbol (status, row count, date range, file size, error
    history) persisted to a single Parquet file:
    `<metadata_dir>/symbols_metadata.parquet`.

    This is the source of truth that makes `download` resumable and
    `verify` auditable, and doubles as a catalog for filtering the universe
    (e.g. "every NASDAQ tech ticker that downloaded successfully") without
    opening a single OHLCV file.
    """

    def __init__(self, path: Path, flush_every: int = 250) -> None:
        self._path = path
        self._flush_every = max(1, flush_every)
        self._lock = threading.Lock()
        self._records: dict[str, dict[str, Any]] = {}
        self._dirty_count = 0
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            table = pq.read_table(self._path)
            for row in table.to_pylist():
                self._records[row["symbol"]] = row

    def get(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            rec = self._records.get(symbol)
            return dict(rec) if rec else None

    def is_downloaded(self, symbol: str) -> bool:
        rec = self.get(symbol)
        return bool(rec and rec.get("status") == "SUCCESS")

    def update(self, symbol: str, **fields: Any) -> None:
        with self._lock:
            rec = self._records.setdefault(symbol, {"symbol": symbol})
            rec.update(fields)
            self._dirty_count += 1
            should_flush = self._dirty_count >= self._flush_every
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            if not self._records:
                return
            records = list(self._records.values())
            self._dirty_count = 0
        columns: dict[str, list[Any]] = {f.name: [] for f in METADATA_SCHEMA}
        for rec in records:
            for f in METADATA_SCHEMA:
                columns[f.name].append(rec.get(f.name))
        table = pa.Table.from_pydict(columns, schema=METADATA_SCHEMA)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, self._path)

    def all_records(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._records.values()]


 
# Run statistics
 

@dataclass
class RunStats:
    command: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    total: int = 0
    success: int = 0
    no_data: int = 0
    failed: int = 0
    skipped: int = 0
    retried_ok: int = 0
    rows_written: int = 0
    bytes_written: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def bump(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def to_dict(self) -> dict[str, Any]:
        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return {
            "command": self.command,
            "started_at": self.started_at.isoformat(),
            "elapsed_seconds": elapsed,
            "total": self.total,
            "success": self.success,
            "no_data": self.no_data,
            "failed": self.failed,
            "skipped": self.skipped,
            "retried_ok": self.retried_ok,
            "rows_written": self.rows_written,
            "bytes_written": self.bytes_written,
            "symbols_per_second": (self.total / elapsed) if elapsed > 0 else 0.0,
        }

    def render_summary(self) -> str:
        d = self.to_dict()
        mb = d["bytes_written"] / (1024 * 1024)
        lines = [
            "",
            "=" * 62,
            f" packing.py -- '{self.command}' run summary",
            "=" * 62,
            f" Elapsed time             : {d['elapsed_seconds']:,.1f} s",
            f" Symbols processed        : {d['total']:,}",
            f"   - success              : {d['success']:,}",
            f"   - no data (empty)      : {d['no_data']:,}",
            f"   - failed               : {d['failed']:,}",
            f"   - skipped (resumed)    : {d['skipped']:,}",
            f"   - recovered on retry   : {d['retried_ok']:,}",
            f" Rows written             : {d['rows_written']:,}",
            f" Data written             : {mb:,.2f} MB",
            f" Throughput               : {d['symbols_per_second']:,.2f} symbols/s",
            "=" * 62,
        ]
        return "\n".join(lines)


 
# Engine: orchestrates threading, retries, metadata, stats
 

class PackingEngine:
    """Runs download / update / verify batches over a symbol universe."""

    def __init__(self, cfg: AppConfig, provider: DataProvider, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.provider = provider
        self.logger = logger
        self.metadata = MetadataStore(cfg.metadata_parquet_path, cfg.metadata_flush_every)
        self.rate_limiter = RateLimiter(cfg.rate_limit_per_sec)

    def _raw_path(self, symbol: str) -> Path:
        return self.cfg.raw_dir / f"{sanitize_filename(symbol)}.parquet"

    # -- per-symbol workers --------------------------------------------- #

    def _process_download(self, info: SymbolInfo, overwrite: bool) -> SymbolResult:
        symbol = info.ticker
        path = self._raw_path(symbol)
        t0 = time.monotonic()

        if not overwrite and path.exists() and self.metadata.is_downloaded(symbol):
            return SymbolResult(symbol, "SKIPPED")

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=self.cfg.lookback_days)
        attempts = {"n": 0}

        def call() -> pd.DataFrame:
            attempts["n"] += 1
            self.rate_limiter.acquire()
            return self.provider.fetch_ohlcv(symbol, start, now, self.cfg.interval)

        try:
            df = fetch_with_retries(
                call, symbol, self.cfg.max_retries,
                self.cfg.retry_backoff_base_seconds, self.cfg.retry_backoff_max_seconds, self.logger,
            )
        except NoDataError as exc:
            return SymbolResult(symbol, "NO_DATA", attempts=attempts["n"], error=str(exc),
                                 duration_seconds=time.monotonic() - t0)
        except TransientProviderError as exc:
            return SymbolResult(symbol, "FAILED", attempts=attempts["n"], error=str(exc),
                                 duration_seconds=time.monotonic() - t0)

        if is_effectively_empty(df):
            return SymbolResult(symbol, "NO_DATA", attempts=attempts["n"], duration_seconds=time.monotonic() - t0)

        clean = clean_ohlcv(df, symbol, self.logger)
        size = write_parquet_atomic(clean, path, self.cfg.compression, self.cfg.compression_level)
        return SymbolResult(
            symbol, "SUCCESS", rows=len(clean), start_date=clean["Date"].min(), end_date=clean["Date"].max(),
            file_size_bytes=size, attempts=attempts["n"], duration_seconds=time.monotonic() - t0,
        )

    def _process_update(self, info: SymbolInfo) -> SymbolResult:
        symbol = info.ticker
        path = self._raw_path(symbol)
        t0 = time.monotonic()
        attempts = {"n": 0}

        def call(start: datetime, end: datetime) -> pd.DataFrame:
            attempts["n"] += 1
            self.rate_limiter.acquire()
            return self.provider.fetch_ohlcv(symbol, start, end, self.cfg.interval)

        now = datetime.now(timezone.utc)

        if not path.exists():
            # No local history at all yet -- bootstrap with a full lookback fetch.
            start = now - timedelta(days=self.cfg.lookback_days)
            try:
                df = fetch_with_retries(lambda: call(start, now), symbol, self.cfg.max_retries,
                                         self.cfg.retry_backoff_base_seconds, self.cfg.retry_backoff_max_seconds,
                                         self.logger)
            except NoDataError as exc:
                return SymbolResult(symbol, "NO_DATA", attempts=attempts["n"], error=str(exc),
                                     duration_seconds=time.monotonic() - t0)
            except TransientProviderError as exc:
                return SymbolResult(symbol, "FAILED", attempts=attempts["n"], error=str(exc),
                                     duration_seconds=time.monotonic() - t0)
            if is_effectively_empty(df):
                return SymbolResult(symbol, "NO_DATA", attempts=attempts["n"], duration_seconds=time.monotonic() - t0)
            clean = clean_ohlcv(df, symbol, self.logger)
            size = write_parquet_atomic(clean, path, self.cfg.compression, self.cfg.compression_level)
            return SymbolResult(symbol, "SUCCESS", rows=len(clean), start_date=clean["Date"].min(),
                                 end_date=clean["Date"].max(), file_size_bytes=size, attempts=attempts["n"],
                                 duration_seconds=time.monotonic() - t0)

        existing = read_symbol_parquet(path)
        last_date = existing["Date"].max() if not existing.empty else None

        def existing_result(status: Status = "SUCCESS") -> SymbolResult:
            return SymbolResult(
                symbol, status, rows=len(existing),
                start_date=existing["Date"].min() if not existing.empty else None,
                end_date=existing["Date"].max() if not existing.empty else None,
                file_size_bytes=path.stat().st_size, attempts=attempts["n"],
                duration_seconds=time.monotonic() - t0,
            )

        if last_date is not None and last_date >= now.date():
            return existing_result()  # already current -- no network call needed

        start = (
            datetime.combine(last_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            if last_date else now - timedelta(days=self.cfg.lookback_days)
        )

        try:
            new_df = fetch_with_retries(lambda: call(start, now), symbol, self.cfg.max_retries,
                                         self.cfg.retry_backoff_base_seconds, self.cfg.retry_backoff_max_seconds,
                                         self.logger)
        except NoDataError:
            # Nothing new in this narrow window (e.g. weekend/holiday) --
            # normal, not a failure of the symbol as a whole.
            return existing_result()
        except TransientProviderError as exc:
            return SymbolResult(symbol, "FAILED", attempts=attempts["n"], error=str(exc),
                                 duration_seconds=time.monotonic() - t0)

        if is_effectively_empty(new_df):
            return existing_result()

        new_clean = clean_ohlcv(new_df, symbol, self.logger)
        combined = pd.concat([existing, new_clean], ignore_index=True)
        combined = combined.drop_duplicates(subset="Date", keep="last").sort_values("Date").reset_index(drop=True)
        size = write_parquet_atomic(combined, path, self.cfg.compression, self.cfg.compression_level)
        return SymbolResult(symbol, "SUCCESS", rows=len(combined), start_date=combined["Date"].min(),
                             end_date=combined["Date"].max(), file_size_bytes=size, attempts=attempts["n"],
                             duration_seconds=time.monotonic() - t0)

    def _process_verify(self, info: SymbolInfo) -> tuple[SymbolResult, list[ValidationIssue]]:
        symbol = info.ticker
        path = self._raw_path(symbol)
        t0 = time.monotonic()
        if not path.exists():
            issue = ValidationIssue(symbol, "ERROR", "parquet file missing")
            return SymbolResult(symbol, "FAILED", error="file missing",
                                 duration_seconds=time.monotonic() - t0), [issue]
        try:
            df = read_symbol_parquet(path)
        except Exception as exc:  # noqa: BLE001 -- a corrupt file must not abort verify
            issue = ValidationIssue(symbol, "ERROR", f"failed to read parquet: {exc}")
            return SymbolResult(symbol, "FAILED", error=f"unreadable: {exc}",
                                 duration_seconds=time.monotonic() - t0), [issue]

        issues = validate_ohlcv(df, symbol, self.cfg.max_allowed_gap_days)
        status: Status = "FAILED" if any(i.severity == "ERROR" for i in issues) else "SUCCESS"
        return SymbolResult(
            symbol, status, rows=len(df),
            start_date=df["Date"].min() if not df.empty else None,
            end_date=df["Date"].max() if not df.empty else None,
            file_size_bytes=path.stat().st_size, duration_seconds=time.monotonic() - t0,
        ), issues

    # -- metadata / stats bookkeeping ------------------------------------ #

    def _apply_result(self, result: SymbolResult, stats: RunStats, final: bool = True) -> None:
        if result.status == "SKIPPED":
            stats.bump(skipped=1)
            return

        prev = self.metadata.get(result.symbol)
        last_success = prev.get("last_success_utc") if prev else None
        if result.status == "SUCCESS":
            last_success = datetime.now(timezone.utc)

        self.metadata.update(
            result.symbol,
            status=result.status,
            rows=result.rows,
            start_date=result.start_date,
            end_date=result.end_date,
            file_size_bytes=result.file_size_bytes,
            attempts=result.attempts,
            last_error=result.error,
            download_seconds=result.duration_seconds,
            last_attempt_utc=datetime.now(timezone.utc),
            last_success_utc=last_success,
        )

        if result.status == "SUCCESS":
            stats.bump(success=1, rows_written=result.rows, bytes_written=result.file_size_bytes)
        elif result.status == "NO_DATA":
            stats.bump(no_data=1)
        elif result.status == "FAILED" and final:
            stats.bump(failed=1)

    # -- the threaded run loop, shared by download & update --------------- #

    def _run_parallel(
        self,
        symbols: list[SymbolInfo],
        worker: Callable[[SymbolInfo], SymbolResult],
        command: str,
        workers: int,
    ) -> RunStats:
        stats = RunStats(command=command, total=len(symbols))
        by_ticker = {s.ticker: s for s in symbols}

        def run_pass(items: list[SymbolInfo], desc: str, n_workers: int, is_last_pass: bool) -> list[str]:
            to_retry: list[str] = []
            with tqdm(total=len(items), desc=desc, unit="sym") as pbar:
                pool = ThreadPoolExecutor(max_workers=max(1, n_workers), thread_name_prefix="pack")
                try:
                    futures = {pool.submit(worker, info): info for info in items}
                    for fut in as_completed(futures):
                        info = futures[fut]
                        try:
                            result = fut.result()
                        except Exception as exc:  # noqa: BLE001 - one bad symbol must never abort the batch
                            self.logger.exception("%s: worker raised unexpectedly", info.ticker)
                            result = SymbolResult(info.ticker, "FAILED", error=str(exc))

                        recovered = (desc != command) and (result.status == "SUCCESS")
                        if result.status == "FAILED" and not is_last_pass:
                            self._apply_result(result, stats, final=False)
                            to_retry.append(info.ticker)
                        else:
                            self._apply_result(result, stats, final=True)
                            if recovered:
                                stats.bump(retried_ok=1)
                        pbar.update(1)
                        pbar.set_postfix(ok=stats.success, nodata=stats.no_data,
                                          failed=stats.failed, skip=stats.skipped, refresh=False)
                except KeyboardInterrupt:
                    self.logger.warning(
                        "Interrupted -- finishing in-flight requests and saving progress (re-run to resume)..."
                    )
                    # wait=True: Python cannot forcibly kill an already-
                    # running thread, so let in-flight requests (bounded by
                    # request_timeout_seconds) finish naturally; the queued-
                    # but-not-yet-started tasks are still discarded via
                    # cancel_futures=True. This lets us salvage every result
                    # that had actually completed work below, instead of
                    # needlessly redownloading it on the next resumed run.
                    pool.shutdown(wait=True, cancel_futures=True)
                    for fut, info in futures.items():
                        if fut.done() and not fut.cancelled():
                            try:
                                salvaged = fut.result()
                            except BaseException:
                                continue
                            self._apply_result(salvaged, stats, final=True)
                    self.metadata.flush()
                    raise
                finally:
                    pool.shutdown(wait=True)
            return to_retry

        remaining = run_pass(symbols, command, workers, is_last_pass=(self.cfg.retry_queue_passes == 0))

        for i in range(self.cfg.retry_queue_passes):
            if not remaining:
                break
            self.logger.warning(
                "Retry-queue pass %d/%d: %d symbol(s) to retry after a %ss cooldown",
                i + 1, self.cfg.retry_queue_passes, len(remaining), self.cfg.retry_queue_cooldown_seconds,
            )
            time.sleep(self.cfg.retry_queue_cooldown_seconds)
            items = [by_ticker[t] for t in remaining]
            is_last = i == self.cfg.retry_queue_passes - 1
            remaining = run_pass(items, f"{command}-retry-{i + 1}", max(1, workers // 2), is_last_pass=is_last)

        self.metadata.flush()
        return stats

    # -- public entry points ---------------------------------------------- #

    def run_download(self, symbols: list[SymbolInfo], overwrite: bool, workers: int | None = None) -> RunStats:
        workers = workers or self.cfg.max_workers

        def worker(info: SymbolInfo) -> SymbolResult:
            return self._process_download(info, overwrite)

        return self._run_parallel(symbols, worker, "download", workers)

    def run_update(self, symbols: list[SymbolInfo], workers: int | None = None) -> RunStats:
        workers = workers or self.cfg.max_workers
        return self._run_parallel(symbols, self._process_update, "update", workers)

    def run_verify(self, symbols: list[SymbolInfo], workers: int | None = None) -> tuple[RunStats, list[ValidationIssue]]:
        workers = workers or self.cfg.max_workers
        stats = RunStats(command="verify", total=len(symbols))
        all_issues: list[ValidationIssue] = []
        with tqdm(total=len(symbols), desc="verify", unit="sym") as pbar:
            with ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="pack-verify") as pool:
                futures = {pool.submit(self._process_verify, info): info for info in symbols}
                for fut in as_completed(futures):
                    result, issues = fut.result()
                    all_issues.extend(issues)
                    if result.status == "SUCCESS":
                        stats.bump(success=1, rows_written=result.rows)
                    else:
                        stats.bump(failed=1)
                    pbar.update(1)
                    pbar.set_postfix(ok=stats.success, failed=stats.failed, refresh=False)
        return stats, all_issues


 
# CLI
 

def build_arg_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: %(default)s)")
    common.add_argument("--log-level", default=None, help="Override the configured log level")
    common.add_argument("--workers", type=int, default=None, help="Override download.max_workers from config.yaml")
    common.add_argument("--symbols", default=None, help="Comma-separated tickers to restrict this run to")
    common.add_argument("--limit", type=int, default=None, help="Only process the first N selected symbols")

    parser = argparse.ArgumentParser(
        prog="packing.py",
        description="Bulk OHLCV ingestion pipeline for a Yahoo Finance ticker universe.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    dl = sub.add_parser("download", parents=[common], help="Download full history for symbols without a local file yet")
    dl.add_argument("--overwrite", action="store_true", help="Re-download and replace files even if already present")

    sub.add_parser("update", parents=[common],
                    help="Fetch new bars since each symbol's last stored date (bootstraps symbols with no file yet)")

    sub.add_parser("verify", parents=[common],
                    help="Validate stored parquet files and write database/metadata/validation_report.parquet")

    return parser


def _select_symbols(universe: list[SymbolInfo], symbols_arg: str | None, limit: int | None) -> list[SymbolInfo]:
    selected = universe
    if symbols_arg:
        wanted = {s.strip().upper() for s in symbols_arg.split(",") if s.strip()}
        selected = [s for s in universe if s.ticker.upper() in wanted]
    if limit is not None:
        selected = selected[:limit]
    return selected


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    logger = setup_logging(cfg, args.log_level)
    logger.info("packing.py starting -- command=%s config=%s", args.cmd, cfg.config_path)

    try:
        universe = load_symbol_universe(cfg.symbols_csv, logger)
    except (FileNotFoundError, ValueError) as exc:
        logger.error(str(exc))
        return 2

    symbols = _select_symbols(universe, args.symbols, args.limit)
    if not symbols:
        logger.error("No symbols selected -- check --symbols/--limit and the symbols CSV.")
        return 2

    workers = args.workers or cfg.max_workers
    provider = YahooFinanceProvider(cfg.request_timeout_seconds, cfg.user_agent, pool_size=max(32, workers))
    engine = PackingEngine(cfg, provider, logger)

    exit_code = 0
    try:
        if args.cmd == "download":
            stats = engine.run_download(symbols, overwrite=getattr(args, "overwrite", False), workers=workers)
        elif args.cmd == "update":
            stats = engine.run_update(symbols, workers=workers)
        else:  # verify
            stats, issues = engine.run_verify(symbols, workers=workers)
            report_path = cfg.metadata_dir / "validation_report.parquet"
            write_validation_report(issues, report_path)
            logger.info("Validation report written to %s (%d issue(s))", report_path, len(issues))
    except KeyboardInterrupt:
        logger.warning("Aborted by user.")
        return 130
    finally:
        provider.close()

    summary = stats.render_summary()
    logger.info(summary)
    print(summary)

    try:
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        (cfg.log_dir / "last_run_stats.json").write_text(json.dumps(stats.to_dict(), indent=2, default=str))
    except OSError as exc:  # pragma: no cover - best-effort only
        logger.warning("Could not write last_run_stats.json: %s", exc)

    if stats.failed > 0:
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
