from __future__ import annotations

import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import packing as pk



def make_raw_df(n: int = 5, start: str = "2024-01-02", nan_row: int | None = None) -> pd.DataFrame:
    dates = pd.date_range(start, periods=n, freq="D")
    df = pd.DataFrame({
            "Open": [10.0 + i for i in range(n)],
            "High": [11.0 + i for i in range(n)],
            "Low": [9.0 + i for i in range(n)],
            "Close": [10.5 + i for i in range(n)],
            "Adj Close": [10.5 + i for i in range(n)],
            "Volume": [1000 + i for i in range(n)],


        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )

    if nan_row is not None:
        df.iloc[nan_row] = np.nan


    return df


def yahoo_payload(n: int = 3, start_ts: int = 1_704_067_200, all_null_close: bool = False) -> dict:
    ts = [start_ts + i * 86400 for i in range(n)]
    closes = [None] * n if all_null_close else [10.5 + i for i in range(n)]
    return {
        "chart": {
            "error": None,
            "result": [{
                "timestamp": ts,
                "indicators": {
                    "quote": [{
                        "open": [10.0 + i for i in range(n)],
                        "high": [11.0 + i for i in range(n)],
                        "low": [9.0 + i for i in range(n)],
                        "close": closes,
                        "volume": [1000 + i for i in range(n)],
                    }],
                    "adjclose": [{"adjclose": [10.5 + i for i in range(n)]}],
                },
            }],
        }
    }


def mock_response(status_code=200, json_data=None, headers=None, raise_http_error=False):
    resp = MagicMock()
    resp.status_code = status_code

    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    if raise_http_error:

        import requests
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("boom")

    else:
        resp.raise_for_status.return_value = None

    return resp


class FakeProvider(pk.DataProvider):


    def __init__(self, responses: dict[str, object]):
        #res ==> pd.
        self.responses = responses
        self.calls: list[str] = []
        self.lock = threading.Lock()

    def fetch_ohlcv(self, symbol, start, end, interval="1d"):
        with self.lock:
            self.calls.append(symbol)
        resp = self.responses.get(symbol)
        if isinstance(resp, Exception):
            raise resp
        if resp is None:
            raise pk.NoDataError(f"{symbol}: no fixture configured")
        return resp.copy()


@pytest.fixture()
def logger():
    return pk.setup_logging.__wrapped__ if False else _test_logger()


def _test_logger():
    import logging
    lg = logging.getLogger("packing-test")
    lg.addHandler(logging.NullHandler())
    lg.setLevel(logging.DEBUG)
    return lg




class TestSanitizeFilename:
    def test_simple_ticker_unchanged(self):
        assert pk.sanitize_filename("AAPL") == "AAPL"

    def test_caret_index_ticker_is_filesystem_safe_as_is(self):
        

        assert pk.sanitize_filename("^GSPC") == "^GSPC"

    def test_slash_is_replaced(self):
        result = pk.sanitize_filename("BRK/B")
        assert "/" not in result and "\\" not in result

    def test_equals_sign_passes_through(self):
        # = both
        assert pk.sanitize_filename("EURUSD=X") == "EURUSD=X"

    def test_whitespace_stripped_and_replaced(self):
        assert "_" not in pk.sanitize_filename("BRK-B")
        assert pk.sanitize_filename("BRK-B") == "BRK-B"

    def test_empty_symbol_never_produces_empty_filename(self):
        assert pk.sanitize_filename("   ") == "_EMPTY_"

    def test_deterministic(self):
        assert pk.sanitize_filename("^GSPC") == pk.sanitize_filename("^GSPC")





class TestCleaning:
    def test_empty_df_is_empty(self):
        assert pk.is_effectively_empty(pd.DataFrame()) is True

    def test_none_is_empty(self):
        assert pk.is_effectively_empty(None) is True

    def test_all_nan_prices_is_empty(self):
        df = make_raw_df(n=3)
        df[["Open", "High", "Low", "Close"]] = np.nan
        assert pk.is_effectively_empty(df) is True

    def test_valid_df_is_not_empty(self):
        assert pk.is_effectively_empty(make_raw_df()) is False

    def test_clean_produces_canonical_columns(self, ):
        df = clean = pk.clean_ohlcv(make_raw_df(), "AAPL", _test_logger())
        assert list(clean.columns) == ["Date", "Symbol", "Open", "High", "Low", "Close", "AdjClose", "Volume"]

    def test_clean_date_is_python_date(self):
        clean = pk.clean_ohlcv(make_raw_df(), "AAPL", _test_logger())
        assert isinstance(clean["Date"].iloc[0], date)

    def test_clean_symbol_column_filled(self):
        clean = pk.clean_ohlcv(make_raw_df(), "MSFT", _test_logger())
        assert (clean["Symbol"] == "MSFT").all()

    def test_clean_drops_all_nan_rows(self):
        raw = make_raw_df(n=4, nan_row=1)
        clean = pk.clean_ohlcv(raw, "AAPL", _test_logger())
        assert len(clean) == 3

    def test_clean_removes_duplicate_dates_keeps_last(self):
        raw = make_raw_df(n=3)
        dup = raw.iloc[[0]].copy()
        dup["Close"] = 999.0
        raw_with_dup = pd.concat([raw, dup])
        clean = pk.clean_ohlcv(raw_with_dup, "AAPL", _test_logger())
        assert len(clean) == 3
        first_row = clean[clean["Date"] == clean["Date"].min()].iloc[0]
        assert first_row["Close"] == 999.0

    def test_clean_sorts_ascending(self):
        raw = make_raw_df(n=5)
        shuffled = raw.iloc[[3, 1, 4, 0, 2]]
        clean = pk.clean_ohlcv(shuffled, "AAPL", _test_logger())
        assert clean["Date"].is_monotonic_increasing

    def test_clean_volume_is_nullable_int(self):
        clean = pk.clean_ohlcv(make_raw_df(), "AAPL", _test_logger())
        assert str(clean["Volume"].dtype) == "Int64"

    def test_clean_is_idempotent_on_already_clean_input(self):
        once = pk.clean_ohlcv(make_raw_df(), "AAPL", _test_logger())
        # Re-running clean on an already-plain-column frame (as update() does
        # when concatenating) must not raise or reintroduce an index.
        twice_input = once.copy()
        twice_input = twice_input.set_index(pd.to_datetime(twice_input["Date"]))
        twice = pk.clean_ohlcv(twice_input.rename_axis("Date"), "AAPL", _test_logger())
        assert list(twice.columns) == list(once.columns)


class TestValidation:
    def _clean(self, n=5):
        return pk.clean_ohlcv(make_raw_df(n=n), "AAPL", _test_logger())

    def test_valid_frame_has_no_errors(self):
        issues = pk.validate_ohlcv(self._clean(), "AAPL", max_allowed_gap_days=10)
        assert not any(i.severity == "ERROR" for i in issues)

    def test_empty_frame_is_error(self):
        issues = pk.validate_ohlcv(pd.DataFrame(), "AAPL", max_allowed_gap_days=10)
        assert any(i.severity == "ERROR" for i in issues)

    def test_unsorted_dates_flagged(self):
        df = self._clean()
        df = df.iloc[::-1].reset_index(drop=True)
        issues = pk.validate_ohlcv(df, "AAPL", max_allowed_gap_days=10)
        assert any("not sorted" in i.message for i in issues)

    def test_duplicate_dates_flagged(self):
        df = self._clean()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        issues = pk.validate_ohlcv(df, "AAPL", max_allowed_gap_days=10)
        assert any("duplicate" in i.message for i in issues)

    def test_high_below_low_is_error(self):
        df = self._clean()
        df.loc[0, "High"] = 1.0
        df.loc[0, "Low"] = 100.0
        issues = pk.validate_ohlcv(df, "AAPL", max_allowed_gap_days=10)
        assert any("High < Low" in i.message for i in issues)

    def test_large_gap_is_warning_not_error(self):
        df = self._clean(n=3)
        df.loc[2, "Date"] = df.loc[0, "Date"] + timedelta(days=60)
        issues = pk.validate_ohlcv(df, "AAPL", max_allowed_gap_days=10)
        gap_issues = [i for i in issues if "gap" in i.message]
        assert len(gap_issues) == 1
        assert gap_issues[0].severity == "WARNING"



class TestBackoff:
    def test_never_exceeds_cap(self):
        for attempt in range(20):
            delay = pk.compute_backoff_seconds(attempt, base=1.0, cap=10.0)
            assert 0 <= delay <= 10.0

    def test_grows_with_attempt_on_average(self):
        random_vals_low = [pk.compute_backoff_seconds(0, base=1.0, cap=1000.0) for _ in range(200)]
        random_vals_high = [pk.compute_backoff_seconds(6, base=1.0, cap=1000.0) for _ in range(200)]
        assert sum(random_vals_high) / len(random_vals_high) > sum(random_vals_low) / len(random_vals_low)



# fetch_with_retries
 

class TestFetchWithRetries:
    def test_succeeds_on_first_try(self):
        calls = {"n": 0}

        def call():
            calls["n"] += 1
            return pd.DataFrame({"Close": [1.0]})

        result = pk.fetch_with_retries(call, "AAPL", max_retries=3, backoff_base=0.001,
                                        backoff_cap=0.01, logger=_test_logger())
        assert calls["n"] == 1
        assert not result.empty

    def test_retries_transient_then_succeeds(self):
        calls = {"n": 0}

        def call():
            calls["n"] += 1
            if calls["n"] < 3:
                raise pk.TransientProviderError("temporary")
            return pd.DataFrame({"Close": [1.0]})

        result = pk.fetch_with_retries(call, "AAPL", max_retries=5, backoff_base=0.001,
                                        backoff_cap=0.01, logger=_test_logger())
        assert calls["n"] == 3
        assert not result.empty

    def test_gives_up_after_max_retries(self):
        calls = {"n": 0}

        def call():
            calls["n"] += 1
            raise pk.TransientProviderError("always fails")

        with pytest.raises(pk.TransientProviderError):
            pk.fetch_with_retries(call, "AAPL", max_retries=2, backoff_base=0.001,
                                   backoff_cap=0.01, logger=_test_logger())
        assert calls["n"] == 3  # initial attempt + 2 retries

    def test_no_data_error_is_never_retried(self):
        calls = {"n": 0}

        def call():
            calls["n"] += 1
            raise pk.NoDataError("confirmed empty")

        with pytest.raises(pk.NoDataError):
            pk.fetch_with_retries(call, "AAPL", max_retries=5, backoff_base=0.001,
                                   backoff_cap=0.01, logger=_test_logger())
        assert calls["n"] == 1  # must NOT retry


 
# RateLimiter
 

class TestRateLimiter:
    def test_zero_rate_never_blocks(self):
        rl = pk.RateLimiter(rate_per_sec=0)
        t0 = time.monotonic()
        for _ in range(5):
            rl.acquire()
        assert time.monotonic() - t0 < 0.05

    def test_limits_throughput_roughly(self):
        rate = 50.0  # interval = 0.02s
        rl = pk.RateLimiter(rate_per_sec=rate)
        t0 = time.monotonic()
        for _ in range(6):
            rl.acquire()
        elapsed = time.monotonic() - t0
        # 6 calls -> at least 5 intervals of spacing, with generous slack for CI jitter
        assert elapsed >= (5 * (1.0 / rate)) * 0.7


 
# RetryQueue
 

class TestRetryQueue:
    def test_add_and_drain(self):
        q = pk.RetryQueue()
        q.add("AAPL")
        q.add("MSFT")
        assert len(q) == 2
        drained = q.drain()
        assert set(drained) == {"AAPL", "MSFT"}
        assert len(q) == 0

    def test_drain_empties_queue(self):
        q = pk.RetryQueue()
        q.add("AAPL")
        q.drain()
        assert q.drain() == []


 
# YahooFinanceProvider (mocked HTTP)
 

class TestYahooFinanceProvider:
    def _provider(self):
        return pk.YahooFinanceProvider(timeout_seconds=5, user_agent="test-agent", pool_size=4)

    def test_success_parses_dataframe(self):
        provider = self._provider()
        with patch.object(provider._session, "get", return_value=mock_response(json_data=yahoo_payload(n=3))):
            df = provider.fetch_ohlcv("AAPL", datetime.now(timezone.utc) - timedelta(days=5),
                                       datetime.now(timezone.utc))
        assert len(df) == 3
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_empty_timestamps_raises_no_data(self):
        provider = self._provider()
        payload = yahoo_payload(n=0)
        payload["chart"]["result"][0]["timestamp"] = []
        with patch.object(provider._session, "get", return_value=mock_response(json_data=payload)):
            with pytest.raises(pk.NoDataError):
                provider.fetch_ohlcv("FAKE", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_all_null_close_raises_no_data(self):
        provider = self._provider()
        with patch.object(provider._session, "get",
                           return_value=mock_response(json_data=yahoo_payload(n=3, all_null_close=True))):
            with pytest.raises(pk.NoDataError):
                provider.fetch_ohlcv("FAKE", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_chart_error_payload_raises_no_data(self):
        provider = self._provider()
        payload = {"chart": {"error": {"code": "Not Found", "description": "No data found"}, "result": None}}
        with patch.object(provider._session, "get", return_value=mock_response(json_data=payload)):
            with pytest.raises(pk.NoDataError):
                provider.fetch_ohlcv("DELISTED", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_empty_result_list_raises_no_data(self):
        provider = self._provider()
        payload = {"chart": {"error": None, "result": []}}
        with patch.object(provider._session, "get", return_value=mock_response(json_data=payload)):
            with pytest.raises(pk.NoDataError):
                provider.fetch_ohlcv("FAKE", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_http_500_raises_transient(self):
        provider = self._provider()
        with patch.object(provider._session, "get", return_value=mock_response(status_code=500)):
            with pytest.raises(pk.TransientProviderError):
                provider.fetch_ohlcv("AAPL", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_http_429_raises_transient_with_retry_after(self):
        provider = self._provider()
        resp = mock_response(status_code=429, headers={"Retry-After": "12"})
        with patch.object(provider._session, "get", return_value=resp):
            with pytest.raises(pk.TransientProviderError) as excinfo:
                provider.fetch_ohlcv("AAPL", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))
        assert excinfo.value.retry_after == 12.0

    def test_http_404_raises_no_data(self):
        provider = self._provider()
        with patch.object(provider._session, "get", return_value=mock_response(status_code=404)):
            with pytest.raises(pk.NoDataError):
                provider.fetch_ohlcv("BADTICKER", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_network_exception_raises_transient(self):
        import requests
        provider = self._provider()
        with patch.object(provider._session, "get", side_effect=requests.exceptions.ConnectionError("boom")):
            with pytest.raises(pk.TransientProviderError):
                provider.fetch_ohlcv("AAPL", datetime.now(timezone.utc) - timedelta(days=5),
                                      datetime.now(timezone.utc))

    def test_close_does_not_raise(self):
        provider = self._provider()
        provider.close()


 
# Parquet I/O
 

class TestParquetIO:
    def test_write_then_read_roundtrip(self, tmp_path):
        clean = pk.clean_ohlcv(make_raw_df(n=4), "AAPL", _test_logger())
        path = tmp_path / "AAPL.parquet"
        size = pk.write_parquet_atomic(clean, path, compression="zstd", compression_level=9)
        assert path.exists()
        assert size > 0

        back = pk.read_symbol_parquet(path)
        assert len(back) == 4
        assert isinstance(back["Date"].iloc[0], date)
        assert str(back["Volume"].dtype) == "Int64"

    def test_write_matches_canonical_schema(self, tmp_path):
        clean = pk.clean_ohlcv(make_raw_df(n=2), "MSFT", _test_logger())
        path = tmp_path / "MSFT.parquet"
        pk.write_parquet_atomic(clean, path, compression="zstd", compression_level=5)
        schema = pq.read_schema(path)
        assert schema.equals(pk.OHLCV_SCHEMA)

    def test_write_is_atomic_no_tmp_file_left_behind(self, tmp_path):
        clean = pk.clean_ohlcv(make_raw_df(n=2), "MSFT", _test_logger())
        path = tmp_path / "MSFT.parquet"
        pk.write_parquet_atomic(clean, path, compression="zstd", compression_level=5)
        leftovers = list(tmp_path.glob("*.tmp*"))
        assert leftovers == []

    def test_validation_report_roundtrip_including_empty(self, tmp_path):
        path = tmp_path / "report.parquet"
        pk.write_validation_report([], path)
        df = pq.read_table(path).to_pandas()
        assert len(df) == 0

        issues = [pk.ValidationIssue("AAPL", "ERROR", "bad data")]
        pk.write_validation_report(issues, path)
        df = pq.read_table(path).to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "AAPL"


 
# Config loading
 

class TestConfig:
    def test_defaults_fill_in_missing_keys(self, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("data:\n  raw_dir: custom_raw\n")
        cfg = pk.load_config(cfg_path)
        assert cfg.raw_dir == (tmp_path / "custom_raw").resolve()
        assert cfg.max_retries == pk.DEFAULT_CONFIG["download"]["max_retries"]
        assert cfg.compression == "zstd"

    def test_relative_paths_resolve_against_config_dir_not_cwd(self, tmp_path):
        sub = tmp_path / "project"
        sub.mkdir()
        cfg_path = sub / "config.yaml"
        cfg_path.write_text("data:\n  raw_dir: database/raw\n")
        cfg = pk.load_config(cfg_path)
        assert cfg.raw_dir == (sub / "database" / "raw").resolve()

    def test_missing_config_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            pk.load_config(tmp_path / "nope.yaml")

    def test_absolute_paths_are_not_rebased(self, tmp_path):
        abs_dir = tmp_path / "abs_raw"
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(f"data:\n  raw_dir: '{abs_dir.as_posix()}'\n")
        cfg = pk.load_config(cfg_path)
        assert cfg.raw_dir == abs_dir.resolve()

    def test_full_default_config_is_internally_consistent(self, tmp_path):
        # Uses the config.yaml shipped alongside packing.py, if present.
        shipped = Path(__file__).resolve().parent.parent / "config.yaml"
        if shipped.exists():
            cfg = pk.load_config(shipped)
            assert cfg.max_workers > 0
            assert cfg.rate_limit_per_sec > 0


 
# load_symbol_universe
 

class TestSymbolUniverse:
    def _write_csv(self, tmp_path, rows):
        path = tmp_path / "symbols.csv"
        header = "Ticker,Name,Exchange,Category Name,Country\n"
        body = "\n".join(",".join(r) for r in rows)
        path.write_text(header + body + "\n")
        return path

    def test_basic_load(self, tmp_path):
        path = self._write_csv(tmp_path, [
            ("AAPL", "Apple Inc", "NASDAQ", "Technology", "United States"),
            ("MSFT", "Microsoft", "NASDAQ", "Technology", "United States"),
        ])
        universe = pk.load_symbol_universe(path, _test_logger())
        assert [s.ticker for s in universe] == ["AAPL", "MSFT"]
        assert universe[0].exchange == "NASDAQ"
        assert universe[0].category == "Technology"

    def test_duplicate_tickers_deduped(self, tmp_path):
        path = self._write_csv(tmp_path, [
            ("AAPL", "Apple Inc", "NASDAQ", "Technology", "United States"),
            ("AAPL", "Apple Inc", "NASDAQ", "Technology", "United States"),
        ])
        universe = pk.load_symbol_universe(path, _test_logger())
        assert len(universe) == 1

    def test_blank_ticker_skipped(self, tmp_path):
        path = self._write_csv(tmp_path, [
            ("AAPL", "Apple Inc", "NASDAQ", "Technology", "United States"),
            ("", "", "", "", ""),
        ])
        universe = pk.load_symbol_universe(path, _test_logger())
        assert len(universe) == 1

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            pk.load_symbol_universe(tmp_path / "nope.csv", _test_logger())

    def test_missing_ticker_column_raises(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("Symbol,Name\nAAPL,Apple\n")
        with pytest.raises(ValueError):
            pk.load_symbol_universe(path, _test_logger())


 
# MetadataStore
 

class TestMetadataStore:
    def test_update_and_get(self, tmp_path):
        store = pk.MetadataStore(tmp_path / "meta.parquet", flush_every=1000)
        store.update("AAPL", status="SUCCESS", rows=10)
        rec = store.get("AAPL")
        assert rec["status"] == "SUCCESS"
        assert rec["rows"] == 10

    def test_is_downloaded(self, tmp_path):
        store = pk.MetadataStore(tmp_path / "meta.parquet", flush_every=1000)
        assert store.is_downloaded("AAPL") is False
        store.update("AAPL", status="SUCCESS")
        assert store.is_downloaded("AAPL") is True
        store.update("MSFT", status="FAILED")
        assert store.is_downloaded("MSFT") is False

    def test_flush_and_reload_roundtrip(self, tmp_path):
        path = tmp_path / "meta.parquet"
        store = pk.MetadataStore(path, flush_every=1000)
        store.update("AAPL", status="SUCCESS", rows=5, start_date=date(2024, 1, 1), end_date=date(2024, 1, 5))
        store.flush()
        assert path.exists()

        reloaded = pk.MetadataStore(path, flush_every=1000)
        rec = reloaded.get("AAPL")
        assert rec["status"] == "SUCCESS"
        assert rec["rows"] == 5
        assert rec["start_date"] == date(2024, 1, 1)

    def test_auto_flush_after_threshold(self, tmp_path):
        path = tmp_path / "meta.parquet"
        store = pk.MetadataStore(path, flush_every=2)
        store.update("AAPL", status="SUCCESS")
        assert not path.exists()  # below threshold
        store.update("MSFT", status="SUCCESS")
        assert path.exists()  # threshold hit -> auto flush

    def test_flush_on_empty_store_is_noop(self, tmp_path):
        path = tmp_path / "meta.parquet"
        store = pk.MetadataStore(path, flush_every=1000)
        store.flush()
        assert not path.exists()


 
# RunStats
 

class TestRunStats:
    def test_bump_is_thread_safe(self):
        stats = pk.RunStats(command="download", total=1000)

        def worker():
            for _ in range(1000):
                stats.bump(success=1)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert stats.success == 8000

    def test_to_dict_has_expected_keys(self):
        stats = pk.RunStats(command="download", total=10, success=8, failed=2)
        d = stats.to_dict()
        assert d["total"] == 10
        assert d["success"] == 8
        assert d["failed"] == 2
        assert "symbols_per_second" in d

    def test_render_summary_contains_command_name(self):
        stats = pk.RunStats(command="verify", total=5)
        summary = stats.render_summary()
        assert "verify" in summary


 
# PackingEngine (integration-style, fully offline via FakeProvider)
 

class TestPackingEngineDownload:
    def _cfg(self, tmp_path, **overrides):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("download:\n  max_retries: 1\n  retry_queue_passes: 0\n  rate_limit_per_sec: 0\n")
        cfg = pk.load_config(cfg_path)
        for k, v in overrides.items():
            setattr(cfg, k, v)
        return cfg

    def test_download_writes_files_for_success(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"AAPL": make_raw_df(n=3)})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        symbols = [pk.SymbolInfo(ticker="AAPL")]
        stats = engine.run_download(symbols, overwrite=False)
        assert stats.success == 1
        assert (cfg.raw_dir / "AAPL.parquet").exists()
        assert engine.metadata.is_downloaded("AAPL")

    def test_download_marks_no_data(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"FAKE": pk.NoDataError("nothing here")})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        stats = engine.run_download([pk.SymbolInfo(ticker="FAKE")], overwrite=False)
        assert stats.no_data == 1
        assert not (cfg.raw_dir / "FAKE.parquet").exists()

    def test_download_marks_failed_after_retries_exhausted(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"BAD": pk.TransientProviderError("still down")})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        stats = engine.run_download([pk.SymbolInfo(ticker="BAD")], overwrite=False)
        assert stats.failed == 1

    def test_download_skips_already_downloaded_symbol(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"AAPL": make_raw_df(n=3)})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        symbols = [pk.SymbolInfo(ticker="AAPL")]

        engine.run_download(symbols, overwrite=False)
        assert len(provider.calls) == 1

        # Second run: resumable -- should skip without calling the provider again
        stats2 = engine.run_download(symbols, overwrite=False)
        assert stats2.skipped == 1
        assert len(provider.calls) == 1  # unchanged

    def test_download_overwrite_forces_refetch(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"AAPL": make_raw_df(n=3)})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        symbols = [pk.SymbolInfo(ticker="AAPL")]

        engine.run_download(symbols, overwrite=False)
        engine.run_download(symbols, overwrite=True)
        assert len(provider.calls) == 2

    def test_retry_queue_recovers_transient_failure(self, tmp_path):
        cfg = self._cfg(tmp_path)
        cfg.retry_queue_passes = 1
        cfg.retry_queue_cooldown_seconds = 0.01

        attempts = {"n": 0}

        class FlakyProvider(pk.DataProvider):
            def fetch_ohlcv(self, symbol, start, end, interval="1d"):
                attempts["n"] += 1
                if attempts["n"] <= 2:  # fails through the whole first pass (max_retries=1 -> 2 tries)
                    raise pk.TransientProviderError("flaky")
                return make_raw_df(n=2)

        engine = pk.PackingEngine(cfg, FlakyProvider(), _test_logger())
        stats = engine.run_download([pk.SymbolInfo(ticker="FLAKY")], overwrite=False)
        assert stats.success == 1
        assert stats.retried_ok == 1
        assert stats.failed == 0

    def test_one_bad_symbol_does_not_abort_the_batch(self, tmp_path):
        cfg = self._cfg(tmp_path)

        class ExplodingProvider(pk.DataProvider):
            def fetch_ohlcv(self, symbol, start, end, interval="1d"):
                if symbol == "BOOM":
                    raise RuntimeError("unexpected bug")
                return make_raw_df(n=2)

        engine = pk.PackingEngine(cfg, ExplodingProvider(), _test_logger())
        symbols = [pk.SymbolInfo(ticker="BOOM"), pk.SymbolInfo(ticker="AAPL")]
        stats = engine.run_download(symbols, overwrite=False)
        assert stats.total == 2
        assert stats.success == 1
        assert stats.failed == 1


class TestPackingEngineUpdate:
    def _cfg(self, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("download:\n  max_retries: 1\n  retry_queue_passes: 0\n  rate_limit_per_sec: 0\n")
        return pk.load_config(cfg_path)

    def test_update_bootstraps_when_no_file_exists(self, tmp_path):
        cfg = self._cfg(tmp_path)
        provider = FakeProvider({"AAPL": make_raw_df(n=3)})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        stats = engine.run_update([pk.SymbolInfo(ticker="AAPL")])
        assert stats.success == 1
        assert (cfg.raw_dir / "AAPL.parquet").exists()

    def test_update_appends_new_rows_to_existing_file(self, tmp_path):
        cfg = self._cfg(tmp_path)
        old = pk.clean_ohlcv(make_raw_df(n=3, start="2024-01-02"), "AAPL", _test_logger())
        path = cfg.raw_dir / "AAPL.parquet"
        pk.write_parquet_atomic(old, path, cfg.compression, cfg.compression_level)
        engine = pk.PackingEngine(cfg, FakeProvider({}), _test_logger())
        engine.metadata.update("AAPL", status="SUCCESS")

        new_rows = make_raw_df(n=2, start="2024-01-10")
        engine.provider = FakeProvider({"AAPL": new_rows})

        stats = engine.run_update([pk.SymbolInfo(ticker="AAPL")])
        assert stats.success == 1
        result = pk.read_symbol_parquet(path)
        assert len(result) == 5
        assert result["Date"].is_monotonic_increasing

    def test_update_already_current_skips_network_call(self, tmp_path):
        cfg = self._cfg(tmp_path)
        today = datetime.now(timezone.utc).date()
        current = pk.clean_ohlcv(make_raw_df(n=1, start=today.isoformat()), "AAPL", _test_logger())
        path = cfg.raw_dir / "AAPL.parquet"
        pk.write_parquet_atomic(current, path, cfg.compression, cfg.compression_level)

        provider = FakeProvider({})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        engine.metadata.update("AAPL", status="SUCCESS")
        stats = engine.run_update([pk.SymbolInfo(ticker="AAPL")])
        assert stats.success == 1
        assert provider.calls == []  # no network call needed

    def test_update_no_new_data_is_not_a_failure(self, tmp_path):
        cfg = self._cfg(tmp_path)
        old = pk.clean_ohlcv(make_raw_df(n=3, start="2024-01-02"), "AAPL", _test_logger())
        path = cfg.raw_dir / "AAPL.parquet"
        pk.write_parquet_atomic(old, path, cfg.compression, cfg.compression_level)

        provider = FakeProvider({"AAPL": pk.NoDataError("no new bars in this window")})
        engine = pk.PackingEngine(cfg, provider, _test_logger())
        engine.metadata.update("AAPL", status="SUCCESS")
        stats = engine.run_update([pk.SymbolInfo(ticker="AAPL")])
        assert stats.success == 1
        assert stats.failed == 0


class TestPackingEngineVerify:
    def _cfg(self, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("validation:\n  max_allowed_gap_days: 10\n")
        return pk.load_config(cfg_path)

    def test_verify_passes_clean_file(self, tmp_path):
        cfg = self._cfg(tmp_path)
        clean = pk.clean_ohlcv(make_raw_df(n=5), "AAPL", _test_logger())
        pk.write_parquet_atomic(clean, cfg.raw_dir / "AAPL.parquet", cfg.compression, cfg.compression_level)
        engine = pk.PackingEngine(cfg, FakeProvider({}), _test_logger())
        stats, issues = engine.run_verify([pk.SymbolInfo(ticker="AAPL")])
        assert stats.success == 1
        assert not any(i.severity == "ERROR" for i in issues)

    def test_verify_flags_missing_file(self, tmp_path):
        cfg = self._cfg(tmp_path)
        engine = pk.PackingEngine(cfg, FakeProvider({}), _test_logger())
        stats, issues = engine.run_verify([pk.SymbolInfo(ticker="GHOST")])
        assert stats.failed == 1
        assert any("missing" in i.message for i in issues)


 
# CLI arg selection helper
 

class TestSelectSymbols:
    def _universe(self):
        return [pk.SymbolInfo(ticker=t) for t in ["AAPL", "MSFT", "GOOG", "AMZN"]]

    def test_no_filters_returns_all(self):
        assert len(pk._select_symbols(self._universe(), None, None)) == 4

    def test_symbols_filter(self):
        selected = pk._select_symbols(self._universe(), "aapl,msft", None)
        assert {s.ticker for s in selected} == {"AAPL", "MSFT"}

    def test_limit(self):
        selected = pk._select_symbols(self._universe(), None, 2)
        assert len(selected) == 2

    def test_symbols_and_limit_combined(self):
        selected = pk._select_symbols(self._universe(), "AAPL,MSFT,GOOG", 1)
        assert len(selected) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
