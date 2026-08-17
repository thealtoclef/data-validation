"""Unit tests for ChunkHash validation (ADR-0003).

All driver tests use mock executors returning canned DataFrames — zero live
database calls. SQL builder tests pin the spike-verified fragments, most
importantly the POSITIVE SUBSTR position (negative offsets silently break
PostgreSQL; see ADR-0003).
"""

import threading

import pandas
import pytest

from data_validation import chunk_hash_validation as chv
from data_validation.exceptions import ValidationException


class MockClient(object):
    def __init__(self, name, handler):
        self.name = name
        self._handler = handler
        self.sql_log = []

    def raw_sql(self, sql):
        self.sql_log.append(sql)
        return self._handler(sql)


def _mm(lo, hi):
    return pandas.DataFrame({"kmin": [lo], "kmax": [hi]})


def _bucket_df(rows):
    # rows: list of (bucket, cnt, checksum)
    return pandas.DataFrame(rows, columns=["bucket", "cnt", "checksum"])


def _seg_df(rows):
    # rows: list of (seg, bucket, cnt, checksum)
    return pandas.DataFrame(rows, columns=["seg", "bucket", "cnt", "checksum"])


def _download_df(rows):
    # rows: list of (key, row_hash)
    return pandas.DataFrame(rows, columns=["id", "row_hash"])


def _config(**overrides):
    defaults = dict(
        primary_keys=["id"],
        comparison_columns=["a", "b"],
        bisection_factor=2,
        num_buckets=4,
        bisection_threshold=10,
        max_depth=4,
        max_diff_rows=1000,
        max_parallelism=1,  # existing tests stay on the deterministic serial path
    )
    defaults.update(overrides)
    return chv.ChunkHashConfig(**defaults)


# --------------------------------------------------------------------------
# Preflight
# --------------------------------------------------------------------------


def test_preflight_unsupported_dialect():
    class Oracleish(object):
        name = "oracle"

    with pytest.raises(ValidationException, match="oracle"):
        chv.preflight(Oracleish(), MockClient("postgres", lambda sql: None), _config())
    with pytest.raises(ValidationException, match="target dialect"):
        chv.preflight(MockClient("postgres", lambda sql: None), Oracleish(), _config())


def test_preflight_rejects_cloud_sql_variants():
    # Dialect scope is deliberately four engines (ADR-0003); Cloud SQL
    # variants and others are rejected at run start.
    for name in ("cloudsql_postgres", "cloudsql_mysql", "mssql", "oracle"):
        client = MockClient(name, None)
        with pytest.raises(ValidationException, match="does not support"):
            chv.preflight(client, MockClient("postgres", None), _config())


def test_preflight_no_primary_keys():
    src, tgt = MockClient("postgres", None), MockClient("mysql", None)
    with pytest.raises(ValidationException, match="primary key"):
        chv.preflight(src, tgt, _config(primary_keys=[]))


def test_preflight_no_comparison_columns():
    src, tgt = MockClient("postgres", None), MockClient("mysql", None)
    with pytest.raises(ValidationException, match="comparison columns"):
        chv.preflight(src, tgt, _config(comparison_columns=[]))


# --------------------------------------------------------------------------
# SQL builders
# --------------------------------------------------------------------------

ALL_DIALECTS = ["bigquery", "spanner", "postgres", "mysql",
                "cloudsql_postgres", "cloudsql_mysql"]

CHECKSUM_FRAGMENTS = {
    "bigquery": ["AS NUMERIC", "CONCAT('0x'", "AS INT64"],
    "spanner": ["AS NUMERIC", "CONCAT('0x'", "AS INT64"],
    "postgres": ["::bit(60)::bigint", "AS NUMERIC"],
    "cloudsql_postgres": ["::bit(60)::bigint", "AS NUMERIC"],
    "mysql": ["CONV(", "AS DECIMAL"],
    "cloudsql_mysql": ["CONV(", "AS DECIMAL"],
}


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_keyset_checksum_sql(dialect):
    sql = chv.build_keyset_checksum_sql(
        dialect, "tbl", ["id"], ["a", "b"], "k1", "k2", False, 16
    )
    assert "MOD(ABS(" in sql and "GROUP BY 1" in sql and "GROUP BY 1, 2" not in sql
    assert "SUBSTR(" in sql and ", 50)" in sql
    assert "COALESCE(SUM(" in sql
    for frag in CHECKSUM_FRAGMENTS[dialect]:
        assert frag in sql, f"{dialect}: missing {frag}"
    assert "-15" not in sql


def test_group_by_checksum_sql_bigquery():
    sql = chv.build_group_by_checksum_sql(
        "bigquery", "tbl", ["id"], ["a"], "k1", "k9", True, ["k3", "k6"], 8
    )
    assert "CASE" in sql and "WHEN `id` < 'k3' THEN 0" in sql and "ELSE 2 END" in sql
    assert "GROUP BY 1, 2" in sql
    assert "`id` >= 'k1' AND `id` <= 'k9'" in sql  # inclusive end
    assert "-15" not in sql


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_bucket_download_sql(dialect):
    sql = chv.build_bucket_download_sql(
        dialect, "tbl", ["id"], ["a"], "k1", "k2", False, 8, buckets=[1, 3]
    )
    assert " AS row_hash" in sql and "IN (1, 3)" in sql
    sql_all = chv.build_bucket_download_sql(
        dialect, "tbl", ["id"], ["a"], "k1", "k2", True, 8, buckets=None
    )
    assert "IN (" not in sql_all and "<= 'k2'" in sql_all
    assert "-15" not in sql and "-15" not in sql_all


def test_boundary_sample_sql():
    sql = chv.build_boundary_sample_sql(
        "postgres", "tbl", "id", "k1", "k2", True, 5, filter_sql=None
    )
    assert "ORDER BY" in sql and "LIMIT 1 OFFSET 5" in sql
    # ADR-0003 F1: sampling must be restricted to the current range, else
    # recursion into a subrange produces overlapping segments.
    assert ">=" in sql and "<= 'k2'" in sql and "ORDER BY" in sql.split("<=")[-1]


def test_range_predicate_end_inclusive():
    sql = chv.build_keyset_checksum_sql("mysql", "t", ["id"], ["a"], "1", "9", True, 4)
    assert "<= '9'" in sql and "< '9'" not in sql


def test_literal_and_ident_escaping():
    assert chv.sql_literal("o'brien") == "'o''brien'"
    assert chv.quote_ident("bigquery", "we`ird") == "`we``ird`"
    assert chv.quote_ident("postgres", 'we"ird') == '"we""ird"'


def test_filter_sql_folded_into_where():
    sql = chv.build_count_sql("postgres", "t", "id", "a", "z", False, "region = 'EU'")
    assert "WHERE" in sql and "region = 'EU'" in sql
    sql_nf = chv.build_count_sql("postgres", "t", "id", "a", "z", False, None)
    assert "WHERE" in sql_nf  # range predicate still present


# --------------------------------------------------------------------------
# Driver (mock executors)
# --------------------------------------------------------------------------


def _identical_handler(sql):
    if "MIN(" in sql:
        return _mm("a", "z")
    if "LIMIT 1 OFFSET" in sql:
        return pandas.DataFrame({"b": ["m"]})
    if "GROUP BY 1" in sql:
        return _bucket_df([(0, 50, "111"), (1, 50, "222")])
    if "COUNT(*)" in sql and "GROUP BY" not in sql:
        return pandas.DataFrame({"cnt": [100]})
    if " AS row_hash" in sql:
        raise AssertionError("download must not happen for identical tables")
    raise AssertionError(f"unexpected sql: {sql}")


def test_driver_identical_tables_no_downloads():
    src = MockClient("postgres", _identical_handler)
    tgt = MockClient("mysql", _identical_handler)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-1", "s", "t")
    assert not any("row_hash" in s for s in src.sql_log)
    statuses = df["validation_status"].tolist()
    assert statuses[-1] == "success" and "failure" not in statuses
    assert df["difference"].sum() == 0


def test_driver_single_row_diff():
    boundary_returned = {"value": "m"}

    def src_handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": [boundary_returned["value"]]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [10]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 5, "111"), (1, 5, "222")])
        if " AS row_hash" in sql:
            return _download_df([("a", "H"), ("m", "H"), ("z", "X")])
        raise AssertionError(f"unexpected: {sql}")

    def tgt_handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [10]})
        if "GROUP BY 1" in sql:
            # Same maps both sides at the top level...
            return _bucket_df([(0, 5, "111"), (1, 5, "222")])
        if " AS row_hash" in sql:
            return _download_df([("a", "H"), ("m", "H"), ("z", "H")])
        raise AssertionError(f"unexpected: {sql}")

    # Force a bucket mismatch: monkey-patch one side's keyset response for the
    # final (inclusive) subrange by keying on the range predicate.
    original_tgt = tgt_handler

    def tgt_handler2(sql):
        df = original_tgt(sql)
        if "GROUP BY 1" in sql and "<= 'z'" in sql:
            df = _bucket_df([(0, 5, "111"), (1, 5, "999")])  # bucket 1 differs
        return df

    src, tgt = MockClient("postgres", src_handler), MockClient("mysql", tgt_handler2)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-2", "s", "t")
    assert (df["validation_status"] == "failure").any()
    assert df["validation_status"].iloc[-1] == "failure"
    fail_rows = df[df["validation_status"] == "failure"]
    assert fail_rows["difference"].iloc[0] == 1
    assert {"id": "z"} in fail_rows["differing_keys"].iloc[0]


def test_driver_budget_exceeded():
    def handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [100]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 50, "111"), (1, 50, "222")])
        if " AS row_hash" in sql:
            raise AssertionError("budget must prevent downloads here")
        raise AssertionError(f"unexpected: {sql}")

    def src_handler(sql):
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 50, "111"), (1, 50, "333")])  # always differs
        return handler(sql)

    src, tgt = MockClient("postgres", src_handler), MockClient("mysql", handler)
    cfg = _config(bisection_threshold=1, max_depth=0, max_diff_rows=0)
    df = chv.run_chunk_hash_validation(src, tgt, cfg, "run-3", "s", "t")
    assert df["validation_status"].iloc[-1] == "budget_exceeded"
    assert not any("row_hash" in s for s in src.sql_log)


def test_driver_both_tables_empty():
    def empty_handler(sql):
        if "MIN(" in sql:
            return _mm(None, None)
        raise AssertionError(f"unexpected: {sql}")

    src, tgt = MockClient("postgres", empty_handler), MockClient("mysql", empty_handler)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-4", "s", "t")
    assert len(df) == 2  # single chunk row + summary
    assert (df["validation_status"] == "success").all()
    assert df["source_agg_value"].iloc[-1] == 0


def test_driver_one_side_empty():
    def empty(sql):
        if "MIN(" in sql:
            return _mm(None, None)
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [0]})
        if "GROUP BY 1" in sql:
            return _bucket_df([])
        if " AS row_hash" in sql:
            return _download_df([])
        raise AssertionError(f"unexpected: {sql}")

    def full(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [10]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 5, "111"), (1, 5, "222")])
        if " AS row_hash" in sql:
            return _download_df([("a", "H"), ("m", "H"), ("z", "H")])
        raise AssertionError(f"unexpected: {sql}")

    src, tgt = MockClient("postgres", empty), MockClient("mysql", full)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-5", "s", "t")
    assert df["validation_status"].iloc[-1] == "failure"
    failures = df[df["validation_status"] == "failure"]
    assert failures["difference"].iloc[0] == 3  # all target rows one-sided
    assert {"id": "z"} in failures["differing_keys"].iloc[0]


def test_driver_bigquery_uses_group_by_strategy():
    def zeta_handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [100]})
        if "GROUP BY 1, 2" in sql:
            return _seg_df([(0, 0, 50, "111"), (0, 1, 50, "222"),
                            (1, 0, 50, "111"), (1, 1, 50, "222")])
        if " AS row_hash" in sql:
            raise AssertionError("identical tables must not download")
        raise AssertionError(f"unexpected: {sql}")

    def row_handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [100]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 50, "111"), (1, 50, "222")])
        raise AssertionError(f"unexpected: {sql}")

    src, tgt = MockClient("bigquery", zeta_handler), MockClient("postgres", row_handler)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-6", "s", "t")
    assert df["validation_status"].iloc[-1] == "success"
    assert any("GROUP BY 1, 2" in s for s in src.sql_log)
    assert not any("GROUP BY 1," in s and "GROUP BY 1, 2" not in s for s in src.sql_log)


# --------------------------------------------------------------------------
# Parallel execution (Phase 4) + regression coverage
#
# The parallel path dispatches independent top-level queries and subranges to
# a ThreadPoolExecutor. MockClient.sql_log order is nondeterministic under
# threads (list.append is GIL-atomic, so no entries are lost), so parallel
# assertions compare the result DataFrame and the SORTED query multiset.
# --------------------------------------------------------------------------


def test_driver_parallel_equals_serial():
    """max_parallelism=4 must produce identical rows and the same query set
    as the serial path."""

    def _run(mp):
        cfg = _config(max_parallelism=mp)
        src = MockClient("postgres", _identical_handler)
        tgt = MockClient("mysql", _identical_handler)
        df = chv.run_chunk_hash_validation(src, tgt, cfg, f"run-{mp}", "s", "t")
        return df, sorted(src.sql_log + tgt.sql_log)

    serial_df, serial_sql = _run(1)
    par_df, par_sql = _run(4)
    cols = ["validation_status", "difference", "source_agg_value", "target_agg_value",
            "chunk_start", "chunk_end"]
    assert serial_df[cols].equals(par_df[cols])
    assert serial_df["differing_keys"].tolist() == par_df["differing_keys"].tolist()
    assert serial_sql == par_sql
    assert (serial_df["validation_status"] == "success").all()


def test_driver_parallel_deterministic():
    """Two parallel runs of the same input must produce identical rows."""

    def _run():
        cfg = _config(max_parallelism=4)
        src = MockClient("postgres", _identical_handler)
        tgt = MockClient("mysql", _identical_handler)
        return chv.run_chunk_hash_validation(src, tgt, cfg, "run-det", "s", "t")

    a, b = _run(), _run()
    assert a["chunk_start"].tolist() == b["chunk_start"].tolist()
    assert a["validation_status"].tolist() == b["validation_status"].tolist()
    assert a["source_agg_value"].tolist() == b["source_agg_value"].tolist()


def test_driver_parallel_budget_exceeded():
    """Under parallelism, all subranges reserve against the same cap; with a
    zero budget every mismatching subrange reports budget_exceeded and no
    downloads happen."""

    def handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [100]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 50, "111"), (1, 50, "222")])
        if " AS row_hash" in sql:
            raise AssertionError("budget must prevent downloads here")
        raise AssertionError(f"unexpected: {sql}")

    def src_handler(sql):
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 50, "111"), (1, 50, "333")])  # always differs
        return handler(sql)

    cfg = _config(bisection_threshold=1, max_depth=0, max_diff_rows=0, max_parallelism=4)
    src = MockClient("postgres", src_handler)
    tgt = MockClient("mysql", handler)
    df = chv.run_chunk_hash_validation(src, tgt, cfg, "run-budget-par", "s", "t")
    assert df["validation_status"].iloc[-1] == "budget_exceeded"
    assert (df["validation_status"] == "budget_exceeded").sum() >= 3  # 2 subranges + summary
    assert not any("row_hash" in s for s in src.sql_log)


def _run_state(cfg, **kwargs):
    return chv._RunState(None, None, "postgres", "postgres", cfg, "s", "t", "r", None, **kwargs)


def test_reserve_enforces_cap():
    cfg = _config(max_diff_rows=10)
    state = _run_state(cfg)
    assert chv._reserve(state, 4) is True
    assert chv._reserve(state, 4) is True
    assert chv._reserve(state, 3) is False  # 8 + 3 > 10
    assert state.budget_exceeded is True
    assert state.downloaded_rows == 8
    # A download that reserved 4 but actually fetched 2 reconciles down.
    chv._reconcile_download(state, 4, 2)
    assert state.downloaded_rows == 6


def test_reserve_thread_safety():
    """Concurrent reservations must never exceed the hard cap."""
    cfg = _config(max_diff_rows=100)
    state = _run_state(cfg)

    def worker():
        for _ in range(20):
            chv._reserve(state, 10)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert state.downloaded_rows == 100  # exactly 10 reservations of 10
    assert state.budget_exceeded is True


def test_driver_recursion_sampling_restricted_to_range():
    """ADR-0003 F1: boundary samples at recursion depth >= 1 must be scoped to
    the current range. The bucket maps differ (forcing recursion through the
    depth cap) and max_diff_rows=0 stops any download; every boundary-sample
    query must carry the current range predicate."""

    def handler(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [50]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 25, "111"), (1, 25, "222")])
        raise AssertionError(f"unexpected: {sql}")

    def src_handler(sql):
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 25, "111"), (1, 25, "333")])  # bucket 1 differs
        return handler(sql)

    src = MockClient("postgres", src_handler)
    tgt = MockClient("mysql", handler)
    cfg = _config(bisection_factor=2, bisection_threshold=10, max_depth=3, max_diff_rows=0)
    chv.run_chunk_hash_validation(src, tgt, cfg, "run-F1", "s", "t")
    samples = [s for s in src.sql_log if "LIMIT 1 OFFSET" in s]
    assert len(samples) >= 3  # root split + at least one recursion level
    for s in samples:
        assert ">= '" in s and ("< '" in s or "<= '" in s)


def test_driver_empty_source_nan_max_not_silent_success():
    """ADR-0003 F2: an empty source whose MIN/MAX surface as NaN must still
    report the target's rows (failure), never a silent success from a bogus
    `<= 'nan'` range. The count handlers model an empty range for 'nan'."""

    def empty(sql):
        if "MIN(" in sql:
            return _mm(float("nan"), float("nan"))
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            return pandas.DataFrame({"cnt": [0]})
        if "GROUP BY 1" in sql:
            return _bucket_df([])
        if " AS row_hash" in sql:
            return _download_df([])
        raise AssertionError(f"unexpected: {sql}")

    def full(sql):
        if "MIN(" in sql:
            return _mm("a", "z")
        if "LIMIT 1 OFFSET" in sql:
            return pandas.DataFrame({"b": ["m"]})
        if "COUNT(*)" in sql and "GROUP BY" not in sql:
            # Empty range (the buggy `<= 'nan'`) yields 0 rows; the real
            # populated range yields 10.
            return pandas.DataFrame({"cnt": [0 if "nan" in sql else 10]})
        if "GROUP BY 1" in sql:
            return _bucket_df([(0, 5, "111"), (1, 5, "222")])
        if " AS row_hash" in sql:
            return _download_df([("a", "H"), ("m", "H"), ("z", "H")])
        raise AssertionError(f"unexpected: {sql}")

    src, tgt = MockClient("postgres", empty), MockClient("mysql", full)
    df = chv.run_chunk_hash_validation(src, tgt, _config(), "run-F2", "s", "t")
    assert df["validation_status"].iloc[-1] == "failure"
    assert df["source_agg_value"].iloc[-1] == 0
    assert df["target_agg_value"].iloc[-1] > 0  # the target's rows are seen, not silently dropped


# --------------------------------------------------------------------------
# Executor adapter (backend families)
# --------------------------------------------------------------------------


class _FakeCursor(object):
    """SQLAlchemy-style result: keys() + fetchall()."""

    def __init__(self, rows, columns):
        self._rows, self._columns = rows, columns

    def keys(self):
        return self._columns

    def fetchall(self):
        return self._rows


class _FakeSpannerStream(object):
    """Spanner StreamedResultSet: fields appear only after consumption."""

    def __init__(self, rows, names):
        self._rows = rows
        self.fields = [type("Field", (), {"name": n})() for n in names]

    def __iter__(self):
        return iter(self._rows)


class _AdapterClient(object):
    def __init__(self, name, result):
        self.name, self._result = name, result

    def raw_sql(self, sql):
        return self._result


def test_client_dataframe_executor_dataframe_passthrough():
    df = pandas.DataFrame({"a": [1]})
    exec_ = chv.client_dataframe_executor(_AdapterClient("pandas", df))
    assert exec_("select") is df


def test_client_dataframe_executor_sqlalchemy_cursor():
    cursor = _FakeCursor([("x", 5, 123)], ["bucket", "cnt", "checksum"])
    exec_ = chv.client_dataframe_executor(_AdapterClient("postgres", cursor))
    df = exec_("select")
    assert list(df.columns) == ["bucket", "cnt", "checksum"]
    assert int(df.iloc[0]["cnt"]) == 5


def test_client_dataframe_executor_spanner_stream():
    stream = _FakeSpannerStream([("x", 5, 123)], ["bucket", "cnt", "checksum"])
    exec_ = chv.client_dataframe_executor(_AdapterClient("spanner", stream))
    df = exec_("select")
    assert list(df.columns) == ["bucket", "cnt", "checksum"]
    assert len(df) == 1


def test_client_dataframe_executor_bigquery_rows():
    exec_ = chv.client_dataframe_executor(
        _AdapterClient("bigquery", [{"kmin": "a", "kmax": "z"}])
    )
    df = exec_("select")
    assert df.iloc[0]["kmin"] == "a"


def test_quote_table_splits_dotted_names():
    assert chv._quote_table("postgres", "public.farmers") == '"public"."farmers"'
    assert chv._quote_table("bigquery", "proj.ds.t") == "`proj`.`ds`.`t`"


class _FakeEngineConn(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def exec_driver_sql(self, sql):
        return self._cursor

    def execution_options(self, **kwargs):
        return self


class _FakeEngine(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def connect(self):
        return _FakeEngineConn(self._cursor)

    def exec_driver_sql(self, sql):
        # Marker for the adapter's SQLAlchemy-engine detection; never called
        # directly (queries go through connect() contexts).
        raise AssertionError("engine.exec_driver_sql must not be called")


class _EngineClient(object):
    def __init__(self, name, cursor):
        self.name = name
        self.con = _FakeEngine(cursor)

    def raw_sql(self, sql):
        raise AssertionError("engine-backed clients must bypass raw_sql")


def test_client_dataframe_executor_prefers_engine_connection():
    cursor = _FakeCursor([("x", 5, 123)], ["bucket", "cnt", "checksum"])
    exec_ = chv.client_dataframe_executor(_EngineClient("postgres", cursor))
    df = exec_("select")
    assert list(df.columns) == ["bucket", "cnt", "checksum"]
    assert int(df.iloc[0]["cnt"]) == 5


class _FakeDbapiCursor(object):
    def __init__(self, rows, names):
        self._rows = rows
        self.description = [(n, None, None, None, None, None, None)
                            for n in names]
        self._done = False

    def execute(self, sql):
        self._done = True

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeDbapiConnection(object):
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class _RawCreatorEngine(object):
    """Engine shape: pool._creator() -> raw DBAPI connection."""

    def __init__(self, connection):
        self._connection = connection
        self.pool = self

    def _creator(self):
        return self._connection

    def exec_driver_sql(self, sql):
        # Marker for the adapter's SQLAlchemy-engine gate; never called.
        raise AssertionError("must use the raw creator path")


class _RawCreatorClient(object):
    def __init__(self, name, connection):
        self.name = name
        self.con = _RawCreatorEngine(connection)

    def raw_sql(self, sql):
        raise AssertionError("raw-creator clients must bypass raw_sql")


def test_client_dataframe_executor_raw_creator_path():
    cursor = _FakeDbapiCursor([("x", 5, 123)], ["bucket", "cnt", "checksum"])
    connection = _FakeDbapiConnection(cursor)
    exec_ = chv.client_dataframe_executor(_RawCreatorClient("postgres", connection))
    df = exec_("select")
    assert list(df.columns) == ["bucket", "cnt", "checksum"]
    assert int(df.iloc[0]["cnt"]) == 5
    assert connection.closed is True  # always released
