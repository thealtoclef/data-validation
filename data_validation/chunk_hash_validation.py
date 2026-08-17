"""ChunkHash validation: cross-database table diff with warehouse-side compute.

Implements ADR-0003 (reladiff-inspired, see the amendment section there):
boundary-sampled key-range segments, per-segment bucket checksums computed
entirely inside the databases, recursive bisection into mismatched segments,
and bucket-filtered row downloads below the threshold. The container never
holds more than one segment's worth of (key, hash) rows.

Execution strategies: the BigQuery side expands a range with ONE
grouped query (CASE-assigned segments); row-store sides (Spanner, PostgreSQL,
MySQL) use per-range keyset queries. Both produce identical bucket
maps ``{bucket: (count, checksum)}``, so comparison is strategy-agnostic and
the two sides may use different strategies in the same run.

SQL fragment invariants (spike-verified; do not change unilaterally):
- SUBSTR position 50 = last 15 hex chars of the 64-char sha256 hex string.
  POSITIVE position only: PostgreSQL substr() with a negative start returns
  the whole string and silently breaks cross-dialect equality.
- Cast to NUMERIC/DECIMAL before SUM: exact arithmetic, no dialect-dependent
  overflow.
- COALESCE(..., 0) inside the aggregate so empty ranges compare equal.

PostgreSQL requires the pgcrypto extension for DIGEST(); create it once per
database: ``CREATE EXTENSION IF NOT EXISTS pgcrypto;``.

Single key column note: range predicates apply to the FIRST primary key
column; additional key columns participate in the key hash (bucketing) only.

Parallel execution: with max_parallelism > 1 the independent top-level work
(boundary sampling, per-side keyset checksums, and subrange resolution) is
dispatched to a ThreadPoolExecutor. Workers never re-enter the pool, and
results are joined in submission order, so output is deterministic. The
max_diff_rows budget is enforced atomically (reserve-then-reconcile).
"""

from __future__ import annotations

import datetime
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
from typing import Callable

import pandas

from data_validation import consts, exceptions
from third_party.ibis.ibis_addon.operations import HASH_SUM_SUBSTR_POSITION

# Tier-1 dialect registry (ADR-0003). Adding a dialect = SQL fragments below
# + an entry here + a spike proving checksum equality. Scope is deliberately
# four dialects only: BigQuery, Spanner, Postgres, MySQL (no MSSQL, Oracle,
# or Cloud SQL variants).
_HASHSUM_SUPPORTED_BACKENDS = frozenset(
    {"bigquery", "spanner", "postgres", "mysql"}
)

_ZETASQL_BACKENDS = frozenset({"bigquery", "spanner"})

# Per-side execution strategy for range expansion (dual-strategy).
_STRATEGY_OVERRIDES = {"bigquery": "group_by"}
_DEFAULT_STRATEGY = "keyset"

RESULT_COLUMNS = [
    "run_id",
    "source_table_name",
    "target_table_name",
    "validation_type",
    "chunk_start",
    "chunk_end",
    "source_rows",
    "target_rows",
    "difference_count",
    "differing_keys",
    "validation_status",
]

VALIDATION_TYPE_CHUNK_HASH = "ChunkHash"


class ChunkHashConfig(object):
    """Configuration for a ChunkHash validation run.

    Args:
        primary_keys: one or more key columns; ranges are cut on the first.
        comparison_columns: columns hashed into the row checksum.
        bisection_factor: sub-segments per expansion.
        num_buckets: intra-segment hash buckets (MOD of key hash).
        bisection_threshold: row count at/under which a mismatching segment
            is downloaded and diffed locally.
        max_depth: bisection recursion cap (reladiff-style cores omit one; we require it).
        max_diff_rows: total downloaded-row budget for the whole run.
        max_parallelism: worker threads for independent top-level queries and
            subranges. 1 disables parallelism entirely (serial execution).
    """

    def __init__(
        self,
        primary_keys,
        comparison_columns,
        bisection_factor=32,
        num_buckets=16,
        bisection_threshold=16000,
        max_depth=8,
        max_diff_rows=100000,
        max_parallelism=4,
    ):
        self.primary_keys = list(primary_keys)
        self.comparison_columns = list(comparison_columns)
        self.bisection_factor = int(bisection_factor)
        self.num_buckets = int(num_buckets)
        self.bisection_threshold = int(bisection_threshold)
        self.max_depth = int(max_depth)
        self.max_diff_rows = int(max_diff_rows)
        self.max_parallelism = max(1, int(max_parallelism))


def preflight(source_client, target_client, config):
    """Fail fast on unsupported dialects or missing configuration.

    No fallback: the caller is told to use row validation instead.
    """
    for role, client in (("source", source_client), ("target", target_client)):
        name = getattr(client, "name", None)
        if name not in _HASHSUM_SUPPORTED_BACKENDS:
            raise exceptions.ValidationException(
                f"ChunkHash validation does not support {role} dialect "
                f"'{name}'. Supported: {sorted(_HASHSUM_SUPPORTED_BACKENDS)}. "
                "Use row validation for this engine."
            )
    if not config.primary_keys:
        raise exceptions.ValidationException(
            "ChunkHash validation requires at least one primary key column. "
            "Use row validation for keyless tables."
        )
    if not config.comparison_columns:
        raise exceptions.ValidationException(
            "ChunkHash validation requires explicit comparison columns."
        )


# --------------------------------------------------------------------------
# SQL builders -- pure functions, no I/O. dialect = backend .name string.
# --------------------------------------------------------------------------


def _is_zetasql(dialect):
    return dialect in _ZETASQL_BACKENDS


def quote_ident(dialect, ident):
    if _is_zetasql(dialect):
        return "`" + ident.replace("`", "``") + "`"
    return '"' + ident.replace('"', '""') + '"'


def _quote_table(dialect, table):
    """Quote a possibly dotted table name: each part quoted separately.

    'public.farmers' must become "public"."farmers", not "public.farmers".
    """
    return ".".join(quote_ident(dialect, part) for part in table.split("."))


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _string_type(dialect):
    return "STRING" if _is_zetasql(dialect) else ("TEXT" if dialect.startswith(("postgres", "cloudsql_postgres")) else "CHAR")


def _canonical(dialect, columns):
    """'|'-joined COALESCE(CAST(col AS string), '<NULL>') over columns."""
    parts = [
        f"COALESCE(CAST({quote_ident(dialect, c)} AS {_string_type(dialect)}), '<NULL>')"
        for c in columns
    ]
    return "CONCAT(" + ", '|', ".join(parts) + ")"


def _sha256_hex(dialect, expr):
    """Full 64-char sha256 hex of a string expression."""
    if _is_zetasql(dialect):
        return f"TO_HEX(SHA256({expr}))"
    if dialect.startswith(("postgres", "cloudsql_postgres")):
        return f"ENCODE(DIGEST({expr}, 'sha256'), 'hex')"
    return f"SHA2({expr}, 256)"


def _hash_int(dialect, expr):
    """Per-row int: last 15 hex chars of sha256 hex, as exact numeric.

    Position is POSITIVE (HASH_SUM_SUBSTR_POSITION). Negative offsets are
    wrong in PostgreSQL (returns the whole string). See module docstring.
    """
    pos = HASH_SUM_SUBSTR_POSITION
    if _is_zetasql(dialect):
        return f"CAST(CONCAT('0x', SUBSTR({_sha256_hex(dialect, expr)}, {pos})) AS INT64)"
    if dialect.startswith(("postgres", "cloudsql_postgres")):
        return f"('x' || SUBSTR({_sha256_hex(dialect, expr)}, {pos}))::bit(60)::bigint"
    return f"CAST(CONV(SUBSTR({_sha256_hex(dialect, expr)}, {pos}), 16, 10) AS DECIMAL(65, 0))"


def _checksum_sum(dialect, expr):
    """COALESCE(SUM(hash_int), 0) -- exact, empty-safe segment checksum."""
    if dialect.startswith(("mysql", "cloudsql_mysql")):
        # hash_int is already DECIMAL; SUM(DECIMAL) is exact in MySQL.
        return f"COALESCE(SUM({_hash_int(dialect, expr)}), 0)"
    return f"COALESCE(SUM(CAST({_hash_int(dialect, expr)} AS NUMERIC)), 0)"


def _bucket_expr(dialect, key_columns, num_buckets):
    key_canon = _canonical(dialect, key_columns)
    return f"MOD(ABS({_hash_int(dialect, key_canon)}), {num_buckets})"


def _range_predicate(dialect, key_column, start, end, end_inclusive):
    """Half-open (or inclusive) range on the first key column."""
    key = quote_ident(dialect, key_column)
    end_op = "<=" if end_inclusive else "<"
    return f"{key} >= {sql_literal(start)} AND {key} {end_op} {sql_literal(end)}"


def _where(*conditions):
    conds = [c for c in conditions if c]
    return ("WHERE " + " AND ".join(conds)) if conds else ""


def build_min_max_sql(dialect, table, key_column, filter_sql=None):
    key = quote_ident(dialect, key_column)
    return (
        f"SELECT MIN({key}) AS kmin, MAX({key}) AS kmax FROM {_quote_table(dialect, table)} "
        + _where(filter_sql)
    )


def build_count_sql(dialect, table, key_column, start, end, end_inclusive, filter_sql=None):
    key = quote_ident(dialect, key_column)
    rng = _range_predicate(dialect, key_column, start, end, end_inclusive)
    return (
        f"SELECT COUNT(*) AS cnt FROM {_quote_table(dialect, table)} "
        + _where(rng, filter_sql)
    )


def build_boundary_sample_sql(
    dialect, table, key_column, start, end, end_inclusive, offset, filter_sql=None,
):
    """One boundary sample: the key at ordinal position `offset` (0-based).

    The sample is taken from WITHIN the range [start, end] (end inclusive per
    end_inclusive). Sampling without the range predicate at recursion depth >= 1
    applies the subrange's ordinal offsets to the whole table, yielding
    boundaries outside the subrange and overlapping sibling segments
    (double-counted totals).
    """
    key = quote_ident(dialect, key_column)
    rng = _range_predicate(dialect, key_column, start, end, end_inclusive)
    return (
        f"SELECT {key} AS b FROM {_quote_table(dialect, table)} "
        + _where(rng, filter_sql)
        + f" ORDER BY {key} LIMIT 1 OFFSET {int(offset)}"
    )


def build_keyset_checksum_sql(
    dialect, table, primary_keys, comparison_columns, start, end, end_inclusive,
    num_buckets, filter_sql=None,
):
    """Per-range bucket checksums (keyset strategy)."""
    bucket = _bucket_expr(dialect, primary_keys, num_buckets)
    ck = _checksum_sum(dialect, _canonical(dialect, comparison_columns))
    rng = _range_predicate(dialect, primary_keys[0], start, end, end_inclusive)
    return (
        f"SELECT {bucket} AS bucket, COUNT(*) AS cnt, {ck} AS checksum "
        f"FROM {_quote_table(dialect, table)} " + _where(rng, filter_sql) + " GROUP BY 1"
    )


def build_group_by_checksum_sql(
    dialect, table, primary_keys, comparison_columns, start, end, end_inclusive,
    boundaries, num_buckets, filter_sql=None,
):
    """ONE query returning bucket checksums for all sub-segments at once.

    `boundaries` are interior split points b1..bk; sub-segment i (0-based) is
    [start,b1), [b1,b2), ..., [bk, end] -- matching the driver's subrange
    construction, with the final segment inheriting the parent's end
    inclusivity.
    """
    key = quote_ident(dialect, primary_keys[0])
    cases = " ".join(f"WHEN {key} < {sql_literal(b)} THEN {i}" for i, b in enumerate(boundaries))
    seg_case = f"CASE {cases} ELSE {len(boundaries)} END"
    bucket = _bucket_expr(dialect, primary_keys, num_buckets)
    ck = _checksum_sum(dialect, _canonical(dialect, comparison_columns))
    rng = _range_predicate(dialect, primary_keys[0], start, end, end_inclusive)
    return (
        f"SELECT {seg_case} AS seg, {bucket} AS bucket, COUNT(*) AS cnt, {ck} AS checksum "
        f"FROM {_quote_table(dialect, table)} " + _where(rng, filter_sql) + " GROUP BY 1, 2"
    )


def build_bucket_download_sql(
    dialect, table, primary_keys, comparison_columns, start, end, end_inclusive,
    num_buckets, buckets=None, filter_sql=None,
):
    """Download (keys, row hash) rows, optionally only for `buckets`."""
    key_sel = ", ".join(quote_ident(dialect, k) for k in primary_keys)
    row_hash = _sha256_hex(dialect, _canonical(dialect, comparison_columns))
    conds = [_range_predicate(dialect, primary_keys[0], start, end, end_inclusive)]
    if buckets:
        bucket = _bucket_expr(dialect, primary_keys, num_buckets)
        conds.append(f"{bucket} IN ({', '.join(str(int(b)) for b in buckets)})")
    return (
        f"SELECT {key_sel}, {row_hash} AS row_hash "
        f"FROM {_quote_table(dialect, table)} " + _where(*conds, filter_sql)
    )


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def _norm_checksum(value):
    """Normalize a checksum value to a canonical string for comparison."""
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return str(value)


def _executor(client):
    """Build a Callable[[str], pandas.DataFrame] from an ibis client.

    Contract: the callable MUST return a pandas DataFrame. Phase-3 wiring may
    inject pre-adapted executors via run_chunk_hash_validation's optional
    parameters when a backend's raw_sql needs translation.
    """
    def _exec(sql):
        result = client.raw_sql(sql)
        if isinstance(result, pandas.DataFrame):
            return result
        raise exceptions.ValidationException(
            "ChunkHash executor must return a pandas DataFrame; got "
            f"{type(result).__name__}. Adapt the client's raw_sql."
        )
    return _exec


def _execute_via_sqlalchemy(engine, sql):
    """Run one read-only query against a SQLAlchemy engine, DataFrame out.

    The vendored SQL backends create engines with pool_pre_ping=True. That
    ping (dbapi.autocommit = True -> psycopg2 set_session) fails with
    "set_session cannot be used inside a transaction" on pooled connections
    returned by successive read queries -- observed deterministically (99 of
    100 checkouts) regardless of isolation options. Fresh DBAPI connections
    per call dodge the pool entirely; a ChunkHash run issues ~100 small
    read-only queries, so the per-call connect cost is irrelevant.

    Uses the engine's own connection factory (``pool._creator``) so
    creator-based engines -- e.g. the Cloud SQL Python Connector, whose URL
    carries no credentials -- keep working.
    """
    rows, columns = [], None
    creator = getattr(getattr(engine, "pool", None), "_creator", None)
    if creator is not None:
        raw = creator()
        try:
            cursor = raw.cursor()
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [d[0] for d in (cursor.description or [])]
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        finally:
            try:
                raw.close()
            except Exception:
                pass
        return pandas.DataFrame(rows, columns=columns or None)
    # Unexpected engine shape: fall back to the pooled path in AUTOCOMMIT.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        res = conn.exec_driver_sql(sql)
        rows = list(res.fetchall())
        columns = list(res.keys())
    return pandas.DataFrame(rows, columns=columns)


def client_dataframe_executor(client):
    """Build a DataFrame-returning executor for an ibis/DVT client.

    Handles the backend families DVT uses:
    - results already materialized as a pandas DataFrame,
    - SQLAlchemy cursors (``fetchall``/``keys``) -- Postgres, MySQL, Cloud SQL,
    - Spanner ``StreamedResultSet`` (column metadata appears only after the
      stream starts being consumed),
    - BigQuery ``RowIterator`` (rows convert via ``dict``).
    """
    def _exec(sql):
        engine = getattr(client, "con", None)
        # Route SQLAlchemy engines (real or wrapped) into the dedicated
        # executor: vendored engines use StaticPool + pool_pre_ping, and the
        # pre-ping fails deterministically once ibis's raw_sql has left the
        # single pooled connection mid-transaction.
        creator = getattr(getattr(engine, "pool", None), "_creator", None)
        if creator is not None or hasattr(engine, "exec_driver_sql"):
            return _execute_via_sqlalchemy(engine, sql)
        res = client.raw_sql(sql)
        if isinstance(res, pandas.DataFrame):
            return res
        if res is None:
            raise exceptions.ValidationException(
                f"ChunkHash: client '{getattr(client, 'name', '?')}' returned "
                f"no result for: {sql[:80]}"
            )
        if hasattr(res, "fetchall"):
            # Consume FIRST: Spanner result metadata (fields) appears only
            # after the stream starts being consumed.
            rows = [tuple(r) for r in res.fetchall()]
            columns = None
            if hasattr(res, "keys"):
                try:
                    columns = list(res.keys())
                except Exception:
                    columns = None
            if columns is None and hasattr(res, "columns"):
                # ibis BigQueryCursor exposes column names as a property;
                # its description is SchemaField objects (not subscriptable).
                try:
                    columns = list(res.columns)
                except Exception:
                    columns = None
            if columns is None and getattr(res, "description", None):
                columns = [c[0] for c in res.description]
            if columns is None and getattr(res, "cursor", None) is not None:
                columns = [c[0] for c in res.cursor.description]
            if columns is None:
                # SpannerCursor keeps the StreamedResultSet on .result
                inner = getattr(res, "result", None)
                fields = getattr(inner, "fields", None) or getattr(
                    res, "fields", None
                )
                if fields:
                    columns = [f.name for f in fields]
            try:
                res.close()
            except Exception:
                pass
            return pandas.DataFrame(rows, columns=columns)
        if hasattr(res, "fields"):  # Spanner StreamedResultSet
            rows = [tuple(r) for r in res]
            columns = [f.name for f in res.fields]
            return pandas.DataFrame(rows, columns=columns)
        try:  # BigQuery RowIterator and dict-able row objects
            return pandas.DataFrame([dict(r) for r in res])
        except (TypeError, ValueError):
            return pandas.DataFrame(list(res))
    return _exec


class _RunState(object):
    def __init__(self, source_exec, target_exec, source_dialect, target_dialect,
                 config, source_table, target_table, run_id, filter_sql, labels=None,
                 pool=None):
        self.source_exec = source_exec
        self.target_exec = target_exec
        self.source_dialect = source_dialect
        self.target_dialect = target_dialect
        self.config = config
        self.source_table = source_table
        self.target_table = target_table
        self.run_id = run_id
        self.filter_sql = filter_sql
        self.labels = labels or []
        self.pool = pool  # ThreadPoolExecutor or None (serial).
        self._lock = threading.Lock()  # guards downloaded_rows/budget flags.
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.rows = []
        self.downloaded_rows = 0
        self.budget_exceeded = False
        self.any_failure = False


def _parse_bucket_map(df, seg_column=None):
    """DataFrame -> {seg: {bucket: (cnt, checksum_str)}} (or no seg level)."""
    def _rows(frame):
        for _, row in frame.iterrows():
            yield int(row["bucket"]), int(row["cnt"]), _norm_checksum(row["checksum"])

    if df is None or df.empty:
        return {}
    if seg_column and seg_column in df.columns:
        maps = {}
        for _, row in df.iterrows():
            maps.setdefault(int(row[seg_column]), {})[int(row["bucket"])] = (
                int(row["cnt"]), _norm_checksum(row["checksum"])
            )
        return maps
    # Flat {bucket: (cnt, checksum)} -- consumers (count sums, comparisons,
    # mismatched-bucket selection) expect NO segment wrapper in this case.
    return {b: (c, k) for b, c, k in _rows(df)}


def _mismatched_buckets(src_map, tgt_map):
    all_buckets = set(src_map) | set(tgt_map)
    return sorted(b for b in all_buckets if src_map.get(b) != tgt_map.get(b))


def _row(run_id, source_table, target_table, chunk_start, chunk_end,
         source_rows, target_rows, difference_count, differing_keys, status):
    return {
        "run_id": run_id,
        "source_table_name": source_table,
        "target_table_name": target_table,
        "validation_type": VALIDATION_TYPE_CHUNK_HASH,
        "chunk_start": None if chunk_start is None else str(chunk_start),
        "chunk_end": None if chunk_end is None else str(chunk_end),
        "source_rows": source_rows,
        "target_rows": target_rows,
        "difference_count": difference_count,
        "differing_keys": differing_keys,
        "validation_status": status,
    }


def _classify_downloads(source_df, target_df, primary_keys):
    """Outer-join downloaded (keys, row_hash) frames; classify differences."""
    if source_df is None or source_df.empty:
        source_df = pandas.DataFrame(columns=primary_keys + ["row_hash"])
    if target_df is None or target_df.empty:
        target_df = pandas.DataFrame(columns=primary_keys + ["row_hash"])
    merged = source_df.merge(
        target_df, on=primary_keys, how="outer",
        suffixes=("_source", "_target"), indicator=True,
    )
    only_source = merged[merged["_merge"] == "left_only"]
    only_target = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]
    hash_mismatch = both[both["row_hash_source"].astype(str) != both["row_hash_target"].astype(str)]

    keys_source = only_source[primary_keys].to_dict(orient="records")
    keys_target = only_target[primary_keys].to_dict(orient="records")
    keys_mismatch = hash_mismatch[primary_keys].to_dict(orient="records")
    sample = (keys_source + keys_target + keys_mismatch)[:10]
    total = len(keys_source) + len(keys_target) + len(keys_mismatch)
    return total, sample


def _reserve(state, est):
    """Atomically reserve `est` rows of the download budget.

    Returns False (and sets budget_exceeded) if the hard cap would be
    exceeded; otherwise reserves and returns True. Reservation is
    conservative on purpose: the cap is never exceeded, even though a burst
    of large reservations can exhaust the budget slightly early (see ADR-0003
    Phase 4 note).
    """
    with state._lock:
        if state.downloaded_rows + est > state.config.max_diff_rows:
            state.budget_exceeded = True
            return False
        state.downloaded_rows += est
        return True


def _reconcile_download(state, reserved, actual):
    """Adjust the reserved download count down to what was actually fetched."""
    with state._lock:
        state.downloaded_rows += actual - reserved


def _download_and_diff(state, start, end, end_inclusive, src_map, tgt_map, buckets):
    """Bucket-filtered download + local classification.

    `buckets` is the precomputed mismatched-bucket list. Returns
    (total, sample, actual_downloaded).
    """
    cfg = state.config
    src_sql = build_bucket_download_sql(
        state.source_dialect, state.source_table, cfg.primary_keys,
        cfg.comparison_columns, start, end, end_inclusive, cfg.num_buckets,
        buckets=buckets, filter_sql=state.filter_sql,
    )
    tgt_sql = build_bucket_download_sql(
        state.target_dialect, state.target_table, cfg.primary_keys,
        cfg.comparison_columns, start, end, end_inclusive, cfg.num_buckets,
        buckets=buckets, filter_sql=state.filter_sql,
    )
    source_df = state.source_exec(src_sql)
    target_df = state.target_exec(tgt_sql)
    actual = int(len(source_df) + len(target_df))
    total, sample = _classify_downloads(source_df, target_df, cfg.primary_keys)
    return total, sample, actual


def _sample_boundaries(state, start, end, end_inclusive, count, sample_exec,
                       sample_dialect, sample_table, parallel=False):
    """Boundary keys at even ordinals within the range (from one side).

    Samples come from WITHIN [start, end] (end inclusivity per end_inclusive)
    so recursion into a subrange cannot produce boundaries that overlap
    sibling segments. `parallel` runs the independent sample queries
    concurrently (top level only; workers never re-enter the pool).
    """
    cfg = state.config
    n = cfg.bisection_factor
    if count <= 0 or n <= 1:
        return []
    offsets = [count * i // n for i in range(1, n) if 0 < count * i // n < count]

    def _sample(offset):
        sql = build_boundary_sample_sql(
            sample_dialect, sample_table, cfg.primary_keys[0],
            start, end, end_inclusive, offset, filter_sql=state.filter_sql,
        )
        df = sample_exec(sql)
        if df is not None and not df.empty:
            value = df.iloc[0, 0]
            if value is not None and str(value) != str(start):
                return value
        return None

    if parallel and state.pool is not None:
        values = list(state.pool.map(_sample, offsets))
    else:
        values = [_sample(o) for o in offsets]
    # Deduplicate while preserving order (sparse keys may repeat samples).
    seen, unique = set(), []
    for b in values:
        if b is None:
            continue
        key = str(b)
        if key not in seen:
            seen.add(key)
            unique.append(b)
    return unique


def _resolve_subrange(state, sub_start, sub_end, sub_inc, sub_src, sub_tgt, depth):
    """Resolve one subrange (which may recurse). Returns a list of rows."""
    cfg = state.config
    sub_src_count = sum(c for c, _ in sub_src.values())
    sub_tgt_count = sum(c for c, _ in sub_tgt.values())
    if sub_src == sub_tgt:
        return [_row(state.run_id, state.source_table, state.target_table,
                     sub_start, sub_end, sub_src_count, sub_tgt_count, 0, [], "success")]
    if max(sub_src_count, sub_tgt_count) <= cfg.bisection_threshold:
        return [_finish_leaf(state, sub_start, sub_end, sub_inc,
                             sub_src_count, sub_tgt_count, sub_src, sub_tgt)]
    if depth + 1 <= cfg.max_depth:
        return _process_range(state, sub_start, sub_end, sub_inc, depth + 1)
    # Depth cap reached: download if the budget allows (the leaf's reserve
    # decides), else a budget_exceeded row.
    return [_finish_leaf(state, sub_start, sub_end, sub_inc,
                         sub_src_count, sub_tgt_count, sub_src, sub_tgt)]


def _process_range(state, start, end, end_inclusive, depth):
    """Resolve a range into result rows (may recurse). Returns a list of rows.

    With parallel execution enabled (state.pool and depth == 0), the boundary
    sampling, per-side keyset map batches, and subrange resolution are
    dispatched to the pool. Workers never re-enter the pool, so recursion
    stays serial and there is no oversubscription. Results are always joined
    in submission order, so output is deterministic regardless of parallelism.
    """
    cfg = state.config
    key_col = cfg.primary_keys[0]
    parallel = state.pool is not None and depth == 0

    src_count_sql = build_count_sql(state.source_dialect, state.source_table, key_col, start, end, end_inclusive, state.filter_sql)
    tgt_count_sql = build_count_sql(state.target_dialect, state.target_table, key_col, start, end, end_inclusive, state.filter_sql)
    src_count = int(state.source_exec(src_count_sql).iloc[0, 0])
    tgt_count = int(state.target_exec(tgt_count_sql).iloc[0, 0])

    if src_count == 0 and tgt_count == 0:
        return [_row(state.run_id, state.source_table, state.target_table,
                     start, end, 0, 0, 0, [], "success")]

    # Sample boundaries from the side that has rows (fall back to target).
    sample_side_source = src_count >= tgt_count
    sample_exec = state.source_exec if sample_side_source else state.target_exec
    sample_dialect = state.source_dialect if sample_side_source else state.target_dialect
    sample_table = state.source_table if sample_side_source else state.target_table
    boundaries = _sample_boundaries(state, start, end, end_inclusive,
                                    max(src_count, tgt_count), sample_exec,
                                    sample_dialect, sample_table, parallel=parallel)

    if not boundaries:
        # Range cannot be split further (all samples collapse): diff directly,
        # guarded by the budget.
        src_sql = build_keyset_checksum_sql(state.source_dialect, state.source_table, cfg.primary_keys, cfg.comparison_columns, start, end, end_inclusive, cfg.num_buckets, state.filter_sql)
        tgt_sql = build_keyset_checksum_sql(state.target_dialect, state.target_table, cfg.primary_keys, cfg.comparison_columns, start, end, end_inclusive, cfg.num_buckets, state.filter_sql)
        src_map = _parse_bucket_map(state.source_exec(src_sql))
        tgt_map = _parse_bucket_map(state.target_exec(tgt_sql))
        return [_finish_leaf(state, start, end, end_inclusive, src_count, tgt_count, src_map, tgt_map)]

    # Subranges: [start,b1), [b1,b2), ..., [bk, end]; the final one inherits
    # the parent's end inclusivity.
    edges = [start] + boundaries
    subranges = [(edges[i], edges[i + 1], False) for i in range(len(edges) - 1)]
    subranges.append((boundaries[-1], end, end_inclusive))

    # Per-side sub-segment bucket maps, each via its optimal strategy.
    def _maps_for(dialect, table, exec_):
        strategy = _STRATEGY_OVERRIDES.get(dialect, _DEFAULT_STRATEGY)
        if strategy == "group_by":
            sql = build_group_by_checksum_sql(
                dialect, table, cfg.primary_keys, cfg.comparison_columns,
                start, end, end_inclusive, boundaries, cfg.num_buckets, state.filter_sql,
            )
            seg_maps = _parse_bucket_map(exec_(sql), seg_column="seg")
            return [seg_maps.get(i, {}) for i in range(len(subranges))]

        def _query(sub):
            sub_start, sub_end, sub_inc = sub
            sql = build_keyset_checksum_sql(
                dialect, table, cfg.primary_keys, cfg.comparison_columns,
                sub_start, sub_end, sub_inc, cfg.num_buckets, state.filter_sql,
            )
            return _parse_bucket_map(exec_(sql))

        if parallel and state.pool is not None:
            return list(state.pool.map(_query, subranges))
        return [_query(sub) for sub in subranges]

    src_maps = _maps_for(state.source_dialect, state.source_table, state.source_exec)
    tgt_maps = _maps_for(state.target_dialect, state.target_table, state.target_exec)

    if parallel and state.pool is not None:
        futures = [
            state.pool.submit(_resolve_subrange, state, sub_start, sub_end,
                              sub_inc, sub_src, sub_tgt, depth)
            for (sub_start, sub_end, sub_inc), sub_src, sub_tgt
            in zip(subranges, src_maps, tgt_maps)
        ]
        rows = []
        for future in futures:
            rows.extend(future.result())
        return rows

    rows = []
    for (sub_start, sub_end, sub_inc), sub_src, sub_tgt in zip(subranges, src_maps, tgt_maps):
        rows.extend(_resolve_subrange(state, sub_start, sub_end, sub_inc,
                                      sub_src, sub_tgt, depth))
    return rows


def _finish_leaf(state, start, end, end_inclusive, src_count, tgt_count, src_map, tgt_map):
    """Download mismatched buckets, classify, and return one result row.

    Equal maps short-circuit to a success row without any download (the
    subrange loop also checks equality before calling; this covers the
    'no boundaries -> leaf' path where repeated boundary samples collapse
    and every segment would otherwise download in full).
    """
    if src_map == tgt_map:
        return _row(state.run_id, state.source_table, state.target_table,
                    start, end, src_count, tgt_count, 0, [], "success")
    buckets = _mismatched_buckets(src_map, tgt_map)
    est = (
        sum(c for b, (c, _) in src_map.items() if b in buckets)
        + sum(c for b, (c, _) in tgt_map.items() if b in buckets)
    )
    if not _reserve(state, est):
        return _row(state.run_id, state.source_table, state.target_table,
                    start, end, src_count, tgt_count, -1, [], "budget_exceeded")
    total, sample, actual = _download_and_diff(
        state, start, end, end_inclusive, src_map, tgt_map, buckets
    )
    _reconcile_download(state, est, actual)
    status = "failure" if total > 0 else "success"
    if total > 0:
        state.any_failure = True
    return _row(state.run_id, state.source_table, state.target_table,
                start, end, src_count, tgt_count, total, sample, status)


def _assemble_result_dataframe(state):
    """Emit DVT's standard result schema (mirrors SchemaValidation's layout).

    ChunkHash-specific detail (chunk ranges, differing keys) rides along as
    extra trailing columns; the standard block keeps every existing result
    handler and results table working unmodified.
    """
    rows = state.rows
    n = len(rows)
    end_time = datetime.datetime.now(datetime.timezone.utc)
    df = pandas.DataFrame(
        {
            consts.SOURCE_COLUMN_NAME: ["hash_sum"] * n,
            consts.TARGET_COLUMN_NAME: ["hash_sum"] * n,
            consts.SOURCE_AGG_VALUE: [r["source_rows"] for r in rows],
            consts.TARGET_AGG_VALUE: [r["target_rows"] for r in rows],
            consts.VALIDATION_STATUS: [r["validation_status"] for r in rows],
        }
    )
    df.insert(loc=0, column=consts.CONFIG_RUN_ID, value=state.run_id)
    df.insert(loc=1, column=consts.VALIDATION_NAME, value="ChunkHash")
    df.insert(loc=2, column=consts.VALIDATION_TYPE, value="ChunkHash")
    df.insert(loc=3, column=consts.CONFIG_LABELS, value=[state.labels] * n)
    df.insert(loc=4, column=consts.CONFIG_START_TIME, value=state.start_time)
    df.insert(loc=5, column=consts.CONFIG_END_TIME, value=end_time)
    df.insert(loc=6, column=consts.SOURCE_TABLE_NAME, value=state.source_table)
    df.insert(loc=7, column=consts.TARGET_TABLE_NAME, value=state.target_table)
    df.insert(loc=10, column=consts.AGGREGATION_TYPE, value="ChunkHash")
    df.insert(
        loc=14,
        column=consts.CONFIG_PRIMARY_KEYS,
        value=[json.dumps(state.config.primary_keys)] * n,
    )
    df.insert(loc=15, column=consts.NUM_RANDOM_ROWS, value=None)
    df.insert(loc=16, column=consts.GROUP_BY_COLUMNS, value=None)
    df.insert(
        loc=17,
        column=consts.VALIDATION_DIFFERENCE,
        value=[r["difference_count"] for r in rows],
    )
    df.insert(loc=18, column=consts.VALIDATION_PCT_THRESHOLD, value=None)
    df["chunk_start"] = [r["chunk_start"] for r in rows]
    df["chunk_end"] = [r["chunk_end"] for r in rows]
    df["differing_keys"] = [r["differing_keys"] for r in rows]
    return df


def run_chunk_hash_validation(
    source_client,
    target_client,
    config,
    run_id,
    source_table,
    target_table,
    filter_sql=None,
    source_executor=None,
    target_executor=None,
    labels=None,
):
    """Run a ChunkHash validation; returns the result as a pandas DataFrame.

    Executors default to the clients' raw_sql and MUST return pandas
    DataFrames (inject adapted callables via source_executor/target_executor
    when a backend needs translation). The result uses DVT's standard result
    schema so existing result handlers work unmodified.
    """
    preflight(source_client, target_client, config)
    source_exec = source_executor or _executor(source_client)
    target_exec = target_executor or _executor(target_client)
    pool = None
    if config.max_parallelism > 1:
        pool = ThreadPoolExecutor(max_workers=config.max_parallelism)
    state = _RunState(
        source_exec, target_exec,
        getattr(source_client, "name", ""), getattr(target_client, "name", ""),
        config, source_table, target_table, run_id, filter_sql, labels=labels,
        pool=pool,
    )

    key_col = config.primary_keys[0]
    src_mm = state.source_exec(build_min_max_sql(state.source_dialect, source_table, key_col, filter_sql))
    tgt_mm = state.target_exec(build_min_max_sql(state.target_dialect, target_table, key_col, filter_sql))
    src_min, src_max = (src_mm.iloc[0, 0], src_mm.iloc[0, 1]) if not src_mm.empty else (None, None)
    tgt_min, tgt_max = (tgt_mm.iloc[0, 0], tgt_mm.iloc[0, 1]) if not tgt_mm.empty else (None, None)

    def _missing(v):
        # Backends may surface empty aggregates as NaN rather than None.
        return v is None or (not isinstance(v, str) and pandas.isna(v))

    src_min = None if _missing(src_min) else src_min
    tgt_min = None if _missing(tgt_min) else tgt_min
    src_max = None if _missing(src_max) else src_max
    tgt_max = None if _missing(tgt_max) else tgt_max

    if src_min is None and tgt_min is None:
        state.rows.append(_row(run_id, source_table, target_table, None, None, 0, 0, 0, [], "success"))
    else:
        lo = src_min if src_min is not None else tgt_min
        hi = src_max if src_max is not None else tgt_max
        if pool is not None:
            # DVT's own executor pattern (data_validation.py _execute_validation):
            # a with block that shuts the pool down (wait=True) on exit.
            with pool:
                state.rows.extend(_process_range(state, lo, hi, True, 0))
        else:
            state.rows.extend(_process_range(state, lo, hi, True, 0))

    # Summary row: aggregates of the terminal chunk rows.
    src_total = sum(r["source_rows"] for r in state.rows if r["source_rows"] >= 0)
    tgt_total = sum(r["target_rows"] for r in state.rows if r["target_rows"] >= 0)
    diff_total = sum(r["difference_count"] for r in state.rows if r["difference_count"] >= 0)
    status = ("budget_exceeded" if state.budget_exceeded
              else "failure" if state.any_failure or diff_total > 0
              else "success")
    state.rows.append(_row(run_id, source_table, target_table, None, None,
                           src_total, tgt_total, diff_total, [], status))

    logging.info(
        "ChunkHash validation finished: status=%s chunks=%d downloaded_rows=%d",
        status, max(len(state.rows) - 1, 0), state.downloaded_rows,
    )
    return _assemble_result_dataframe(state)
