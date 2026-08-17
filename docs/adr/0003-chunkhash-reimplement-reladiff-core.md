# ChunkHash validation: reimplement the reladiff hash-diff core in DVT

```yaml
status: accepted
date: 2026-08-15
```

Row validation today pulls every row hash from both sides into pandas and
outer-joins in memory — `O(rows)` memory and network even when tables are
identical, contradicting this fork's thin-container goal (all heavy compute in the
warehouse, like column validation). The `reladiff` project (MIT) proves the
algorithm class: checksum bisection where identical chunks are proven identical by
transferring two integers.

**Decision**: **reimplement the ~300-line hash-diff core** as a new additive
validation type (`ChunkHash`) in DVT rather than importing reladiff, reusing DVT's
existing per-dialect hash/concat machinery — which already solves the hard part,
cross-dialect hash consistency. The chunk checksum is `SUM(hash_as_int)` (NOT
hash-XOR — XOR cancels duplicate rows) cast to NUMERIC before SUM so it is exact
and overflow-free on every dialect. Missing prerequisites (primary key, supported
dialect) hard-error at validation start; no fallback.

**Dialect scope is deliberately restricted** to Tier 1: BigQuery, Spanner,
Postgres, MySQL. Cloud SQL variants and other engines (MSSQL, Oracle, …) are
out of scope and rejected at run start. Enforced by a registry in
`chunk_hash_validation.py`. Upstream DVT's engine breadth is explicitly not a goal
of this fork. BQ↔Spanner get a ZetaSQL fast path (`FARM_FINGERPRINT` → INT64
directly, no hex conversion).

## Considered options

- Import reladiff as a dependency — rejected: pulls in `sqeleton` (~15k LOC), a
  second SQL generator competing with ibis; no Spanner dialect; single-maintainer
  velocity; we'd write a Spanner dialect for it anyway.
- reladiff's joindiff mode — rejected: requires both tables co-located on one
  connection (federation); we refuse to move data across sides.
- Polars/DuckDB compare engine — rejected: fixes container RAM but still moves all
  rows over the wire; chunk-hash makes that unnecessary for the common case.
- Status quo (pandas brute-force row validation) — kept as the fallback path for
  keyless tables.

## Consequences

- Common case (few diffs) drops to KBs transferred and near-zero container memory;
  pathological case (everything differs) is worse than brute force — guarded by a
  `max_diff_rows` budget that stops bisecting and defers to row validation.
- MSSQL is permanently risky: data-diff dropped it over MD5 throughput (~100×
  slower than Postgres). Measure before ever adding it.
- Cross-dialect hash-int determinism (hex→int truncation SQL) must be proven by a
  live spike before implementation — divergence fails silently as "every chunk
  differs".

## Spike outcome (Phase 0, 2026-08-17 — gate RESOLVED)

Live proof on 5000 identical rows (Spanner `Farmers` ↔ local PostgreSQL 16),
whole-table + 8 key-range chunks + empty range:

- **PASS — checksum equality**: `SUM(CAST(<hash_int> AS NUMERIC))` identical on
  every range. Verified expressions:
  - ZetaSQL (Spanner/BQ): `CAST(CONCAT('0x', SUBSTR(TO_HEX(SHA256(canon)), 50)) AS INT64)`
  - PostgreSQL: `('x' || substr(encode(digest(canon,'sha256'),'hex'), 50))::bit(60)::bigint`
- **PASS — empty range**: `SUM` of empty set → NULL → `COALESCE → 0` on both engines.
- **PASS — sensitivity**: excluding one row changes the checksum.
- **Load-bearing trap found**: `SUBSTR(h, -15)` means "last 15 chars" in ZetaSQL
  but in PostgreSQL a negative start returns the whole string — checksums
  diverged on every range while row counts still matched (i.e. the failure mode
  is "every chunk differs", triggering full downloads, not an error). Positive
  position (50 = last 15 of 64 hex chars) is mandatory everywhere. Do not
  "simplify" back to negative offsets.
- **FARM_FINGERPRINT fast path (BQ↔Spanner)**: cross-engine equality was
  formally unproven at spike time (BigQuery was blocked from that environment by
  VPC Service Controls). Resolved by live verification 2026-08-18 — see the
  "Live BigQuery verification" section below. Tier-1 note: PG canonical
  columns were restricted to STRING+INT64 for the spike (timestamp/bool/float
  normalization is inherited-DVT machinery, Phase 4).
- Key-range semantics: UUID string keys compare lexicographically; PG columns
  need `COLLATE "C"` to match Spanner's byte-wise ordering.

## Amendment (2026-08-17): orchestration refinements

Outcome: **not reladiff word-for-word — the reladiff checksum-bisection core
with refined orchestration, our spike-verified checksum, and our guardrails.**

- **Independent validation**: a reladiff-class checksum implementation converged
  on expressions identical to ours (SUBSTR(sha256hex, 50) → 60-bit int →
  NUMERIC SUM). Independent implementations agreeing confirms the Phase 0/1
  design.
- **Refinements adopted**:
  1. Boundary sampling (`SELECT key ORDER BY key LIMIT 1 OFFSET n`) replaces
     arithmetic range splitting — balanced segments for any key type,
     including UUID strings.
  2. Dual execution strategy, comparison-agnostic: the columnar side (BigQuery)
     runs ONE `GROUP BY (CASE segment_id, MOD(ABS(key_hash), n))` query; row
     sides (Spanner/PG/MySQL) run N keyset range queries. Both produce identical
     `{segment: {bucket: (count, checksum)}}` maps; the comparator never knows
     which strategy produced them. This is the "two strategies, two DBs, same
     result" architecture.
  3. Intra-segment hash buckets narrow below-threshold downloads to mismatched
     buckets only.
- **Not adopted**: polars leaf diff (new dependency for ≤16k-row joins pandas
  handles); first-column-only compound-key splitting (we keep full-key-prefix
  ranges); a missing recursion cap (we keep max_depth + max_diff_rows).

## Live BigQuery verification (2026-08-18) — G-A and G-B CLOSED

Run from an environment outside the VPC perimeter, with BigQuery as the SOURCE
(execution project `cake-user-adhoc`, fixture `cake-data-non-production` dataset
`dvt_chunkhash_spike.farmers`, 5000 rows copied from Spanner `Farmers`) and
Spanner as the target (`sp_conn`, 5000 rows). Validation command mirrors the
PG proof:

```
validate chunk-hash -sc bq_conn -tc sp_conn
  -tbls 'cake-data-non-production.dvt_chunkhash_spike.farmers=Farmers'
  -pk FarmerId -cc FirstName,LastName,Email,PhoneNumber,Address,City,Province,PostalCode
```

- **G-A PASS — `group_by` strategy executes live on BigQuery.** Identical
  fixtures: all 32 chunks `success`, summary 5000=5000. One-row diff seeded in
  the BQ copy (FirstName changed on a single FarmerId): rerun finished
  `status=failure chunks=32 downloaded_rows=26`, and the mismatching leaf chunk
  reported `differing_keys=[{'FarmerId': '<seeded uuid>'}]` — the same
  bisect-to-leaf + differing_keys behavior as the PG proof. This is the first
  live execution of `build_group_by_checksum_sql` (single `CASE ... GROUP BY 1,2`
  query per expansion) and of a BigQuery↔Spanner checksum comparison.
- **G-B PASS — FARM_FINGERPRINT equality.** 50-row sample of
  `FARM_FINGERPRINT(<8-column canonical>)` compared between BigQuery and
  Spanner: identical INT64 on all 50, 0 mismatches. The ZetaSQL fast path
  (fingerprint → INT64 directly, no hex round-trip) is accepted for BQ↔Spanner.
- **Executor hardening**: live BQ execution exposed one gap in
  `client_dataframe_executor` — ibis's `BigQueryCursor` exposes column names via
  a `columns` property, not `.keys()`, and its `.description` is a list of
  `SchemaField` objects (not subscriptable). A `columns`-property fallback was
  added to the cursor branch; the Spanner path is unaffected (SpannerCursor has
  no `columns`, falls through to its existing `.fields` handling). Verified by
  the G-A runs above and the unit suite.
- Cross-engine canonical STRING equality for the 8 STRING columns is now proven
  live on both the hex-SHA256 path (G-A) and the FARM_FINGERPRINT path (G-B).

## Phase 4: parallel chunk execution (2026-08-18)

The orchestrator was serial end to end: for a row-store side the first
expansion issued ~32 boundary-sample queries then ~32 keyset-checksum queries
one at a time, then resolved each subrange serially (each recursing serially).
All of that is independent I/O — the ChunkHash executors are already
connection-per-query by design (SQLAlchemy fresh DBAPI conn per call via the
engine's pool `_creator`; Spanner opens a fresh snapshot per `raw_sql`;
BigQuery issues a fresh `client.query()` per call with only the idempotent
`_make_session` lazy-init shared), so concurrent queries are safe.

**Decision**: parallelize the independent top-level work with a
`ThreadPoolExecutor`, preserving exact serial result semantics, deterministic
result-row order, and the `max_diff_rows` hard budget.

- **Parallelism model**: only the top level (`depth == 0`) dispatches to the
  pool — one `_resolve_subrange` future per subrange plus batched boundary
  sampling and batched keyset-checksum queries (BigQuery stays a single
  `GROUP BY` query). Workers never re-enter the pool, so recursion stays serial
  and there is no oversubscription. Results are joined in submission order, so
  output is deterministic regardless of parallelism.
- **Knob**: `--max-parallelism` (default 4). `1` disables parallelism entirely —
  no pool is created and the serial path runs unchanged.
- **Budget under concurrency**: downloads use atomic reserve-then-reconcile on a
  per-run lock. `_reserve` takes the estimate atomically and refuses past
  `max_diff_rows`; the download runs outside the lock (concurrent downloads
  overlap — the point of the feature); `_reconcile_download` brings the counter
  back to the actual fetch after. Trade-off: reservations are conservative — a
  burst of large reservations can exhaust the budget slightly early before
  concurrent downloads reconcile. Accepted: the cap is never exceeded, and the
  strict hold-lock-across-download alternative would serialize the download
  phase, defeating the feature.
- **Confirmed bugs fixed in the same change** (found by an adversarial review of
  the bisection/budget logic; none had been exercised live because the 5000-row
  runs never recurse):
  1. Boundary sampling ignored the current range — `build_boundary_sample_sql`
     had no range predicate, so at recursion depth ≥ 1 the subrange's ordinal
     offsets were applied to the whole table, producing overlapping segments and
     double-counted totals. Now scoped to `[start, end]` (F1).
  2. An empty side's MAX was not NaN-normalized like its MIN, so a one-side-empty
     run could compute `hi = NaN` and silently report success while dropping the
     populated side's rows (F2).
  3. The "no boundaries → leaf" path lacked the equality short-circuit, so an
     identical single-key segment downloaded in full (or tripped the budget)
     instead of comparing equal (F3).
  4. The download budget was estimated from ALL buckets while the download is
     bucket-filtered, causing false budget_exceeded that masked real differences
     (F4).
  5. Boundary sampling picked its table by executor identity, which broke when
     both sides shared one executor (F5).
