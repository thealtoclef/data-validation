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
Postgres/CloudSQL-PG, MySQL/CloudSQL-MySQL. Enforced by a registry in
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
