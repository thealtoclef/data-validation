# Spanner Data Boost: pre-selected routing by SQL shape, no fallback

```yaml
status: accepted
```

`use_databoost` on the Spanner source must route eligible analytical queries through
serverless compute. Data Boost is not a flag on `execute_sql`: the proto
hard-requires `partition_token` with `data_boost_enabled`, forcing a partitioned
execution path — and eligibility is plan-dependent (the query root must be a
distributed union). A live spike settled eligibility: row-validation queries
(full SELECT, hash/concat over rows) are root-partitionable; column-validation
queries (COUNT/SUM/GROUP BY/checksums) are **structurally ineligible** — the
aggregation operator sits above any distributed union in the plan, always.

**Decision**: the backend **pre-selects** the execution route from the compiled SQL
shape (`_is_aggregate_sql`): aggregate SQL always takes the default snapshot path
even when `use_databoost=true`; non-aggregate row queries take the partitioned
`to_pandas_databoost()` path. There is **no fallback and no retry** — if the
partition path fails (e.g. missing `spanner.databases.useDataBoost` IAM), the error
propagates. No guard lives in `data_validation.py`; the routing decision belongs
entirely to the Spanner backend.

## Considered options

- Fallback to snapshot path on partition failure — rejected: silent cost/perf
  surprises; failures should be loud (same philosophy as hard-erroring).
- Runtime guard in `DataValidation.execute()` blocking column validation with the
  flag — rejected: column validation works fine through the default path; the flag
  simply doesn't apply to it, so nothing needs blocking.
- Per-query eligibility probing — rejected: plan-shape rules were proven structural
  by the spike; regex on compiled SQL is sufficient for DVT-generated queries.

## Consequences

- A custom query whose aggregate hides in a subquery but is still partitionable
  would be routed to the default path — safe (correct results, no Data Boost), never
  the reverse.
- Aggregate routing is safe by construction: the aggregate list is explicit, so a
  false negative (aggregate sent to partition path) cannot happen.
- Below-threshold row downloads in future features are automatically
  Data-Boost-eligible (non-aggregate).
