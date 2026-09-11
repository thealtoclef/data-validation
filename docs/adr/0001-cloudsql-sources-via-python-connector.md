# Cloud SQL sources as new source types via the Python Connector

```yaml
status: accepted
```

This fork must reach Cloud SQL Postgres/MySQL without a running `cloud_sql_proxy`
process, with all auth modes (IAM, PSC, private IP). Ibis backends construct their
own SQLAlchemy engine from `host/port/user/password` and accept no `creator=`
callable, so the Cloud SQL Python Connector cannot be injected via connection
parameters — it requires a different engine-construction path.

**Decision**: add `CloudSQLPostgres` and `CloudSQLMySQL` as **new source types**
(subclasses of the existing ibis backends, `third_party/ibis/ibis_cloudsql/` with a
shared `_connector.py` owning Connector lifecycle) instead of flags on the existing
`Postgres`/`MySQL` types. Rationale: zero edits to upstream backend behaviour —
upstream methods are inherited, rebase conflicts stay confined to end-of-dict
appends (`consts.py`, `cli_tools.py`, `clients.py`, `setup.py`).

## Considered options

- Flag/field on existing Postgres/MySQL types — rejected: edits upstream
  `do_connect`, hurts rebasability.
- Keeping the proxy — rejected: it is the runtime dependency this fork removes.

## Consequences

- `pg8000` added as a dependency (psycopg2 coexists; do not swap).
- Connector instances are cached per `(instance, ip_type, iam)` and must be disposed
  on engine disposal / process exit.
- Two more source types in the CLI surface; users must know which to pick.
