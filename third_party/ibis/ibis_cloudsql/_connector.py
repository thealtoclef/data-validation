# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared Cloud SQL Python Connector lifecycle for the Cloud SQL backends.

This module is the single DRY anchor used by both
``CloudSQLPostgresBackend`` and ``CloudSQLMySQLBackend``. It owns:

* the process-wide ``Connector`` cache (one ``Connector`` per
  ``(instance_connection_name, ip_type, enable_iam_auth)`` tuple, reused for
  the engine's lifetime rather than created per query),
* the ``creator=`` callable factory for ``sqlalchemy.create_engine``,
* ``ip_type`` / ``enable_iam_auth`` normalization (CLI/JSON values arrive as
  strings),
* ``atexit`` cleanup so long-lived processes (Airflow, Cloud Run Jobs) do not
  leak ``Connector`` instances.
"""

import atexit

from google.cloud.sql.connector import Connector, IPTypes

# Cache keyed by (instance_connection_name, ip_type, enable_iam_auth) so one
# engine reuses one Connector for its entire lifetime. Thread-safe: the
# connector supports concurrent connect() calls from multiple threads.
_connectors = {}

# "LAZY" is recommended for serverless environments (Cloud Run, Cloud
# Functions): it avoids background-refresh CPU throttling on short-lived
# processes.
_DEFAULT_REFRESH_STRATEGY = "LAZY"

# String forms accepted for ip_type, mapped onto the IPTypes enum.
_IP_TYPE_MAP = {
    "public": IPTypes.PUBLIC,
    "private": IPTypes.PRIVATE,
    "psc": IPTypes.PSC,
}

# String forms accepted for boolean-ish fields from CLI/JSON config.
_TRUE_VALUES = ("true", "1", "yes", "on")


def _as_ip_type(ip_type):
    """Return an IPTypes member for a string or an IPTypes value.

    Accepts "public" / "private" / "psc" (case-insensitive) or an existing
    ``IPTypes`` member, which is passed through unchanged.
    """
    if isinstance(ip_type, IPTypes):
        return ip_type
    return _IP_TYPE_MAP[str(ip_type).lower()]


def _as_bool(value):
    """Coerce a CLI/JSON string or bool to a real bool."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUE_VALUES


def _cache_key(instance_connection_name, ip_type, enable_iam_auth):
    return (
        instance_connection_name,
        str(_as_ip_type(ip_type)),
        _as_bool(enable_iam_auth),
    )


def get_connector(
    instance_connection_name,
    ip_type="public",
    enable_iam_auth=False,
    credentials=None,
):
    """Return a cached (or newly created) Cloud SQL ``Connector``.

    Parameters
    ----------
    instance_connection_name : str
        The Cloud SQL instance connection name, e.g.
        ``project:region:instance``.
    ip_type : str or IPTypes (default "public")
        IP type to use: "public", "private", or "psc".
    enable_iam_auth : bool (default False)
        Use IAM database authentication for the connection.
    credentials : google.auth.credentials.Credentials, optional
        Credentials forwarded to the ``Connector``.

    Returns
    -------
    google.cloud.sql.connector.Connector
        A process-wide Connector for the given instance/ip_type/iam combo.
    """
    key = _cache_key(instance_connection_name, ip_type, enable_iam_auth)
    connector = _connectors.get(key)
    if connector is None:
        connector = Connector(
            ip_type=_as_ip_type(ip_type),
            enable_iam_auth=_as_bool(enable_iam_auth),
            refresh_strategy=_DEFAULT_REFRESH_STRATEGY,
            credentials=credentials,
        )
        _connectors[key] = connector
    return connector


def make_creator(
    connector,
    driver,
    instance_connection_name,
    user,
    password,
    db,
    enable_iam_auth=False,
):
    """Return a ``creator=`` callable for ``sqlalchemy.create_engine``.

    The returned callable returns a fresh DBAPI connection through the Cloud
    SQL Connector; SQLAlchemy calls it when the pool needs a new connection.
    """

    iam_auth = _as_bool(enable_iam_auth)

    def _creator():
        return connector.connect(
            instance_connection_name,
            driver,
            user=user,
            password=password,
            db=db,
            enable_iam_auth=iam_auth,
        )

    return _creator


def close_all_connectors():
    """Close every cached Connector.

    Registered via ``atexit`` at module import so long-lived processes do not
    leak Connector instances on shutdown.
    """
    for connector in list(_connectors.values()):
        try:
            connector.close()
        except Exception:
            # Never let cleanup failures mask the normal exit path.
            pass
    _connectors.clear()


atexit.register(close_all_connectors)
