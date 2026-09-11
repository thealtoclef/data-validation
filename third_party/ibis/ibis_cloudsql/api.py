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

"""Factory functions for the Cloud SQL backends.

These signatures must match the field names declared in
``data_validation.cli_tools.CONNECTION_SOURCE_FIELDS`` because
``clients.get_data_client`` dispatches via ``CLIENT_LOOKUP[source_type](**config)``.
"""

from third_party.ibis.ibis_cloudsql.mysql import CloudSQLMySQLBackend
from third_party.ibis.ibis_cloudsql.postgres import CloudSQLPostgresBackend


def cloudsql_postgres_connect(
    instance_connection_name,
    user=None,
    password=None,
    database=None,
    ip_type="public",
    enable_iam_auth=False,
    credentials=None,
):
    """Create a Cloud SQL PostgreSQL backend for use with Ibis.

    Parameters
    ----------
    instance_connection_name : str
        The Cloud SQL instance connection name, e.g. ``project:region:instance``.
    user : str, optional
        Database user (or IAM principal email when ``enable_iam_auth``).
    password : str, optional
        Password for the supplied user (omit for IAM auth).
    database : str, optional
        Database to connect to.
    ip_type : str, optional
        IP type: "public", "private", or "psc" (default "public").
    enable_iam_auth : bool, optional
        Use IAM database authentication (default False).
    credentials : google.auth.credentials.Credentials, optional
        Credentials forwarded to the Cloud SQL Connector.
    """
    backend = CloudSQLPostgresBackend()
    backend.do_connect(
        instance_connection_name=instance_connection_name,
        user=user,
        password=password,
        database=database,
        ip_type=ip_type,
        enable_iam_auth=enable_iam_auth,
        credentials=credentials,
    )
    return backend


def cloudsql_mysql_connect(
    instance_connection_name,
    user=None,
    password=None,
    database=None,
    ip_type="public",
    enable_iam_auth=False,
    credentials=None,
):
    """Create a Cloud SQL MySQL backend for use with Ibis.

    Parameters
    ----------
    instance_connection_name : str
        The Cloud SQL instance connection name, e.g. ``project:region:instance``.
    user : str, optional
        Database user (or IAM principal email when ``enable_iam_auth``).
    password : str, optional
        Password for the supplied user (omit for IAM auth).
    database : str, optional
        Database to connect to.
    ip_type : str, optional
        IP type: "public", "private", or "psc" (default "public").
    enable_iam_auth : bool, optional
        Use IAM database authentication (default False).
    credentials : google.auth.credentials.Credentials, optional
        Credentials forwarded to the Cloud SQL Connector.
    """
    backend = CloudSQLMySQLBackend()
    backend.do_connect(
        instance_connection_name=instance_connection_name,
        user=user,
        password=password,
        database=database,
        ip_type=ip_type,
        enable_iam_auth=enable_iam_auth,
        credentials=credentials,
    )
    return backend
