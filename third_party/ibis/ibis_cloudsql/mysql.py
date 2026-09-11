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

"""Cloud SQL MySQL backend (via the Cloud SQL Python Connector)."""

# Import the fork's MySQL overrides (list_primary_key_columns,
# dvt_tuple_in_supported, compiler/datatype patches) so they are active on
# this subclass too.
import third_party.ibis.ibis_mysql  # noqa: F401

import sqlalchemy as sa
from ibis.backends.mysql import Backend as MySQLBackend

from third_party.ibis.ibis_cloudsql import _connector


class CloudSQLMySQLBackend(MySQLBackend):
    name = "cloudsql_mysql"

    def do_connect(
        self,
        instance_connection_name,
        user=None,
        password=None,
        database=None,
        ip_type="public",
        enable_iam_auth=False,
        credentials=None,
        **kwargs,
    ):
        """Connect to a Cloud SQL MySQL instance.

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
        connector = _connector.get_connector(
            instance_connection_name,
            ip_type=ip_type,
            enable_iam_auth=enable_iam_auth,
            credentials=credentials,
        )
        creator = _connector.make_creator(
            connector,
            "pymysql",
            instance_connection_name,
            user,
            password,
            database,
            enable_iam_auth,
        )

        # Dummy URL: the dialect selects the pymysql driver; the creator is
        # authoritative for opening each connection through the Connector.
        self.con = sa.create_engine(
            "mysql+pymysql://", creator=creator, pool_pre_ping=True
        )

        # Equivalent of super().do_connect() below (mirrors the base ibis
        # MySQL backend finalization: con, _inspector, _schemas, _temp_views).
        self._inspector = sa.inspect(self.con)
        self._schemas = {}
        self._temp_views = set()
        self.database_name = database
