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

"""Cloud SQL backends (PostgreSQL + MySQL) via the Cloud SQL Python Connector."""

from third_party.ibis.ibis_cloudsql.api import (
    cloudsql_mysql_connect,
    cloudsql_postgres_connect,
)
from third_party.ibis.ibis_cloudsql.mysql import CloudSQLMySQLBackend
from third_party.ibis.ibis_cloudsql.postgres import CloudSQLPostgresBackend

__all__ = [
    "CloudSQLPostgresBackend",
    "CloudSQLMySQLBackend",
    "cloudsql_postgres_connect",
    "cloudsql_mysql_connect",
]
