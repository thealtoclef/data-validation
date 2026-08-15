# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64

from google.cloud.spanner_v1.types import TypeCode
from pandas import DataFrame


def _frame_from_streamed_rows(rows_iterable, fields):
    """Frame already-consumed Spanner result rows into a pandas DataFrame.

    The Spanner client only populates result-set metadata (and therefore the
    result stream's ``fields`` attribute) once the stream has started being
    consumed, so callers must pass the rows and the ``fields`` captured from
    the stream after iterating it.

    :param rows_iterable: iterable of decoded result rows.
    :param fields: the ``fields`` attribute of the consumed result stream.
    """
    data = []
    for row in rows_iterable:
        data.append(row)

    columns = [_.name for _ in fields]
    bytes_columns = [
        _.name for _ in fields if _.type_.code == TypeCode.BYTES
    ]

    # Creating pandas dataframe from data and columns
    df = DataFrame(data, columns=columns)

    # Spanner BYTES columns are returned as a base64 string.
    # Here we convert them to a byte string to match other DVT supported engines.
    for bytes_column in bytes_columns:
        df[bytes_column] = df[bytes_column].map(base64.b64decode)

    return df


class pandas_df:
    def to_pandas(snapshot, sql, query_parameters):

        if query_parameters:
            param = {}
            param_type = {}
            for i in query_parameters:
                param.update(i["params"])
                param_type.update(i["param_types"])

            data_qry = snapshot.execute_sql(sql, params=param, param_types=param_type)

        else:
            data_qry = snapshot.execute_sql(sql)

        # The Spanner client populates the result-set metadata (and so the
        # ``fields`` property) lazily as the stream is consumed, so the rows
        # are materialized before ``fields`` is read.
        data = []
        for row in data_qry:
            data.append(row)

        return _frame_from_streamed_rows(data, data_qry.fields)

    def to_pandas_databoost(database, sql, query_parameters=None):
        """Execute a query via the Spanner partitioned-query (Data Boost) path.

        Data Boost cannot be requested via ``snapshot.execute_sql()`` directly:
        it requires a partitioned query with a partition token. This function
        generates partitions via ``generate_query_batches`` (with
        ``data_boost_enabled=True``) and processes each partition through the
        batch snapshot, collecting all rows before framing them into a single
        DataFrame.

        No fallback: callers are responsible for pre-selecting this path only
        for queries that are root-partitionable (e.g. row-validation scans).
        If the query is not partitionable or the IAM permission is missing,
        the underlying Spanner error is raised.
        """
        if query_parameters:
            param = {}
            param_type = {}
            for i in query_parameters:
                param.update(i["params"])
                param_type.update(i["param_types"])
        else:
            param = None
            param_type = None

        batch = database.batch_snapshot()
        try:
            partitions = batch.generate_query_batches(
                sql,
                params=param,
                param_types=param_type,
                data_boost_enabled=True,
            )
            rows = []
            fields = None
            for partition in partitions:
                result = batch.process(partition)
                for row in result:
                    rows.append(row)
                # Spanner returns consistent fields across all partitions,
                # so the first partition's fields describe the result.
                if fields is None:
                    fields = result.fields

            if fields is None:
                # No partitions were produced; return an empty DataFrame with
                # no columns.
                return DataFrame()

            return _frame_from_streamed_rows(rows, fields)
        finally:
            batch.close()
