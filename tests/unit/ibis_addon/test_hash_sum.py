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

# We use the per-backend compiler-matrix pattern from
# tests/unit/ibis_addon/test_compiler_matrix.py because test_operations.py
# exercises the pandas backend only, and the hash_sum compile rules are
# per-dialect SQL strings that need a SQL compiler to assert on.

import ibis
import pytest

from ibis.backends.bigquery.compiler import BigQueryCompiler
from ibis.backends.mysql.compiler import MySQLCompiler
from ibis.backends.postgres.compiler import PostgreSQLCompiler

# Import required in order to register DVT operations.
import third_party.ibis.ibis_addon.operations  # noqa: F401
from third_party.ibis.ibis_addon import operations
from third_party.ibis.ibis_cloud_spanner.compiler import SpannerCompiler

TABLE = ibis.table({"h": "string"}, name="t")


def _compile(compiler_class, expr):
    return str(compiler_class().to_sql(expr))


def _assert_fragments(sql, expected_fragments, unexpected_fragments):
    for fragment in expected_fragments:
        assert fragment in sql
    for fragment in unexpected_fragments:
        assert fragment not in sql


# NOTE: the compiled SQL MUST use SUBSTR(<h>, 50) -- a POSITIVE position
# (position 50 = last 15 chars of a 64-char sha256 hex string). A NEGATIVE
# offset (e.g. substr(h, -15)) returns the whole string in PostgreSQL, causing
# silent checksum divergence across dialects (verified by live spike; see
# ADR-0003). "-15" must never appear in any compiled SQL.
HASH_SUM_COMPILER_CASES = [
    pytest.param(
        BigQueryCompiler,
        [
            "COALESCE(SUM(",
            "SUBSTR(",
            ", 50)",
            "CONCAT('0x'",
            "AS INT64",
            "AS NUMERIC",
        ],
        [],
        id="bigquery",
    ),
    pytest.param(
        SpannerCompiler,
        [
            "COALESCE(SUM(",
            "SUBSTR(",
            ", 50)",
            "CONCAT('0x'",
            "AS INT64",
            "AS NUMERIC",
        ],
        [],
        id="spanner",
    ),
    pytest.param(
        PostgreSQLCompiler,
        [
            "COALESCE(SUM(",
            "SUBSTR(",
            ", 50)",
            "::bit(60)::bigint",
        ],
        [],
        id="postgres",
    ),
    pytest.param(
        MySQLCompiler,
        [
            "COALESCE(SUM(",
            "SUBSTR(",
            ", 50)",
            "CONV(",
            "AS DECIMAL",
        ],
        [],
        id="mysql",
    ),
]


@pytest.mark.parametrize(
    "compiler_class,expected_fragments,unexpected_fragments",
    HASH_SUM_COMPILER_CASES,
)
def test_hash_sum_compiler_matrix(
    compiler_class, expected_fragments, unexpected_fragments
):
    sql = _compile(
        compiler_class, operations.compile_hash_sum(TABLE["h"]).name("checksum")
    )

    _assert_fragments(sql, expected_fragments, unexpected_fragments)
    assert "-15" not in sql


@pytest.mark.parametrize(
    "compiler_class,expected_fragments,unexpected_fragments",
    HASH_SUM_COMPILER_CASES,
)
def test_hash_sum_hashbytes_input_compiler_matrix(
    compiler_class, expected_fragments, unexpected_fragments
):
    # The per-row hash expression (SHA256 as hex) is the realistic input to
    # hash_sum, e.g. hash_sum(hashbytes(concat(col_a, col_b))).
    sql = _compile(
        compiler_class,
        operations.compile_hash_sum(TABLE["h"].hashbytes("sha256")).name("checksum"),
    )

    _assert_fragments(sql, expected_fragments, unexpected_fragments)
    assert "-15" not in sql
