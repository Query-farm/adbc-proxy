# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Independent ADBC metadata schemas matching adbc_core 0.24's public contract."""

import pyarrow as pa

INFO_VALUE = pa.dense_union(
    [
        pa.field("string_value", pa.string()),
        pa.field("bool_value", pa.bool_()),
        pa.field("int64_value", pa.int64()),
        pa.field("int32_bitmask", pa.int32()),
        pa.field("string_list", pa.list_(pa.string())),
        pa.field("int32_to_int32_list_map", pa.map_(pa.int32(), pa.list_(pa.int32()))),
    ],
    type_codes=list(range(6)),
)
INFO_SCHEMA = pa.schema([pa.field("info_name", pa.uint32(), False), pa.field("info_value", INFO_VALUE)])
TABLE_TYPES_SCHEMA = pa.schema([pa.field("table_type", pa.string(), False)])
STATISTIC_NAMES_SCHEMA = pa.schema(
    [pa.field("statistic_name", pa.string(), False), pa.field("statistic_key", pa.int16(), False)]
)
STATISTIC_VALUE = pa.dense_union(
    [
        pa.field("int64", pa.int64()),
        pa.field("uint64", pa.uint64()),
        pa.field("float64", pa.float64()),
        pa.field("binary", pa.binary()),
    ],
    type_codes=list(range(4)),
)
STATISTIC = pa.struct(
    [
        pa.field("table_name", pa.string(), False),
        pa.field("column_name", pa.string()),
        pa.field("statistic_key", pa.int16(), False),
        pa.field("statistic_value", STATISTIC_VALUE, False),
        pa.field("statistic_is_approximate", pa.bool_(), False),
    ]
)
STATISTIC_DB = pa.struct(
    [pa.field("db_schema_name", pa.string()), pa.field("db_schema_statistics", pa.list_(STATISTIC), False)]
)
STATISTICS_SCHEMA = pa.schema(
    [pa.field("catalog_name", pa.string()), pa.field("catalog_db_schemas", pa.list_(STATISTIC_DB), False)]
)
USAGE = pa.struct(
    [
        pa.field("fk_catalog", pa.string()),
        pa.field("fk_db_schema", pa.string()),
        pa.field("fk_table", pa.string(), False),
        pa.field("fk_column_name", pa.string(), False),
    ]
)
CONSTRAINT = pa.struct(
    [
        pa.field("constraint_name", pa.string()),
        pa.field("constraint_type", pa.string(), False),
        pa.field("constraint_column_names", pa.list_(pa.string()), False),
        pa.field("constraint_column_usage", pa.list_(USAGE)),
    ]
)
COLUMN = pa.struct(
    [
        pa.field("column_name", pa.string(), False),
        pa.field("ordinal_position", pa.int32()),
        pa.field("remarks", pa.string()),
        pa.field("xdbc_data_type", pa.int16()),
        pa.field("xdbc_type_name", pa.string()),
        pa.field("xdbc_column_size", pa.int32()),
        pa.field("xdbc_decimal_digits", pa.int16()),
        pa.field("xdbc_num_prec_radix", pa.int16()),
        pa.field("xdbc_nullable", pa.int16()),
        pa.field("xdbc_column_def", pa.string()),
        pa.field("xdbc_sql_data_type", pa.int16()),
        pa.field("xdbc_datetime_sub", pa.int16()),
        pa.field("xdbc_char_octet_length", pa.int32()),
        pa.field("xdbc_is_nullable", pa.string()),
        pa.field("xdbc_scope_catalog", pa.string()),
        pa.field("xdbc_scope_schema", pa.string()),
        pa.field("xdbc_scope_table", pa.string()),
        pa.field("xdbc_is_autoincrement", pa.bool_()),
        pa.field("xdbc_is_generatedcolumn", pa.bool_()),
    ]
)
TABLE = pa.struct(
    [
        pa.field("table_name", pa.string(), False),
        pa.field("table_type", pa.string(), False),
        pa.field("table_columns", pa.list_(COLUMN)),
        pa.field("table_constraints", pa.list_(CONSTRAINT)),
    ]
)
OBJECT_DB = pa.struct([pa.field("db_schema_name", pa.string()), pa.field("db_schema_tables", pa.list_(TABLE))])
OBJECTS_SCHEMA = pa.schema([pa.field("catalog_name", pa.string()), pa.field("catalog_db_schemas", pa.list_(OBJECT_DB))])
