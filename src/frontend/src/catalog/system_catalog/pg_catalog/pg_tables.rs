// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The view `pg_tables` provides access to useful information about each table in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-tables.html`]
#[system_catalog(
    view,
    "pg_catalog.pg_tables",
    "SELECT s.name AS schemaname,
            t.tablename,
            pg_catalog.pg_get_userbyid(t.owner) AS tableowner,
            NULL AS tablespace
    FROM
        (SELECT name AS tablename,
                schema_id,
                owner
                FROM nim_catalog.nim_tables
            UNION
            SELECT name AS tablename,
                schema_id,
                owner
                FROM nim_catalog.nim_system_tables) AS t
        JOIN nim_catalog.nim_schemas s ON t.schema_id = s.id
        AND s.name <> 'nim_catalog'"
)]
#[derive(Fields)]
struct PgTable {
    schemaname: String,
    tablename: String,
    tableowner: String,
    // Since we don't have any concept of tablespace, we will set this to null.
    tablespace: String,
}
