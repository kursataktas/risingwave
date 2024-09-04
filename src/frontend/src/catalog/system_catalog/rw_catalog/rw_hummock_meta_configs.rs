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

use itertools::Itertools;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwHummockMetaConfig {
    #[primary_key]
    config_name: String,
    config_value: String,
}

#[system_catalog(table, "nim_catalog.nim_hummock_meta_configs")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwHummockMetaConfig>> {
    let configs = reader
        .meta_client
        .list_hummock_meta_configs()
        .await?
        .into_iter()
        .sorted()
        .map(|(k, v)| RwHummockMetaConfig {
            config_name: k,
            config_value: v,
        })
        .collect();
    Ok(configs)
}
