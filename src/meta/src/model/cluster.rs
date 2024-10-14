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

use std::cmp;
use std::ops::{Add, Deref};
use std::time::{Duration, SystemTime};

use risingwave_pb::common::worker_node::Resource;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use uuid::Uuid;

use super::MetadataModelError;
use crate::model::{MetadataModel, MetadataModelResult};
use crate::storage::{MetaStore, MetaStoreError, Snapshot};

/// Column family name for cluster.
const WORKER_CF_NAME: &str = "cf/worker";

pub const INVALID_EXPIRE_AT: u64 = 0;

#[derive(Clone, Debug, PartialEq)]
pub struct Worker {
    pub worker_node: WorkerNode,

    // Volatile values updated by meta node as follows.
    //
    // Unix timestamp that the worker will expire at.
    pub expire_at: u64,
    pub started_at: Option<u64>,

    // Volatile values updated by worker as follows:
    pub resource: Option<Resource>,
}

impl MetadataModel for Worker {
    type KeyType = HostAddress;
    type PbType = WorkerNode;

    fn cf_name() -> String {
        WORKER_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.worker_node.clone()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        Self {
            worker_node: prost,
            expire_at: INVALID_EXPIRE_AT,
            started_at: None,
            resource: None,
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.worker_node.get_host()?.clone())
    }
}

impl Worker {
    pub fn worker_id(&self) -> u32 {
        self.worker_node.id
    }

    pub fn transactional_id(&self) -> Option<u32> {
        self.worker_node.transactional_id
    }

    pub fn worker_type(&self) -> WorkerType {
        self.worker_node.r#type()
    }

    pub fn update_expire_at(&mut self, ttl: Duration) {
        let expire_at = cmp::max(
            self.expire_at,
            SystemTime::now()
                .add(ttl)
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Clock may have gone backwards")
                .as_secs(),
        );
        self.expire_at = expire_at;
    }

    pub fn update_started_at(&mut self, started_at: u64) {
        self.started_at = Some(started_at);
    }

    pub fn update_resource(&mut self, resource: Option<Resource>) {
        self.resource = resource;
    }
}

const CLUSTER_ID_CF_NAME: &str = "cf";
const CLUSTER_ID_KEY: &[u8] = "cluster_id".as_bytes();

#[derive(Clone, Debug)]
pub struct ClusterId(String);

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    fn from_bytes(bytes: Vec<u8>) -> MetadataModelResult<Self> {
        Ok(Self(
            String::from_utf8(bytes).map_err(MetadataModelError::internal)?,
        ))
    }

    pub async fn from_meta_store<S: MetaStore>(
        meta_store: &S,
    ) -> MetadataModelResult<Option<Self>> {
        Self::from_snapshot::<S>(&meta_store.snapshot().await).await
    }

    pub async fn from_snapshot<S: MetaStore>(s: &S::Snapshot) -> MetadataModelResult<Option<Self>> {
        match s.get_cf(CLUSTER_ID_CF_NAME, CLUSTER_ID_KEY).await {
            Ok(bytes) => Ok(Some(Self::from_bytes(bytes)?)),
            Err(e) => match e {
                MetaStoreError::ItemNotFound(_) => Ok(None),
                _ => Err(e.into()),
            },
        }
    }

    pub async fn put_at_meta_store<S: MetaStore>(&self, meta_store: &S) -> MetadataModelResult<()> {
        Ok(meta_store
            .put_cf(
                CLUSTER_ID_CF_NAME,
                CLUSTER_ID_KEY.to_vec(),
                self.0.clone().into_bytes(),
            )
            .await?)
    }
}

impl From<ClusterId> for String {
    fn from(value: ClusterId) -> Self {
        value.0
    }
}

impl From<String> for ClusterId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for ClusterId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}
