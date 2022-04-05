// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::hummock::VacuumTask;
use risingwave_storage::hummock::HummockSSTableId;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::hummock::model::INVALID_TIMESTAMP;
use crate::hummock::{CompactorManager, HummockManagerRef};
use crate::storage::MetaStore;

/// Vacuum is triggered at this rate.
const VACUUM_TRIGGER_INTERVAL: Duration = Duration::from_secs(30);
/// Orphan SST will be deleted after this interval.
const ORPHAN_SST_RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

/// A SST's lifecycle is tracked in `HummockManager::Versioning` via `SstableIdInfo`:
/// - 1 A SST id is generated by meta node. Set `SstableIdInfo::id_create_timestamp`.
/// - 2 Corresponding SST file is created in object store.
/// - 3 The SST is tracked in meta node. Set `SstableIdInfo::meta_create_timestamp`.
/// - 4.1 The SST is compacted and vacuumed as tracked data. Set
///   `SstableIdInfo::meta_delete_timestamp` and delete asynchronously.
/// - 4.2 Or if step 3 didn't happen after some time, the SST is delete as orphan data
///   asynchronously.
pub struct VacuumTrigger<S: MetaStore> {
    hummock_manager: HummockManagerRef<S>,
    /// Use the CompactorManager to dispatch VacuumTask.
    compactor_manager: Arc<CompactorManager>,
    /// SST ids which have been dispatched to vacuum nodes but are not replied yet.
    pending_sst_ids: parking_lot::RwLock<HashSet<HummockSSTableId>>,
}

impl<S> VacuumTrigger<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: Arc<CompactorManager>,
    ) -> Self {
        Self {
            hummock_manager,
            compactor_manager,
            pending_sst_ids: Default::default(),
        }
    }

    /// Start a task to periodically vacuum hummock
    pub fn start_vacuum_trigger(
        vacuum: Arc<VacuumTrigger<S>>,
    ) -> (JoinHandle<()>, UnboundedSender<()>)
    where
        S: MetaStore,
    {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let mut min_trigger_interval = tokio::time::interval(VACUUM_TRIGGER_INTERVAL);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_trigger_interval.tick() => {},
                    // Shutdown vacuum
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Vacuum is shutting down");
                        return;
                    }
                }
                if let Err(err) = Self::vacuum_version_metadata(&vacuum).await {
                    tracing::warn!("Vacuum tracked data error {}", err);
                }
                // vacuum_orphan_data can be invoked less frequently.
                if let Err(err) =
                    Self::vacuum_sst_data(&vacuum, ORPHAN_SST_RETENTION_INTERVAL).await
                {
                    tracing::warn!("Vacuum orphan data error {}", err);
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Qualified versions' metadata are deleted and related stale SSTs are marked for deletion.
    /// Return number of deleted versions.
    /// A version can be deleted when:
    /// - It's the smallest current version.
    /// - And it's is not the greatest version at the same time. We never vacuum the greatest
    ///   version.
    /// - And it's not being referenced, and we know it won't be referenced in the future because
    ///   only greatest version can be newly referenced.
    async fn vacuum_version_metadata(
        vacuum: &VacuumTrigger<S>,
    ) -> risingwave_common::error::Result<u64> {
        let mut vacuum_count: u64 = 0;
        let version_ids = vacuum.hummock_manager.list_version_ids_asc().await?;
        if version_ids.is_empty() {
            return Ok(0);
        }
        // Iterate version ids in ascending order. Skip the greatest version id.
        for version_id in version_ids.iter().take(version_ids.len() - 1) {
            let pin_count = vacuum
                .hummock_manager
                .get_version_pin_count(*version_id)
                .await?;
            if pin_count > 0 {
                // The smallest version is still referenced.
                return Ok(vacuum_count);
            }

            // Delete version metadata and mark SST as orphan (set meta_delete_timestamp).
            // TODO delete in batch
            vacuum.hummock_manager.delete_version(*version_id).await?;
            vacuum_count += 1;
        }
        Ok(vacuum_count)
    }

    /// Qualified SSTs and their metadata(aka `SstableIdInfo`) are deleted.
    /// Return number of SSTs to delete.
    /// Two types of SSTs can be deleted:
    /// - Orphan SST. The SST is 1) not tracked in meta, that's to say `meta_create_timestamp` is
    ///   not set, 2) and the SST has existed longer than `ORPHAN_SST_RETENTION_INTERVAL` since
    ///   `id_create_timestamp`. Its `meta_delete_timestamp` field will then be set.
    /// - SST marked for deletion. The SST is marked for deletion by `vacuum_tracked_data`, that's
    ///   to say `meta_delete_timestamp` is set.
    async fn vacuum_sst_data(
        vacuum: &VacuumTrigger<S>,
        orphan_sst_retention_interval: Duration,
    ) -> risingwave_common::error::Result<Vec<HummockSSTableId>> {
        // Select SSTs to delete.
        let ssts_to_delete = {
            // 1. Retry the pending SSTs first.
            // It is possible some vacuum workers have been asked to vacuum these SSTs previously,
            // but they don't report the results yet due to either latency or failure.
            // This is OK since trying to delete the same SST multiple times is safe.
            let pending_sst_ids = vacuum.pending_sst_ids.read().iter().cloned().collect_vec();
            if !pending_sst_ids.is_empty() {
                pending_sst_ids
            } else {
                // 2. If no pending SSTs, then fetch new ones.
                // Set orphan SSTs' meta_delete_timestamp field.
                vacuum
                    .hummock_manager
                    .mark_orphan_ssts(orphan_sst_retention_interval)
                    .await?;
                let ssts_to_delete = vacuum
                    .hummock_manager
                    .list_sstable_id_infos()
                    .await?
                    .into_iter()
                    .filter(|sstable_id_info| {
                        sstable_id_info.meta_delete_timestamp != INVALID_TIMESTAMP
                    })
                    .map(|sstable_id_info| sstable_id_info.id)
                    .collect_vec();
                if ssts_to_delete.is_empty() {
                    return Ok(vec![]);
                }
                // Keep these SST ids, so that we can remove them from metadata later.
                vacuum
                    .pending_sst_ids
                    .write()
                    .extend(ssts_to_delete.clone());
                ssts_to_delete
            }
        };

        // 1. Pick a worker.
        let compactor = match vacuum.compactor_manager.next_compactor() {
            None => {
                return Ok(vec![]);
            }
            Some(compactor) => compactor,
        };

        // 2. Send task.
        match compactor
            .send_task(
                None,
                Some(VacuumTask {
                    // The SST id doesn't necessarily have a counterpart SST file in S3, but
                    // it's OK trying to delete it.
                    sstable_ids: ssts_to_delete.clone(),
                }),
            )
            .await
        {
            Ok(_) => {
                tracing::debug!(
                    "Try to vacuum SSTs {:?} in worker {}.",
                    ssts_to_delete,
                    compactor.context_id()
                );
                Ok(ssts_to_delete)
            }
            Err(err) => {
                tracing::warn!("Failed to send vacuum task. {}", err);
                vacuum
                    .compactor_manager
                    .remove_compactor(compactor.context_id());
                Ok(vec![])
            }
        }
    }

    pub async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        let deleted_sst_ids = self
            .pending_sst_ids
            .read()
            .iter()
            .filter(|p| vacuum_task.sstable_ids.contains(p))
            .cloned()
            .collect_vec();
        if !deleted_sst_ids.is_empty() {
            self.hummock_manager
                .delete_sstable_ids(&deleted_sst_ids)
                .await?;
            self.pending_sst_ids
                .write()
                .retain(|p| !deleted_sst_ids.contains(p));
        }
        tracing::info!("Finish vacuuming SSTs {:?}", vacuum_task.sstable_ids);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_pb::hummock::VacuumTask;

    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::{CompactorManager, VacuumTrigger};

    #[tokio::test]
    async fn test_shutdown_vacuum() {
        let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::new());
        let vacuum = Arc::new(VacuumTrigger::new(hummock_manager, compactor_manager));
        let (join_handle, shutdown_sender) = VacuumTrigger::start_vacuum_trigger(vacuum);
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_vacuum_version_metadata() {
        let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = Arc::new(VacuumTrigger::new(
            hummock_manager.clone(),
            compactor_manager.clone(),
        ));

        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();

        // Vacuum no version because the smallest v0 is pinned.
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            0
        );
        hummock_manager
            .unpin_version(context_id, vec![pinned_version.id])
            .await
            .unwrap();

        add_test_tables(hummock_manager.as_ref(), context_id).await;
        // Current state: {v0: [], v1: [test_tables uncommitted], v2: [test_tables], v3:
        // [test_tables_2, to_delete:test_tables], v4: [test_tables_2, test_tables_3 uncommitted]}

        // Vacuum v0, v1, v2, v3
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            4
        );
    }

    #[tokio::test]
    async fn test_vacuum_orphan_sst_data() {
        let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = VacuumTrigger::new(hummock_manager.clone(), compactor_manager.clone());
        // 1. acquire 2 SST ids.
        hummock_manager.get_new_table_id().await.unwrap();
        hummock_manager.get_new_table_id().await.unwrap();
        // 2. no expired SST id.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(60),)
                .await
                .unwrap()
                .len(),
            0
        );
        // 3. 2 expired SST id but no vacuum node.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(0),)
                .await
                .unwrap()
                .len(),
            0
        );
        let _receiver = compactor_manager.add_compactor(0);
        // 4. 2 expired SST ids.
        let sst_ids = VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(0))
            .await
            .unwrap();
        assert_eq!(sst_ids.len(), 2);
        // 5. got the same 2 expired sst ids because the previous pending SST ids are not
        // reported.
        let sst_ids_2 = VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(0))
            .await
            .unwrap();
        assert_eq!(sst_ids, sst_ids_2);
        // 6. report the previous pending SST ids to indicate their success.
        vacuum
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_ids_2,
            })
            .await
            .unwrap();
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(0),)
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_vacuum_marked_for_deletion_sst_data() {
        let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = Arc::new(VacuumTrigger::new(
            hummock_manager.clone(),
            compactor_manager.clone(),
        ));
        let _receiver = compactor_manager.add_compactor(0);

        let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
        // Current state: {v0: [], v1: [test_tables uncommitted], v2: [test_tables], v3:
        // [test_tables_2, to_delete:test_tables], v4: [test_tables_2, test_tables_3 uncommitted]}

        // Vacuum v0, v1, v2, v3
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            4
        );

        // Found test_table is marked for deletion.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(600),)
                .await
                .unwrap()
                .len(),
            1
        );

        // The vacuum task is not reported yet.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(600),)
                .await
                .unwrap()
                .len(),
            1
        );

        // The vacuum task is reported.
        vacuum
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_infos
                    .first()
                    .unwrap()
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
            })
            .await
            .unwrap();

        // test_table is already reported.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum, Duration::from_secs(600),)
                .await
                .unwrap()
                .len(),
            0
        );
    }
}
