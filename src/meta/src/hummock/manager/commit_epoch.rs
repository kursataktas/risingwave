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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::change_log::ChangeLogDelta;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{
    add_prost_table_stats_map, purge_prost_table_stats, to_prost_table_stats_map, PbTableStatsMap,
};
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockSstableObjectId, LocalSstableInfo,
};
use risingwave_pb::hummock::compact_task::{self};
use risingwave_pb::hummock::CompactionConfig;
use sea_orm::TransactionTrait;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::transaction::{
    HummockVersionStatsTransaction, HummockVersionTransaction,
};
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{
    get_or_create_local_table_stat, trigger_local_table_stat, trigger_sst_stat,
};
use crate::hummock::model::CompactionGroup;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_object_id};
use crate::hummock::time_travel::should_mark_next_time_travel_version_snapshot;
use crate::hummock::{
    commit_multi_var_with_provided_txn, start_measure_real_process_timer, HummockManager,
};

pub enum NewTableFragmentInfo {
    Normal {
        mv_table_id: Option<TableId>,
        internal_table_ids: Vec<TableId>,
    },
    NewCompactionGroup {
        table_ids: HashSet<TableId>,
    },
}

#[derive(Default)]
pub struct CommitEpochInfo {
    pub sstables: Vec<LocalSstableInfo>,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    pub new_table_fragment_infos: Vec<NewTableFragmentInfo>,
    pub change_log_delta: HashMap<TableId, ChangeLogDelta>,
    /// `table_id` -> `committed_epoch`
    pub tables_to_commit: HashMap<TableId, u64>,
}

impl HummockManager {
    /// Caller should ensure `epoch` > `committed_epoch` of `tables_to_commit`
    /// if tables are not newly added via `new_table_fragment_info`
    pub async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> Result<()> {
        let CommitEpochInfo {
            mut sstables,
            new_table_watermarks,
            sst_to_context,
            new_table_fragment_infos,
            change_log_delta,
            tables_to_commit,
        } = commit_info;
        let mut versioning_guard = self.versioning.write().await;
        let _timer = start_measure_real_process_timer!(self, "commit_epoch");
        // Prevent commit new epochs if this flag is set
        if versioning_guard.disable_commit_epochs {
            return Ok(());
        }

        assert!(!tables_to_commit.is_empty());

        let versioning: &mut Versioning = &mut versioning_guard;
        self.commit_epoch_sanity_check(
            &tables_to_commit,
            &sstables,
            &sst_to_context,
            &versioning.current_version,
        )
        .await?;

        // Consume and aggregate table stats.
        let mut table_stats_change = PbTableStatsMap::default();
        for s in &mut sstables {
            add_prost_table_stats_map(
                &mut table_stats_change,
                &to_prost_table_stats_map(s.table_stats.clone()),
            );
        }

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );

        let state_table_info = &version.latest_version().state_table_info;
        let mut table_compaction_group_mapping = state_table_info.build_table_compaction_group_id();
        let mut new_table_ids = HashMap::new();
        let mut new_compaction_groups = HashMap::new();
        let mut compaction_group_manager_txn = None;
        let mut compaction_group_config: Option<Arc<CompactionConfig>> = None;

        // Add new table
        for new_table_fragment_info in new_table_fragment_infos {
            match new_table_fragment_info {
                NewTableFragmentInfo::Normal {
                    mv_table_id,
                    internal_table_ids,
                } => {
                    on_handle_add_new_table(
                        state_table_info,
                        &internal_table_ids,
                        StaticCompactionGroupId::StateDefault as u64,
                        &mut table_compaction_group_mapping,
                        &mut new_table_ids,
                    )?;

                    on_handle_add_new_table(
                        state_table_info,
                        &mv_table_id,
                        StaticCompactionGroupId::MaterializedView as u64,
                        &mut table_compaction_group_mapping,
                        &mut new_table_ids,
                    )?;
                }
                NewTableFragmentInfo::NewCompactionGroup { table_ids } => {
                    let (compaction_group_manager, compaction_group_config) =
                        if let Some(compaction_group_manager) = &mut compaction_group_manager_txn {
                            (
                                compaction_group_manager,
                                (*compaction_group_config
                                    .as_ref()
                                    .expect("must be set with compaction_group_manager_txn"))
                                .clone(),
                            )
                        } else {
                            let compaction_group_manager_guard =
                                self.compaction_group_manager.write().await;
                            let new_compaction_group_config =
                                compaction_group_manager_guard.default_compaction_config();
                            compaction_group_config = Some(new_compaction_group_config.clone());
                            (
                                compaction_group_manager_txn.insert(
                                    CompactionGroupManager::start_owned_compaction_groups_txn(
                                        compaction_group_manager_guard,
                                    ),
                                ),
                                new_compaction_group_config,
                            )
                        };
                    let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
                    new_compaction_groups
                        .insert(new_compaction_group_id, compaction_group_config.clone());
                    compaction_group_manager.insert(
                        new_compaction_group_id,
                        CompactionGroup {
                            group_id: new_compaction_group_id,
                            compaction_config: compaction_group_config,
                        },
                    );

                    on_handle_add_new_table(
                        state_table_info,
                        &table_ids,
                        new_compaction_group_id,
                        &mut table_compaction_group_mapping,
                        &mut new_table_ids,
                    )?;
                }
            }
        }

        let commit_sstables = self
            .correct_commit_ssts(sstables, &table_compaction_group_mapping)
            .await?;

        let modified_compaction_groups: Vec<_> = commit_sstables.keys().cloned().collect();

        let time_travel_delta = version.pre_commit_epoch(
            &tables_to_commit,
            new_compaction_groups,
            commit_sstables,
            &new_table_ids,
            new_table_watermarks,
            change_log_delta,
        );
        if should_mark_next_time_travel_version_snapshot(&time_travel_delta) {
            // Unable to invoke mark_next_time_travel_version_snapshot because versioning is already mutable borrowed.
            versioning.time_travel_snapshot_interval_counter = u64::MAX;
        }

        // Apply stats changes.
        let mut version_stats = HummockVersionStatsTransaction::new(
            &mut versioning.version_stats,
            self.env.notification_manager(),
        );
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        if purge_prost_table_stats(&mut version_stats.table_stats, version.latest_version()) {
            self.metrics.version_stats.reset();
            versioning.local_metrics.clear();
        }

        trigger_local_table_stat(
            &self.metrics,
            &mut versioning.local_metrics,
            &version_stats,
            &table_stats_change,
        );
        for (table_id, stats) in &table_stats_change {
            if stats.total_key_size == 0
                && stats.total_value_size == 0
                && stats.total_key_count == 0
            {
                continue;
            }
            let stats_value = std::cmp::max(0, stats.total_key_size + stats.total_value_size);
            let table_metrics = get_or_create_local_table_stat(
                &self.metrics,
                *table_id,
                &mut versioning.local_metrics,
            );
            table_metrics.inc_write_throughput(stats_value as u64);
        }
        let mut time_travel_version = None;
        if versioning.time_travel_snapshot_interval_counter
            >= self.env.opts.hummock_time_travel_snapshot_interval
        {
            versioning.time_travel_snapshot_interval_counter = 0;
            time_travel_version = Some(version.latest_version());
        } else {
            versioning.time_travel_snapshot_interval_counter = versioning
                .time_travel_snapshot_interval_counter
                .saturating_add(1);
        }
        let group_parents = version
            .latest_version()
            .levels
            .values()
            .map(|g| (g.group_id, g.parent_group_id))
            .collect();
        let time_travel_tables_to_commit =
            table_compaction_group_mapping
                .iter()
                .filter_map(|(table_id, cg_id)| {
                    tables_to_commit
                        .get(table_id)
                        .map(|committed_epoch| (table_id, cg_id, *committed_epoch))
                });
        let mut txn = self.env.meta_store_ref().conn.begin().await?;
        let version_snapshot_sst_ids = self
            .write_time_travel_metadata(
                &txn,
                time_travel_version,
                time_travel_delta,
                &group_parents,
                &versioning.last_time_travel_snapshot_sst_ids,
                time_travel_tables_to_commit,
            )
            .await?;
        commit_multi_var_with_provided_txn!(
            txn,
            version,
            version_stats,
            compaction_group_manager_txn
        )?;
        if let Some(version_snapshot_sst_ids) = version_snapshot_sst_ids {
            versioning.last_time_travel_snapshot_sst_ids = version_snapshot_sst_ids;
        }

        for compaction_group_id in &modified_compaction_groups {
            trigger_sst_stat(
                &self.metrics,
                None,
                &versioning.current_version,
                *compaction_group_id,
            );
        }

        drop(versioning_guard);

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in &modified_compaction_groups {
                self.try_send_compaction_request(*id, compact_task::TaskType::Dynamic);
            }
            if !table_stats_change.is_empty() {
                self.collect_table_write_throughput(table_stats_change)
                    .await;
            }
        }
        if !modified_compaction_groups.is_empty() {
            self.try_update_write_limits(&modified_compaction_groups)
                .await;
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(())
    }

    async fn collect_table_write_throughput(&self, table_stats: PbTableStatsMap) {
        let params = self.env.system_params_reader().await;
        let barrier_interval_ms = params.barrier_interval_ms() as u64;
        let checkpoint_secs = {
            std::cmp::max(
                1,
                params.checkpoint_frequency() * barrier_interval_ms / 1000,
            )
        };

        let mut table_infos = self.history_table_throughput.write();
        let max_table_stat_throuput_window_seconds = std::cmp::max(
            self.env.opts.table_stat_throuput_window_seconds_for_split,
            self.env.opts.table_stat_throuput_window_seconds_for_merge,
        );

        let max_sample_size = (max_table_stat_throuput_window_seconds as f64
            / checkpoint_secs as f64)
            .ceil() as usize;
        for (table_id, stat) in table_stats {
            let throughput = (stat.total_value_size + stat.total_key_size) as u64;
            let entry = table_infos.entry(table_id).or_default();
            entry.push_back(throughput);
            if entry.len() > max_sample_size {
                entry.pop_front();
            }
        }
    }

    async fn correct_commit_ssts(
        &self,
        sstables: Vec<LocalSstableInfo>,
        table_compaction_group_mapping: &HashMap<TableId, CompactionGroupId>,
    ) -> Result<BTreeMap<CompactionGroupId, Vec<SstableInfo>>> {
        let mut new_sst_id_number = 0;
        let mut sst_to_cg_vec = Vec::with_capacity(sstables.len());
        for commit_sst in sstables {
            let mut group_table_ids: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            for table_id in &commit_sst.sst_info.table_ids {
                match table_compaction_group_mapping.get(&TableId::new(*table_id)) {
                    Some(cg_id_from_meta) => {
                        group_table_ids
                            .entry(*cg_id_from_meta)
                            .or_default()
                            .push(*table_id);
                    }
                    None => {
                        tracing::warn!(
                            "table {} in SST {} doesn't belong to any compaction group",
                            table_id,
                            commit_sst.sst_info.object_id,
                        );
                    }
                }
            }

            new_sst_id_number += group_table_ids.len() * 2; // `split_sst` will split the SST into two parts and consumer 2 SST IDs
            sst_to_cg_vec.push((commit_sst, group_table_ids));
        }

        // Generate new SST IDs for each compaction group
        // `next_sstable_object_id` will update the global SST ID and reserve the new SST IDs
        // So we need to get the new SST ID first and then split the SSTs
        let mut new_sst_id = next_sstable_object_id(&self.env, new_sst_id_number).await?;
        let mut commit_sstables: BTreeMap<u64, Vec<SstableInfo>> = BTreeMap::new();

        for (mut sst, group_table_ids) in sst_to_cg_vec {
            let len = group_table_ids.len();
            for (index, (group_id, match_ids)) in group_table_ids.into_iter().enumerate() {
                if sst.sst_info.table_ids == match_ids {
                    // The SST contains all the tables in the group should be last key
                    assert!(index == len - 1);
                    commit_sstables
                        .entry(group_id)
                        .or_default()
                        .push(sst.sst_info);
                    break;
                }

                let origin_sst_size = sst.sst_info.sst_size;
                let new_sst_size = match_ids
                    .iter()
                    .map(|id| {
                        let stat = sst.table_stats.get(id).unwrap();
                        stat.total_compressed_size
                    })
                    .sum();

                // TODO(li0k): replace with `split_sst`
                let branch_sst = split_sst_with_table_ids(
                    &mut sst.sst_info,
                    &mut new_sst_id,
                    origin_sst_size - new_sst_size,
                    new_sst_size,
                    match_ids,
                );

                commit_sstables
                    .entry(group_id)
                    .or_default()
                    .push(branch_sst);
            }
        }

        Ok(commit_sstables)
    }
}

fn on_handle_add_new_table(
    state_table_info: &HummockVersionStateTableInfo,
    table_ids: impl IntoIterator<Item = &TableId>,
    compaction_group_id: CompactionGroupId,
    table_compaction_group_mapping: &mut HashMap<TableId, CompactionGroupId>,
    new_table_ids: &mut HashMap<TableId, CompactionGroupId>,
) -> Result<()> {
    for table_id in table_ids {
        if let Some(info) = state_table_info.info().get(table_id) {
            return Err(Error::CompactionGroup(format!(
                "table {} already exist {:?}",
                table_id.table_id, info,
            )));
        }
        table_compaction_group_mapping.insert(*table_id, compaction_group_id);
        new_table_ids.insert(*table_id, compaction_group_id);
    }

    Ok(())
}
