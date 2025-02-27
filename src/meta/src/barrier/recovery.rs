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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::WorkerSlotId;
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::{StreamingParallelism, WorkerId};
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{PausedReason, Recovery};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StreamActor};
use thiserror_ext::AsReport;
use tokio::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, info, warn, Instrument};

use super::{
    CheckpointControl, GlobalBarrierWorker, GlobalBarrierWorkerContext, RecoveryReason, TracedEpoch,
};
use crate::barrier::info::{BarrierInfo, InflightGraphInfo, InflightSubscriptionInfo};
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::state::BarrierWorkerState;
use crate::barrier::{BarrierKind, GlobalBarrierWorkerContextImpl};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, TableFragments, TableParallelism};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{build_actor_connector_splits, RescheduleOptions, TableResizePolicy};
use crate::{model, MetaError, MetaResult};

impl<C> GlobalBarrierWorker<C> {
    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    #[inline(always)]
    /// Initialize a retry strategy for operation in recovery.
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(Self::RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(Self::RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }
}

impl GlobalBarrierWorkerContextImpl {
    /// Clean catalogs for creating streaming jobs that are in foreground mode or table fragments not persisted.
    async fn clean_dirty_streaming_jobs(&self) -> MetaResult<()> {
        self.metadata_manager
            .catalog_controller
            .clean_dirty_subscription()
            .await?;
        let source_ids = self
            .metadata_manager
            .catalog_controller
            .clean_dirty_creating_jobs()
            .await?;

        // unregister cleaned sources.
        self.source_manager.unregister_sources(source_ids).await;

        Ok(())
    }

    async fn purge_state_table_from_hummock(
        &self,
        all_state_table_ids: &HashSet<TableId>,
    ) -> MetaResult<()> {
        self.hummock_manager.purge(all_state_table_ids).await?;
        Ok(())
    }

    // FIXME: didn't consider Values here
    async fn recover_background_mv_progress(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, TableFragments)>> {
        let mgr = &self.metadata_manager;
        let mviews = mgr
            .catalog_controller
            .list_background_creating_mviews(false)
            .await?;

        let mut mview_map = HashMap::new();
        for mview in &mviews {
            let table_id = TableId::new(mview.table_id as _);
            let table_fragments = mgr
                .catalog_controller
                .get_job_fragments_by_id(mview.table_id)
                .await?;
            let table_fragments = TableFragments::from_protobuf(table_fragments);
            mview_map.insert(table_id, (mview.definition.clone(), table_fragments));
        }

        // If failed, enter recovery mode.

        Ok(mview_map)
    }
}

impl ScheduledBarriers {
    /// Pre buffered drop and cancel command, return true if any.
    fn pre_apply_drop_cancel(&self) -> bool {
        let (dropped_actors, cancelled) = self.pre_apply_drop_cancel_scheduled();

        !dropped_actors.is_empty() || !cancelled.is_empty()
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(
        &mut self,
        paused_reason: Option<PausedReason>,
        err: Option<MetaError>,
        recovery_reason: RecoveryReason,
    ) {
        self.context.abort_and_mark_blocked(recovery_reason);
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.control_stream_manager.clear();

        self.recovery_inner(paused_reason, err).await;
        self.context.mark_ready();
    }
}

impl GlobalBarrierWorkerContextImpl {
    pub(super) async fn reload_runtime_info_impl(
        &self,
    ) -> MetaResult<(
        ActiveStreamingWorkerNodes,
        InflightGraphInfo,
        InflightSubscriptionInfo,
        Option<TracedEpoch>,
        HashMap<WorkerId, Vec<StreamActor>>,
        HashMap<ActorId, Vec<SplitImpl>>,
        HashMap<TableId, (String, TableFragments)>,
        HummockVersionStats,
    )> {
        {
            {
                {
                    self.clean_dirty_streaming_jobs()
                        .await
                        .context("clean dirty streaming jobs")?;

                    // Mview progress needs to be recovered.
                    tracing::info!("recovering mview progress");
                    let background_mview_progress = self
                        .recover_background_mv_progress()
                        .await
                        .context("recover mview progress should not fail")?;
                    tracing::info!("recovered mview progress");

                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self.scheduled_barriers.pre_apply_drop_cancel();

                    let mut active_streaming_nodes =
                        ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone())
                            .await?;

                    let background_streaming_jobs = self
                        .metadata_manager
                        .list_background_creating_jobs()
                        .await?;

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    // FIXME: Transactions should be used.
                    // TODO(error-handling): attach context to the errors and log them together, instead of inspecting everywhere.
                    let mut info = if !self.env.opts.disable_automatic_parallelism_control
                        && background_streaming_jobs.is_empty()
                    {
                        self.scale_actors(&active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "scale actors failed");
                            })?;

                        self.resolve_graph_info().await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    } else {
                        // Migrate actors in expired CN to newly joined one.
                        self.migrate_actors(&mut active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "migrate actors failed");
                            })?
                    };

                    if self.scheduled_barriers.pre_apply_drop_cancel() {
                        info = self.resolve_graph_info().await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    }

                    let info = info;

                    self.purge_state_table_from_hummock(&info.existing_table_ids().collect())
                        .await
                        .context("purge state table from hummock")?;

                    let prev_epoch = self
                        .hummock_manager
                        .on_current_version(|version| {
                            let state_table_info = version.state_table_info.info();
                            let committed_epoch = state_table_info
                                .values()
                                .map(|info| info.committed_epoch)
                                .next();
                            let existing_table_ids = info.existing_table_ids();
                            for table_id in existing_table_ids {
                                assert!(
                                    state_table_info.contains_key(&table_id),
                                    "table id {table_id} not registered to hummock but in recovered job {:?}. hummock table info{:?}",
                                    info.existing_table_ids().collect_vec(),
                                    state_table_info
                                );
                            }
                            if let Some(committed_epoch) = committed_epoch {
                                for (table_id, info) in version.state_table_info.info() {
                                    assert_eq!(
                                        info.committed_epoch, committed_epoch,
                                        "table {} with invisible epoch is not purged",
                                        table_id
                                    );
                                }
                            }
                            committed_epoch.map(|committed_epoch| {
                                TracedEpoch::new(Epoch::from(committed_epoch))
                            })
                        })
                        .await;

                    let subscription_info = InflightSubscriptionInfo {
                        mv_depended_subscriptions: self
                            .metadata_manager
                            .get_mv_depended_subscriptions()
                            .await?,
                    };

                    self.sink_manager.reset().await;

                    // update and build all actors.
                    let node_actors = self
                        .load_all_actors(&info, &active_streaming_nodes)
                        .await
                        .inspect_err(|err| {
                            warn!(error = %err.as_report(), "update actors failed");
                        })?;

                    // get split assignments for all actors
                    let source_split_assignments = self.source_manager.list_assignments().await;
                    Ok((
                        active_streaming_nodes,
                        info,
                        subscription_info,
                        prev_epoch,
                        node_actors,
                        source_split_assignments,
                        background_mview_progress,
                        self.hummock_manager.get_version_stats().await,
                    ))
                }
            }
        }
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    async fn recovery_inner(
        &mut self,
        paused_reason: Option<PausedReason>,
        err: Option<MetaError>,
    ) {
        tracing::info!("recovery start!");
        let retry_strategy = Self::get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = GLOBAL_META_METRICS.recovery_latency.start_timer();

        let new_state = tokio_retry::Retry::spawn(retry_strategy, || async {
            if let Some(err) = &err {
                self.context.notify_creating_job_failed(err).await;
            };
            let (
                active_streaming_nodes,
                info,
                subscription_info,
                prev_epoch,
                node_actors,
                source_split_assignments,
                background_mview_progress,
                version_stats,
            ) = self.context.reload_runtime_info().await?;

            let mut control_stream_manager = ControlStreamManager::new(self.env.clone());
            let reset_start_time = Instant::now();
            control_stream_manager
                .reset(
                    &subscription_info,
                    active_streaming_nodes.current(),
                    &*self.context,
                )
                .await
                .inspect_err(|err| {
                    warn!(error = %err.as_report(), "reset compute nodes failed");
                })?;
            info!(elapsed=?reset_start_time.elapsed(), "control stream reset");

            let recovery_result: MetaResult<_> = try {
                {
                    let mutation = Mutation::Add(AddMutation {
                        // Actors built during recovery is not treated as newly added actors.
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: build_actor_connector_splits(&source_split_assignments),
                        pause: paused_reason.is_some(),
                        subscriptions_to_add: Default::default(),
                    });

                    let new_epoch = if let Some(prev_epoch) = prev_epoch {
                        // Use a different `curr_epoch` for each recovery attempt.
                        let curr_epoch = prev_epoch.next();
                        let barrier_info = BarrierInfo {
                            prev_epoch,
                            curr_epoch,
                            kind: BarrierKind::Initial,
                        };

                        let mut node_to_collect = control_stream_manager.inject_barrier(
                            None,
                            Some(mutation),
                            &barrier_info,
                            info.fragment_infos(),
                            info.fragment_infos(),
                            Some(node_actors),
                            vec![],
                            vec![],
                        )?;
                        debug!(?node_to_collect, "inject initial barrier");
                        while !node_to_collect.is_empty() {
                            let (worker_id, result) =
                                control_stream_manager.next_collect_barrier_response().await;
                            let resp = result?;
                            assert_eq!(resp.epoch, barrier_info.prev_epoch());
                            assert!(node_to_collect.remove(&worker_id));
                        }
                        debug!("collected initial barrier");
                        Some(barrier_info.curr_epoch)
                    } else {
                        assert!(info.is_empty());
                        None
                    };

                    (
                        BarrierWorkerState::new(new_epoch, info, subscription_info, paused_reason),
                        active_streaming_nodes,
                        control_stream_manager,
                        CreateMviewProgressTracker::recover(
                            background_mview_progress,
                            &version_stats,
                        ),
                        version_stats,
                    )
                }
            };
            if recovery_result.is_err() {
                GLOBAL_META_METRICS.recovery_failure_cnt.inc();
            }
            recovery_result
        })
        .instrument(tracing::info_span!("recovery_attempt"))
        .await
        .expect("Retry until recovery success.");

        recovery_timer.observe_duration();

        let create_mview_tracker: CreateMviewProgressTracker;
        let state: BarrierWorkerState;
        let version_stats: HummockVersionStats;
        (
            state,
            self.active_streaming_nodes,
            self.control_stream_manager,
            create_mview_tracker,
            version_stats,
        ) = new_state;

        tracing::info!(
            epoch = state.in_flight_prev_epoch().map(|epoch| epoch.value().0),
            paused = ?state.paused_reason(),
            "recovery success"
        );

        self.checkpoint_control =
            CheckpointControl::new(create_mview_tracker, state, version_stats);

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}

impl GlobalBarrierWorkerContextImpl {
    // Migration timeout.
    const RECOVERY_FORCE_MIGRATION_TIMEOUT: Duration = Duration::from_secs(300);

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(
        &self,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<InflightGraphInfo> {
        let mgr = &self.metadata_manager;

        // all worker slots used by actors
        let all_inuse_worker_slots: HashSet<_> = mgr
            .catalog_controller
            .all_inuse_worker_slots()
            .await?
            .into_iter()
            .collect();

        let active_worker_slots: HashSet<_> = active_nodes
            .current()
            .values()
            .flat_map(|node| {
                (0..node.parallelism).map(|idx| WorkerSlotId::new(node.id, idx as usize))
            })
            .collect();

        let expired_worker_slots: BTreeSet<_> = all_inuse_worker_slots
            .difference(&active_worker_slots)
            .cloned()
            .collect();

        if expired_worker_slots.is_empty() {
            debug!("no expired worker slots, skipping.");
            return self.resolve_graph_info().await;
        }

        debug!("start migrate actors.");
        let mut to_migrate_worker_slots = expired_worker_slots.into_iter().rev().collect_vec();
        debug!("got to migrate worker slots {:#?}", to_migrate_worker_slots);

        let mut inuse_worker_slots: HashSet<_> = all_inuse_worker_slots
            .intersection(&active_worker_slots)
            .cloned()
            .collect();

        let start = Instant::now();
        let mut plan = HashMap::new();
        'discovery: while !to_migrate_worker_slots.is_empty() {
            let mut new_worker_slots = active_nodes
                .current()
                .values()
                .flat_map(|worker| {
                    (0..worker.parallelism).map(move |i| WorkerSlotId::new(worker.id, i as _))
                })
                .collect_vec();

            new_worker_slots.retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));
            let to_migration_size = to_migrate_worker_slots.len();
            let mut available_size = new_worker_slots.len();

            if available_size < to_migration_size
                && start.elapsed() > Self::RECOVERY_FORCE_MIGRATION_TIMEOUT
            {
                let mut factor = 2;

                while available_size < to_migration_size {
                    let mut extended_worker_slots = active_nodes
                        .current()
                        .values()
                        .flat_map(|worker| {
                            (0..worker.parallelism * factor)
                                .map(move |i| WorkerSlotId::new(worker.id, i as _))
                        })
                        .collect_vec();

                    extended_worker_slots
                        .retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));

                    extended_worker_slots.sort_by(|a, b| {
                        a.slot_idx()
                            .cmp(&b.slot_idx())
                            .then(a.worker_id().cmp(&b.worker_id()))
                    });

                    available_size = extended_worker_slots.len();
                    new_worker_slots = extended_worker_slots;

                    factor *= 2;
                }

                tracing::info!(
                    "migration timed out, extending worker slots to {:?} by factor {}",
                    new_worker_slots,
                    factor,
                );
            }

            if !new_worker_slots.is_empty() {
                debug!("new worker slots found: {:#?}", new_worker_slots);
                for target_worker_slot in new_worker_slots {
                    if let Some(from) = to_migrate_worker_slots.pop() {
                        debug!(
                            "plan to migrate from worker slot {} to {}",
                            from, target_worker_slot
                        );
                        inuse_worker_slots.insert(target_worker_slot);
                        plan.insert(from, target_worker_slot);
                    } else {
                        break 'discovery;
                    }
                }
            }

            if to_migrate_worker_slots.is_empty() {
                break;
            }

            // wait to get newly joined CN
            let changed = active_nodes
                .wait_changed(
                    Duration::from_millis(5000),
                    Self::RECOVERY_FORCE_MIGRATION_TIMEOUT,
                    |active_nodes| {
                        let current_nodes = active_nodes
                            .current()
                            .values()
                            .map(|node| (node.id, &node.host, node.parallelism))
                            .collect_vec();
                        warn!(
                            current_nodes = ?current_nodes,
                            "waiting for new workers to join, elapsed: {}s",
                            start.elapsed().as_secs()
                        );
                    },
                )
                .await;
            warn!(?changed, "get worker changed or timed out. Retry migrate");
        }

        mgr.catalog_controller.migrate_actors(plan).await?;

        debug!("migrate actors succeed.");

        self.resolve_graph_info().await
    }

    async fn scale_actors(&self, active_nodes: &ActiveStreamingWorkerNodes) -> MetaResult<()> {
        let Ok(_guard) = self.scale_controller.reschedule_lock.try_write() else {
            return Err(anyhow!("scale_actors failed to acquire reschedule_lock").into());
        };

        match self.scale_controller.integrity_check().await {
            Ok(_) => {
                info!("integrity check passed");
            }
            Err(_) => {
                bail!("integrity check failed");
            }
        }

        let mgr = &self.metadata_manager;

        debug!("start resetting actors distribution");

        let available_parallelism = active_nodes
            .current()
            .values()
            .map(|worker_node| worker_node.parallelism as usize)
            .sum();

        let table_parallelisms: HashMap<_, _> = {
            let streaming_parallelisms = mgr
                .catalog_controller
                .get_all_created_streaming_parallelisms()
                .await?;

            let mut result = HashMap::new();

            for (object_id, streaming_parallelism) in streaming_parallelisms {
                let actual_fragment_parallelism = mgr
                    .catalog_controller
                    .get_actual_job_fragment_parallelism(object_id)
                    .await?;

                let table_parallelism = match streaming_parallelism {
                    StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                    StreamingParallelism::Custom => model::TableParallelism::Custom,
                    StreamingParallelism::Fixed(n) => model::TableParallelism::Fixed(n as _),
                };

                let target_parallelism = Self::derive_target_parallelism(
                    available_parallelism,
                    table_parallelism,
                    actual_fragment_parallelism,
                    self.env.opts.default_parallelism,
                );

                if target_parallelism != table_parallelism {
                    tracing::info!(
                        "resetting table {} parallelism from {:?} to {:?}",
                        object_id,
                        table_parallelism,
                        target_parallelism
                    );
                }

                result.insert(object_id as u32, target_parallelism);
            }

            result
        };

        let schedulable_worker_ids = active_nodes
            .current()
            .values()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id as WorkerId)
            .collect();

        let plan = self
            .scale_controller
            .generate_table_resize_plan(TableResizePolicy {
                worker_ids: schedulable_worker_ids,
                table_parallelisms: table_parallelisms.clone(),
            })
            .await?;

        let table_parallelisms: HashMap<_, _> = table_parallelisms
            .into_iter()
            .map(|(table_id, parallelism)| {
                debug_assert_ne!(parallelism, TableParallelism::Custom);
                (TableId::new(table_id), parallelism)
            })
            .collect();

        let mut compared_table_parallelisms = table_parallelisms.clone();

        // skip reschedule if no reschedule is generated.
        let reschedule_fragment = if plan.is_empty() {
            HashMap::new()
        } else {
            self.scale_controller
                .analyze_reschedule_plan(
                    plan,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: true,
                        skip_create_new_actors: true,
                    },
                    Some(&mut compared_table_parallelisms),
                )
                .await?
        };

        // Because custom parallelism doesn't exist, this function won't result in a no-shuffle rewrite for table parallelisms.
        debug_assert_eq!(compared_table_parallelisms, table_parallelisms);

        if let Err(e) = self
            .scale_controller
            .post_apply_reschedule(&reschedule_fragment, &table_parallelisms)
            .await
        {
            tracing::error!(
                error = %e.as_report(),
                "failed to apply reschedule for offline scaling in recovery",
            );

            return Err(e);
        }

        debug!("scaling actors succeed.");
        Ok(())
    }

    // We infer the new parallelism strategy based on the prior level of parallelism of the table.
    // If the parallelism strategy is Fixed or Auto, we won't make any modifications.
    // For Custom, we'll assess the parallelism of the core fragment;
    // if the parallelism is higher than the currently available parallelism, we'll set it to Adaptive.
    // If it's lower, we'll set it to Fixed.
    // If it was previously set to Adaptive, but the default_parallelism in the configuration isn’t Full,
    // and it matches the actual fragment parallelism, in this case, it will be handled by downgrading to Fixed.
    fn derive_target_parallelism(
        available_parallelism: usize,
        assigned_parallelism: TableParallelism,
        actual_fragment_parallelism: Option<usize>,
        default_parallelism: DefaultParallelism,
    ) -> TableParallelism {
        match assigned_parallelism {
            TableParallelism::Custom => {
                if let Some(fragment_parallelism) = actual_fragment_parallelism {
                    if fragment_parallelism >= available_parallelism {
                        TableParallelism::Adaptive
                    } else {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                } else {
                    TableParallelism::Adaptive
                }
            }
            TableParallelism::Adaptive => {
                match (default_parallelism, actual_fragment_parallelism) {
                    (DefaultParallelism::Default(n), Some(fragment_parallelism))
                        if fragment_parallelism == n.get() =>
                    {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                    _ => TableParallelism::Adaptive,
                }
            }
            _ => assigned_parallelism,
        }
    }

    /// Update all actors in compute nodes.
    async fn load_all_actors(
        &self,
        info: &InflightGraphInfo,
        active_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<HashMap<WorkerId, Vec<StreamActor>>> {
        let actor_map = InflightFragmentInfo::actor_ids_to_collect(info.fragment_infos());
        if actor_map.is_empty() {
            tracing::debug!("no actor to update, skipping.");
            return Ok(HashMap::new());
        }

        let all_node_actors = self.metadata_manager.all_node_actors(false).await?;

        // Check if any actors were dropped after info resolved.
        if all_node_actors.iter().any(|(node_id, node_actors)| {
            !active_nodes.current().contains_key(node_id)
                || actor_map
                    .get(node_id)
                    .map(|actors| actors.len() != node_actors.len())
                    .unwrap_or(true)
        }) {
            return Err(anyhow!("actors dropped during update").into());
        }

        {
            for (node_id, actors) in &actor_map {
                if !actors.is_empty() && !all_node_actors.contains_key(node_id) {
                    return Err(anyhow!("streaming job dropped during update").into());
                }
            }
        }

        Ok(all_node_actors)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    #[test]
    fn test_derive_target_parallelism() {
        // total 10, assigned custom, actual 5, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(5),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 10, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(10),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 11, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(11),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned fixed(5), actual _, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual _, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual 5, default 5 -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(5),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );

        // total 10, assigned adaptive, actual 6, default 5 -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(6),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );
    }
}
