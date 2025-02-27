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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::{ActorMapping, VnodeCountCompat};
use risingwave_common::secret::SecretEncryption;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::{
    visit_stream_node, visit_stream_node_cont_mut,
};
use risingwave_common::{bail, hash, must_match};
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceProperties, SplitEnumerator,
};
use risingwave_connector::{dispatch_source_prop, WithOptionsSecResolved};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::{
    ConnectionId, DatabaseId, FunctionId, IndexId, ObjectId, SchemaId, SecretId, SinkId, SourceId,
    SubscriptionId, TableId, UserId, ViewId,
};
use risingwave_pb::catalog::{
    Comment, Connection, CreateType, Database, Function, PbSink, Schema, Secret, Sink, Source,
    Subscription, Table, View,
};
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_pb::ddl_service::{
    alter_name_request, alter_set_schema_request, alter_swap_rename_request, DdlProgress,
    TableJobType, WaitVersion,
};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::PbFragment;
use risingwave_pb::meta::PbTableParallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::update_mutation::PbMergeUpdate;
use risingwave_pb::stream_plan::{
    Dispatcher, DispatcherType, FragmentTypeFlag, MergeNode, PbStreamFragmentGraph,
    StreamFragmentGraph as StreamFragmentGraphProto,
};
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::Instrument;

use crate::barrier::BarrierManagerRef;
use crate::controller::catalog::ReleaseContext;
use crate::controller::cluster::StreamingClusterInfo;
use crate::error::{bail_invalid_parameter, bail_unavailable};
use crate::manager::{
    DdlType, LocalNotification, MetaSrvEnv, MetadataManager, NotificationVersion, StreamingJob,
    IGNORED_NOTIFICATION_VERSION,
};
use crate::model::{FragmentId, StreamContext, TableFragments, TableParallelism};
use crate::stream::{
    create_source_worker_handle, validate_sink, ActorGraphBuildResult, ActorGraphBuilder,
    CompleteStreamFragmentGraph, CreateStreamingJobContext, CreateStreamingJobOption,
    GlobalStreamManagerRef, ReplaceTableContext, SourceManagerRef, StreamFragmentGraph,
};
use crate::{MetaError, MetaResult};

#[derive(PartialEq)]
pub enum DropMode {
    Restrict,
    Cascade,
}

impl DropMode {
    pub fn from_request_setting(cascade: bool) -> DropMode {
        if cascade {
            DropMode::Cascade
        } else {
            DropMode::Restrict
        }
    }
}

pub enum StreamingJobId {
    MaterializedView(TableId),
    Sink(SinkId),
    Table(Option<SourceId>, TableId),
    Index(IndexId),
}

impl StreamingJobId {
    #[allow(dead_code)]
    fn id(&self) -> TableId {
        match self {
            StreamingJobId::MaterializedView(id)
            | StreamingJobId::Sink(id)
            | StreamingJobId::Table(_, id)
            | StreamingJobId::Index(id) => *id,
        }
    }
}

// It’s used to describe the information of the table that needs to be replaced and it will be used during replacing table and creating sink into table operations.
pub struct ReplaceTableInfo {
    pub streaming_job: StreamingJob,
    pub fragment_graph: StreamFragmentGraphProto,
    pub col_index_mapping: Option<ColIndexMapping>,
}

pub enum DdlCommand {
    CreateDatabase(Database),
    DropDatabase(DatabaseId),
    CreateSchema(Schema),
    DropSchema(SchemaId),
    CreateSourceWithoutStreamingJob(Source),
    DropSource(SourceId, DropMode),
    CreateFunction(Function),
    DropFunction(FunctionId),
    CreateView(View),
    DropView(ViewId, DropMode),
    CreateStreamingJob(
        StreamingJob,
        StreamFragmentGraphProto,
        CreateType,
        Option<ReplaceTableInfo>,
    ),
    DropStreamingJob(StreamingJobId, DropMode, Option<ReplaceTableInfo>),
    AlterName(alter_name_request::Object, String),
    AlterSwapRename(alter_swap_rename_request::Object),
    ReplaceTable(ReplaceTableInfo),
    AlterSourceColumn(Source),
    AlterObjectOwner(Object, UserId),
    AlterSetSchema(alter_set_schema_request::Object, SchemaId),
    CreateConnection(Connection),
    DropConnection(ConnectionId),
    CreateSecret(Secret),
    DropSecret(SecretId),
    CommentOn(Comment),
    CreateSubscription(Subscription),
    DropSubscription(SubscriptionId, DropMode),
}

impl DdlCommand {
    fn allow_in_recovery(&self) -> bool {
        match self {
            DdlCommand::DropDatabase(_)
            | DdlCommand::DropSchema(_)
            | DdlCommand::DropSource(_, _)
            | DdlCommand::DropFunction(_)
            | DdlCommand::DropView(_, _)
            | DdlCommand::DropStreamingJob(_, _, _)
            | DdlCommand::DropConnection(_)
            | DdlCommand::DropSecret(_) => true,

            // Simply ban all other commands in recovery.
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct DdlController {
    pub(crate) env: MetaSrvEnv,

    pub(crate) metadata_manager: MetadataManager,
    pub(crate) stream_manager: GlobalStreamManagerRef,
    pub(crate) source_manager: SourceManagerRef,
    barrier_manager: BarrierManagerRef,

    // The semaphore is used to limit the number of concurrent streaming job creation.
    pub(crate) creating_streaming_job_permits: Arc<CreatingStreamingJobPermit>,
}

#[derive(Clone)]
pub struct CreatingStreamingJobPermit {
    pub(crate) semaphore: Arc<Semaphore>,
}

impl CreatingStreamingJobPermit {
    async fn new(env: &MetaSrvEnv) -> Self {
        let mut permits = env
            .system_params_reader()
            .await
            .max_concurrent_creating_streaming_jobs() as usize;
        if permits == 0 {
            // if the system parameter is set to zero, use the max permitted value.
            permits = Semaphore::MAX_PERMITS;
        }
        let semaphore = Arc::new(Semaphore::new(permits));

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;
        let semaphore_clone = semaphore.clone();
        tokio::spawn(async move {
            while let Some(notification) = local_notification_rx.recv().await {
                let LocalNotification::SystemParamsChange(p) = &notification else {
                    continue;
                };
                let mut new_permits = p.max_concurrent_creating_streaming_jobs() as usize;
                if new_permits == 0 {
                    new_permits = Semaphore::MAX_PERMITS;
                }
                match permits.cmp(&new_permits) {
                    Ordering::Less => {
                        semaphore_clone.add_permits(new_permits - permits);
                    }
                    Ordering::Equal => continue,
                    Ordering::Greater => {
                        semaphore_clone
                            .acquire_many((permits - new_permits) as u32)
                            .await
                            .unwrap()
                            .forget();
                    }
                }
                tracing::info!(
                    "max_concurrent_creating_streaming_jobs changed from {} to {}",
                    permits,
                    new_permits
                );
                permits = new_permits;
            }
        });

        Self { semaphore }
    }
}

impl DdlController {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        barrier_manager: BarrierManagerRef,
    ) -> Self {
        let creating_streaming_job_permits = Arc::new(CreatingStreamingJobPermit::new(&env).await);
        Self {
            env,
            metadata_manager,
            stream_manager,
            source_manager,
            barrier_manager,
            creating_streaming_job_permits,
        }
    }

    /// `run_command` spawns a tokio coroutine to execute the target ddl command. When the client
    /// has been interrupted during executing, the request will be cancelled by tonic. Since we have
    /// a lot of logic for revert, status management, notification and so on, ensuring consistency
    /// would be a huge hassle and pain if we don't spawn here.
    ///
    /// Though returning `Option`, it's always `Some`, to simplify the handling logic
    pub async fn run_command(&self, command: DdlCommand) -> MetaResult<Option<WaitVersion>> {
        if !command.allow_in_recovery() {
            self.barrier_manager.check_status_running()?;
        }
        let ctrl = self.clone();
        let fut = async move {
            match command {
                DdlCommand::CreateDatabase(database) => ctrl.create_database(database).await,
                DdlCommand::DropDatabase(database_id) => ctrl.drop_database(database_id).await,
                DdlCommand::CreateSchema(schema) => ctrl.create_schema(schema).await,
                DdlCommand::DropSchema(schema_id) => ctrl.drop_schema(schema_id).await,
                DdlCommand::CreateSourceWithoutStreamingJob(source) => {
                    ctrl.create_source_without_streaming_job(source).await
                }
                DdlCommand::DropSource(source_id, drop_mode) => {
                    ctrl.drop_source(source_id, drop_mode).await
                }
                DdlCommand::CreateFunction(function) => ctrl.create_function(function).await,
                DdlCommand::DropFunction(function_id) => ctrl.drop_function(function_id).await,
                DdlCommand::CreateView(view) => ctrl.create_view(view).await,
                DdlCommand::DropView(view_id, drop_mode) => {
                    ctrl.drop_view(view_id, drop_mode).await
                }
                DdlCommand::CreateStreamingJob(
                    stream_job,
                    fragment_graph,
                    _create_type,
                    affected_table_replace_info,
                ) => {
                    ctrl.create_streaming_job(
                        stream_job,
                        fragment_graph,
                        affected_table_replace_info,
                    )
                    .await
                }
                DdlCommand::DropStreamingJob(job_id, drop_mode, target_replace_info) => {
                    ctrl.drop_streaming_job(job_id, drop_mode, target_replace_info)
                        .await
                }
                DdlCommand::ReplaceTable(ReplaceTableInfo {
                    streaming_job,
                    fragment_graph,
                    col_index_mapping,
                }) => {
                    ctrl.replace_table(streaming_job, fragment_graph, col_index_mapping)
                        .await
                }
                DdlCommand::AlterName(relation, name) => ctrl.alter_name(relation, &name).await,
                DdlCommand::AlterObjectOwner(object, owner_id) => {
                    ctrl.alter_owner(object, owner_id).await
                }
                DdlCommand::AlterSetSchema(object, new_schema_id) => {
                    ctrl.alter_set_schema(object, new_schema_id).await
                }
                DdlCommand::CreateConnection(connection) => {
                    ctrl.create_connection(connection).await
                }
                DdlCommand::DropConnection(connection_id) => {
                    ctrl.drop_connection(connection_id).await
                }
                DdlCommand::CreateSecret(secret) => ctrl.create_secret(secret).await,
                DdlCommand::DropSecret(secret_id) => ctrl.drop_secret(secret_id).await,
                DdlCommand::AlterSourceColumn(source) => ctrl.alter_source(source).await,
                DdlCommand::CommentOn(comment) => ctrl.comment_on(comment).await,
                DdlCommand::CreateSubscription(subscription) => {
                    ctrl.create_subscription(subscription).await
                }
                DdlCommand::DropSubscription(subscription_id, drop_mode) => {
                    ctrl.drop_subscription(subscription_id, drop_mode).await
                }
                DdlCommand::AlterSwapRename(objects) => ctrl.alter_swap_rename(objects).await,
            }
        }
        .in_current_span();
        let notification_version = tokio::spawn(fut).await.map_err(|e| anyhow!(e))??;
        Ok(Some(WaitVersion {
            catalog_version: notification_version,
            hummock_version_id: self.barrier_manager.get_hummock_version_id().await.to_u64(),
        }))
    }

    pub async fn get_ddl_progress(&self) -> MetaResult<Vec<DdlProgress>> {
        self.barrier_manager.get_ddl_progress().await
    }

    async fn create_database(&self, database: Database) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_database(database)
            .await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn alter_parallelism(
        &self,
        table_id: u32,
        parallelism: PbTableParallelism,
        mut deferred: bool,
    ) -> MetaResult<()> {
        tracing::info!("alter parallelism");
        if self.barrier_manager.check_status_running().is_err() {
            tracing::info!(
                "alter parallelism is set to deferred mode because the system is in recovery state"
            );
            deferred = true;
        }

        if !deferred
            && !self
                .metadata_manager
                .list_background_creating_jobs()
                .await?
                .is_empty()
        {
            bail!("The system is creating jobs in the background, please try again later")
        }

        self.stream_manager
            .alter_table_parallelism(table_id, parallelism.into(), deferred)
            .await
    }

    async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        self.drop_object(
            ObjectType::Database,
            database_id as _,
            DropMode::Cascade,
            None,
        )
        .await
    }

    async fn create_schema(&self, schema: Schema) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_schema(schema)
            .await
    }

    async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Schema, schema_id as _, DropMode::Restrict, None)
            .await
    }

    /// Shared source is handled in [`Self::create_streaming_job`]
    async fn create_source_without_streaming_job(
        &self,
        source: Source,
    ) -> MetaResult<NotificationVersion> {
        let handle = create_source_worker_handle(&source, self.source_manager.metrics.clone())
            .await
            .context("failed to create source worker")?;

        let (source_id, version) = self
            .metadata_manager
            .catalog_controller
            .create_source(source)
            .await?;
        self.source_manager
            .register_source_with_handle(source_id, handle)
            .await;
        Ok(version)
    }

    async fn drop_source(
        &self,
        source_id: SourceId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::Source, source_id as _, drop_mode, None)
            .await
    }

    /// This replaces the source in the catalog.
    /// Note: `StreamSourceInfo` in downstream MVs' `SourceExecutor`s are not updated.
    async fn alter_source(&self, source: Source) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .alter_source(source)
            .await
    }

    async fn create_function(&self, function: Function) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_function(function)
            .await
    }

    async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .drop_function(function_id as _)
            .await
    }

    async fn create_view(&self, view: View) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_view(view)
            .await
    }

    async fn drop_view(
        &self,
        view_id: ViewId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(ObjectType::View, view_id as _, drop_mode, None)
            .await
    }

    async fn create_connection(&self, connection: Connection) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .create_connection(connection)
            .await
    }

    async fn drop_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<NotificationVersion> {
        self.drop_object(
            ObjectType::Connection,
            connection_id as _,
            DropMode::Restrict,
            None,
        )
        .await
    }

    async fn create_secret(&self, mut secret: Secret) -> MetaResult<NotificationVersion> {
        // The 'secret' part of the request we receive from the frontend is in plaintext;
        // here, we need to encrypt it before storing it in the catalog.
        let secret_plain_payload = secret.value.clone();
        let secret_store_private_key = self
            .env
            .opts
            .secret_store_private_key
            .clone()
            .ok_or_else(|| anyhow!("secret_store_private_key is not configured"))?;

        let encrypted_payload = {
            let encrypted_secret = SecretEncryption::encrypt(
                secret_store_private_key.as_slice(),
                secret.get_value().as_slice(),
            )
            .context(format!("failed to encrypt secret {}", secret.name))?;
            encrypted_secret
                .serialize()
                .context(format!("failed to serialize secret {}", secret.name))?
        };
        secret.value = encrypted_payload;

        self.metadata_manager
            .catalog_controller
            .create_secret(secret, secret_plain_payload)
            .await
    }

    async fn drop_secret(&self, secret_id: SecretId) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .drop_secret(secret_id as _)
            .await
    }

    async fn create_subscription(
        &self,
        mut subscription: Subscription,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("create subscription");
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        self.metadata_manager
            .catalog_controller
            .create_subscription_catalog(&mut subscription)
            .await?;
        self.stream_manager
            .create_subscription(&subscription)
            .await
            .inspect_err(|e| {
                tracing::debug!(error = %e.as_report(), "cancel create subscription");
            })?;

        let version = self
            .metadata_manager
            .catalog_controller
            .notify_create_subscription(subscription.id)
            .await?;
        tracing::debug!("finish create subscription");
        Ok(version)
    }

    async fn drop_subscription(
        &self,
        subscription_id: SubscriptionId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("preparing drop subscription");
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let subscription = self
            .metadata_manager
            .catalog_controller
            .get_subscription_by_id(subscription_id)
            .await?;
        let table_id = subscription.dependent_table_id;
        let database_id = subscription.database_id.into();
        let (_, version) = self
            .metadata_manager
            .catalog_controller
            .drop_relation(ObjectType::Subscription, subscription_id as _, drop_mode)
            .await?;
        self.stream_manager
            .drop_subscription(database_id, subscription_id as _, table_id)
            .await;
        tracing::debug!("finish drop subscription");
        Ok(version)
    }

    /// Validates the connect properties in the `cdc_table_desc` stored in the `StreamCdcScan` node
    pub(crate) async fn validate_cdc_table(
        table: &Table,
        table_fragments: &TableFragments,
    ) -> MetaResult<()> {
        let stream_scan_fragment = table_fragments
            .fragments
            .values()
            .filter(|f| f.fragment_type_mask & FragmentTypeFlag::StreamScan as u32 != 0)
            .exactly_one()
            .ok()
            .with_context(|| {
                format!(
                    "expect exactly one stream scan fragment, got: {:?}",
                    table_fragments.fragments
                )
            })?;

        async fn new_enumerator_for_validate<P: SourceProperties>(
            source_props: P,
        ) -> Result<P::SplitEnumerator, ConnectorError> {
            P::SplitEnumerator::new(source_props, SourceEnumeratorContext::dummy().into()).await
        }

        for actor in &stream_scan_fragment.actors {
            if let Some(NodeBody::StreamCdcScan(ref stream_cdc_scan)) =
                actor.nodes.as_ref().unwrap().node_body
                && let Some(ref cdc_table_desc) = stream_cdc_scan.cdc_table_desc
            {
                let options_with_secret = WithOptionsSecResolved::new(
                    cdc_table_desc.connect_properties.clone(),
                    cdc_table_desc.secret_refs.clone(),
                );
                let mut props = ConnectorProperties::extract(options_with_secret, true)?;
                props.init_from_pb_cdc_table_desc(cdc_table_desc);

                dispatch_source_prop!(props, props, {
                    new_enumerator_for_validate(*props).await?;
                });
                tracing::debug!(?table.id, "validate cdc table success");
            }
        }
        Ok(())
    }

    // Here we modify the union node of the downstream table by the TableFragments of the to-be-created sink upstream.
    // The merge in the union has already been set up in the frontend and will be filled with specific upstream actors in this function.
    // Meanwhile, the Dispatcher corresponding to the upstream of the merge will also be added to the replace table context here.
    pub(crate) async fn inject_replace_table_job_for_table_sink(
        &self,
        dummy_id: u32,
        mgr: &MetadataManager,
        stream_ctx: StreamContext,
        sink: Option<&Sink>,
        creating_sink_table_fragments: Option<&TableFragments>,
        dropping_sink_id: Option<SinkId>,
        streaming_job: &StreamingJob,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(ReplaceTableContext, TableFragments)> {
        let (mut replace_table_ctx, mut table_fragments) = self
            .build_replace_table(
                stream_ctx,
                streaming_job,
                fragment_graph,
                None,
                dummy_id as _,
            )
            .await?;

        let mut union_fragment_id = None;

        for (fragment_id, fragment) in &mut table_fragments.fragments {
            for actor in &mut fragment.actors {
                if let Some(node) = &mut actor.nodes {
                    visit_stream_node(node, |body| {
                        if let NodeBody::Union(_) = body {
                            if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                // The union fragment should be unique.
                                assert_eq!(*union_fragment_id, *fragment_id);
                            } else {
                                union_fragment_id = Some(*fragment_id);
                            }
                        }
                    })
                };
            }
        }

        let target_table = streaming_job.table().unwrap();

        let target_fragment_id =
            union_fragment_id.expect("fragment of placeholder merger not found");

        if let Some(creating_sink_table_fragments) = creating_sink_table_fragments {
            let sink_fragment = creating_sink_table_fragments.sink_fragment().unwrap();
            let sink = sink.expect("sink not found");
            Self::inject_replace_table_plan_for_sink(
                Some(sink.id),
                &sink_fragment,
                target_table,
                &mut replace_table_ctx,
                &mut table_fragments,
                target_fragment_id,
                None,
            );
        }

        let [table_catalog]: [_; 1] = mgr
            .get_table_catalog_by_ids(vec![target_table.id])
            .await?
            .try_into()
            .expect("Target table should exist in sink into table");

        assert_eq!(table_catalog.incoming_sinks, target_table.incoming_sinks);

        {
            let catalogs = mgr
                .get_sink_catalog_by_ids(&table_catalog.incoming_sinks)
                .await?;

            for sink in catalogs {
                let sink_id = sink.id;

                if let Some(dropping_sink_id) = dropping_sink_id
                    && sink_id == (dropping_sink_id as u32)
                {
                    continue;
                };

                let sink_table_fragments = mgr
                    .get_job_fragments_by_id(&risingwave_common::catalog::TableId::new(sink_id))
                    .await?;

                let sink_fragment = sink_table_fragments.sink_fragment().unwrap();

                Self::inject_replace_table_plan_for_sink(
                    Some(sink_id),
                    &sink_fragment,
                    target_table,
                    &mut replace_table_ctx,
                    &mut table_fragments,
                    target_fragment_id,
                    Some(&sink.unique_identity()),
                );
            }
        }

        // check if the union fragment is fully assigned.
        for fragment in table_fragments.fragments.values_mut() {
            for actor in &mut fragment.actors {
                if let Some(node) = &mut actor.nodes {
                    visit_stream_node(node, |node| {
                        if let NodeBody::Merge(merge_node) = node {
                            assert!(!merge_node.upstream_actor_id.is_empty(), "All the mergers for the union should have been fully assigned beforehand.");
                        }
                    });
                }
            }
        }

        Ok((replace_table_ctx, table_fragments))
    }

    pub(crate) fn inject_replace_table_plan_for_sink(
        sink_id: Option<u32>,
        sink_fragment: &PbFragment,
        table: &Table,
        replace_table_ctx: &mut ReplaceTableContext,
        table_fragments: &mut TableFragments,
        target_fragment_id: FragmentId,
        unique_identity: Option<&str>,
    ) {
        let sink_actor_ids = sink_fragment
            .actors
            .iter()
            .map(|a| a.actor_id)
            .collect_vec();

        let union_fragment = table_fragments
            .fragments
            .get_mut(&target_fragment_id)
            .unwrap();

        let downstream_actor_ids = union_fragment
            .actors
            .iter()
            .map(|actor| actor.actor_id)
            .collect_vec();

        let mut sink_fields = None;

        for actor in &sink_fragment.actors {
            if let Some(node) = &actor.nodes {
                sink_fields = Some(node.fields.clone());
                break;
            }
        }

        let sink_fields = sink_fields.expect("sink fields not found");

        let output_indices = sink_fields
            .iter()
            .enumerate()
            .map(|(idx, _)| idx as _)
            .collect_vec();

        let dist_key_indices = table.distribution_key.iter().map(|i| *i as _).collect_vec();

        let mapping = match union_fragment.get_distribution_type().unwrap() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => None,
            FragmentDistributionType::Hash => {
                let actor_bitmaps: HashMap<_, _> = union_fragment
                    .actors
                    .iter()
                    .map(|actor| {
                        (
                            actor.actor_id as hash::ActorId,
                            Bitmap::from(actor.vnode_bitmap.as_ref().unwrap()),
                        )
                    })
                    .collect();

                let actor_mapping = ActorMapping::from_bitmaps(&actor_bitmaps);
                Some(actor_mapping)
            }
        };

        let upstream_actors = sink_fragment.get_actors();

        for actor in upstream_actors {
            replace_table_ctx.dispatchers.insert(
                actor.actor_id,
                vec![Dispatcher {
                    r#type: DispatcherType::Hash as _,
                    dist_key_indices: dist_key_indices.clone(),
                    output_indices: output_indices.clone(),
                    hash_mapping: mapping.as_ref().map(|m| m.to_protobuf()),
                    dispatcher_id: union_fragment.fragment_id as _,
                    downstream_actor_id: downstream_actor_ids.clone(),
                }],
            );
        }

        let upstream_fragment_id = sink_fragment.fragment_id;

        for actor in &mut union_fragment.actors {
            if let Some(node) = &mut actor.nodes {
                visit_stream_node_cont_mut(node, |node| {
                    if let Some(NodeBody::Union(_)) = &mut node.node_body {
                        for input_project_node in &mut node.input {
                            if let Some(NodeBody::Project(_)) = &mut input_project_node.node_body {
                                let merge_stream_node =
                                    input_project_node.input.iter_mut().exactly_one().unwrap();

                                // we need to align nodes here
                                if input_project_node.identity.as_str()
                                    != unique_identity
                                        .unwrap_or(PbSink::UNIQUE_IDENTITY_FOR_CREATING_TABLE_SINK)
                                {
                                    continue;
                                }

                                if let Some(NodeBody::Merge(merge_node)) =
                                    &mut merge_stream_node.node_body
                                    && merge_node.upstream_actor_id.is_empty()
                                {
                                    if let Some(sink_id) = sink_id {
                                        merge_stream_node.identity =
                                            format!("MergeExecutor(from sink {})", sink_id);

                                        input_project_node.identity =
                                            format!("ProjectExecutor(from sink {})", sink_id);
                                    }

                                    *merge_node = MergeNode {
                                        upstream_actor_id: sink_actor_ids.clone(),
                                        upstream_fragment_id,
                                        upstream_dispatcher_type: DispatcherType::Hash as _,
                                        fields: sink_fields.to_vec(),
                                    };

                                    merge_stream_node.fields = sink_fields.to_vec();

                                    return false;
                                }
                            }
                        }
                    }
                    true
                });
            }
        }

        // update downstream actors' upstream_actor_id and upstream_fragment_id
        for actor in &mut union_fragment.actors {
            actor.upstream_actor_id.extend(sink_actor_ids.clone());
        }

        union_fragment
            .upstream_fragment_ids
            .push(upstream_fragment_id);
    }

    /// For [`CreateType::Foreground`], the function will only return after backfilling finishes
    /// ([`crate::manager::MetadataManager::wait_streaming_job_finished`]).
    pub async fn create_streaming_job(
        &self,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());
        self.metadata_manager
            .catalog_controller
            .create_job_catalog(
                &mut streaming_job,
                &ctx,
                &fragment_graph.parallelism,
                fragment_graph.max_parallelism as _,
            )
            .await?;
        let job_id = streaming_job.id();

        tracing::debug!(
            id = job_id,
            definition = streaming_job.definition(),
            create_type = streaming_job.create_type().as_str_name(),
            "starting streaming job",
        );
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let id = streaming_job.id();
        let name = streaming_job.name();
        let definition = streaming_job.definition();
        let source_id = match &streaming_job {
            StreamingJob::Table(Some(src), _, _) | StreamingJob::Source(src) => Some(src.id),
            _ => None,
        };

        // create streaming job.
        match self
            .create_streaming_job_inner(
                ctx,
                streaming_job,
                fragment_graph,
                affected_table_replace_info,
            )
            .await
        {
            Ok(version) => Ok(version),
            Err(err) => {
                tracing::error!(id = job_id, error = %err.as_report(), "failed to create streaming job");
                let event = risingwave_pb::meta::event_log::EventCreateStreamJobFail {
                    id,
                    name,
                    definition,
                    error: err.as_report().to_string(),
                };
                self.env.event_log_manager_ref().add_event_logs(vec![
                    risingwave_pb::meta::event_log::Event::CreateStreamJobFail(event),
                ]);
                let (aborted, _) = self
                    .metadata_manager
                    .catalog_controller
                    .try_abort_creating_streaming_job(job_id as _, false)
                    .await?;
                if aborted {
                    tracing::warn!(id = job_id, "aborted streaming job");
                    if let Some(source_id) = source_id {
                        self.source_manager
                            .unregister_sources(vec![source_id as SourceId])
                            .await;
                    }
                }
                Err(err)
            }
        }
    }

    async fn create_streaming_job_inner(
        &self,
        ctx: StreamContext,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let mut fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
        streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());

        // create internal table catalogs and refill table id.
        let incomplete_internal_tables = fragment_graph
            .incomplete_internal_tables()
            .into_values()
            .collect_vec();
        let table_id_map = self
            .metadata_manager
            .catalog_controller
            .create_internal_table_catalog(&streaming_job, incomplete_internal_tables)
            .await?;
        fragment_graph.refill_internal_table_ids(table_id_map);

        let affected_table_replace_info = match affected_table_replace_info {
            Some(replace_table_info) => {
                let ReplaceTableInfo {
                    mut streaming_job,
                    fragment_graph,
                    ..
                } = replace_table_info;

                // Ensure the max parallelism unchanged before replacing table.
                let original_max_parallelism = self
                    .metadata_manager
                    .get_job_max_parallelism(streaming_job.id().into())
                    .await?;
                let fragment_graph = PbStreamFragmentGraph {
                    max_parallelism: original_max_parallelism as _,
                    ..fragment_graph
                };

                let fragment_graph =
                    StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
                streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
                streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
                let streaming_job = streaming_job;

                Some((streaming_job, fragment_graph))
            }
            None => None,
        };

        // create fragment and actor catalogs.
        tracing::debug!(id = streaming_job.id(), "building streaming job");
        let (ctx, table_fragments) = self
            .build_stream_job(
                ctx,
                streaming_job,
                fragment_graph,
                affected_table_replace_info,
            )
            .await?;

        let streaming_job = &ctx.streaming_job;

        match streaming_job {
            StreamingJob::Table(None, table, TableJobType::SharedCdcSource) => {
                Self::validate_cdc_table(table, &table_fragments).await?;
            }
            StreamingJob::Table(Some(source), ..) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            StreamingJob::Sink(sink, _) => {
                // Validate the sink on the connector node.
                validate_sink(sink).await?;
            }
            StreamingJob::Source(source) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            _ => {}
        }

        self.metadata_manager
            .catalog_controller
            .prepare_streaming_job(&table_fragments, streaming_job, false)
            .await?;

        // create streaming jobs.
        let stream_job_id = streaming_job.id();
        match (streaming_job.create_type(), &streaming_job) {
            (CreateType::Unspecified, _)
            | (CreateType::Foreground, _)
            // FIXME(kwannoel): Unify background stream's creation path with MV below.
            | (CreateType::Background, StreamingJob::Sink(_, _)) => {
                let version = self.stream_manager
                    .create_streaming_job(table_fragments, ctx)
                    .await?;
                Ok(version)
            }
            (CreateType::Background, _) => {
                let ctrl = self.clone();
                let fut = async move {
                    let _ = ctrl
                        .stream_manager
                        .create_streaming_job(table_fragments, ctx)
                        .await.inspect_err(|err| {
                        tracing::error!(id = stream_job_id, error = ?err.as_report(), "failed to create background streaming job");
                    });
                };
                tokio::spawn(fut);
                Ok(IGNORED_NOTIFICATION_VERSION)
            }
        }
    }

    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
        target_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let (release_ctx, mut version) = match object_type {
            ObjectType::Database => {
                self.metadata_manager
                    .catalog_controller
                    .drop_database(object_id)
                    .await?
            }
            ObjectType::Schema => {
                return self
                    .metadata_manager
                    .catalog_controller
                    .drop_schema(object_id, drop_mode)
                    .await;
            }
            ObjectType::Function => {
                return self
                    .metadata_manager
                    .catalog_controller
                    .drop_function(object_id)
                    .await;
            }
            ObjectType::Connection => {
                let (version, _conn) = self
                    .metadata_manager
                    .catalog_controller
                    .drop_connection(object_id)
                    .await?;
                return Ok(version);
            }
            _ => {
                self.metadata_manager
                    .catalog_controller
                    .drop_relation(object_type, object_id, drop_mode)
                    .await?
            }
        };

        if let Some(replace_table_info) = target_replace_info {
            let stream_ctx =
                StreamContext::from_protobuf(replace_table_info.fragment_graph.get_ctx().unwrap());

            let ReplaceTableInfo {
                mut streaming_job,
                fragment_graph,
                ..
            } = replace_table_info;

            let sink_id = if let ObjectType::Sink = object_type {
                object_id as _
            } else {
                panic!("additional replace table event only occurs when dropping sink into table")
            };

            // Ensure the max parallelism unchanged before replacing table.
            let original_max_parallelism = self
                .metadata_manager
                .get_job_max_parallelism(streaming_job.id().into())
                .await?;
            let fragment_graph = PbStreamFragmentGraph {
                max_parallelism: original_max_parallelism as _,
                ..fragment_graph
            };

            let fragment_graph =
                StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
            streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
            streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
            let streaming_job = streaming_job;

            let table = streaming_job.table().unwrap();

            tracing::debug!(id = streaming_job.id(), "replacing table for dropped sink");
            let dummy_id = self
                .metadata_manager
                .catalog_controller
                .create_job_catalog_for_replace(
                    &streaming_job,
                    &stream_ctx,
                    table.get_version()?,
                    &fragment_graph.specified_parallelism(),
                    fragment_graph.max_parallelism(),
                )
                .await? as u32;

            let (ctx, table_fragments) = self
                .inject_replace_table_job_for_table_sink(
                    dummy_id,
                    &self.metadata_manager,
                    stream_ctx,
                    None,
                    None,
                    Some(sink_id),
                    &streaming_job,
                    fragment_graph,
                )
                .await?;

            let result: MetaResult<Vec<PbMergeUpdate>> = try {
                let merge_updates = ctx.merge_updates.clone();

                self.metadata_manager
                    .catalog_controller
                    .prepare_streaming_job(&table_fragments, &streaming_job, true)
                    .await?;

                self.stream_manager
                    .replace_table(table_fragments, ctx)
                    .await?;

                merge_updates
            };

            version = match result {
                Ok(merge_updates) => {
                    let version = self
                        .metadata_manager
                        .catalog_controller
                        .finish_replace_streaming_job(
                            dummy_id as _,
                            streaming_job,
                            merge_updates,
                            None,
                            None,
                            Some(sink_id),
                            vec![],
                        )
                        .await?;
                    Ok(version)
                }
                Err(err) => {
                    tracing::error!(id = object_id, error = ?err.as_report(), "failed to replace table");
                    let _ = self.metadata_manager
                        .catalog_controller
                        .try_abort_replacing_streaming_job(dummy_id as _)
                        .await
                        .inspect_err(|err| {
                            tracing::error!(id = object_id, error = ?err.as_report(), "failed to abort replacing table");
                        });
                    Err(err)
                }
            }?;
        }

        let ReleaseContext {
            database_id,
            streaming_job_ids,
            state_table_ids,
            source_ids,
            source_fragments,
            removed_actors,
            removed_fragments,
            ..
        } = release_ctx;

        // unregister sources.
        self.source_manager
            .unregister_sources(source_ids.into_iter().map(|id| id as _).collect())
            .await;

        // unregister fragments and actors from source manager.
        self.source_manager
            .drop_source_fragments(
                source_fragments
                    .into_iter()
                    .map(|(source_id, fragments)| {
                        (
                            source_id,
                            fragments.into_iter().map(|id| id as u32).collect(),
                        )
                    })
                    .collect(),
                removed_actors.iter().map(|id| *id as _).collect(),
            )
            .await;

        // drop streaming jobs.
        self.stream_manager
            .drop_streaming_jobs(
                risingwave_common::catalog::DatabaseId::new(database_id as _),
                removed_actors.into_iter().map(|id| id as _).collect(),
                streaming_job_ids,
                state_table_ids,
                removed_fragments.iter().map(|id| *id as _).collect(),
            )
            .await;

        Ok(version)
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN`.
    pub async fn replace_table(
        &self,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        table_col_index_mapping: Option<ColIndexMapping>,
    ) -> MetaResult<NotificationVersion> {
        let job_id = streaming_job.id();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());

        // Ensure the max parallelism unchanged before replacing table.
        let original_max_parallelism = self
            .metadata_manager
            .get_job_max_parallelism(streaming_job.id().into())
            .await?;
        let fragment_graph = PbStreamFragmentGraph {
            max_parallelism: original_max_parallelism as _,
            ..fragment_graph
        };

        // 1. build fragment graph.
        let fragment_graph = StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job)?;
        streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
        let streaming_job = streaming_job;

        let StreamingJob::Table(_, table, ..) = &streaming_job else {
            unreachable!("unexpected job: {streaming_job:?}")
        };
        let dummy_id = self
            .metadata_manager
            .catalog_controller
            .create_job_catalog_for_replace(
                &streaming_job,
                &ctx,
                table.get_version()?,
                &fragment_graph.specified_parallelism(),
                fragment_graph.max_parallelism(),
            )
            .await?;

        tracing::debug!(id = streaming_job.id(), "building replace streaming job");
        let mut updated_sink_catalogs = vec![];

        let result: MetaResult<Vec<PbMergeUpdate>> = try {
            let (mut ctx, mut table_fragments) = self
                .build_replace_table(
                    ctx,
                    &streaming_job,
                    fragment_graph,
                    table_col_index_mapping.clone(),
                    dummy_id as _,
                )
                .await?;

            let mut union_fragment_id = None;

            for (fragment_id, fragment) in &mut table_fragments.fragments {
                for actor in &mut fragment.actors {
                    if let Some(node) = &mut actor.nodes {
                        visit_stream_node(node, |body| {
                            if let NodeBody::Union(_) = body {
                                if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                    // The union fragment should be unique.
                                    assert_eq!(*union_fragment_id, *fragment_id);
                                } else {
                                    union_fragment_id = Some(*fragment_id);
                                }
                            }
                        })
                    };
                }
            }

            let target_fragment_id =
                union_fragment_id.expect("fragment of placeholder merger not found");

            let catalogs = self
                .metadata_manager
                .get_sink_catalog_by_ids(&table.incoming_sinks)
                .await?;

            for sink in catalogs {
                let sink_id = &sink.id;

                let sink_table_fragments = self
                    .metadata_manager
                    .get_job_fragments_by_id(&risingwave_common::catalog::TableId::new(*sink_id))
                    .await?;

                let sink_fragment = sink_table_fragments.sink_fragment().unwrap();

                Self::inject_replace_table_plan_for_sink(
                    Some(*sink_id),
                    &sink_fragment,
                    table,
                    &mut ctx,
                    &mut table_fragments,
                    target_fragment_id,
                    Some(&sink.unique_identity()),
                );

                if sink.original_target_columns.is_empty() {
                    updated_sink_catalogs.push(sink.id as _);
                }
            }

            let merge_updates = ctx.merge_updates.clone();

            self.metadata_manager
                .catalog_controller
                .prepare_streaming_job(&table_fragments, &streaming_job, true)
                .await?;

            self.stream_manager
                .replace_table(table_fragments, ctx)
                .await?;
            merge_updates
        };

        match result {
            Ok(merge_updates) => {
                let version = self
                    .metadata_manager
                    .catalog_controller
                    .finish_replace_streaming_job(
                        dummy_id,
                        streaming_job,
                        merge_updates,
                        table_col_index_mapping,
                        None,
                        None,
                        updated_sink_catalogs,
                    )
                    .await?;
                Ok(version)
            }
            Err(err) => {
                tracing::error!(id = job_id, error = ?err.as_report(), "failed to replace table");
                let _ = self.metadata_manager
                    .catalog_controller
                    .try_abort_replacing_streaming_job(dummy_id)
                    .await.inspect_err(|err| {
                    tracing::error!(id = job_id, error = ?err.as_report(), "failed to abort replacing table");
                });
                Err(err)
            }
        }
    }

    async fn drop_streaming_job(
        &self,
        job_id: StreamingJobId,
        drop_mode: DropMode,
        target_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let (object_id, object_type) = match job_id {
            StreamingJobId::MaterializedView(id) => (id as _, ObjectType::Table),
            StreamingJobId::Sink(id) => (id as _, ObjectType::Sink),
            StreamingJobId::Table(_, id) => (id as _, ObjectType::Table),
            StreamingJobId::Index(idx) => (idx as _, ObjectType::Index),
        };

        let version = self
            .drop_object(object_type, object_id, drop_mode, target_replace_info)
            .await?;

        Ok(version)
    }

    /// Resolve the parallelism of the stream job based on the given information.
    ///
    /// Returns error if user specifies a parallelism that cannot be satisfied.
    fn resolve_stream_parallelism(
        &self,
        specified: Option<NonZeroUsize>,
        max: NonZeroUsize,
        cluster_info: &StreamingClusterInfo,
    ) -> MetaResult<NonZeroUsize> {
        let available = cluster_info.parallelism();
        let Some(available) = NonZeroUsize::new(available) else {
            bail_unavailable!("no available slots to schedule");
        };

        if let Some(specified) = specified {
            if specified > max {
                bail_invalid_parameter!(
                    "specified parallelism {} should not exceed max parallelism {}",
                    specified,
                    max,
                );
            }
            if specified > available {
                bail_unavailable!(
                    "not enough parallelism to schedule, required: {}, available: {}",
                    specified,
                    available,
                );
            }
            Ok(specified)
        } else {
            // Use configured parallelism if no default parallelism is specified.
            let default_parallelism = match self.env.opts.default_parallelism {
                DefaultParallelism::Full => available,
                DefaultParallelism::Default(num) => {
                    if num > available {
                        bail_unavailable!(
                            "not enough parallelism to schedule, required: {}, available: {}",
                            num,
                            available,
                        );
                    }
                    num
                }
            };

            if default_parallelism > max {
                tracing::warn!(
                    "too many parallelism available, use max parallelism {} instead",
                    max
                );
            }
            Ok(default_parallelism.min(max))
        }
    }

    /// Builds the actor graph:
    /// - Add the upstream fragments to the fragment graph
    /// - Schedule the fragments based on their distribution
    /// - Expand each fragment into one or several actors
    pub(crate) async fn build_stream_job(
        &self,
        stream_ctx: StreamContext,
        mut stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraph,
        affected_table_replace_info: Option<(StreamingJob, StreamFragmentGraph)>,
    ) -> MetaResult<(CreateStreamingJobContext, TableFragments)> {
        let id = stream_job.id();
        let specified_parallelism = fragment_graph.specified_parallelism();
        let expr_context = stream_ctx.to_expr_context();
        let max_parallelism = NonZeroUsize::new(fragment_graph.max_parallelism()).unwrap();

        // 1. Resolve the upstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.

        let (upstream_root_fragments, existing_actor_location) = self
            .metadata_manager
            .get_upstream_root_fragments(fragment_graph.dependent_table_ids())
            .await?;

        let upstream_root_actors: HashMap<_, _> = upstream_root_fragments
            .iter()
            .map(|(&table_id, fragment)| {
                (
                    table_id,
                    fragment.actors.iter().map(|a| a.actor_id).collect_vec(),
                )
            })
            .collect();

        let snapshot_backfill_info = fragment_graph.collect_snapshot_backfill_info()?;
        if snapshot_backfill_info.is_some() {
            if stream_job.create_type() == CreateType::Background {
                return Err(anyhow!("snapshot_backfill must be used as Foreground mode").into());
            }
            match stream_job {
                StreamingJob::MaterializedView(_)
                | StreamingJob::Sink(_, _)
                | StreamingJob::Index(_, _) => {}
                StreamingJob::Table(_, _, _) | StreamingJob::Source(_) => {
                    return Err(
                        anyhow!("snapshot_backfill not enabled for table and source").into(),
                    );
                }
            }
        }

        let complete_graph = CompleteStreamFragmentGraph::with_upstreams(
            fragment_graph,
            upstream_root_fragments,
            existing_actor_location,
            (&stream_job).into(),
        )?;

        // 2. Build the actor graph.
        let cluster_info = self.metadata_manager.get_streaming_cluster_info().await?;

        let parallelism =
            self.resolve_stream_parallelism(specified_parallelism, max_parallelism, &cluster_info)?;

        let actor_graph_builder =
            ActorGraphBuilder::new(id, complete_graph, cluster_info, parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder.generate_graph(&self.env, &stream_job, expr_context)?;
        assert!(merge_updates.is_empty());

        // 3. Build the table fragments structure that will be persisted in the stream manager,
        // and the context that contains all information needed for building the
        // actors on the compute nodes.

        // If the frontend does not specify the degree of parallelism and the default_parallelism is set to full, then set it to ADAPTIVE.
        // Otherwise, it defaults to FIXED based on deduction.
        let table_parallelism = match (specified_parallelism, &self.env.opts.default_parallelism) {
            (None, DefaultParallelism::Full) => TableParallelism::Adaptive,
            _ => TableParallelism::Fixed(parallelism.get()),
        };

        let table_fragments = TableFragments::new(
            id.into(),
            graph,
            &building_locations.actor_locations,
            stream_ctx.clone(),
            table_parallelism,
            max_parallelism.get(),
        );
        let internal_tables = table_fragments.internal_tables();

        if let Some(mview_fragment) = table_fragments.mview_fragment() {
            stream_job.set_table_vnode_count(mview_fragment.vnode_count());
        }

        let replace_table_job_info = match affected_table_replace_info {
            Some((streaming_job, fragment_graph)) => {
                if snapshot_backfill_info.is_some() {
                    return Err(anyhow!(
                        "snapshot backfill should not have replace table info: {streaming_job:?}"
                    )
                    .into());
                }
                let StreamingJob::Sink(s, target_table) = &mut stream_job else {
                    bail!("additional replace table event only occurs when sinking into table");
                };

                let table = streaming_job.table().unwrap();
                let dummy_id = self
                    .metadata_manager
                    .catalog_controller
                    .create_job_catalog_for_replace(
                        &streaming_job,
                        &stream_ctx,
                        table.get_version()?,
                        &fragment_graph.specified_parallelism(),
                        fragment_graph.max_parallelism(),
                    )
                    .await? as u32;

                let (context, table_fragments) = self
                    .inject_replace_table_job_for_table_sink(
                        dummy_id,
                        &self.metadata_manager,
                        stream_ctx,
                        Some(s),
                        Some(&table_fragments),
                        None,
                        &streaming_job,
                        fragment_graph,
                    )
                    .await?;
                // When sinking into table occurs, some variables of the target table may be modified,
                // such as `fragment_id` being altered by `prepare_replace_table`.
                // At this point, it’s necessary to update the table info carried with the sink.
                must_match!(&streaming_job, StreamingJob::Table(source, table, _) => {
                    // The StreamingJob in ReplaceTableInfo must be StreamingJob::Table
                    *target_table = Some((table.clone(), source.clone()));
                });

                Some((streaming_job, context, table_fragments))
            }
            None => None,
        };

        let ctx = CreateStreamingJobContext {
            dispatchers,
            upstream_root_actors,
            internal_tables,
            building_locations,
            existing_locations,
            definition: stream_job.definition(),
            mv_table_id: stream_job.mv_table(),
            create_type: stream_job.create_type(),
            ddl_type: (&stream_job).into(),
            streaming_job: stream_job,
            replace_table_job_info,
            option: CreateStreamingJobOption {},
            snapshot_backfill_info,
        };

        Ok((ctx, table_fragments))
    }

    /// `build_replace_table` builds a table replacement and returns the context and new table
    /// fragments.
    ///
    /// Note that we use a dummy ID for the new table fragments and replace it with the real one after
    /// replacement is finished.
    pub(crate) async fn build_replace_table(
        &self,
        stream_ctx: StreamContext,
        stream_job: &StreamingJob,
        mut fragment_graph: StreamFragmentGraph,
        table_col_index_mapping: Option<ColIndexMapping>,
        dummy_table_id: TableId,
    ) -> MetaResult<(ReplaceTableContext, TableFragments)> {
        let id = stream_job.id();
        let expr_context = stream_ctx.to_expr_context();

        let old_table_fragments = self
            .metadata_manager
            .get_job_fragments_by_id(&id.into())
            .await?;
        let old_internal_table_ids = old_table_fragments.internal_table_ids();
        let old_internal_tables = self
            .metadata_manager
            .get_table_catalog_by_ids(old_internal_table_ids)
            .await?;

        fragment_graph.fit_internal_table_ids(old_internal_tables)?;

        // 1. Resolve the edges to the downstream fragments, extend the fragment graph to a complete
        // graph that contains all information needed for building the actor graph.
        let original_table_fragment = old_table_fragments
            .mview_fragment()
            .expect("mview fragment not found");

        let ddl_type = DdlType::from(stream_job);
        let DdlType::Table(table_job_type) = &ddl_type else {
            bail!(
                "only support replacing table streaming job, ddl_type: {:?}",
                ddl_type
            )
        };

        // Map the column indices in the dispatchers with the given mapping.
        let (downstream_fragments, downstream_actor_location) = self
            .metadata_manager
            .get_downstream_chain_fragments(id)
            .await?;
        let downstream_fragments = downstream_fragments
            .into_iter()
            .map(|(d, f)|
                if let Some(mapping) = &table_col_index_mapping {
                    Some((mapping.rewrite_dispatch_strategy(&d)?, f))
                } else {
                    Some((d, f))
                })
            .collect::<Option<_>>()
            .ok_or_else(|| {
                // The `rewrite` only fails if some column is dropped.
                MetaError::invalid_parameter(
                    "unable to drop the column due to being referenced by downstream materialized views or sinks",
                )
            })?;

        // build complete graph based on the table job type
        let complete_graph = match table_job_type {
            TableJobType::General => CompleteStreamFragmentGraph::with_downstreams(
                fragment_graph,
                original_table_fragment.fragment_id,
                downstream_fragments,
                downstream_actor_location,
                ddl_type,
            )?,

            TableJobType::SharedCdcSource => {
                // get the upstream fragment which should be the cdc source
                let (upstream_root_fragments, upstream_actor_location) = self
                    .metadata_manager
                    .get_upstream_root_fragments(fragment_graph.dependent_table_ids())
                    .await?;

                CompleteStreamFragmentGraph::with_upstreams_and_downstreams(
                    fragment_graph,
                    upstream_root_fragments,
                    upstream_actor_location,
                    original_table_fragment.fragment_id,
                    downstream_fragments,
                    downstream_actor_location,
                    ddl_type,
                )?
            }
            TableJobType::Unspecified => {
                unreachable!()
            }
        };

        // 2. Build the actor graph.
        let cluster_info = self.metadata_manager.get_streaming_cluster_info().await?;

        let parallelism = NonZeroUsize::new(original_table_fragment.get_actors().len())
            .expect("The number of actors in the original table fragment should be greater than 0");

        let actor_graph_builder =
            ActorGraphBuilder::new(id, complete_graph, cluster_info, parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder.generate_graph(&self.env, stream_job, expr_context)?;

        // general table job type does not have upstream job, so the dispatchers should be empty
        if matches!(table_job_type, TableJobType::General) {
            assert!(dispatchers.is_empty());
        }

        // 3. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
        let table_fragments = TableFragments::new(
            (dummy_table_id as u32).into(),
            graph,
            &building_locations.actor_locations,
            stream_ctx,
            old_table_fragments.assigned_parallelism,
            old_table_fragments.max_parallelism,
        );

        // Note: no need to set `vnode_count` as it's already set by the frontend.
        // See `get_replace_table_plan`.

        let ctx = ReplaceTableContext {
            old_table_fragments,
            merge_updates,
            dispatchers,
            building_locations,
            existing_locations,
            streaming_job: stream_job.clone(),
            dummy_id: dummy_table_id as _,
        };

        Ok((ctx, table_fragments))
    }

    async fn alter_name(
        &self,
        relation: alter_name_request::Object,
        new_name: &str,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id) = match relation {
            alter_name_request::Object::TableId(id) => (ObjectType::Table, id as ObjectId),
            alter_name_request::Object::ViewId(id) => (ObjectType::View, id as ObjectId),
            alter_name_request::Object::IndexId(id) => (ObjectType::Index, id as ObjectId),
            alter_name_request::Object::SinkId(id) => (ObjectType::Sink, id as ObjectId),
            alter_name_request::Object::SourceId(id) => (ObjectType::Source, id as ObjectId),
            alter_name_request::Object::SchemaId(id) => (ObjectType::Schema, id as ObjectId),
            alter_name_request::Object::DatabaseId(id) => (ObjectType::Database, id as ObjectId),
            alter_name_request::Object::SubscriptionId(id) => {
                (ObjectType::Subscription, id as ObjectId)
            }
        };
        self.metadata_manager
            .catalog_controller
            .alter_name(obj_type, id, new_name)
            .await
    }

    async fn alter_swap_rename(
        &self,
        object: alter_swap_rename_request::Object,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, src_id, dst_id) = match object {
            alter_swap_rename_request::Object::Schema(_) => unimplemented!("schema swap"),
            alter_swap_rename_request::Object::Table(objs) => {
                let (src_id, dst_id) = (
                    objs.src_object_id as ObjectId,
                    objs.dst_object_id as ObjectId,
                );
                (ObjectType::Table, src_id, dst_id)
            }
            alter_swap_rename_request::Object::View(objs) => {
                let (src_id, dst_id) = (
                    objs.src_object_id as ObjectId,
                    objs.dst_object_id as ObjectId,
                );
                (ObjectType::View, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Source(objs) => {
                let (src_id, dst_id) = (
                    objs.src_object_id as ObjectId,
                    objs.dst_object_id as ObjectId,
                );
                (ObjectType::Source, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Sink(objs) => {
                let (src_id, dst_id) = (
                    objs.src_object_id as ObjectId,
                    objs.dst_object_id as ObjectId,
                );
                (ObjectType::Sink, src_id, dst_id)
            }
            alter_swap_rename_request::Object::Subscription(objs) => {
                let (src_id, dst_id) = (
                    objs.src_object_id as ObjectId,
                    objs.dst_object_id as ObjectId,
                );
                (ObjectType::Subscription, src_id, dst_id)
            }
        };

        self.metadata_manager
            .catalog_controller
            .alter_swap_rename(obj_type, src_id, dst_id)
            .await
    }

    async fn alter_owner(
        &self,
        object: Object,
        owner_id: UserId,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id) = match object {
            Object::TableId(id) => (ObjectType::Table, id as ObjectId),
            Object::ViewId(id) => (ObjectType::View, id as ObjectId),
            Object::SourceId(id) => (ObjectType::Source, id as ObjectId),
            Object::SinkId(id) => (ObjectType::Sink, id as ObjectId),
            Object::SchemaId(id) => (ObjectType::Schema, id as ObjectId),
            Object::DatabaseId(id) => (ObjectType::Database, id as ObjectId),
            Object::SubscriptionId(id) => (ObjectType::Subscription, id as ObjectId),
        };
        self.metadata_manager
            .catalog_controller
            .alter_owner(obj_type, id, owner_id as _)
            .await
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        let (obj_type, id) = match object {
            alter_set_schema_request::Object::TableId(id) => (ObjectType::Table, id as ObjectId),
            alter_set_schema_request::Object::ViewId(id) => (ObjectType::View, id as ObjectId),
            alter_set_schema_request::Object::SourceId(id) => (ObjectType::Source, id as ObjectId),
            alter_set_schema_request::Object::SinkId(id) => (ObjectType::Sink, id as ObjectId),
            alter_set_schema_request::Object::FunctionId(id) => {
                (ObjectType::Function, id as ObjectId)
            }
            alter_set_schema_request::Object::ConnectionId(id) => {
                (ObjectType::Connection, id as ObjectId)
            }
            alter_set_schema_request::Object::SubscriptionId(id) => {
                (ObjectType::Subscription, id as ObjectId)
            }
        };
        self.metadata_manager
            .catalog_controller
            .alter_schema(obj_type, id, new_schema_id as _)
            .await
    }

    pub async fn wait(&self) -> MetaResult<()> {
        let timeout_ms = 30 * 60 * 1000;
        for _ in 0..timeout_ms {
            if self
                .metadata_manager
                .catalog_controller
                .list_background_creating_mviews(true)
                .await?
                .is_empty()
            {
                return Ok(());
            }

            sleep(Duration::from_millis(1)).await;
        }
        Err(MetaError::cancelled(format!(
            "timeout after {timeout_ms}ms"
        )))
    }

    async fn comment_on(&self, comment: Comment) -> MetaResult<NotificationVersion> {
        self.metadata_manager
            .catalog_controller
            .comment_on(comment)
            .await
    }
}
