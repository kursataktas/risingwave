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

use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_emrserverless::types::builders::SparkSubmitBuilder;
use aws_sdk_emrserverless::types::{JobDriver, JobRunState};
use aws_sdk_emrserverless::Client;
use aws_types::region::Region;
use tokio::time::sleep;
use tracing::info;

pub struct NimtableCompactionConfig {
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    execution_role_arn: String,
    application_id: String,
    entrypoint: String,
}

impl NimtableCompactionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        use std::env::var;
        Ok(NimtableCompactionConfig {
            region: var("NIMTABLE_COMPACTION_REGION").ok(),
            access_key: var("NIMTABLE_COMPACTION_ACCESS_KEY").ok(),
            secret_key: var("NIMTABLE_COMPACTION_SECRET_KEY").ok(),
            execution_role_arn: var("NIMTABLE_COMPACTION_EXECUTION_ROLE_ARN").map_err(|_| {
                anyhow!("NIMTABLE_COMPACTION_EXECUTION_ROLE_ARN not set in env var")
            })?,
            application_id: var("NIMTABLE_COMPACTION_APPLICATION_ID")
                .map_err(|_| anyhow!("NIMTABLE_COMPACTION_APPLICATION_ID not set in env var"))?,
            entrypoint: var("NIMTABLE_COMPACTION_ENTRYPOINT")
                .map_err(|_| anyhow!("NIMTABLE_COMPACTION_ENTRYPOINT not set in env var"))?,
        })
    }
}

pub struct NimtableCompactionClient {
    client: Client,
    config: NimtableCompactionConfig,
}

impl NimtableCompactionClient {
    pub async fn new(config: NimtableCompactionConfig) -> Self {
        let config_loader = aws_config::from_env();
        let config_loader = if let Some(region) = &config.region {
            config_loader.region(Region::new(region.clone()))
        } else {
            config_loader
        };
        let config_loader = if let (Some(access_key), Some(secret_key)) =
            (&config.access_key, &config.secret_key)
        {
            config_loader.credentials_provider(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    access_key.clone(),
                    secret_key.clone(),
                    None,
                ),
            ))
        } else {
            config_loader
        };
        let sdk_config = config_loader.load().await;
        Self {
            client: Client::new(&sdk_config),
            config,
        }
    }

    async fn wait_job_finish(&self, job_run_id: String) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let success_job_run = loop {
            let output = self
                .client
                .get_job_run()
                .job_run_id(&job_run_id)
                .application_id(self.config.application_id.clone())
                .send()
                .await?;
            let job_run = output.job_run.ok_or_else(|| anyhow!("empty job run"))?;
            match &job_run.state {
                JobRunState::Cancelled | JobRunState::Cancelling | JobRunState::Failed => {
                    return Err(anyhow!(
                        "fail state: {}. Detailed: {}",
                        job_run.state,
                        job_run.state_details
                    ));
                }
                JobRunState::Pending
                | JobRunState::Queued
                | JobRunState::Running
                | JobRunState::Scheduled
                | JobRunState::Submitted => {
                    info!(
                        elapsed = ?start_time.elapsed(),
                        job_status = ?job_run.state,
                        "waiting job."
                    );
                    sleep(Duration::from_secs(5)).await;
                }
                JobRunState::Success => {
                    break job_run;
                }
                state => {
                    return Err(anyhow!("unhandled state: {:?}", state));
                }
            };
        };
        info!(
            job_run_id,
            details = success_job_run.state_details,
            elapsed = ?start_time.elapsed(),
            "job run finish"
        );
        Ok(())
    }

    pub async fn compact(
        &self,
        warehouse: String,
        db: String,
        table: String,
    ) -> anyhow::Result<()> {
        let start_result = self
            .client
            .start_job_run()
            .name(format!(
                "job-run-{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ))
            .application_id(self.config.application_id.clone())
            .execution_role_arn(self.config.execution_role_arn.clone())
            .job_driver(JobDriver::SparkSubmit(
                SparkSubmitBuilder::default()
                    .entry_point(self.config.entrypoint.clone())
                    .entry_point_arguments(warehouse)
                    .entry_point_arguments(db)
                    .entry_point_arguments(table)
                    .build()
                    .unwrap(),
            ))
            .send()
            .await
            .context("start job")?;
        info!(job_run_id = start_result.job_run_id, "job started");
        self.wait_job_finish(start_result.job_run_id).await
    }
}

#[tokio::test]
#[ignore]
async fn trigger_compaction() {
    tracing_subscriber::fmt().init();
    let warehouse = "s3://nimtable-spark/iceberg/";
    let db = "db";
    let table = "table";

    let config = NimtableCompactionConfig::from_env().unwrap();
    let client = NimtableCompactionClient::new(config).await;
    let result = client
        .compact(warehouse.to_owned(), db.to_owned(), table.to_owned())
        .await;

    info!(?result, "job result");
}
