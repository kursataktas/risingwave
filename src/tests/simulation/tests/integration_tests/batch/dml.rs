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

use std::time::Duration;
use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use madsim::time::sleep;

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

#[tokio::test]
async fn test_dml_rate_limit() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();
    session.run("SET STREAMING_RATE_LIMIT=1").await?;
    session.run("CREATE TABLE t (v1 int)").await?;
    // 3 CN * 1 = 3 records / s
    {
        let mut session = cluster.start_session();
        tokio::spawn(async move {
            let _ = session.run("INSERT INTO t SELECT * FROM generate_series(1, 1000)").await;
        });
    }
    sleep(Duration::from_secs(100)).await;
    let result = session.run("SELECT count(*) FROM t").await?;
    let result = result.parse::<usize>().unwrap();

    // 3 CN * 1 = 3 records / s
    assert!(result > 90 && result < 110, "result: {}, expected result > 2000 && result < 4000", result);

    // TODO(kwannoel): Use alter table instead.
    session.run("DROP TABLE t").await?;
    session.run("SET STREAMING_RATE_LIMIT=10").await?;
    session.run("CREATE TABLE t (v1 int)").await?;
    // 3 CN * 1 = 3 records / s
    {
        let mut session = cluster.start_session();
        tokio::spawn(async move {
            let _ = session.run("INSERT INTO t SELECT * FROM generate_series(1, 10000)").await;
        });
    }
    sleep(Duration::from_secs(100)).await;
    let result = session.run("SELECT count(*) FROM t").await?;
    let result = result.parse::<usize>().unwrap();
    // 3 CN * 1 = 3 records / s
    assert!(result > 900 && result < 1100, "result: {}, expected result > 2000 && result < 4000", result);
    sleep(Duration::from_secs(3)).await;
    Ok(())
}
