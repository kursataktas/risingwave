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

use core::time::Duration;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use rumqttc::v5::{ConnectionError, Event, Incoming};
use rumqttc::Outgoing;
use thiserror_ext::AsReport;
use tokio::sync::RwLock;

use super::source::MqttSplit;
use super::{MqttError, MqttProperties};
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

static CONNECTED_TIMEOUT_MS: u64 = 500;

struct ConnectedState {
    is_connected: AtomicBool,
    notify: tokio::sync::Notify,
}
impl ConnectedState {
    fn new() -> Self {
        Self {
            is_connected: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn set_is_connected(&self, connected: bool) {
        self.is_connected
            .store(connected, std::sync::atomic::Ordering::Relaxed);
        if connected {
            self.notify.notify_waiters();
        }
    }

    async fn wait_connected(&self, timeout_ms: u64) -> bool {
        if self.is_connected.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        let mut timeout = tokio::time::interval(Duration::from_millis(timeout_ms));
        timeout.tick().await;
        loop {
            let notified = self.notify.notified();
            tokio::select! {
                _ = timeout.tick() => {
                    return false;
                }
                _ = notified => {
                    if self.is_connected.load(std::sync::atomic::Ordering::Relaxed) {
                        return true;
                    }
                }
            }
        }
    }
}
pub struct MqttSplitEnumerator {
    topic: String,
    #[expect(dead_code)]
    client: rumqttc::v5::AsyncClient,
    topics: Arc<RwLock<HashSet<String>>>,
    connected_state: Arc<ConnectedState>,
    stopped: Arc<AtomicBool>,
}

#[async_trait]
impl SplitEnumerator for MqttSplitEnumerator {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<MqttSplitEnumerator> {
        let (client, mut eventloop) = properties.common.build_client(context.info.source_id, 0)?;

        let topic = properties.topic.clone();
        let mut topics = HashSet::new();
        if !topic.contains('#') && !topic.contains('+') {
            topics.insert(topic.clone());
        }

        client
            .subscribe(topic.clone(), rumqttc::v5::mqttbytes::QoS::AtMostOnce)
            .await?;

        let cloned_client = client.clone();

        let topics = Arc::new(RwLock::new(topics));

        let connected_state = Arc::new(ConnectedState::new());
        let connected_state_clone = connected_state.clone();

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();

        let topics_clone = topics.clone();
        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Subscribe(_))) => {
                        connected_state_clone.set_is_connected(true);
                    }
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        let exist = {
                            let topics = topics_clone.read().await;
                            topics.contains(&topic)
                        };

                        if !exist {
                            let mut topics = topics_clone.write().await;
                            topics.insert(topic);
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if let ConnectionError::Timeout(_) = err {
                            continue;
                        }
                        tracing::error!(
                            "Failed to subscribe to topic {}: {}",
                            topic,
                            err.as_report(),
                        );
                        connected_state_clone.set_is_connected(false);
                        cloned_client
                            .subscribe(topic.clone(), rumqttc::v5::mqttbytes::QoS::AtMostOnce)
                            .await
                            .unwrap();
                    }
                }
            }
        });

        Ok(Self {
            client,
            topics,
            topic: properties.topic,
            connected_state,
            stopped,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MqttSplit>> {
        if !self
            .connected_state
            .wait_connected(CONNECTED_TIMEOUT_MS)
            .await
        {
            return Err(ConnectorError::from(MqttError(format!(
                "Failed to connect to MQTT broker for topic {}",
                self.topic
            ))));
        }

        let topics = self.topics.read().await;
        Ok(topics.iter().cloned().map(MqttSplit::new).collect())
    }
}

impl Drop for MqttSplitEnumerator {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
