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

use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscriber::ReceivedMessage;
use risingwave_common::types::{Datum, ScalarImpl};

use crate::impl_source_meta_extract_func;
use crate::source::{SourceMessage, SourceMeta, SplitId};

#[derive(Debug, Clone)]
pub struct GooglePubsubMeta {
    // timestamp(milliseconds) of message append in mq
    pub timestamp: Option<i64>,
    pub split_id: SplitId,
    pub offset: i64,
}

/// Tag a `ReceivedMessage` from cloud pubsub so we can inject the virtual split-id into the
/// `SourceMessage`
pub(crate) struct TaggedReceivedMessage(pub(crate) SplitId, pub(crate) ReceivedMessage);

impl From<TaggedReceivedMessage> for SourceMessage {
    fn from(tagged_message: TaggedReceivedMessage) -> Self {
        let TaggedReceivedMessage(split_id, message) = tagged_message;

        let timestamp = message
            .message
            .publish_time
            .map(|t| {
                Utc.timestamp_opt(t.seconds, t.nanos as u32)
                    .single()
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        Self {
            key: None,
            payload: {
                let payload = message.message.data;
                match payload.len() {
                    0 => None,
                    _ => Some(payload),
                }
            },
            meta: SourceMeta::GooglePubsub(GooglePubsubMeta {
                timestamp: Some(timestamp.timestamp_millis()),
                offset: timestamp.timestamp_nanos_opt().unwrap(),
                split_id,
            }),
        }
    }
}

impl_source_meta_extract_func!(GooglePubsubMeta, Int64, offset, split_id);
