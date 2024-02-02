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

use risingwave_common::error::{ErrorCode, RwError};

use super::{Access, AccessError, AccessResult, ChangeEvent};
use crate::parser::unified::ChangeEventOperation;
use crate::parser::SourceStreamChunkRowWriter;
use crate::source::SourceColumnDesc;

pub fn apply_row_operation_on_stream_chunk_writer_with_op(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
    op: ChangeEventOperation,
) -> AccessResult<()> {
    let f = |column: &SourceColumnDesc| row_op.access_field(column);
    match op {
        ChangeEventOperation::Upsert => writer.insert(f),
        ChangeEventOperation::Delete => writer.delete(f),
    }
}

pub fn apply_row_operation_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> AccessResult<()> {
    let op = row_op.op()?;
    apply_row_operation_on_stream_chunk_writer_with_op(row_op, writer, op)
}

pub fn apply_row_accessor_on_stream_chunk_writer(
    accessor: impl Access,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> AccessResult<()> {
    writer.insert(|column| accessor.access(&[&column.name], Some(&column.data_type)))
}

// TODO(error-handling): remove this
impl From<AccessError> for RwError {
    fn from(val: AccessError) -> Self {
        ErrorCode::InternalError(format!("AccessError: {:?}", val)).into()
    }
}
