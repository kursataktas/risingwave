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

use super::super::plan_node::*;
use super::{BoxedRule, OResult, Rule};
pub struct ProjectJoinSeparateRule {}

impl ProjectJoinSeparateRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}

impl Rule for ProjectJoinSeparateRule {
    fn apply(&self, plan: PlanRef) -> OResult<PlanRef> {
        let join = match plan.as_logical_join() {
            Some(join) => join,
            None => return Ok(None),
        };

        if join.is_full_out() {
            Ok(None)
        } else {
            let (left, right, on, join_type, output_indices) = join.clone().decompose();
            let new_join = LogicalJoin::new(left, right, join_type, on);
            let project =
                LogicalProject::with_out_col_idx(new_join.into(), output_indices.into_iter());
            Ok(Some(project.into()))
        }
    }
}
