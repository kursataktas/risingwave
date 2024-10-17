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

use super::{BoxedRule, OResult, Rule};
use crate::optimizer::plan_node::{LogicalApply, LogicalMaxOneRow};
use crate::optimizer::plan_visitor::LogicalCardinalityExt;
use crate::optimizer::PlanRef;

/// Eliminate max one row restriction from `LogicalApply`.
///
/// If we cannot guarantee that the right side of `Apply` will return at most one row
/// in compile time, we will add a `MaxOneRow` that does runtime check to satisfy the
/// max one row restriction.
///
/// As a result, the `max_one_row` flag of `LogicalApply` will always be `false`
/// after applying this rule.
pub struct MaxOneRowEliminateRule {}
impl Rule for MaxOneRowEliminateRule {
    fn apply(&self, plan: PlanRef) -> OResult<PlanRef> {
        let apply = match plan.as_logical_apply() {
            Some(apply) => apply,
            None => return Ok(None),
        };

        let (left, mut right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        if !max_one_row {
            return Ok(None);
        }

        if !right.max_one_row() {
            right = LogicalMaxOneRow::create(right);
            debug_assert!(right.max_one_row());
        }

        Ok(Some(LogicalApply::create(
            left,
            right,
            join_type,
            on,
            correlated_id,
            correlated_indices,
            false,
        )))
    }
}

impl MaxOneRowEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(MaxOneRowEliminateRule {})
    }
}
