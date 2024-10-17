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
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{LogicalUnion, PlanTreeNode};
use crate::optimizer::PlanRef;

/// Convert union to distinct + union all
pub struct UnionToDistinctRule {}
impl Rule for UnionToDistinctRule {
    fn apply(&self, plan: PlanRef) -> OResult<PlanRef> {
        let union = match plan.as_logical_union() {
            Some(union) => union,
            None => return Ok(None),
        };

        if !union.all() {
            let union_all = LogicalUnion::create(true, union.inputs().into_iter().collect());
            let distinct = Agg::new(vec![], (0..union.base.schema().len()).collect(), union_all)
                .with_enable_two_phase(false);
            Ok(Some(distinct.into()))
        } else {
            Ok(None)
        }
    }
}

impl UnionToDistinctRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionToDistinctRule {})
    }
}
