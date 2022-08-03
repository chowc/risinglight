// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tracing::debug;

use super::*;
use crate::optimizer::plan_nodes::{LogicalTopN, PlanTreeNodeUnary};

pub struct LimitOrderRule {}

// LogicaLimit 转换成 LogicalTopN
impl Rule for LimitOrderRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        debug!("before LimitOrderRule {:#?}", plan);
        let limit = plan.as_logical_limit()?;
        let child = limit.child();
        let order = child.as_logical_order()?.clone();
        let new_plan = LogicalTopN::new(
            limit.offset(),
            limit.limit(),
            order.comparators().to_owned(),
            order.child(),
        );
        debug!("after LimitOrderRule {:#?}", new_plan);
        Ok(Arc::new(new_plan))
    }
}
