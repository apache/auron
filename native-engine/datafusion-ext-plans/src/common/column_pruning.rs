// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode},
    },
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{PhysicalExprRef, expressions::Column, utils::collect_columns},
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
};
use datafusion_ext_commons::{downcast_any, downcast_any_dyn};
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;

use crate::{broadcast_join_exec::BroadcastJoinExec, sort_merge_join_exec::SortMergeJoinExec};

pub trait ExecuteWithColumnPruning {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream>;
}

impl ExecuteWithColumnPruning for dyn ExecutionPlan {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        if projection.len() == self.schema().fields().len()
            && projection.iter().enumerate().all(|(i, &proj)| proj == i)
        {
            return self.execute(partition, context);
        }
        let schema = Arc::new(self.schema().project(projection)?);
        // Join output is assembled left then right. Keep reordering and
        // duplicates on the generic path to preserve the requested column order.
        if projection.windows(2).all(|pair| pair[0] < pair[1]) {
            if let Some(join) = self.downcast_ref::<BroadcastJoinExec>() {
                return join.execute_projected(partition, context, projection);
            }
            if let Some(join) = self.downcast_ref::<SortMergeJoinExec>() {
                return join.execute_projected(partition, context, projection);
            }
        }
        let projection = projection.to_vec();
        let stream =
            self.execute(partition, context)?
                .map(move |batch_result: Result<RecordBatch>| {
                    batch_result.and_then(|batch| Ok(batch.project(&projection)?))
                });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

pub fn prune_columns(exprs: &[PhysicalExprRef]) -> Result<(Vec<PhysicalExprRef>, Vec<usize>)> {
    let required_columns: Vec<Column> = exprs
        .iter()
        .flat_map(|expr| collect_columns(expr).into_iter())
        .collect::<HashSet<Column>>()
        .into_iter()
        .sorted_unstable_by_key(|column| column.index())
        .collect();
    let required_columns_mapping: HashMap<usize, usize> = required_columns
        .iter()
        .enumerate()
        .map(|(to, from)| (from.index(), to))
        .collect();
    let mapped_exprs: Vec<PhysicalExprRef> = exprs
        .iter()
        .map(|expr| map_columns(expr, &required_columns_mapping))
        .collect();

    Ok((
        mapped_exprs,
        required_columns.into_iter().map(|c| c.index()).collect(),
    ))
}

pub fn extend_projection_by_expr(
    projection: &mut Vec<usize>,
    expr: &PhysicalExprRef,
) -> PhysicalExprRef {
    let projection = Arc::new(Mutex::new(projection));
    expr.clone()
        .transform_down(&|node: PhysicalExprRef| {
            Ok(Transformed::yes(
                if let Ok(column) = downcast_any_dyn!(node, Column) {
                    let mut projection = projection.lock();
                    if let Some(existed_idx) =
                        projection.iter().position(|&idx| idx == column.index())
                    {
                        Arc::new(Column::new(column.name(), existed_idx)) as PhysicalExprRef
                    } else {
                        let new_idx = projection.len();
                        projection.push(column.index());
                        Arc::new(Column::new(column.name(), new_idx)) as PhysicalExprRef
                    }
                } else {
                    node
                },
            ))
        })
        .map(|r| r.data)
        .expect("expr transform failed")
}

pub fn map_columns(expr: &PhysicalExprRef, mapping: &HashMap<usize, usize>) -> PhysicalExprRef {
    expr.clone()
        .transform_down(&|node: PhysicalExprRef| {
            Ok(Transformed::yes(
                if let Ok(column) = downcast_any_dyn!(node, Column) {
                    let mapped_idx = mapping[&column.index()];
                    Arc::new(Column::new(column.name(), mapped_idx))
                } else {
                    node
                },
            ))
        })
        .map(|r| r.data)
        .expect("expr transform failed")
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Int32Array},
        compute::concat_batches,
        datatypes::{DataType, Field, Schema},
    };
    use auron_memmgr::MemManager;
    use datafusion::{
        common::JoinSide,
        physical_plan::{common::collect, joins::utils::build_join_schema, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use super::*;
    use crate::joins::join_utils::JoinType;

    fn memory(batch: RecordBatch) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )?))
    }

    fn batch(prefix: &str, keys: Vec<Option<i32>>, values: Vec<i32>) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter_with_nullable(vec![
            (
                format!("{prefix}k"),
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                true,
            ),
            (
                format!("{prefix}v"),
                Arc::new(Int32Array::from(values)) as ArrayRef,
                true,
            ),
        ])?)
    }

    #[tokio::test]
    async fn projected_execution_preserves_schema_values_and_row_count() -> Result<()> {
        let original = batch("l", vec![Some(1), Some(2), Some(3)], vec![10, 20, 30])?;
        let metadata = HashMap::from([("test".to_owned(), "preserved".to_owned())]);
        let mut fields = original.schema().fields().to_vec();
        fields[0] = Arc::new(fields[0].as_ref().clone().with_metadata(metadata.clone()));
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let batch = RecordBatch::try_new(schema, original.columns().to_vec())?;
        let input = memory(batch.clone())?;
        let context = SessionContext::new().task_ctx();
        for projection in [vec![], vec![0], vec![1], vec![1, 0, 1], vec![0, 1]] {
            let expected = batch.project(&projection)?;
            let stream = input.execute_projected(0, context.clone(), &projection)?;
            assert_eq!(stream.schema(), expected.schema());
            assert_eq!(
                concat_batches(&expected.schema(), &collect(stream).await?)?,
                expected
            );
        }
        assert!(input.execute_projected(0, context, &[2]).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn joins_preserve_projected_values_and_row_count() -> Result<()> {
        MemManager::init(1_000_000);
        let left = memory(batch(
            "l",
            vec![None, Some(1), Some(2), Some(3)],
            vec![0, 10, 20, 30],
        )?)?;
        let right = memory(batch(
            "r",
            vec![None, Some(1), Some(2), Some(2), Some(4)],
            vec![0, 100, 200, 201, 400],
        )?)?;
        let context = SessionContext::new().task_ctx();
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::Existence,
        ] {
            let schema = if join_type == JoinType::Existence {
                Arc::new(Schema::new(
                    [
                        left.schema().fields().to_vec(),
                        vec![Arc::new(Field::new("exists", DataType::Boolean, false))],
                    ]
                    .concat(),
                ))
            } else {
                Arc::new(
                    build_join_schema(&left.schema(), &right.schema(), &join_type.try_into()?).0,
                )
            };
            for build_side in [None, Some(JoinSide::Left), Some(JoinSide::Right)] {
                let join = || -> Result<Arc<dyn ExecutionPlan>> {
                    let on = vec![(
                        Arc::new(Column::new("lk", 0)) as PhysicalExprRef,
                        Arc::new(Column::new("rk", 0)) as PhysicalExprRef,
                    )];
                    Ok(match build_side {
                        None => Arc::new(SortMergeJoinExec::try_new(
                            schema.clone(),
                            left.clone(),
                            right.clone(),
                            on,
                            join_type,
                            None,
                            vec![Default::default()],
                        )?),
                        Some(side) => Arc::new(BroadcastJoinExec::try_new(
                            schema.clone(),
                            left.clone(),
                            right.clone(),
                            on,
                            join_type,
                            side,
                            false,
                            None,
                            false,
                            None,
                        )?),
                    })
                };
                let full = concat_batches(
                    &schema,
                    &collect(join()?.execute(0, context.clone())?).await?,
                )?;
                let join = join()?;
                let last = schema.fields().len() - 1;
                for projection in [
                    vec![],
                    vec![0],
                    vec![last],
                    vec![0, last],
                    vec![last, 0, last],
                ] {
                    let expected = full.project(&projection)?;
                    let stream = join.execute_projected(0, context.clone(), &projection)?;
                    assert_eq!(stream.schema(), expected.schema());
                    assert_eq!(
                        concat_batches(&expected.schema(), &collect(stream).await?)?,
                        expected,
                        "join_type={join_type:?}, build_side={build_side:?}, projection={projection:?}"
                    );
                }
                let stream = join.execute(0, context.clone())?;
                assert_eq!(stream.schema(), schema);
                assert_eq!(concat_batches(&schema, &collect(stream).await?)?, full);
            }
        }
        Ok(())
    }
}
