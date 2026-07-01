// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions},
    compute::{SortOptions, prep_null_mask_filter},
    datatypes::{DataType, SchemaRef},
};
use datafusion::{
    common::{JoinSide, Result, ScalarValue, cast::as_boolean_array},
    physical_expr::PhysicalExprRef,
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;
use stream_cursor::StreamCursor;

use crate::joins::join_utils::JoinType;

pub mod join_utils;

// join implementations
pub mod bhj;
pub mod join_hash_map;
pub mod smj;
pub mod stream_cursor;
mod test;

#[derive(Debug, Clone)]
pub struct JoinParams {
    pub join_type: JoinType,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub left_keys: Vec<PhysicalExprRef>,
    pub right_keys: Vec<PhysicalExprRef>,
    pub key_data_types: Vec<DataType>,
    pub sort_options: Vec<SortOptions>,
    pub projection: JoinProjection,
    pub join_filter: Option<JoinFilter>,
    pub batch_size: usize,
    pub is_null_aware_anti_join: bool,
}

#[derive(Debug, Clone)]
pub struct JoinFilter {
    pub expression: PhysicalExprRef,
    pub column_indices: Vec<ColumnIndex>,
    pub schema: SchemaRef,
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnIndex {
    pub side: JoinSide,
    pub index: usize,
}

impl JoinFilter {
    pub fn evaluate(
        &self,
        left_cols: &[ArrayRef],
        right_cols: &[ArrayRef],
        num_rows: usize,
    ) -> Result<BooleanArray> {
        let cols = self
            .column_indices
            .iter()
            .map(|col| {
                Ok(match col.side {
                    JoinSide::Left => left_cols.get(col.index).cloned().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "join filter left column index out of range: {}",
                            col.index
                        ))
                    })?,
                    JoinSide::Right => right_cols.get(col.index).cloned().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "join filter right column index out of range: {}",
                            col.index
                        ))
                    })?,
                    JoinSide::None => {
                        df_execution_err!("join filter column side must be left or right")?
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Join filters are compiled against a compact schema containing only
        // columns referenced by the residual condition. Build a temporary
        // batch with those columns in the same order before evaluating it.
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        match self.expression.evaluate(&batch)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                Ok(BooleanArray::from(vec![true; num_rows]))
            }
            ColumnarValue::Scalar(_) => Ok(BooleanArray::from(vec![false; num_rows])),
            ColumnarValue::Array(selected) => {
                let mut selected = as_boolean_array(&selected)?.clone();
                // Spark treats a NULL residual predicate as not matched.
                if selected.null_count() > 0 {
                    selected = prep_null_mask_filter(&selected);
                }
                Ok(selected)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinProjection {
    pub schema: SchemaRef,
    pub left: Vec<usize>,
    pub right: Vec<usize>,
}

impl JoinProjection {
    pub fn try_new(
        join_type: JoinType,
        schema: &SchemaRef,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        projection: &[usize],
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(projection)?);
        let mut left = vec![];
        let mut right = vec![];

        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                for &i in projection {
                    if i < left_schema.fields().len() {
                        left.push(i);
                    } else if i - left_schema.fields().len() < right_schema.fields().len() {
                        right.push(i - left_schema.fields().len());
                    }
                }
            }
            JoinType::LeftAnti | JoinType::LeftSemi => {
                left = projection.to_vec();
            }
            JoinType::RightAnti | JoinType::RightSemi => {
                right = projection.to_vec();
            }
            JoinType::Existence => {
                for &i in projection {
                    if i < left_schema.fields().len() {
                        left.push(i);
                    }
                }
            }
        }
        Ok(Self {
            schema: projected_schema,
            left,
            right,
        })
    }

    pub fn project_left(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.left.iter().map(|&i| cols[i].clone()).collect()
    }

    pub fn project_right(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.right.iter().map(|&i| cols[i].clone()).collect()
    }
}

pub type Idx = (usize, usize);
pub type StreamCursors = (StreamCursor, StreamCursor);
pub type StreamCursorsWithKeyRows = (StreamCursor, StreamCursor);
