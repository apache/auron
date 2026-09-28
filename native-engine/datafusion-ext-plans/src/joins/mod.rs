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
    common::{
        JoinSide, Result, ScalarValue,
        cast::as_boolean_array,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    physical_expr::{PhysicalExprRef, expressions::Column, utils::collect_columns},
    physical_plan::{ColumnarValue, metrics::Time},
};
use datafusion_ext_commons::df_execution_err;
use stream_cursor::StreamCursor;

use crate::joins::join_utils::JoinType;

pub mod join_utils;

// join implementations
pub mod bhj;
pub mod join_filter;
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
    pub residual_filter: Option<Arc<join_filter::JoinFilter>>,
    pub output_time: Time,
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
    /// Expand the compact wire schema into left columns followed by right
    /// columns, so the prepared evaluator can plan reads on each input side.
    pub fn physical_expr(&self, left_width: usize) -> Result<PhysicalExprRef> {
        if self.schema.fields().len() != self.column_indices.len() {
            df_execution_err!("join filter schema and column indices have different lengths")?;
        }
        self.expression
            .clone()
            .transform_up(|expr| {
                let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Some(source) = self.column_indices.get(column.index()) else {
                    return df_execution_err!(
                        "join filter compact column index out of range: {}",
                        column.index()
                    );
                };
                let index = match source.side {
                    JoinSide::Left if source.index < left_width => source.index,
                    JoinSide::Left => {
                        return df_execution_err!(
                            "join filter left column index out of range: {}",
                            source.index
                        );
                    }
                    JoinSide::Right => match left_width.checked_add(source.index) {
                        Some(index) => index,
                        None => {
                            return df_execution_err!("join filter right column index overflow");
                        }
                    },
                    JoinSide::None => {
                        return df_execution_err!("join filter column side must be left or right");
                    }
                };
                Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    index,
                ))))
            })
            .data()
    }

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

    /// Input columns needed for output or condition evaluation.
    pub left_read: Vec<usize>,
    pub right_read: Vec<usize>,

    /// Output positions in the read batches; condition-only columns follow this
    /// prefix.
    pub left_output: Vec<usize>,
    pub right_output: Vec<usize>,

    /// Projected position of the synthesized existence flag, absent from either
    /// input.
    pub existence_output: Option<usize>,
}

impl JoinProjection {
    pub fn try_new(
        join_type: JoinType,
        schema: &SchemaRef,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        projection: &[usize],
    ) -> Result<Self> {
        Self::try_new_with_filter(
            join_type,
            schema,
            left_schema,
            right_schema,
            projection,
            None,
        )
    }

    /// Include condition columns in reads; expression indices address left then
    /// right.
    pub fn try_new_with_filter(
        join_type: JoinType,
        schema: &SchemaRef,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        projection: &[usize],
        filter_expr: Option<&PhysicalExprRef>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(projection)?);
        let mut left = vec![];
        let mut right = vec![];
        let mut existence_output = None;

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

        if matches!(join_type, JoinType::Existence) {
            existence_output = projection
                .iter()
                .position(|&i| i >= left_schema.fields().len());
        }

        let mut left_read = left.clone();
        let mut right_read = right.clone();
        if let Some(filter_expr) = filter_expr {
            let (lfilter, rfilter) = filter_read_columns(filter_expr, left_schema.fields().len());
            for (read, extra) in [(&mut left_read, lfilter), (&mut right_read, rfilter)] {
                for idx in extra {
                    if !read.contains(&idx) {
                        read.push(idx);
                    }
                }
            }
        }

        Ok(Self {
            schema: projected_schema,
            left_output: (0..left.len()).collect(),
            right_output: (0..right.len()).collect(),
            left,
            right,
            left_read,
            right_read,
            existence_output,
        })
    }

    pub fn project_left(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.left.iter().map(|&i| cols[i].clone()).collect()
    }

    pub fn project_right(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.right.iter().map(|&i| cols[i].clone()).collect()
    }

    pub fn output_left(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.left_output.iter().map(|&i| cols[i].clone()).collect()
    }

    pub fn output_right(&self, cols: &[ArrayRef]) -> Vec<ArrayRef> {
        self.right_output.iter().map(|&i| cols[i].clone()).collect()
    }
}

/// Split condition-column indices by input side; the expression addresses left
/// then right.
pub fn filter_read_columns(
    filter_expr: &PhysicalExprRef,
    num_left_cols: usize,
) -> (Vec<usize>, Vec<usize>) {
    let (mut left, mut right) = (vec![], vec![]);
    for column in collect_columns(filter_expr) {
        match column.index().checked_sub(num_left_cols) {
            Some(idx) => right.push(idx),
            None => left.push(column.index()),
        }
    }
    left.sort_unstable();
    right.sort_unstable();
    (left, right)
}

pub type Idx = (usize, usize);

/// Contiguous buffered rows; equality groups may span several batch ranges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdxRange {
    pub batch_idx: usize,
    pub start: usize,
    pub end: usize,
}

impl IdxRange {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Idx> + Clone {
        let batch_idx = self.batch_idx;
        (self.start..self.end).map(move |row| (batch_idx, row))
    }
}

#[inline]
pub fn ranges_num_rows(ranges: &[IdxRange]) -> usize {
    ranges.iter().map(|range| range.len()).sum()
}

#[inline]
pub fn ranges_iter(ranges: &[IdxRange]) -> impl Iterator<Item = Idx> + Clone + '_ {
    ranges.iter().flat_map(|range| range.iter())
}

/// Lazily yield Cartesian blocks of at most `max_pairs`, without crossing batch
/// boundaries. Offsets are group-relative; iterator state stays constant-sized.
pub fn product_blocks<'a>(
    lranges: &'a [IdxRange],
    rranges: &'a [IdxRange],
    max_pairs: usize,
) -> impl Iterator<Item = ProductBlock> + 'a {
    let max_pairs = max_pairs.max(1);
    // Use whole left rows when the right group fits, keeping each block
    // rectangular.
    let rnum = ranges_num_rows(rranges);
    let lranges = if rnum == 0 { &[] } else { lranges };
    let lstep = (max_pairs / rnum.max(1)).max(1);
    lranges
        .iter()
        .scan(0, |offset, lrange| {
            let loffset = *offset;
            *offset += lrange.len();
            Some((lrange, loffset))
        })
        .flat_map(move |(lrange, loffset)| {
            (0..lrange.len()).step_by(lstep).flat_map(move |lskip| {
                let lcount = lstep.min(lrange.len() - lskip);
                rranges
                    .iter()
                    .scan(0, |offset, rrange| {
                        let roffset = *offset;
                        *offset += rrange.len();
                        Some((rrange, roffset))
                    })
                    .flat_map(move |(rrange, roffset)| {
                        (0..rrange.len()).step_by(max_pairs).map(move |rskip| {
                            let rcount = max_pairs.min(rrange.len() - rskip);
                            ProductBlock {
                                lrange: IdxRange {
                                    batch_idx: lrange.batch_idx,
                                    start: lrange.start + lskip,
                                    end: lrange.start + lskip + lcount,
                                },
                                loffset: loffset + lskip,
                                rrange: IdxRange {
                                    batch_idx: rrange.batch_idx,
                                    start: rrange.start + rskip,
                                    end: rrange.start + rskip + rcount,
                                },
                                roffset: roffset + rskip,
                            }
                        })
                    })
            })
        })
}

#[derive(Clone, Copy, Debug)]
pub struct ProductBlock {
    pub lrange: IdxRange,
    /// Position of `lrange` within the left group
    pub loffset: usize,
    pub rrange: IdxRange,
    pub roffset: usize,
}

pub fn ranges_slice(ranges: &[IdxRange], offset: usize, len: usize, out: &mut Vec<IdxRange>) {
    out.clear();
    let mut skip = offset;
    let mut left = len;
    for range in ranges {
        if left == 0 {
            break;
        }
        if skip >= range.len() {
            skip -= range.len();
            continue;
        }
        let start = range.start + skip;
        let end = range.end.min(start + left);
        out.push(IdxRange {
            batch_idx: range.batch_idx,
            start,
            end,
        });
        left -= end - start;
        skip = 0;
    }
}
pub type StreamCursors = (StreamCursor, StreamCursor);
pub type StreamCursorsWithKeyRows = (StreamCursor, StreamCursor);

#[cfg(test)]
mod filter_protocol_tests {
    use arrow::{
        array::Int32Array,
        datatypes::{Field, Schema},
    };
    use datafusion::{
        logical_expr::Operator, physical_expr::expressions::BinaryExpr,
        physical_plan::metrics::Time,
    };

    use super::*;

    #[test]
    fn compact_filter_preserves_sides_and_hidden_columns() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("l_key", DataType::Int32, false),
            Field::new("l_hidden", DataType::Int32, false),
            Field::new("l_value", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("r_key", DataType::Int32, false),
            Field::new("r_value", DataType::Int32, false),
            Field::new("r_hidden", DataType::Int32, false),
        ]));
        // Wire columns may be right-first; their indices differ from full input
        // positions.
        let filter = JoinFilter {
            expression: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("r_hidden", 0)),
                Operator::Gt,
                Arc::new(Column::new("l_hidden", 1)),
            )),
            column_indices: vec![
                ColumnIndex {
                    side: JoinSide::Right,
                    index: 2,
                },
                ColumnIndex {
                    side: JoinSide::Left,
                    index: 1,
                },
            ],
            schema: Arc::new(Schema::new(vec![
                right_schema.field(2).clone(),
                left_schema.field(1).clone(),
            ])),
        };
        let expr = filter.physical_expr(left_schema.fields().len())?;
        let output_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let projection = JoinProjection::try_new_with_filter(
            JoinType::Inner,
            &output_schema,
            &left_schema,
            &right_schema,
            &[2, 4],
            Some(&expr),
        )?;
        assert_eq!(projection.left_read, vec![2, 1]);
        assert_eq!(projection.right_read, vec![1, 2]);
        assert_eq!(projection.left_output, vec![0]);
        assert_eq!(projection.right_output, vec![0]);
        let prepared = join_filter::JoinFilter::try_new_remapped(
            expr,
            &left_schema,
            &right_schema,
            &projection.left_read,
            &projection.right_read,
            Time::default(),
        )?;
        // Condition-only columns must be read without entering the output projection.
        let selected = prepared.apply_rows(
            vec![
                Arc::new(Int32Array::from(vec![11, 11, 13, 13])),
                Arc::new(Int32Array::from(vec![1, 1, 3, 3])),
                Arc::new(Int32Array::from(vec![22, 24, 22, 24])),
                Arc::new(Int32Array::from(vec![2, 4, 2, 4])),
            ],
            4,
        )?;
        assert_eq!(selected, vec![0, 1, 3]);
        Ok(())
    }

    #[test]
    fn compact_filter_rejects_invalid_indices() {
        for source in [
            ColumnIndex {
                side: JoinSide::Left,
                index: 3,
            },
            ColumnIndex {
                side: JoinSide::Right,
                index: usize::MAX,
            },
            ColumnIndex {
                side: JoinSide::None,
                index: 0,
            },
        ] {
            let filter = JoinFilter {
                expression: Arc::new(Column::new("value", 0)),
                column_indices: vec![source],
                schema: Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Boolean,
                    false,
                )])),
            };
            assert!(filter.physical_expr(3).is_err());
        }
        let filter = JoinFilter {
            expression: Arc::new(Column::new("value", 1)),
            column_indices: vec![ColumnIndex {
                side: JoinSide::Left,
                index: 0,
            }],
            schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Boolean,
                false,
            )])),
        };
        assert!(filter.physical_expr(3).is_err());
    }

    #[test]
    fn product_blocks_can_read_a_prefix_of_a_large_group() {
        let ranges = [IdxRange {
            batch_idx: 0,
            start: 0,
            end: 1_000_000_000,
        }];
        let prefix = product_blocks(&ranges, &ranges, 1)
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(prefix.len(), 3);
        for (i, block) in prefix.iter().enumerate() {
            assert_eq!(block.loffset, 0);
            assert_eq!(block.roffset, i);
            assert_eq!(block.lrange.len() * block.rrange.len(), 1);
        }
        assert!(product_blocks(&ranges, &[], 1).next().is_none());
    }
}
