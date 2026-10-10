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

//! Evaluate residual join conditions on bounded candidate pairs.
//! Deterministic one-sided expressions retain input cardinality; cross-side
//! comparisons read runs directly. Lazy and volatile subtrees evaluate per
//! pair.

use std::{cell::OnceCell, collections::HashMap, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBufferBuilder, GenericByteArray, PrimitiveArray,
        RecordBatch, RecordBatchOptions, UInt32Array, new_null_array,
    },
    buffer::{BooleanBuffer, NullBuffer},
    compute::take,
    datatypes::{
        ArrowPrimitiveType, ByteArrayType, DataType, Date32Type, Date64Type, Decimal128Type,
        Decimal256Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Field, GenericBinaryType, GenericStringType, Int8Type, Int16Type,
        Int32Type, Int64Type, Schema, SchemaRef, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    },
};
use datafusion::{
    common::{
        Result, ScalarValue,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    logical_expr::{Operator, Volatility},
    physical_expr::{
        PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr,
        expressions::{
            BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, LikeExpr, Literal,
            NegativeExpr, NotExpr, TryCastExpr,
        },
        utils::collect_columns,
    },
    physical_plan::{ColumnarValue, metrics::Time},
};
use datafusion_ext_commons::{df_execution_err, downcast_any};

use crate::joins::{Idx, IdxRange, ProductBlock, ranges_num_rows};

fn as_boolean(array: &ArrayRef) -> Result<&BooleanArray> {
    match array.as_any().downcast_ref::<BooleanArray>() {
        Some(boolean) => Ok(boolean),
        None => df_execution_err!(
            "join filter: expected boolean result, got {}",
            array.data_type()
        ),
    }
}

fn as_boolean_scalar(scalar: &ScalarValue) -> Result<bool> {
    match scalar {
        ScalarValue::Boolean(value) => Ok(value.unwrap_or(false)),
        other => df_execution_err!(
            "join filter: expected boolean result, got {}",
            other.data_type()
        ),
    }
}

/// Select true positions; nulls do not satisfy the join condition.
fn set_positions(mask: &BooleanArray) -> Vec<u32> {
    let selected = match mask.nulls() {
        Some(nulls) => mask.values() & nulls.inner(),
        None => mask.values().clone(),
    };
    selected.set_indices().map(|i| i as u32).collect()
}

/// Pre-analyzed expression with non-literal children mapped to temporary
/// columns. Only known eager, deterministic nodes are decomposed; other
/// subtrees remain intact.
#[derive(Debug)]
struct AnalyzedPhysicalExpr {
    expr: PhysicalExprRef,
    children: Vec<Arc<AnalyzedPhysicalExpr>>,
    children_schema: SchemaRef,
    /// Lazy or volatile subtrees must retain pair cardinality, even without
    /// input columns.
    evaluate_per_pair: bool,
}

impl AnalyzedPhysicalExpr {
    /// `input_schema` contains left columns followed by right columns.
    fn analyze(expr: &PhysicalExprRef, input_schema: &SchemaRef) -> Result<Arc<Self>> {
        if !Self::can_evaluate_children_eagerly(expr) {
            let mut columns = collect_columns(expr).into_iter().collect::<Vec<_>>();
            columns.sort_unstable_by_key(Column::index);
            let mut children = vec![];
            let mut fields = vec![];
            let mut remapped = HashMap::new();
            for column in columns {
                remapped.insert(column.index(), children.len());
                fields.push(input_schema.field(column.index()).clone());
                children.push(Self::analyze(
                    &(Arc::new(column) as PhysicalExprRef),
                    input_schema,
                )?);
            }
            let expr = expr
                .clone()
                .transform_up(|expr| {
                    let Ok(column) = downcast_any!(expr, Column) else {
                        return Ok(Transformed::no(expr));
                    };
                    Ok(Transformed::yes(Arc::new(Column::new(
                        column.name(),
                        remapped[&column.index()],
                    ))))
                })
                .data()?;
            return Ok(Arc::new(Self {
                expr,
                children,
                children_schema: Arc::new(Schema::new(fields)),
                evaluate_per_pair: true,
            }));
        }

        let mut children = vec![];
        let mut fields = vec![];
        let mut new_children = vec![];

        for child in expr.children() {
            if downcast_any!(child, Literal).is_ok() {
                new_children.push(child.clone());
                continue;
            }
            let name = format!("c{}", children.len());
            new_children.push(Arc::new(Column::new(&name, children.len())) as PhysicalExprRef);
            fields.push(Field::new(
                name,
                child.data_type(input_schema)?,
                child.nullable(input_schema)?,
            ));
            children.push(Self::analyze(child, input_schema)?);
        }

        let expr = match new_children.is_empty() {
            true => expr.clone(),
            false => expr.clone().with_new_children(new_children)?,
        };
        Ok(Arc::new(Self {
            expr,
            children,
            children_schema: Arc::new(Schema::new(fields)),
            evaluate_per_pair: false,
        }))
    }

    /// Unknown expressions may be lazy or volatile; keep them intact.
    fn can_evaluate_children_eagerly(expr: &PhysicalExprRef) -> bool {
        let any = expr.as_any();
        if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
            return !matches!(binary.op(), Operator::And | Operator::Or);
        }
        if let Some(function) = any.downcast_ref::<ScalarFunctionExpr>() {
            return function.fun().signature().volatility == Volatility::Immutable;
        }
        any.is::<Column>()
            || any.is::<Literal>()
            || any.is::<CastExpr>()
            || any.is::<TryCastExpr>()
            || any.is::<IsNullExpr>()
            || any.is::<IsNotNullExpr>()
            || any.is::<NegativeExpr>()
            || any.is::<NotExpr>()
            || any.is::<LikeExpr>()
    }
}

/// Evaluate residual conditions without materializing full joined rows.
#[derive(Debug, Clone)]
pub struct JoinFilter {
    filter_expr: PhysicalExprRef,
    analyzed_filter_expr: Arc<AnalyzedPhysicalExpr>,
    filter_schema: SchemaRef,
    num_left_cols: usize,
    /// Condition-column indices within each input schema, in evaluation order.
    left_read: Vec<usize>,
    right_read: Vec<usize>,
    post_filter_time: Time,
}

impl JoinFilter {
    /// `filter_expr` addresses `filter_schema`: `num_left_cols` left columns,
    /// then right.
    pub fn try_new(
        filter_expr: PhysicalExprRef,
        filter_schema: SchemaRef,
        num_left_cols: usize,
        post_filter_time: Time,
    ) -> Result<Self> {
        let num_right_cols = filter_schema.fields().len() - num_left_cols;
        Ok(Self {
            analyzed_filter_expr: AnalyzedPhysicalExpr::analyze(&filter_expr, &filter_schema)?,
            filter_expr,
            filter_schema,
            num_left_cols,
            left_read: (0..num_left_cols).collect(),
            right_read: (0..num_right_cols).collect(),
            post_filter_time,
        })
    }

    pub fn filter_expr(&self) -> &PhysicalExprRef {
        &self.filter_expr
    }

    /// Remap full left/right expression indices to the columns in `left_read` /
    /// `right_read`.
    pub fn try_new_remapped(
        filter_expr: PhysicalExprRef,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        left_read: &[usize],
        right_read: &[usize],
        post_filter_time: Time,
    ) -> Result<Self> {
        let mut fields = vec![];
        let mut remapped = HashMap::new();
        for (new_idx, &old_idx) in left_read.iter().enumerate() {
            fields.push(left_schema.field(old_idx).clone());
            remapped.insert(old_idx, new_idx);
        }
        for (new_idx, &old_idx) in right_read.iter().enumerate() {
            fields.push(right_schema.field(old_idx).clone());
            remapped.insert(
                old_idx + left_schema.fields().len(),
                new_idx + left_read.len(),
            );
        }

        let filter_expr = filter_expr
            .transform_up(|expr| {
                let Ok(column) = downcast_any!(expr, Column) else {
                    return Ok(Transformed::no(expr));
                };
                match remapped.get(&column.index()) {
                    Some(&new_idx) => Ok(Transformed::yes(Arc::new(Column::new(
                        column.name(),
                        new_idx,
                    )))),
                    None => df_execution_err!(
                        "join filter: column {} is not read by the join",
                        column.name()
                    ),
                }
            })
            .data()?;

        let mut filter = Self::try_new(
            filter_expr,
            Arc::new(Schema::new(fields)),
            left_read.len(),
            post_filter_time,
        )?;
        filter.left_read = left_read.to_vec();
        filter.right_read = right_read.to_vec();
        Ok(filter)
    }

    pub fn filter_schema(&self) -> &SchemaRef {
        &self.filter_schema
    }

    pub fn num_left_cols(&self) -> usize {
        self.num_left_cols
    }

    pub fn left_read(&self) -> &[usize] {
        &self.left_read
    }

    pub fn right_read(&self) -> &[usize] {
        &self.right_read
    }

    /// Evaluate runs over untaken `cols` (left then right); null results do not
    /// match. Clear and fill `lpos` / `rpos` with positions in concatenated
    /// run segments, enumerated by run and then in left-major order.
    pub fn apply(&self, cols: &[ArrayRef], runs: &[Run], out: &mut Pairs) -> Result<()> {
        if cols.len() != self.filter_schema.fields().len() {
            df_execution_err!(
                "join filter: expected {} columns, got {}",
                self.filter_schema.fields().len(),
                cols.len()
            )?;
        }

        let _timer = self.post_filter_time.timer();
        let ctx = EvalCtx::new(cols, self.num_left_cols, runs);
        let evaluated = ctx.eval(&self.analyzed_filter_expr)?;
        ctx.write_pairs(evaluated, out)
    }

    /// Evaluate pair-aligned `cols` (left then right), returning matching row
    /// indices. The caller gathers hash-join candidates; null results do
    /// not match.
    pub fn apply_rows(&self, cols: Vec<ArrayRef>, num_rows: usize) -> Result<Vec<u32>> {
        if cols.len() != self.filter_schema.fields().len() {
            df_execution_err!(
                "join filter: expected {} columns, got {}",
                self.filter_schema.fields().len(),
                cols.len()
            )?;
        }

        let _timer = self.post_filter_time.timer();
        let batch = RecordBatch::try_new_with_options(
            self.filter_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        let result = self.filter_expr.evaluate(&batch)?.into_array(num_rows)?;
        Ok(set_positions(as_boolean(&result)?))
    }
}

/// Pending Cartesian runs sharing one pair of input batches.
/// Small groups share an evaluation without expanding every candidate into
/// indices.
#[derive(Default)]
pub struct RunBatch {
    runs: Vec<Run>,
    /// Position of each run's rows in the caller's own numbering
    bases: Vec<(u32, u32)>,
    batches: (usize, usize),
    num_pairs: usize,
    pairs: Pairs,
}

/// Surviving run positions plus offsets into input batches and caller row
/// numbering. Offsets may encode negative displacements; combine them with
/// `wrapping_add`.
pub struct MatchedRun<'a> {
    /// Batch the run's rows belong to
    pub lbatch: usize,
    pub rbatch: usize,
    /// Converts a left position to the row number addressed by `lbatch`
    pub lrow: u32,
    pub rrow: u32,
    /// Converts a left position to the caller's own numbering
    pub lbase: u32,
    pub rbase: u32,
    pub lpos: &'a [u32],
    pub rpos: &'a [u32],
}

impl RunBatch {
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn num_pairs(&self) -> usize {
        self.num_pairs
    }

    pub fn accepts(&self, lbatch: usize, rbatch: usize) -> bool {
        self.runs.is_empty() || self.batches == (lbatch, rbatch)
    }

    /// Add a block, numbering its rows starting at `lbase` / `rbase` in the
    /// caller's own numbering.
    pub fn push(&mut self, block: &ProductBlock, lbase: usize, rbase: usize) {
        let (lrange, rrange) = (&block.lrange, &block.rrange);
        debug_assert!(self.accepts(lrange.batch_idx, rrange.batch_idx));
        self.batches = (lrange.batch_idx, rrange.batch_idx);
        self.num_pairs += lrange.len() * rrange.len();
        self.runs.push(Run {
            lstart: lrange.start as u32,
            lnum: lrange.len() as u32,
            rstart: rrange.start as u32,
            rnum: rrange.len() as u32,
        });
        self.bases.push((
            (lbase + block.loffset) as u32,
            (rbase + block.roffset) as u32,
        ));
    }

    /// Evaluate and clear pending runs, reporting survivors separately for each
    /// run.
    pub fn evaluate(
        &mut self,
        filter: &JoinFilter,
        lbatches: &[RecordBatch],
        rbatches: &[RecordBatch],
        mut out: impl FnMut(MatchedRun<'_>),
    ) -> Result<()> {
        if self.runs.is_empty() {
            return Ok(());
        }
        let (lbatch, rbatch) = self.batches;
        let cols = [lbatches[lbatch].columns(), rbatches[rbatch].columns()].concat();
        let mut pairs = std::mem::take(&mut self.pairs);
        let result = filter.apply(&cols, &self.runs, &mut pairs);

        if result.is_ok() {
            let (mut loff, mut roff) = (0, 0);
            for ((run, &(lbase, rbase)), (lpos, rpos)) in
                self.runs.iter().zip(&self.bases).zip(pairs.runs())
            {
                if !lpos.is_empty() {
                    out(MatchedRun {
                        lbatch,
                        rbatch,
                        lrow: run.lstart.wrapping_sub(loff),
                        rrow: run.rstart.wrapping_sub(roff),
                        lbase: lbase.wrapping_sub(loff),
                        rbase: rbase.wrapping_sub(roff),
                        lpos,
                        rpos,
                    });
                }
                loff += run.lnum;
                roff += run.rnum;
            }
        }
        self.pairs = pairs;
        self.runs.clear();
        self.bases.clear();
        self.num_pairs = 0;
        result
    }
}

/// Surviving pairs and run boundaries, recorded during expansion to avoid later
/// searches.
#[derive(Default)]
pub struct Pairs {
    /// Positions of surviving pairs, in each side's own concatenated layout
    lpos: Vec<u32>,
    rpos: Vec<u32>,
    /// End position of run i's pairs in `lpos`/`rpos`;
    /// the start is the previous run's end
    run_ends: Vec<u32>,
}

impl Pairs {
    fn clear(&mut self) {
        self.lpos.clear();
        self.rpos.clear();
        self.run_ends.clear();
    }

    /// Mark the end of the current run. Must be called for every run, even if
    /// it left no pairs.
    #[inline]
    fn end_run(&mut self) {
        self.run_ends.push(self.lpos.len() as u32);
    }

    fn runs(&self) -> impl Iterator<Item = (&[u32], &[u32])> {
        let mut start = 0;
        self.run_ends.iter().map(move |&end| {
            let (from, to) = (start, end as usize);
            start = to;
            (&self.lpos[from..to], &self.rpos[from..to])
        })
    }
}

/// Cartesian product of left and right batch ranges, in left-major order.
/// Pair `i` addresses `(lstart + i / rnum, rstart + i % rnum)` without index
/// arrays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
    pub lstart: u32,
    pub lnum: u32,
    pub rstart: u32,
    pub rnum: u32,
}

impl Run {
    #[inline]
    pub fn num_pairs(&self) -> usize {
        self.lnum as usize * self.rnum as usize
    }
}

/// Track scalar, input-row, or candidate-pair cardinality to avoid unnecessary
/// expansion.
enum Evaluated {
    Scalar(ScalarValue),
    Left(ArrayRef),
    Right(ArrayRef),
    Product(ArrayRef),
}

struct EvalCtx<'a> {
    cols: &'a [ArrayRef],
    num_left_cols: usize,
    runs: &'a [Run],
    /// Total left rows in the concatenated run layout.
    lnum: usize,
    rnum: usize,
    num_pairs: usize,
    /// Concatenated left segments; a single run uses a slice instead.
    lrows: OnceCell<UInt32Array>,
    rrows: OnceCell<UInt32Array>,
    /// Pair-to-left-row indices, built lazily when an expression needs both
    /// sides.
    lkeys: OnceCell<UInt32Array>,
    rkeys: OnceCell<UInt32Array>,
}

impl<'a> EvalCtx<'a> {
    fn new(cols: &'a [ArrayRef], num_left_cols: usize, runs: &'a [Run]) -> Self {
        let mut lnum = 0;
        let mut rnum = 0;
        let mut num_pairs = 0;
        for run in runs {
            lnum += run.lnum as usize;
            rnum += run.rnum as usize;
            num_pairs += run.num_pairs();
        }
        Self {
            cols,
            num_left_cols,
            runs,
            lnum,
            rnum,
            num_pairs,
            lrows: OnceCell::new(),
            rrows: OnceCell::new(),
            lkeys: OnceCell::new(),
            rkeys: OnceCell::new(),
        }
    }

    fn eval(&self, analyzed: &AnalyzedPhysicalExpr) -> Result<Evaluated> {
        // A single run can read each input column as a slice.
        if let Ok(column) = downcast_any!(&analyzed.expr, Column) {
            let idx = column.index();
            let is_left = idx < self.num_left_cols;
            let col = self.side_column(&self.cols[idx], is_left)?;
            return Ok(match is_left {
                true => Evaluated::Left(col),
                false => Evaluated::Right(col),
            });
        }

        if let Ok(literal) = downcast_any!(&analyzed.expr, Literal) {
            return Ok(Evaluated::Scalar(literal.value().clone()));
        }

        let children = analyzed
            .children
            .iter()
            .map(|child| self.eval(child))
            .collect::<Result<Vec<_>>>()?;
        let (mut left, mut right, mut product) = (false, false, false);
        for child in &children {
            match child {
                Evaluated::Scalar(_) => {}
                Evaluated::Left(_) => left = true,
                Evaluated::Right(_) => right = true,
                Evaluated::Product(_) => product = true,
            }
        }

        if analyzed.evaluate_per_pair {
            return Ok(Evaluated::Product(
                self.eval_product(analyzed, &children)?
                    .into_array(self.num_pairs)?,
            ));
        }

        if !left && !right && !product {
            // Immutable expressions may return arrays; those must be evaluated at pair
            // count.
            if let ColumnarValue::Scalar(scalar) = self.eval_side(analyzed, &children, 1)? {
                return Ok(Evaluated::Scalar(scalar));
            }
        } else if !product && left != right {
            let num_rows = match left {
                true => self.lnum,
                false => self.rnum,
            };
            let array = self
                .eval_side(analyzed, &children, num_rows)?
                .into_array(num_rows)?;
            return Ok(match left {
                true => Evaluated::Left(array),
                false => Evaluated::Right(array),
            });
        }
        Ok(Evaluated::Product(
            self.eval_product(analyzed, &children)?
                .into_array(self.num_pairs)?,
        ))
    }

    fn eval_side(
        &self,
        analyzed: &AnalyzedPhysicalExpr,
        children: &[Evaluated],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let columns = children
            .iter()
            .map(|child| {
                Ok(match child {
                    Evaluated::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
                    Evaluated::Left(array) | Evaluated::Right(array) => array.clone(),
                    Evaluated::Product(_) => {
                        df_execution_err!("join filter: product operand in a single-sided node")?
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.evaluate(analyzed, columns, num_rows)
    }

    fn eval_product(
        &self,
        analyzed: &AnalyzedPhysicalExpr,
        children: &[Evaluated],
    ) -> Result<ColumnarValue> {
        // Compare cross-side columns before `to_product` would copy them per pair.
        if let Some(result) = self.try_compare_columns(&analyzed.expr, children)? {
            return Ok(ColumnarValue::Array(result));
        }

        let columns = children
            .iter()
            .map(|child| self.to_product(child))
            .collect::<Result<Vec<_>>>()?;
        self.evaluate(analyzed, columns, self.num_pairs)
    }

    fn evaluate(
        &self,
        analyzed: &AnalyzedPhysicalExpr,
        columns: Vec<ArrayRef>,
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let batch = RecordBatch::try_new_with_options(
            analyzed.children_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        analyzed.expr.evaluate(&batch)
    }

    /// Compare opposite-side columns directly; return `None` for the general
    /// path.
    fn try_compare_columns(
        &self,
        expr: &PhysicalExprRef,
        children: &[Evaluated],
    ) -> Result<Option<ArrayRef>> {
        let Ok(binary) = downcast_any!(expr, BinaryExpr) else {
            return Ok(None);
        };
        let (lhs, rhs, swapped) = match children {
            [Evaluated::Left(lhs), Evaluated::Right(rhs)] => (lhs, rhs, false),
            [Evaluated::Right(rhs), Evaluated::Left(lhs)] => (lhs, rhs, true),
            _ => return Ok(None),
        };
        if lhs.data_type() != rhs.data_type() {
            return Ok(None);
        }
        let Some(op) = comparison_op(binary.op(), swapped) else {
            return Ok(None);
        };

        // Specialize the comparison predicate to avoid a three-way comparison in the
        // inner loop.
        macro_rules! dispatch {
            ($compare:ident, $t:ty) => {
                match op {
                    Operator::Eq => self.$compare::<$t>(lhs, rhs, |a, b| a == b)?,
                    Operator::NotEq => self.$compare::<$t>(lhs, rhs, |a, b| a != b)?,
                    Operator::Lt => self.$compare::<$t>(lhs, rhs, |a, b| a < b)?,
                    Operator::LtEq => self.$compare::<$t>(lhs, rhs, |a, b| a <= b)?,
                    Operator::Gt => self.$compare::<$t>(lhs, rhs, |a, b| a > b)?,
                    Operator::GtEq => self.$compare::<$t>(lhs, rhs, |a, b| a >= b)?,
                    _ => return Ok(None),
                }
            };
        }
        // Keep floats on Arrow's path: `Ord` disagrees with its NaN semantics.
        let result = match lhs.data_type() {
            DataType::Int8 => dispatch!(compare_primitives, Int8Type),
            DataType::Int16 => dispatch!(compare_primitives, Int16Type),
            DataType::Int32 => dispatch!(compare_primitives, Int32Type),
            DataType::Int64 => dispatch!(compare_primitives, Int64Type),
            DataType::UInt8 => dispatch!(compare_primitives, UInt8Type),
            DataType::UInt16 => dispatch!(compare_primitives, UInt16Type),
            DataType::UInt32 => dispatch!(compare_primitives, UInt32Type),
            DataType::UInt64 => dispatch!(compare_primitives, UInt64Type),
            DataType::Date32 => dispatch!(compare_primitives, Date32Type),
            DataType::Date64 => dispatch!(compare_primitives, Date64Type),
            DataType::Time32(TimeUnit::Second) => dispatch!(compare_primitives, Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => {
                dispatch!(compare_primitives, Time32MillisecondType)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                dispatch!(compare_primitives, Time64MicrosecondType)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                dispatch!(compare_primitives, Time64NanosecondType)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                dispatch!(compare_primitives, TimestampSecondType)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                dispatch!(compare_primitives, TimestampMillisecondType)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                dispatch!(compare_primitives, TimestampMicrosecondType)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                dispatch!(compare_primitives, TimestampNanosecondType)
            }
            DataType::Duration(TimeUnit::Second) => {
                dispatch!(compare_primitives, DurationSecondType)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                dispatch!(compare_primitives, DurationMillisecondType)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                dispatch!(compare_primitives, DurationMicrosecondType)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                dispatch!(compare_primitives, DurationNanosecondType)
            }
            DataType::Decimal128(..) => dispatch!(compare_primitives, Decimal128Type),
            DataType::Decimal256(..) => dispatch!(compare_primitives, Decimal256Type),
            DataType::Utf8 => dispatch!(compare_bytes, GenericStringType<i32>),
            DataType::LargeUtf8 => dispatch!(compare_bytes, GenericStringType<i64>),
            DataType::Binary => dispatch!(compare_bytes, GenericBinaryType<i32>),
            DataType::LargeBinary => dispatch!(compare_bytes, GenericBinaryType<i64>),
            _ => return Ok(None),
        };
        Ok(Some(Arc::new(result)))
    }

    fn compare_primitives<T>(
        &self,
        lhs: &ArrayRef,
        rhs: &ArrayRef,
        cmp: impl Fn(T::Native, T::Native) -> bool,
    ) -> Result<BooleanArray>
    where
        T: ArrowPrimitiveType,
        T::Native: Ord,
    {
        let lhs = downcast_any!(lhs, PrimitiveArray<T>)?;
        let rhs = downcast_any!(rhs, PrimitiveArray<T>)?;
        let lvalues = lhs.values();
        let rvalues = rhs.values();

        let mut values = vec![false; self.num_pairs];
        let (mut pos, mut loff, mut roff) = (0, 0, 0);
        for run in self.runs {
            let (lnum, rnum) = (run.lnum as usize, run.rnum as usize);
            let rvals = &rvalues[roff..roff + rnum];
            for l in loff..loff + lnum {
                let lv = lvalues[l];
                for (out, rv) in values[pos..pos + rnum].iter_mut().zip(rvals) {
                    *out = cmp(lv, *rv);
                }
                pos += rnum;
            }
            loff += lnum;
            roff += rnum;
        }

        Ok(BooleanArray::new(
            BooleanBuffer::from(&values[..]),
            self.product_nulls(lhs.nulls(), rhs.nulls()),
        ))
    }

    fn compare_bytes<T: ByteArrayType>(
        &self,
        lhs: &ArrayRef,
        rhs: &ArrayRef,
        cmp: impl Fn(&[u8], &[u8]) -> bool,
    ) -> Result<BooleanArray> {
        let lhs = downcast_any!(lhs, GenericByteArray<T>)?;
        let rhs = downcast_any!(rhs, GenericByteArray<T>)?;

        let mut values = vec![false; self.num_pairs];
        let (mut pos, mut loff, mut roff) = (0, 0, 0);
        for run in self.runs {
            let (lnum, rnum) = (run.lnum as usize, run.rnum as usize);
            for l in loff..loff + lnum {
                let lv: &[u8] = lhs.value(l).as_ref();
                for (out, r) in values[pos..pos + rnum].iter_mut().zip(roff..roff + rnum) {
                    *out = cmp(lv, rhs.value(r).as_ref());
                }
                pos += rnum;
            }
            loff += lnum;
            roff += rnum;
        }

        Ok(BooleanArray::new(
            BooleanBuffer::from(&values[..]),
            self.product_nulls(lhs.nulls(), rhs.nulls()),
        ))
    }

    /// Build validity separately from values to keep comparisons branch-free.
    /// Each left row repeats the right segment validity, unless the left row is
    /// null.
    fn product_nulls(
        &self,
        lnulls: Option<&NullBuffer>,
        rnulls: Option<&NullBuffer>,
    ) -> Option<NullBuffer> {
        if lnulls.is_none() && rnulls.is_none() {
            return None;
        }
        let mut nulls = BooleanBufferBuilder::new(self.num_pairs);
        let (mut loff, mut roff) = (0, 0);
        for run in self.runs {
            let (lnum, rnum) = (run.lnum as usize, run.rnum as usize);
            for l in loff..loff + lnum {
                if lnulls.is_some_and(|lnulls| lnulls.is_null(l)) {
                    nulls.append_n(rnum, false);
                    continue;
                }
                match rnulls {
                    // Sliced validity buffers may start at a nonzero bit offset.
                    Some(rnulls) => {
                        let offset = rnulls.inner().offset() + roff;
                        nulls.append_packed_range(offset..offset + rnum, rnulls.validity());
                    }
                    None => nulls.append_n(rnum, true),
                }
            }
            loff += lnum;
            roff += rnum;
        }
        Some(NullBuffer::new(nulls.finish()))
    }

    fn to_product(&self, value: &Evaluated) -> Result<ArrayRef> {
        Ok(match value {
            Evaluated::Scalar(scalar) => scalar.to_array_of_size(self.num_pairs)?,
            Evaluated::Product(array) => array.clone(),
            Evaluated::Left(array) => take(array.as_ref(), self.left_keys(), None)?,
            Evaluated::Right(array) => take(array.as_ref(), self.right_keys(), None)?,
        })
    }

    /// Concatenate the column segments addressed by the runs on one side.
    fn side_column(&self, col: &ArrayRef, is_left: bool) -> Result<ArrayRef> {
        if let [run] = self.runs {
            return Ok(match is_left {
                true => col.slice(run.lstart as usize, run.lnum as usize),
                false => col.slice(run.rstart as usize, run.rnum as usize),
            });
        }
        Ok(take(col.as_ref(), self.side_rows(is_left), None)?)
    }

    fn side_rows(&self, is_left: bool) -> &UInt32Array {
        let (cell, num_rows) = match is_left {
            true => (&self.lrows, self.lnum),
            false => (&self.rrows, self.rnum),
        };
        cell.get_or_init(|| {
            let mut rows = Vec::with_capacity(num_rows);
            for run in self.runs {
                let (start, num) = match is_left {
                    true => (run.lstart, run.lnum),
                    false => (run.rstart, run.rnum),
                };
                rows.extend(start..start + num);
            }
            UInt32Array::from(rows)
        })
    }

    /// Map pairs to positions in the concatenated side layout, not the input
    /// batch.
    fn left_keys(&self) -> &UInt32Array {
        self.lkeys.get_or_init(|| {
            let mut keys = Vec::with_capacity(self.num_pairs);
            let mut loff = 0;
            for run in self.runs {
                for l in 0..run.lnum {
                    keys.resize(keys.len() + run.rnum as usize, loff + l);
                }
                loff += run.lnum;
            }
            UInt32Array::from(keys)
        })
    }

    fn right_keys(&self) -> &UInt32Array {
        self.rkeys.get_or_init(|| {
            let mut keys = Vec::with_capacity(self.num_pairs);
            let mut roff = 0;
            for run in self.runs {
                for _ in 0..run.lnum {
                    keys.extend(roff..roff + run.rnum);
                }
                roff += run.rnum;
            }
            UInt32Array::from(keys)
        })
    }

    /// Expand one-sided results only for surviving pairs.
    fn write_pairs(&self, evaluated: Evaluated, out: &mut Pairs) -> Result<()> {
        out.clear();

        match evaluated {
            Evaluated::Scalar(scalar) => {
                let keep = as_boolean_scalar(&scalar)?;
                let (mut loff, mut roff) = (0, 0);
                for run in self.runs {
                    if keep {
                        for l in 0..run.lnum {
                            out.lpos
                                .resize(out.lpos.len() + run.rnum as usize, loff + l);
                            out.rpos.extend(roff..roff + run.rnum);
                        }
                    }
                    loff += run.lnum;
                    roff += run.rnum;
                    out.end_run();
                }
            }
            Evaluated::Left(array) => {
                let matched = set_positions(as_boolean(&array)?);
                let (mut i, mut loff, mut roff) = (0, 0, 0);
                for run in self.runs {
                    while matched.get(i).is_some_and(|&m| m < loff + run.lnum) {
                        out.lpos
                            .resize(out.lpos.len() + run.rnum as usize, matched[i]);
                        out.rpos.extend(roff..roff + run.rnum);
                        i += 1;
                    }
                    loff += run.lnum;
                    roff += run.rnum;
                    out.end_run();
                }
            }
            Evaluated::Right(array) => {
                let matched = set_positions(as_boolean(&array)?);
                let (mut i, mut loff, mut roff) = (0, 0, 0);
                for run in self.runs {
                    let start = i;
                    while matched.get(i).is_some_and(|&m| m < roff + run.rnum) {
                        i += 1;
                    }
                    let survived = &matched[start..i];
                    for l in 0..run.lnum {
                        out.lpos.resize(out.lpos.len() + survived.len(), loff + l);
                        out.rpos.extend_from_slice(survived);
                    }
                    loff += run.lnum;
                    roff += run.rnum;
                    out.end_run();
                }
            }
            Evaluated::Product(array) => {
                let matched = set_positions(as_boolean(&array)?);
                let (mut i, mut loff, mut roff) = (0, 0, 0);
                let mut base = 0;
                for run in self.runs {
                    let end = base + run.num_pairs();
                    while matched.get(i).is_some_and(|&m| (m as usize) < end) {
                        let local = matched[i] - base as u32;
                        out.lpos.push(loff + local / run.rnum);
                        out.rpos.push(roff + local % run.rnum);
                        i += 1;
                    }
                    loff += run.lnum;
                    roff += run.rnum;
                    base = end;
                    out.end_run();
                }
            }
        }
        debug_assert_eq!(out.run_ends.len(), self.runs.len());
        Ok(())
    }
}

/// Normalize comparisons to left op right, reversing the operator for swapped
/// operands.
fn comparison_op(op: &Operator, swapped: bool) -> Option<Operator> {
    Some(match (op, swapped) {
        (Operator::Eq, _) => Operator::Eq,
        (Operator::NotEq, _) => Operator::NotEq,
        (Operator::Lt, false) | (Operator::Gt, true) => Operator::Lt,
        (Operator::LtEq, false) | (Operator::GtEq, true) => Operator::LtEq,
        (Operator::Gt, false) | (Operator::Lt, true) => Operator::Gt,
        (Operator::GtEq, false) | (Operator::LtEq, true) => Operator::GtEq,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{BinaryArray, Int32Array, StringArray},
        datatypes::Field,
    };
    use datafusion::{
        common::ScalarValue,
        physical_expr::expressions::{self as phys_expr, IsNotNullExpr, LikeExpr},
    };
    use datafusion_ext_exprs::string_starts_with::StringStartsWithExpr;

    use super::*;
    use crate::joins::{product_blocks, ranges_iter};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("l_str", DataType::Utf8, true),
            Field::new("l_int", DataType::Int32, true),
            Field::new("r_str", DataType::Utf8, true),
            Field::new("r_int", DataType::Int32, true),
        ]))
    }

    fn cols() -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![Some("aa"), Some("bb"), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec![Some("bb"), Some("aa")])),
            Arc::new(Int32Array::from(vec![Some(2), Some(9)])),
        ]
    }

    fn apply_diagonal(filter: &JoinFilter, cols: &[ArrayRef], n: usize) -> Vec<bool> {
        let mask = apply_mask(filter, cols, n, n);
        (0..n).map(|i| mask[i * n + i]).collect()
    }

    fn apply_mask(filter: &JoinFilter, cols: &[ArrayRef], lnum: usize, rnum: usize) -> Vec<bool> {
        let mut pairs = Pairs::default();
        filter
            .apply(
                cols,
                &[Run {
                    lstart: 0,
                    lnum: lnum as u32,
                    rstart: 0,
                    rnum: rnum as u32,
                }],
                &mut pairs,
            )
            .expect("apply failed");
        let mut mask = vec![false; lnum * rnum];
        for (&l, &r) in pairs.lpos.iter().zip(&pairs.rpos) {
            mask[l as usize * rnum + r as usize] = true;
        }
        mask
    }

    fn apply(expr: PhysicalExprRef) -> Vec<bool> {
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        apply_mask(&filter, &cols(), 3, 2)
    }

    fn apply_naive(expr: PhysicalExprRef) -> Vec<Option<bool>> {
        let left_indices = UInt32Array::from(vec![0, 0, 1, 1, 2, 2]);
        let right_indices = UInt32Array::from(vec![0, 1, 0, 1, 0, 1]);
        let cols = cols();
        let taken = cols
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let indices = if i < 2 { &left_indices } else { &right_indices };
                take(col.as_ref(), indices, None).expect("valid test take indices")
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema(), taken).expect("valid test batch");
        let array = expr
            .evaluate(&batch)
            .expect("test expression evaluation succeeds")
            .into_array(6)
            .expect("test result converts to array");
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("expected test array type")
            .iter()
            .collect()
    }

    /// Compare selection masks: null and false both reject a candidate pair.
    fn as_mask(values: &[Option<bool>]) -> Vec<bool> {
        values.iter().map(|v| v.unwrap_or(false)).collect()
    }

    fn check(expr: PhysicalExprRef) -> Vec<bool> {
        let optimized = apply(expr.clone());
        assert_eq!(optimized, as_mask(&apply_naive(expr)), "optimized != naive");
        optimized
    }

    #[test]
    fn test_string_compare_across_sides() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            Operator::Eq,
            phys_expr::col("r_str", &schema()).expect("test column exists"),
        ));
        assert_eq!(check(expr), vec![false, true, true, false, false, false]);
    }

    #[test]
    fn test_case_preserves_lazy_evaluation() {
        use datafusion::physical_expr::expressions::CaseExpr;

        let lit = |v| Arc::new(Literal::new(ScalarValue::Int32(Some(v)))) as PhysicalExprRef;
        let condition = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("l_int", 1)),
            Operator::Gt,
            lit(100),
        ));
        let cast = Arc::new(CastExpr::new(
            Arc::new(Column::new("l_str", 0)),
            DataType::Int32,
            None,
        ));
        let case = Arc::new(
            CaseExpr::try_new(None, vec![(condition, cast)], Some(lit(0)))
                .expect("valid test case expression"),
        );
        let expr = Arc::new(BinaryExpr::new(
            case,
            Operator::Lt,
            Arc::new(Column::new("r_int", 3)),
        ));
        // No l_int exceeds 100. The invalid cast must remain unreachable.
        assert_eq!(check(expr), vec![true; 6]);
    }

    #[test]
    fn test_case_selects_cross_side_branches() {
        use datafusion::physical_expr::expressions::CaseExpr;

        let s = Arc::new(Schema::new(vec![
            Field::new("l_str", DataType::Utf8, true),
            Field::new("l_int", DataType::Int32, true),
            Field::new("r_int", DataType::Int32, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("1"), Some("invalid"), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(0), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        ];
        let expr = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("l_int", 1)),
                        Operator::Eq,
                        Arc::new(Column::new("r_int", 2)),
                    )) as PhysicalExprRef,
                    Arc::new(BinaryExpr::new(
                        Arc::new(CastExpr::new(
                            Arc::new(Column::new("l_str", 0)),
                            DataType::Int32,
                            None,
                        )),
                        Operator::Eq,
                        Arc::new(Column::new("r_int", 2)),
                    )) as PhysicalExprRef,
                )],
                Some(Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))),
            )
            .expect("valid test case expression"),
        );
        let filter =
            JoinFilter::try_new(expr, s, 2, Time::default()).expect("valid test join filter");
        assert_eq!(
            apply_mask(&filter, &columns, 3, 2),
            vec![true, false, false, false, false, false]
        );
    }

    #[test]
    fn test_logical_short_circuit_skips_invalid_cast() {
        use datafusion::physical_expr::expressions::{SCAndExpr, SCOrExpr};

        for op in [Operator::And, Operator::Or] {
            for specialized in [false, true] {
                let guard: PhysicalExprRef =
                    Arc::new(Literal::new(ScalarValue::Boolean(Some(op == Operator::Or))));
                let invalid: PhysicalExprRef = Arc::new(BinaryExpr::new(
                    Arc::new(CastExpr::new(
                        Arc::new(Column::new("l_str", 0)),
                        DataType::Int32,
                        None,
                    )),
                    Operator::Lt,
                    Arc::new(Column::new("r_int", 3)),
                ));
                let expr: PhysicalExprRef = match (op, specialized) {
                    (Operator::And, true) => Arc::new(SCAndExpr::new(guard, invalid)),
                    (Operator::Or, true) => Arc::new(SCOrExpr::new(guard, invalid)),
                    _ => Arc::new(BinaryExpr::new(guard, op, invalid)),
                };
                assert_eq!(check(expr), vec![op == Operator::Or; 6]);
            }
        }
    }

    #[test]
    fn test_logical_short_circuit_selects_rows() {
        use datafusion::physical_expr::expressions::{SCAndExpr, SCOrExpr};

        let s = Arc::new(Schema::new(vec![
            Field::new("l_str", DataType::Utf8, false),
            Field::new("l_int", DataType::Int32, false),
            Field::new("r_int", DataType::Int32, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["1", "invalid"])),
            Arc::new(Int32Array::from(vec![1, 0])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ];
        for op in [Operator::And, Operator::Or] {
            let guard: PhysicalExprRef = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("l_int", 1)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(
                    if op == Operator::And { 1 } else { 0 },
                )))),
            ));
            let selected: PhysicalExprRef = Arc::new(BinaryExpr::new(
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("l_str", 0)),
                    DataType::Int32,
                    None,
                )),
                Operator::Lt,
                Arc::new(Column::new("r_int", 2)),
            ));
            let expr: PhysicalExprRef = if op == Operator::And {
                Arc::new(SCAndExpr::new(guard, selected))
            } else {
                Arc::new(SCOrExpr::new(guard, selected))
            };
            let filter = JoinFilter::try_new(expr, s.clone(), 2, Time::default())
                .expect("valid test join filter");
            assert_eq!(
                apply_mask(&filter, &columns, 2, 2),
                vec![false, true, op == Operator::Or, op == Operator::Or],
            );
        }
    }

    #[test]
    fn test_volatile_functions_keep_pair_cardinality() {
        use std::{
            any::Any,
            sync::atomic::{AtomicUsize, Ordering},
        };

        use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};

        #[derive(Debug)]
        struct Sequence {
            signature: Signature,
            rows: Arc<AtomicUsize>,
        }

        impl ScalarUDFImpl for Sequence {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "sequence"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int32)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let start = self.rows.fetch_add(args.number_rows, Ordering::Relaxed);
                Ok(ColumnarValue::Array(Arc::new(
                    Int32Array::from_iter_values(
                        (start..start + args.number_rows).map(|i| i as i32),
                    ),
                )))
            }
        }

        // Volatile functions must see each pair exactly once, without sampling or
        // per-side reuse.
        for with_column in [false, true] {
            let rows = Arc::new(AtomicUsize::new(0));
            let (args, types) = if with_column {
                (
                    vec![Arc::new(Column::new("l_int", 1)) as PhysicalExprRef],
                    vec![DataType::Int32],
                )
            } else {
                (vec![], vec![])
            };
            let udf = Arc::new(ScalarUDF::from(Sequence {
                signature: Signature::exact(types, Volatility::Volatile),
                rows: rows.clone(),
            }));
            let expr = Arc::new(BinaryExpr::new(
                Arc::new(
                    ScalarFunctionExpr::try_new(udf, args, &schema())
                        .expect("valid test scalar function"),
                ),
                Operator::Lt,
                Arc::new(Column::new("r_int", 3)),
            ));
            assert_eq!(apply(expr), vec![true, true, false, true, false, true]);
            assert_eq!(rows.load(Ordering::Relaxed), 6);
        }
    }

    #[test]
    fn test_int_compare_across_sides() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_int", &schema()).expect("test column exists"),
        ));
        assert_eq!(check(expr), vec![true, true, false, true, false, false]);
    }

    #[test]
    fn test_string_compare_with_literal() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("aa".to_owned())))),
        ));
        assert_eq!(check(expr), vec![true, true, false, false, false, false]);
    }

    #[test]
    fn test_non_comparison_expression() {
        let expr: PhysicalExprRef = Arc::new(StringStartsWithExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            "a".to_owned(),
        ));
        assert_eq!(check(expr), vec![true, true, false, false, false, false]);
    }

    #[test]
    fn test_conjunction() {
        let s = schema();
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                phys_expr::col("l_str", &s).expect("test column exists"),
                Operator::Eq,
                phys_expr::col("r_str", &s).expect("test column exists"),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                phys_expr::col("l_int", &s).expect("test column exists"),
                Operator::Lt,
                phys_expr::col("r_int", &s).expect("test column exists"),
            )),
        ));
        assert_eq!(check(expr), vec![false, true, false, false, false, false]);
    }

    #[test]
    fn test_is_not_null() {
        let expr: PhysicalExprRef = Arc::new(IsNotNullExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
        ));
        assert_eq!(check(expr), vec![true, true, true, true, false, false]);
    }

    #[test]
    fn test_empty_indices() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            Operator::Eq,
            phys_expr::col("r_str", &schema()).expect("test column exists"),
        ));
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        let result = apply_mask(&filter, &cols(), 0, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_byte_comparison_all_ops() {
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            for swapped in [false, true] {
                let (lhs, rhs) = match swapped {
                    false => ("l_str", "r_str"),
                    true => ("r_str", "l_str"),
                };
                let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
                    phys_expr::col(lhs, &schema()).expect("test column exists"),
                    op,
                    phys_expr::col(rhs, &schema()).expect("test column exists"),
                ));
                let optimized = apply(expr.clone());
                assert_eq!(
                    optimized,
                    as_mask(&apply_naive(expr)),
                    "mismatch for {lhs} {op} {rhs}"
                );
            }
        }
    }

    /// Sliced validity buffers must preserve bit offsets when expanded to
    /// pairs.
    #[test]
    fn test_comparison_sliced_nulls() {
        // Both sides start mid-array, and neither offset is a multiple of 8
        let run = Run {
            lstart: 1,
            lnum: 3,
            rstart: 2,
            rnum: 3,
        };
        let lints = vec![Some(0), None, Some(1), Some(5), None, Some(9)];
        let rints = vec![Some(0), Some(0), None, Some(7), Some(2)];
        let lstrs = vec![Some("x"), None, Some("a"), Some("m"), None, Some("z")];
        let rstrs = vec![Some("x"), Some("x"), None, Some("q"), Some("b")];

        let cols = vec![
            Arc::new(StringArray::from(lstrs.clone())) as ArrayRef,
            Arc::new(Int32Array::from(lints.clone())) as ArrayRef,
            Arc::new(StringArray::from(rstrs.clone())) as ArrayRef,
            Arc::new(Int32Array::from(rints.clone())) as ArrayRef,
        ];

        for (lcol, rcol) in [("l_int", "r_int"), ("l_str", "r_str")] {
            let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
                phys_expr::col(lcol, &schema()).expect("test column exists"),
                Operator::Lt,
                phys_expr::col(rcol, &schema()).expect("test column exists"),
            ));
            let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
                .expect("valid test join filter");
            let mut pairs = Pairs::default();
            filter
                .apply(&cols, &[run], &mut pairs)
                .expect("apply failed");
            let mut got = vec![false; (run.lnum * run.rnum) as usize];
            for (&l, &r) in pairs.lpos.iter().zip(&pairs.rpos) {
                got[(l * run.rnum + r) as usize] = true;
            }

            let expected = (0..run.lnum)
                .flat_map(|l| (0..run.rnum).map(move |r| (l, r)))
                .map(|(l, r)| {
                    let (l, r) = ((run.lstart + l) as usize, (run.rstart + r) as usize);
                    match lcol {
                        "l_int" => matches!((lints[l], rints[r]), (Some(a), Some(b)) if a < b),
                        _ => matches!((lstrs[l], rstrs[r]), (Some(a), Some(b)) if a < b),
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(got, expected, "mismatch for {lcol} < {rcol}");
        }
    }

    #[test]
    fn test_primitive_comparison_all_ops() {
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            for swapped in [false, true] {
                let (lhs, rhs) = match swapped {
                    false => ("l_int", "r_int"),
                    true => ("r_int", "l_int"),
                };
                let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
                    phys_expr::col(lhs, &schema()).expect("test column exists"),
                    op,
                    phys_expr::col(rhs, &schema()).expect("test column exists"),
                ));
                let optimized = apply(expr.clone());
                assert_eq!(
                    optimized,
                    as_mask(&apply_naive(expr)),
                    "mismatch for {lhs} {op} {rhs}"
                );
            }
        }
    }

    /// Cover nulls on either side and the no-validity-buffer fast path.
    #[test]
    fn test_primitive_comparison_null_shapes() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_int", &schema()).expect("test column exists"),
        ));
        let filter = JoinFilter::try_new(expr.clone(), schema(), 2, Time::default())
            .expect("valid test join filter");

        for (lvalues, rvalues) in [
            (vec![Some(1), Some(5), None], vec![Some(2), None]),
            (vec![Some(1), Some(5), Some(9)], vec![Some(2), Some(7)]),
            (vec![None, None, None], vec![None, None]),
        ] {
            let lnum = lvalues.len();
            let rnum = rvalues.len();
            let lcol = Arc::new(Int32Array::from(lvalues)) as ArrayRef;
            let rcol = Arc::new(Int32Array::from(rvalues)) as ArrayRef;
            let cols = vec![
                Arc::new(StringArray::from(vec![None::<&str>; lnum])) as ArrayRef,
                lcol.clone(),
                Arc::new(StringArray::from(vec![None::<&str>; rnum])) as ArrayRef,
                rcol.clone(),
            ];
            let got = apply_mask(&filter, &cols, lnum, rnum);

            let lvals = lcol
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("expected test array type");
            let rvals = rcol
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("expected test array type");
            let expected = (0..lnum)
                .flat_map(|l| {
                    (0..rnum).map(move |r| {
                        lvals.is_valid(l) && rvals.is_valid(r) && lvals.value(l) < rvals.value(r)
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn test_byte_comparison_ordering() {
        let left: ArrayRef = Arc::new(StringArray::from(vec!["a", "ab", "b", "", "z"]));
        let right: ArrayRef = Arc::new(StringArray::from(vec!["aa", "ab", "", "b", "y"]));
        let cols = vec![left, Arc::new(Int32Array::from(vec![0; 5])) as ArrayRef];
        let cols = [cols, vec![right, Arc::new(Int32Array::from(vec![0; 5]))]].concat();

        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_str", &schema()).expect("test column exists"),
        ));
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        let result = apply_diagonal(&filter, &cols, 5);

        assert_eq!(result, vec![true, false, false, true, false]);
    }

    #[test]
    fn test_binary_comparison() {
        let bin_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("l_bin", DataType::Binary, true),
            Field::new("r_bin", DataType::Binary, true),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from_vec(vec![b"aa", b"bb"])),
            Arc::new(BinaryArray::from_vec(vec![b"ab", b"bb"])),
        ];
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_bin", &bin_schema).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_bin", &bin_schema).expect("test column exists"),
        ));
        let filter = JoinFilter::try_new(expr, bin_schema, 1, Time::default())
            .expect("valid test join filter");
        let result = apply_diagonal(&filter, &cols, 2);
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_non_comparison_expression_across_sides() {
        let expr: PhysicalExprRef = Arc::new(LikeExpr::new(
            false,
            false,
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            phys_expr::col("r_str", &schema()).expect("test column exists"),
        ));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["aa", "bb"])),
            Arc::new(Int32Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["bb", "aa"])),
            Arc::new(Int32Array::from(vec![0, 0])),
        ];
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        let result = apply_mask(&filter, &cols, 2, 2);

        assert_eq!(result, vec![false, true, true, false]);
    }

    fn lschema() -> SchemaRef {
        Arc::new(Schema::new(schema().fields()[..2].to_vec()))
    }

    fn rschema() -> SchemaRef {
        Arc::new(Schema::new(schema().fields()[2..].to_vec()))
    }

    fn lbatches() -> Vec<RecordBatch> {
        vec![RecordBatch::try_new(lschema(), cols()[..2].to_vec()).expect("valid test batch")]
    }

    fn rbatches() -> Vec<RecordBatch> {
        vec![RecordBatch::try_new(rschema(), cols()[2..].to_vec()).expect("valid test batch")]
    }

    fn lbatches_split() -> Vec<RecordBatch> {
        let cols = cols();
        vec![
            RecordBatch::try_new(lschema(), cols[..2].iter().map(|c| c.slice(0, 2)).collect())
                .expect("valid test batch"),
            RecordBatch::try_new(lschema(), cols[..2].iter().map(|c| c.slice(2, 1)).collect())
                .expect("valid test batch"),
        ]
    }

    /// Batched runs must match separate evaluations after adjusting segment
    /// offsets.
    #[test]
    fn test_apply_runs_answer_independently() {
        for expr in [
            Arc::new(BinaryExpr::new(
                phys_expr::col("l_int", &schema()).expect("test column exists"),
                Operator::Lt,
                phys_expr::col("r_int", &schema()).expect("test column exists"),
            )) as PhysicalExprRef,
            Arc::new(BinaryExpr::new(
                phys_expr::col("r_int", &schema()).expect("test column exists"),
                Operator::Gt,
                phys_expr::col("l_int", &schema()).expect("test column exists"),
            )),
            Arc::new(BinaryExpr::new(
                phys_expr::col("l_str", &schema()).expect("test column exists"),
                Operator::Eq,
                phys_expr::col("r_str", &schema()).expect("test column exists"),
            )),
            Arc::new(BinaryExpr::new(
                phys_expr::col("l_int", &schema()).expect("test column exists"),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            )),
            Arc::new(BinaryExpr::new(
                phys_expr::col("r_int", &schema()).expect("test column exists"),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(9)))),
            )),
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            )),
        ] {
            let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
                .expect("valid test join filter");
            let cols = cols();
            let runs = [
                Run {
                    lstart: 0,
                    lnum: 1,
                    rstart: 0,
                    rnum: 2,
                },
                Run {
                    lstart: 1,
                    lnum: 2,
                    rstart: 1,
                    rnum: 1,
                },
                Run {
                    lstart: 2,
                    lnum: 1,
                    rstart: 0,
                    rnum: 1,
                },
            ];

            let mut pairs = Pairs::default();
            let mut expected = vec![];
            let (mut loff, mut roff) = (0, 0);
            for run in &runs {
                filter
                    .apply(&cols, &[*run], &mut pairs)
                    .expect("test filter evaluation succeeds");
                expected.extend(
                    pairs
                        .lpos
                        .iter()
                        .zip(&pairs.rpos)
                        .map(|(&l, &r)| (loff + l, roff + r)),
                );
                loff += run.lnum;
                roff += run.rnum;
            }

            filter
                .apply(&cols, &runs, &mut pairs)
                .expect("test filter evaluation succeeds");
            let got = pairs
                .lpos
                .iter()
                .copied()
                .zip(pairs.rpos.iter().copied())
                .collect::<Vec<_>>();
            assert_eq!(got, expected);

            // Run boundaries must partition survivors without mixing group positions.
            assert_eq!(pairs.run_ends.len(), runs.len());
            let (mut seg_start, mut seg_loff) = (0usize, 0u32);
            for (run, &end) in runs.iter().zip(&pairs.run_ends) {
                let end = end as usize;
                assert!(
                    pairs.lpos[seg_start..end]
                        .iter()
                        .all(|&l| l >= seg_loff && l < seg_loff + run.lnum)
                );
                seg_start = end;
                seg_loff += run.lnum;
            }
            assert_eq!(seg_start, pairs.lpos.len());
        }
    }

    fn left_cases() -> [(Vec<RecordBatch>, Vec<IdxRange>); 2] {
        [
            (
                lbatches(),
                vec![IdxRange {
                    batch_idx: 0,
                    start: 0,
                    end: 3,
                }],
            ),
            (
                lbatches_split(),
                vec![
                    IdxRange {
                        batch_idx: 0,
                        start: 0,
                        end: 2,
                    },
                    IdxRange {
                        batch_idx: 1,
                        start: 0,
                        end: 1,
                    },
                ],
            ),
        ]
    }

    fn product_naive(expr: PhysicalExprRef) -> Vec<(usize, usize)> {
        apply(expr)
            .into_iter()
            .enumerate()
            .filter(|&(_, kept)| kept)
            .map(|(pos, _)| (pos / 2, pos % 2))
            .collect()
    }

    /// Evaluate product blocks and return group-relative pairs.
    /// Sort the result because block order can differ from whole-product order.
    fn evaluate_group(
        filter: &JoinFilter,
        lbatches: &[RecordBatch],
        lranges: &[IdxRange],
        rranges: &[IdxRange],
        max_pairs: usize,
    ) -> Vec<(usize, usize)> {
        let blocks = product_blocks(lranges, rranges, max_pairs);

        let mut pending = RunBatch::default();
        let mut matched = vec![];
        let mut collect = |pending: &mut RunBatch| {
            pending
                .evaluate(filter, lbatches, &rbatches(), |run| {
                    for (&l, &r) in run.lpos.iter().zip(run.rpos) {
                        matched.push((
                            l.wrapping_add(run.lbase) as usize,
                            r.wrapping_add(run.rbase) as usize,
                        ));
                    }
                })
                .expect("test failed")
        };

        for block in blocks {
            if !pending.accepts(block.lrange.batch_idx, block.rrange.batch_idx) {
                collect(&mut pending);
            }
            pending.push(&block, 0, 0);
            if pending.num_pairs() >= max_pairs {
                collect(&mut pending);
            }
        }
        collect(&mut pending);
        matched.sort_unstable();
        matched
    }

    fn check_group(expr: PhysicalExprRef) {
        let expected = product_naive(expr.clone());
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        let rranges = [IdxRange {
            batch_idx: 0,
            start: 0,
            end: 2,
        }];
        for (lbatches, lranges) in left_cases() {
            for max_pairs in [1, 2, 3, 5, 6, 100] {
                let got = evaluate_group(&filter, &lbatches, &lranges, &rranges, max_pairs);
                assert_eq!(got, expected, "max_pairs = {max_pairs}");
            }
        }
    }

    #[test]
    fn test_group_matches_whole_product() {
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_int", &schema()).expect("test column exists"),
        )));
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("r_int", &schema()).expect("test column exists"),
            Operator::GtEq,
            phys_expr::col("l_int", &schema()).expect("test column exists"),
        )));
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("l_str", &schema()).expect("test column exists"),
            Operator::Eq,
            phys_expr::col("r_str", &schema()).expect("test column exists"),
        )));
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        )));
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1000)))),
        )));
        check_group(Arc::new(BinaryExpr::new(
            phys_expr::col("r_int", &schema()).expect("test column exists"),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1000)))),
        )));
        check_group(Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        )));
        check_group(Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        )));
    }

    #[test]
    fn test_group_empty_side() {
        let expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
            phys_expr::col("l_int", &schema()).expect("test column exists"),
            Operator::Lt,
            phys_expr::col("r_int", &schema()).expect("test column exists"),
        ));
        let filter = JoinFilter::try_new(expr, schema(), 2, Time::default())
            .expect("valid test join filter");
        let one = [IdxRange {
            batch_idx: 0,
            start: 0,
            end: 1,
        }];

        for (lranges, rranges) in [(&[][..], &one[..]), (&one[..], &[][..]), (&[][..], &[][..])] {
            assert!(evaluate_group(&filter, &lbatches(), lranges, rranges, 4).is_empty());
        }
    }

    #[test]
    fn test_product_blocks_cover_the_whole_product() {
        let lranges = [
            IdxRange {
                batch_idx: 0,
                start: 3,
                end: 7,
            },
            IdxRange {
                batch_idx: 1,
                start: 0,
                end: 2,
            },
        ];
        let rranges = [
            IdxRange {
                batch_idx: 0,
                start: 1,
                end: 4,
            },
            IdxRange {
                batch_idx: 2,
                start: 5,
                end: 6,
            },
        ];

        for max_pairs in [1, 2, 3, 4, 7, 12, 1000] {
            let blocks = product_blocks(&lranges, &rranges, max_pairs);

            let mut pairs = vec![];
            for block in blocks {
                assert!(
                    block.lrange.len() * block.rrange.len() <= max_pairs.max(1),
                    "block too big for max_pairs = {max_pairs}"
                );
                for l in 0..block.lrange.len() {
                    for r in 0..block.rrange.len() {
                        pairs.push((
                            (block.loffset + l, block.roffset + r),
                            (
                                (block.lrange.batch_idx, block.lrange.start + l),
                                (block.rrange.batch_idx, block.rrange.start + r),
                            ),
                        ));
                    }
                }
            }
            pairs.sort_unstable();

            let expected = ranges_iter(&lranges)
                .enumerate()
                .flat_map(|(l, lidx)| {
                    ranges_iter(&rranges)
                        .enumerate()
                        .map(move |(r, ridx)| ((l, r), (lidx, ridx)))
                })
                .collect::<Vec<_>>();
            assert_eq!(pairs, expected, "max_pairs = {max_pairs}");
        }
    }
}
