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

use std::{
    any::Any,
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    compute::{filter, filter_record_batch, prep_null_mask_filter},
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::as_boolean_array,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr, PhysicalExprRef,
        expressions::{BinaryExpr, CaseExpr, Column, Literal, NoOp, SCAndExpr, SCOrExpr},
    },
    physical_expr_common::{physical_expr::DynEq, utils::scatter},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::{
    arrow::cast::cast, downcast_any, downcast_any_dyn, uda::UserDefinedArray,
};
use itertools::Itertools;
use parking_lot::Mutex;

use super::lazy::{CACHE, ColumnBatch, LazyBatch, LazyBatchRef, column, compose_selection};

pub struct CachedExprsEvaluator {
    transformed_projection_exprs: Vec<(PhysicalExprRef, Vec<usize>)>,
    transformed_pruned_filter_exprs: Vec<(PhysicalExprRef, Vec<usize>)>,
    output_schema: SchemaRef,
    cache: Cache,
}

impl CachedExprsEvaluator {
    pub fn try_new(
        filter_exprs: Vec<PhysicalExprRef>,
        projection_exprs: Vec<PhysicalExprRef>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let (transformed_exprs, cache) =
            transform_to_cached_exprs(&[filter_exprs.clone(), projection_exprs.clone()].concat())?;
        let (transformed_filter_exprs, transformed_projection_exprs) =
            transformed_exprs.split_at(filter_exprs.len());

        let transformed_pruned_filter_exprs = transformed_filter_exprs
            .into_iter()
            .map(|expr| prune_expr_cols(expr))
            .collect();
        let transformed_projection_exprs = transformed_projection_exprs
            .iter()
            .map(prune_expr_cols)
            .collect();

        Ok(Self {
            transformed_projection_exprs,
            transformed_pruned_filter_exprs,
            output_schema,
            cache,
        })
    }

    pub fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let projections = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| Arc::new(Column::new(field.name(), index)) as PhysicalExprRef)
            .collect::<Vec<_>>();
        CACHE.with_scope(|| {
            self.cache.with(|_| {
                self.evaluate_impl(
                    batch,
                    &projections.iter().map(prune_expr_cols).collect::<Vec<_>>(),
                    batch.schema(),
                )?
                .materialize()
            })
        })
    }

    pub fn filter_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.evaluate_optimized(batch)?.materialize()
    }

    pub(crate) fn evaluate_optimized(&self, batch: &dyn LazyBatch) -> Result<LazyBatchRef> {
        CACHE.with_scope(|| {
            self.cache.with(|_| {
                self.evaluate_impl(
                    batch,
                    &self.transformed_projection_exprs,
                    self.output_schema.clone(),
                )
            })
        })
    }

    fn evaluate_impl(
        &self,
        batch: &dyn LazyBatch,
        projections: &[(PhysicalExprRef, Vec<usize>)],
        schema: SchemaRef,
    ) -> Result<LazyBatchRef> {
        if projections.len() != schema.fields().len() {
            return Err(datafusion::common::DataFusionError::Execution(
                "project expression count mismatch".to_owned(),
            ));
        }
        let mut state = BatchState::new(batch);
        for (expr, dependencies) in &self.transformed_pruned_filter_exprs {
            if state.rows() == 0 {
                break;
            }
            let input = state.input(dependencies)?;
            match filter_one_pred(&input, expr)? {
                FilterStat::AllRetained => {}
                FilterStat::AllFiltered => return Ok(Arc::new(RecordBatch::new_empty(schema))),
                FilterStat::Some(relative) => {
                    self.cache.update_all(|value| {
                        Ok(match value {
                            Some(ColumnarValue::Array(array)) => Some(ColumnarValue::Array(
                                if let Some(uda) = array.as_any().downcast_ref::<UserDefinedArray>()
                                {
                                    Arc::new(uda.filter(&relative)?)
                                } else {
                                    filter(array.as_ref(), &relative)?
                                },
                            )),
                            value => value,
                        })
                    })?;
                    state.advance(&relative)?;
                }
            }
        }
        if state.rows() == 0 {
            return Ok(Arc::new(RecordBatch::new_empty(schema)));
        }
        let direct_index =
            |expr: &PhysicalExprRef, dependencies: &[usize], field: &arrow::datatypes::FieldRef| {
                expr.downcast_ref::<Column>()
                    .map(|col| dependencies[col.index()])
                    .filter(|&index| batch.schema().field(index).data_type() == field.data_type())
            };
        let computed = projections
            .iter()
            .zip(schema.fields())
            .map(|((expr, dependencies), field)| {
                if direct_index(expr, dependencies, field).is_some() {
                    return Ok(None);
                }
                let input = state.input(dependencies)?;
                let mut array = expr.evaluate(&input)?.into_array(state.rows())?;
                if array.data_type() != field.data_type() {
                    array = cast(array.as_ref(), field.data_type())?;
                }
                Ok(Some(column(array)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut outputs = HashMap::new();
        let columns = projections
            .iter()
            .zip(schema.fields())
            .zip(computed)
            .map(|(((expr, dependencies), field), computed)| {
                if let Some(computed) = computed {
                    return Ok(computed);
                }
                let index = direct_index(expr, dependencies, field).expect("direct projection");
                if let Some(output) = outputs.get(&index) {
                    return Ok(Arc::clone(output));
                }
                let output = state.output(index)?;
                outputs.insert(index, output.clone());
                Ok(output)
            })
            .collect::<Result<Vec<_>>>()?;
        ColumnBatch::try_new(schema, columns, state.rows())
    }
}

struct Dependency {
    array: ArrayRef,
    selection: Option<BooleanArray>,
    stage: usize,
}

struct BatchState<'a> {
    batch: &'a dyn LazyBatch,
    selection: Option<BooleanArray>,
    stage: usize,
    dependencies: Vec<Option<Dependency>>,
    relative_masks: HashMap<usize, BooleanArray>,
}

impl<'a> BatchState<'a> {
    fn new(batch: &'a dyn LazyBatch) -> Self {
        Self {
            batch,
            selection: None,
            stage: 0,
            dependencies: (0..batch.schema().fields().len()).map(|_| None).collect(),
            relative_masks: HashMap::new(),
        }
    }

    fn rows(&self) -> usize {
        self.selection
            .as_ref()
            .map_or(self.batch.num_rows(), BooleanArray::true_count)
    }

    fn advance(&mut self, relative: &BooleanArray) -> Result<()> {
        self.selection = Some(match &self.selection {
            Some(selection) => compose_selection(selection, relative)?,
            None => relative.clone(),
        });
        self.stage += 1;
        self.relative_masks.clear();
        Ok(())
    }

    fn dependency(&mut self, index: usize) -> Result<ArrayRef> {
        if let Some(dependency) = &mut self.dependencies[index] {
            if dependency.stage != self.stage {
                let relative = if let Some(relative) = self.relative_masks.get(&dependency.stage) {
                    relative.clone()
                } else {
                    let selection = self.selection.as_ref().expect("selection advanced");
                    let relative = match &dependency.selection {
                        Some(prefix) => as_boolean_array(&filter(selection, prefix)?)?.clone(),
                        None => selection.clone(),
                    };
                    self.relative_masks
                        .insert(dependency.stage, relative.clone());
                    relative
                };
                dependency.array = filter(dependency.array.as_ref(), &relative)?;
                dependency.selection = self.selection.clone();
                dependency.stage = self.stage;
            }
            return Ok(dependency.array.clone());
        }
        let col = self.batch.get_column(index)?;
        let array = match &self.selection {
            Some(selection) => col.materialize_selection(selection)?,
            None => col.materialize()?,
        };
        self.dependencies[index] = Some(Dependency {
            array: array.clone(),
            selection: self.selection.clone(),
            stage: self.stage,
        });
        Ok(array)
    }

    fn input(&mut self, dependencies: &[usize]) -> Result<RecordBatch> {
        let columns = dependencies
            .iter()
            .map(|&index| self.dependency(index))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            Arc::new(self.batch.schema().project(dependencies)?),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(self.rows())),
        )?)
    }

    fn output(&mut self, index: usize) -> Result<super::lazy::LazyColumnRef> {
        if self.dependencies[index].is_some() {
            return Ok(column(self.dependency(index)?));
        }
        let col = self.batch.get_column(index)?;
        match &self.selection {
            Some(selection) => col.filter(selection),
            None => Ok(col),
        }
    }
}

fn transform_to_cached_exprs(exprs: &[PhysicalExprRef]) -> Result<(Vec<PhysicalExprRef>, Cache)> {
    // count all children exprs
    fn count(expr: &PhysicalExprRef, expr_counts: &mut HashMap<ExprKey, usize>) {
        expr_counts
            .entry(ExprKey(expr.clone()))
            .and_modify(|count| *count += 1)
            .or_insert(1);
        expr.children()
            .iter()
            .for_each(|child| count(&child, expr_counts));
    }
    let mut expr_counts = HashMap::new();
    for expr in exprs {
        count(expr, &mut expr_counts);
    }

    // find all duplicated exprs (which count is larger than its parent)
    fn collect_dups(
        expr: &PhysicalExprRef,
        parent_count: usize,
        expr_counts: &HashMap<ExprKey, usize>,
        dups: &mut HashSet<ExprKey>,
    ) {
        // ignore trivial leaf exprs
        if matches!(downcast_any_dyn!(expr, NoOp), Ok(_))
            || matches!(downcast_any_dyn!(expr, Column), Ok(_))
            || matches!(downcast_any_dyn!(expr, Literal), Ok(_))
        {
            return;
        }

        // insert exprs with occurrences more than its parent
        let expr_key = ExprKey(expr.clone());
        let current_count = expr_counts.get(&expr_key).cloned().unwrap_or(0);
        if current_count > parent_count {
            dups.insert(expr_key);
        }

        // traverse children, excluding exprs with short circuiting evaluation
        if matches!(downcast_any_dyn!(expr, CaseExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, SCAndExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, SCOrExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::And)
            || matches!(downcast_any_dyn!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::Or)
        {
            // short circuiting expression - only first child can be cached
            // first `when` expr can also be cached
            collect_dups(&expr.children()[0], current_count, expr_counts, dups);
            if let Ok(case_expr) = downcast_any_dyn!(expr, CaseExpr)
                && case_expr.expr().is_some()
            {
                let children = case_expr.children();
                if children.len() >= 2 {
                    // cache first `when` expr
                    collect_dups(&expr.children()[1], current_count, expr_counts, dups);
                }
            }
        } else {
            expr.children().iter().for_each(|child| {
                collect_dups(child, current_count, expr_counts, dups);
            });
        }
    }
    let mut dups = HashSet::new();
    for expr in exprs {
        collect_dups(expr, 1, &expr_counts, &mut dups);
    }

    // generate cached expr ids
    let cached_expr_ids: HashMap<ExprKey, usize> = dups
        .into_iter()
        .enumerate()
        .map(|(id, expr)| (expr, id))
        .collect();

    // transform all exprs with CachedExpr using dup_exprs
    fn transform(
        expr: PhysicalExprRef,
        cached_expr_ids: &HashMap<ExprKey, usize>,
        cache: &Cache,
    ) -> Result<PhysicalExprRef> {
        // ignore trivial leaf exprs
        if matches!(downcast_any_dyn!(expr, NoOp), Ok(_))
            || matches!(downcast_any_dyn!(expr, Column), Ok(_))
            || matches!(downcast_any_dyn!(expr, Literal), Ok(_))
        {
            return Ok(expr);
        }

        // get cache id if exists of current expr
        let expr_key = ExprKey(expr.clone());
        let current_cache_id = cached_expr_ids.get(&expr_key).cloned();

        // transform children
        let transformed_expr = if matches!(downcast_any_dyn!(expr, CaseExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, SCAndExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, SCOrExpr), Ok(_))
            || matches!(downcast_any_dyn!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::And)
            || matches!(downcast_any_dyn!(expr, BinaryExpr), Ok(e) if e.op() == &Operator::Or)
        {
            // short circuiting expression - only first child can be cached
            // first `when` expr can also be cached
            let mut children = expr
                .children()
                .iter()
                .map(|&child| child.clone())
                .collect::<Vec<_>>();
            children[0] = transform(children[0].clone(), cached_expr_ids, cache)?;

            if let Some(case_expr) = expr.downcast_ref::<CaseExpr>()
                && children.len() >= 2
                && case_expr.expr().is_some()
            {
                // cache first `when` expr
                children[1] = transform(children[1].clone(), cached_expr_ids, cache)?;
            }
            expr.clone().with_new_children(children)?
        } else {
            expr.clone().with_new_children(
                expr.children()
                    .into_iter()
                    .map(|child| transform(child.clone(), cached_expr_ids, cache))
                    .collect::<Result<_>>()?,
            )?
        };

        Ok(if let Some(cache_id) = current_cache_id {
            Arc::new(CachedExpr {
                cache: cache.clone(),
                id: cache_id,
                orig_expr: transformed_expr,
            })
        } else {
            transformed_expr
        })
    }

    let cache = Cache::new(cached_expr_ids.len());
    let transformed_exprs = exprs
        .iter()
        .map(|expr| transform(expr.clone(), &cached_expr_ids, &cache))
        .collect::<Result<_>>()?;
    Ok((transformed_exprs, cache))
}

/// A physical expr wrapper to use in HashSet/HashMap
#[derive(Clone, Debug, Hash)]
struct ExprKey(PhysicalExprRef);

impl PartialEq for ExprKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().eq(other.0.as_ref())
    }
}

impl Eq for ExprKey {}

/// A physical expr wrapper which supports caching when evaluated more than once
#[derive(Clone)]
struct CachedExpr {
    cache: Cache,
    id: usize,
    orig_expr: PhysicalExprRef,
}

impl Display for CachedExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Debug for CachedExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cached(")?;
        std::fmt::Debug::fmt(&self.orig_expr, f)?;
        write!(f, ")")?;
        Ok(())
    }
}

impl PartialEq for CachedExpr {
    fn eq(&self, other: &Self) -> bool {
        other.id == self.id
    }
}

impl Eq for CachedExpr {}

impl Hash for CachedExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PhysicalExpr for CachedExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.orig_expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.orig_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.cache.get(self.id, || self.orig_expr.evaluate(batch))
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        if selection.true_count() == selection.len() {
            return self.evaluate(batch);
        }
        let _scope = self.cache.disable();
        if selection.true_count() == 0 {
            return Ok(ColumnarValue::Array(arrow::array::new_null_array(
                &self.data_type(batch.schema_ref())?,
                batch.num_rows(),
            )));
        }
        let input = filter_record_batch(batch, selection)?;
        match self.orig_expr.evaluate(&input)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                if let Some(uda) = array.as_any().downcast_ref::<UserDefinedArray>() {
                    Arc::new(uda.scatter(selection)?)
                } else {
                    scatter(selection, array.as_ref())?
                },
            )),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                Ok(ColumnarValue::Array(Arc::new(selection.clone())))
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => Ok(ColumnarValue::Array(
                Arc::new(BooleanArray::from(vec![None; batch.num_rows()])),
            )),
            scalar => Ok(scalar),
        }
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        self.orig_expr.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self {
            cache: self.cache.clone(),
            id: self.id,
            orig_expr: self.orig_expr.clone().with_new_children(children)?,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

/// A struct holding all evaluated values of cacheable expressions
#[derive(Clone)]
struct Cache {
    values: Arc<Mutex<Vec<Option<ColumnarValue>>>>,
    disabled: Arc<AtomicUsize>,
}

struct CacheReset<'a>(&'a Cache);

impl Drop for CacheReset<'_> {
    fn drop(&mut self) {
        self.0.reset();
    }
}

struct CacheDisabled<'a>(&'a Cache);

impl Drop for CacheDisabled<'_> {
    fn drop(&mut self) {
        self.0.disabled.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Cache {
    fn new(len: usize) -> Self {
        Self {
            values: Arc::new(Mutex::new(vec![None; len])),
            disabled: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with<T>(&self, func: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        self.reset();
        let _scope = CacheReset(self);
        func(self)
    }

    fn disable(&self) -> CacheDisabled<'_> {
        self.disabled.fetch_add(1, Ordering::Relaxed);
        CacheDisabled(self)
    }

    fn get(
        &self,
        id: usize,
        evaluate_on_vacant: impl Fn() -> Result<ColumnarValue>,
    ) -> Result<ColumnarValue> {
        if self.disabled.load(Ordering::Relaxed) != 0 {
            return evaluate_on_vacant();
        }
        if let Some(cached) = &self.values.lock()[id] {
            return Ok(cached.clone());
        }
        let cached = evaluate_on_vacant()?;
        self.values.lock()[id] = Some(cached.clone());
        Ok(cached)
    }

    fn update_all(
        &self,
        on_update: impl Fn(Option<ColumnarValue>) -> Result<Option<ColumnarValue>>,
    ) -> Result<()> {
        let current_values = self.values.lock().clone();
        let updated_values = current_values
            .into_iter()
            .map(|value| on_update(value))
            .collect::<Result<_>>()?;
        *self.values.lock() = updated_values;
        Ok(())
    }

    fn reset(&self) {
        self.values.lock().fill(None);
    }
}

/// A enum that represents filter result
pub enum FilterStat {
    AllRetained,
    AllFiltered,
    Some(BooleanArray),
}

/// Get pruned expr with minimal set of input columns
fn prune_expr_cols(expr: &PhysicalExprRef) -> (PhysicalExprRef, Vec<usize>) {
    let used_cols: Rc<RefCell<HashMap<usize, usize>>> = Rc::new(RefCell::default());

    let transformed = expr
        .clone()
        .transform(&|expr: PhysicalExprRef| {
            if let Some(col) = expr.downcast_ref::<Column>() {
                let used_cols = used_cols.clone();
                let mut used_cols_ref = used_cols.borrow_mut();
                let new_idx = used_cols_ref.len();

                let mapped_idx = *used_cols_ref.entry(col.index()).or_insert(new_idx);
                let mapped_col: PhysicalExprRef = Arc::new(Column::new(col.name(), mapped_idx));
                Ok(Transformed::yes(mapped_col))
            } else {
                Ok(Transformed::yes(expr))
            }
        })
        .expect("expr transform failed")
        .data;

    let mapped_cols: Vec<usize> = used_cols
        .take()
        .into_iter()
        .sorted_by_key(|(_orig_idx, mapped_idx)| *mapped_idx)
        .map(|(orig_idx, _mapped_idx)| orig_idx)
        .collect();
    (transformed, mapped_cols)
}

/// Execute one filter predicate expr on a record batch with existed FilterStat
fn filter_one_pred(batch: &RecordBatch, pruned_pred_expr: &PhysicalExprRef) -> Result<FilterStat> {
    let pred_ret = pruned_pred_expr.evaluate(batch)?;

    match pred_ret {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => Ok(FilterStat::AllRetained),
        ColumnarValue::Scalar(_) => Ok(FilterStat::AllFiltered),
        ColumnarValue::Array(new_selected) => {
            let mut new_selected = as_boolean_array(&new_selected)?.clone();
            if new_selected.null_count() > 0 {
                new_selected = prep_null_mask_filter(&new_selected);
            }
            if new_selected.true_count() == new_selected.len() {
                return Ok(FilterStat::AllRetained);
            }
            Ok(match new_selected.true_count() {
                0 => FilterStat::AllFiltered,
                count if count == new_selected.len() => FilterStat::AllRetained,
                _ => FilterStat::Some(new_selected),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::Int32Array, datatypes::Field};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn batch() -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3]))],
        )?)
    }

    fn increment() -> PhysicalExprRef {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ))
    }

    #[derive(Debug, Clone, Eq, Hash)]
    struct FallibleExpr {
        child: PhysicalExprRef,
        mode: usize,
    }

    impl PartialEq for FallibleExpr {
        fn eq(&self, other: &Self) -> bool {
            self.mode == other.mode && self.child.as_ref().eq(other.child.as_ref())
        }
    }

    impl Display for FallibleExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "fallible")
        }
    }

    impl PhysicalExpr for FallibleExpr {
        fn data_type(&self, _: &Schema) -> Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn nullable(&self, _: &Schema) -> Result<bool> {
            Ok(false)
        }
        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            self.child.evaluate(batch)?;
            let value = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("i32")
                .value(0);
            if value == 0 {
                if self.mode == 1 {
                    std::panic::resume_unwind(Box::new("expression panic"));
                }
                return Err(datafusion::common::DataFusionError::Execution(
                    "expression error".to_owned(),
                ));
            }
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        }
        fn children(&self) -> Vec<&PhysicalExprRef> {
            vec![&self.child]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<PhysicalExprRef>,
        ) -> Result<PhysicalExprRef> {
            Ok(Arc::new(Self {
                child: children[0].clone(),
                mode: self.mode,
            }))
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "fallible")
        }
    }

    #[test]
    fn evaluator_releases_results_after_expression_error_and_panic() -> Result<()> {
        let first = batch()?;
        let second = RecordBatch::try_new(
            first.schema(),
            vec![Arc::new(Int32Array::from(vec![10, 11, 12, 13]))],
        )?;
        for mode in [0, 1] {
            let predicate: PhysicalExprRef = Arc::new(BinaryExpr::new(
                increment(),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
            ));
            let evaluator = CachedExprsEvaluator::try_new(
                vec![
                    predicate,
                    Arc::new(FallibleExpr {
                        child: increment(),
                        mode,
                    }),
                ],
                vec![increment()],
                first.schema(),
            )?;
            let failed = catch_unwind(AssertUnwindSafe(|| evaluator.filter_project(&first)));
            if mode == 0 {
                assert!(failed.expect("error not panic").is_err());
            } else {
                assert!(failed.is_err());
            }
            assert!(evaluator.cache.values.lock().iter().all(Option::is_none));
            assert_eq!(
                evaluator.filter_project(&second)?.column(0).as_ref(),
                &Int32Array::from(vec![11, 12, 13, 14])
            );
        }
        Ok(())
    }

    #[test]
    fn selection_cache_isolates_subtrees_and_restores_after_unwind() -> Result<()> {
        let batch = batch()?;
        let cache = Cache::new(2);
        let inner: PhysicalExprRef = Arc::new(CachedExpr {
            cache: cache.clone(),
            id: 0,
            orig_expr: increment(),
        });
        let outer = CachedExpr {
            cache: cache.clone(),
            id: 1,
            orig_expr: Arc::new(BinaryExpr::new(
                inner,
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )),
        };
        cache.with(|_| {
            for warm in [false, true] {
                cache.reset();
                if warm {
                    outer.evaluate(&batch)?;
                }
                for mask in [
                    vec![true, false, true, false],
                    vec![false; 4],
                    vec![true; 4],
                ] {
                    let mask = BooleanArray::from(mask);
                    let selected = outer.evaluate_selection(&batch, &mask)?.into_array(4)?;
                    let expected = if mask.true_count() == 4 {
                        Int32Array::from(vec![Some(2), Some(3), Some(4), Some(5)])
                    } else {
                        Int32Array::from_iter(
                            mask.iter()
                                .enumerate()
                                .map(|(i, keep)| (keep == Some(true)).then_some(i as i32 + 2)),
                        )
                    };
                    assert_eq!(selected.as_ref(), &expected);
                    assert_eq!(
                        outer.evaluate(&batch)?.into_array(4)?.as_ref(),
                        &Int32Array::from(vec![2, 3, 4, 5])
                    );
                }
            }
            Ok(())
        })?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _: Result<()> = cache.with(|_| {
                    outer.evaluate(&batch)?;
                    let _disabled = cache.disable();
                    std::panic::resume_unwind(Box::new("scope unwind"));
                });
            }))
            .is_err()
        );
        assert_eq!(cache.disabled.load(Ordering::Relaxed), 0);
        assert!(cache.values.lock().iter().all(Option::is_none));
        let error: Result<()> = cache.with(|_| {
            outer.evaluate(&batch)?;
            let _disabled = cache.disable();
            Err(datafusion::common::DataFusionError::Execution(
                "scope error".to_owned(),
            ))
        });
        assert!(error.is_err());
        assert_eq!(cache.disabled.load(Ordering::Relaxed), 0);
        assert!(cache.values.lock().iter().all(Option::is_none));
        Ok(())
    }

    #[test]
    fn computed_cse_wrappers_and_lazy_domains() -> Result<()> {
        let batch = batch()?;
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ],
            HashMap::from([("test".to_owned(), "metadata".to_owned())]),
        ));
        for filtered in [false, true] {
            let filters = if filtered {
                vec![Arc::new(BinaryExpr::new(
                    increment(),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
                )) as PhysicalExprRef]
            } else {
                vec![]
            };
            let evaluator = CachedExprsEvaluator::try_new(
                filters,
                vec![increment(), increment()],
                schema.clone(),
            )?;
            let plain: LazyBatchRef = Arc::new(batch.clone());
            let indexed = plain.take(None, &arrow::array::Int32Array::from(vec![3, 0, 2, 1]))?;
            let composite = super::super::lazy::concat_batches(
                batch.schema(),
                &[plain.clone(), indexed.clone()],
            )?;
            let selected = plain.filter(&BooleanArray::from(vec![true, false, true, true]))?;
            for input in [plain, indexed, composite, selected] {
                for _ in 0..2 {
                    let output = evaluator.evaluate_optimized(input.as_ref())?;
                    let a = output.get_column(0)?.materialize()?;
                    let b = output.get_column(1)?.materialize()?;
                    assert!(Arc::ptr_eq(&a, &b));
                    assert_eq!(output.schema(), schema);
                    assert_eq!(
                        output.materialize()?,
                        evaluator.filter_project(&input.materialize()?)?
                    );
                    assert_eq!(
                        evaluator.filter(&input.materialize()?)?.schema(),
                        batch.schema()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn false_filters_do_not_evaluate_fallible_projections_or_predicates() -> Result<()> {
        let batch = batch()?;
        let division: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            Operator::Divide,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let later: PhysicalExprRef = Arc::new(BinaryExpr::new(
            division.clone(),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let evaluator = CachedExprsEvaluator::try_new(
            vec![
                Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
                later,
            ],
            vec![division],
            batch.schema(),
        )?;
        assert_eq!(evaluator.filter_project(&batch)?.num_rows(), 0);
        assert_eq!(evaluator.filter(&batch)?.num_rows(), 0);
        Ok(())
    }

    #[test]
    fn short_circuit_projections_match_uncached_selection() -> Result<()> {
        let batch = batch()?;
        let positive: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let zero: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let division: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(12)))),
                Operator::Divide,
                Arc::new(Column::new("v", 0)),
            )),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]));
        for expr in [
            Arc::new(SCAndExpr::new(positive, division.clone())) as PhysicalExprRef,
            Arc::new(SCOrExpr::new(zero, division)) as PhysicalExprRef,
        ] {
            let expected = expr.evaluate(&batch)?.into_array(4)?;
            let evaluator =
                CachedExprsEvaluator::try_new(vec![], vec![expr.clone(), expr], schema.clone())?;
            let result = evaluator.filter_project(&batch)?;
            assert_eq!(result.column(0).as_ref(), expected.as_ref());
            assert!(Arc::ptr_eq(result.column(0), result.column(1)));
        }
        Ok(())
    }

    #[test]
    fn case_subset_does_not_reuse_full_domain_cache() -> Result<()> {
        let batch = batch()?;
        let condition: PhysicalExprRef = Arc::new(BinaryExpr::new(
            increment(),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let case: PhysicalExprRef = Arc::new(CaseExpr::try_new(
            None,
            vec![(condition.clone(), increment())],
            Some(Arc::new(Literal::new(ScalarValue::Int32(Some(9))))),
        )?);
        let schema = Arc::new(Schema::new(vec![
            Field::new("case", DataType::Int32, false),
            Field::new("inc", DataType::Int32, false),
        ]));
        let evaluator = CachedExprsEvaluator::try_new(vec![], vec![case, increment()], schema)?;
        assert_eq!(
            evaluator.filter_project(&batch)?.column(0).as_ref(),
            &Int32Array::from(vec![9, 9, 3, 4])
        );
        Ok(())
    }
}
