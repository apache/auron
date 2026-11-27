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
    cell::RefCell,
    collections::HashMap,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBufferBuilder, Int32Array, RecordBatch,
        RecordBatchOptions, new_empty_array,
    },
    compute::take,
    datatypes::{DataType, SchemaRef},
};
use datafusion::{
    common::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    physical_expr::PhysicalExprRef,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use datafusion_ext_commons::arrow::{array_size::ArraySize, selection::create_array_interleaver};
use futures::{Stream, StreamExt};

use super::column_pruning::prune_columns;

pub type LazyColumnRef = Arc<dyn LazyColumn>;
pub type LazyBatchRef = Arc<dyn LazyBatch>;
pub type SendableLazyBatchStream = Pin<Box<dyn LazyBatchStream + Send>>;

pub trait LazyColumn: Send + Sync {
    fn is_materialized(&self) -> bool {
        false
    }
    /// Returns true if this column already memoizes [`LazyColumn::materialize`]
    /// (see [`OnceColumn`]), so wrapping it again is unnecessary.
    fn is_once(&self) -> bool {
        false
    }
    fn data_type(&self) -> &DataType;
    fn len(&self) -> usize;
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef>;
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef>;
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef>;
    fn concat(&self, others: &[LazyColumnRef]) -> Result<ArrayRef> {
        CACHE.with_scope(|| {
            let first = self.materialize()?;
            if others.is_empty() {
                return Ok(first);
            }
            let arrays = std::iter::once(Ok(first))
                .chain(others.iter().map(|col| col.materialize()))
                .collect::<Result<Vec<_>>>()?;
            Ok(arrow::compute::concat(
                &arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?)
        })
    }
    fn materialize(&self) -> Result<ArrayRef>;
    fn materialize_selection(&self, selection: &BooleanArray) -> Result<ArrayRef> {
        self.filter(selection)?.materialize()
    }
    fn has_nulls(&self) -> bool;
    fn get_estimated_materialized_mem_size(&self) -> usize;
}

impl LazyColumn for ArrayRef {
    fn is_materialized(&self) -> bool {
        true
    }
    fn data_type(&self) -> &DataType {
        Array::data_type(self.as_ref())
    }
    fn len(&self) -> usize {
        Array::len(self.as_ref())
    }
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef> {
        Ok(take(self.as_ref(), indices, None)?)
    }
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef> {
        let cache = CACHE.current();
        let len = Array::len(self.as_ref());
        if cache.is_identity(indices, len) {
            return Ok(column(self.clone()));
        }
        Ok(Arc::new(IndexedColumn {
            source: self.clone(),
            indices: indices.clone(),
        }))
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef> {
        filtered_column(self.clone(), predicate.clone())
    }
    fn materialize(&self) -> Result<ArrayRef> {
        Ok(self.clone())
    }
    fn materialize_selection(&self, selection: &BooleanArray) -> Result<ArrayRef> {
        if selection.true_count() == selection.len() {
            return Ok(self.clone());
        }
        Ok(arrow::compute::filter(self.as_ref(), selection)?)
    }
    fn has_nulls(&self) -> bool {
        self.logical_nulls()
            .is_some_and(|nulls| nulls.null_count() != 0)
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        ArraySize::get_array_mem_size(self.as_ref())
    }
}

#[derive(Clone)]
struct FilteredColumn {
    source: ArrayRef,
    selection: BooleanArray,
}

fn filtered_column(source: ArrayRef, selection: BooleanArray) -> Result<LazyColumnRef> {
    if !selection.is_empty() && selection.true_count() == selection.len() {
        return Ok(column(source));
    }
    Ok(Arc::new(FilteredColumn { source, selection }))
}

impl LazyColumn for FilteredColumn {
    fn data_type(&self) -> &DataType {
        self.source.as_ref().data_type()
    }
    fn len(&self) -> usize {
        self.selection.true_count()
    }
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef> {
        self.take_lazy(indices)?.materialize()
    }
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef> {
        let cache = CACHE.current();
        if cache.is_identity(indices, self.len()) {
            return Ok(Arc::new(self.clone()));
        }
        let original = cache.mask_indices(&self.selection)?;
        indexed_column(self.source.clone(), cache.select(&original, indices)?)
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef> {
        filtered_column(
            self.source.clone(),
            CACHE.current().compose_masks(&self.selection, predicate)?,
        )
    }
    fn materialize(&self) -> Result<ArrayRef> {
        Ok(arrow::compute::filter(
            self.source.as_ref(),
            &self.selection,
        )?)
    }
    fn materialize_selection(&self, selection: &BooleanArray) -> Result<ArrayRef> {
        let combined = CACHE.current().compose_masks(&self.selection, selection)?;
        self.source.materialize_selection(&combined)
    }
    fn has_nulls(&self) -> bool {
        self.source.logical_nulls().is_some_and(|nulls| {
            self.selection
                .iter()
                .enumerate()
                .any(|(row, keep)| keep == Some(true) && nulls.is_null(row))
        })
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        estimated_selected_mem_size(&self.source, self.len())
    }
}

#[derive(Clone)]
struct IndexedColumn {
    source: ArrayRef,
    indices: Int32Array,
}

fn indexed_column(source: ArrayRef, indices: Int32Array) -> Result<LazyColumnRef> {
    Ok(Arc::new(IndexedColumn { source, indices }))
}

impl LazyColumn for IndexedColumn {
    fn data_type(&self) -> &DataType {
        self.source.as_ref().data_type()
    }
    fn len(&self) -> usize {
        self.indices.len()
    }
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef> {
        let cache = CACHE.current();
        let indices = cache.select(&self.indices, indices)?;
        Ok(take(self.source.as_ref(), &indices, None)?)
    }
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef> {
        let cache = CACHE.current();
        if cache.is_identity(indices, self.indices.len()) {
            return Ok(Arc::new(self.clone()));
        }
        Ok(Arc::new(IndexedColumn {
            source: self.source.clone(),
            indices: cache.select(&self.indices, indices)?,
        }))
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef> {
        self.take_lazy(&filter_indices(predicate))
    }
    fn materialize_selection(&self, selection: &BooleanArray) -> Result<ArrayRef> {
        self.take(&filter_indices(selection))
    }
    fn materialize(&self) -> Result<ArrayRef> {
        Ok(take(self.source.as_ref(), &self.indices, None)?)
    }
    fn has_nulls(&self) -> bool {
        self.indices.null_count() != 0
            || self.source.logical_nulls().is_some_and(|nulls| {
                nulls.null_count() != 0
                    && self
                        .indices
                        .values()
                        .iter()
                        .any(|&index| nulls.is_null(index as usize))
            })
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        estimated_selected_mem_size(&self.source, self.indices.len())
    }
}

#[derive(Clone)]
struct ConcatColumn {
    parts: Vec<LazyColumnRef>,
    row_offsets: Arc<Vec<usize>>,
}

impl LazyColumn for ConcatColumn {
    fn data_type(&self) -> &DataType {
        self.parts[0].data_type()
    }
    fn len(&self) -> usize {
        *self.row_offsets.last().expect("concat row offsets")
    }
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef> {
        CACHE.with_scope(|| {
            let Some(min_row) = arrow::compute::min(indices) else {
                return Ok(arrow::array::new_null_array(
                    self.data_type(),
                    indices.len(),
                ));
            };
            let max_row = arrow::compute::max(indices).expect("nonempty indices");
            if min_row < 0 || max_row as usize >= self.len() {
                return Err(datafusion::common::DataFusionError::Execution(
                    "concat column index out of bounds".to_owned(),
                ));
            }
            let resolve =
                |row: usize| self.row_offsets.partition_point(|&offset| offset <= row) - 1;
            let first = resolve(min_row as usize);
            let last = resolve(max_row as usize);
            if first == last {
                let base = self.row_offsets[first] as i32;
                let local = indices.unary(|row| row - base);
                return self.parts[first].take(&local);
            }
            let mut arrays = self.parts[first..=last]
                .iter()
                .map(|part| part.materialize())
                .collect::<Result<Vec<_>>>()?;
            let null_part = arrays.len();
            if indices.null_count() != 0 {
                arrays.push(arrow::array::new_null_array(self.data_type(), 1));
            }
            let pairs = indices
                .iter()
                .map(|index| match index {
                    Some(row) => {
                        let row = row as usize;
                        let part = resolve(row);
                        (part - first, row - self.row_offsets[part])
                    }
                    None => (null_part, 0),
                })
                .collect::<Vec<_>>();
            create_array_interleaver(&arrays, false)?(&pairs)
        })
    }
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef> {
        if CACHE.current().is_identity(indices, self.len()) {
            return Ok(Arc::new(self.clone()));
        }
        Ok(column(self.take(indices)?))
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef> {
        self.take_lazy(&filter_indices(predicate))
    }
    fn materialize(&self) -> Result<ArrayRef> {
        self.parts[0].concat(&self.parts[1..])
    }
    fn has_nulls(&self) -> bool {
        self.parts.iter().any(|part| part.has_nulls())
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        self.parts.iter().fold(0usize, |bytes, part| {
            bytes.saturating_add(part.get_estimated_materialized_mem_size())
        })
    }
}

enum OnceColumnState {
    Lazy(LazyColumnRef),
    Materialized(ArrayRef),
}

/// A lazy column wrapper that memoizes [`LazyColumn::materialize`]: the first
/// materialization computes the array and REPLACES the lazy source, so later
/// calls return the cached array and the source (e.g. a filter mask over a
/// large upstream batch) is released. This makes column materialization
/// happen at most once even when several passes touch the same batch, e.g.
/// `evaluate_columns` followed by `LazyBatch::materialize`.
pub struct OnceColumn {
    state: Mutex<OnceColumnState>,
    data_type: DataType,
    len: usize,
}

impl OnceColumn {
    pub fn new(column: LazyColumnRef) -> Self {
        let data_type = column.data_type().clone();
        let len = column.len();
        Self {
            state: Mutex::new(OnceColumnState::Lazy(column)),
            data_type,
            len,
        }
    }

    fn with_current<'a>(&self, state: &'a OnceColumnState) -> &'a dyn LazyColumn {
        match state {
            OnceColumnState::Materialized(array) => array,
            OnceColumnState::Lazy(column) => column.as_ref(),
        }
    }
}

impl LazyColumn for OnceColumn {
    fn is_materialized(&self) -> bool {
        match &*self.state.lock().expect("once column lock poisoned") {
            OnceColumnState::Materialized(_) => true,
            OnceColumnState::Lazy(column) => column.is_materialized(),
        }
    }
    fn is_once(&self) -> bool {
        true
    }
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
    fn len(&self) -> usize {
        self.len
    }
    fn take(&self, indices: &Int32Array) -> Result<ArrayRef> {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state).take(indices)
    }
    fn take_lazy(&self, indices: &Int32Array) -> Result<LazyColumnRef> {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state).take_lazy(indices)
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyColumnRef> {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state).filter(predicate)
    }
    fn materialize_selection(&self, selection: &BooleanArray) -> Result<ArrayRef> {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state).materialize_selection(selection)
    }
    fn materialize(&self) -> Result<ArrayRef> {
        // compute outside the lock so other readers can still proceed
        let inner = {
            let state = self.state.lock().expect("once column lock poisoned");
            match &*state {
                OnceColumnState::Materialized(array) => return Ok(array.clone()),
                OnceColumnState::Lazy(column) => column.clone(),
            }
        };
        let array = inner.materialize()?;
        let mut state = self.state.lock().expect("once column lock poisoned");
        match &*state {
            OnceColumnState::Materialized(existing) => Ok(existing.clone()),
            OnceColumnState::Lazy(_) => {
                *state = OnceColumnState::Materialized(array.clone());
                Ok(array)
            }
        }
    }
    fn has_nulls(&self) -> bool {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state).has_nulls()
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        let state = self.state.lock().expect("once column lock poisoned");
        self.with_current(&state)
            .get_estimated_materialized_mem_size()
    }
}

fn estimated_selected_mem_size(source: &ArrayRef, rows: usize) -> usize {
    let source_rows = source.as_ref().len();
    if source_rows == 0 || rows == 0 {
        return 0;
    }
    let bytes = ArraySize::get_array_mem_size(source.as_ref()) as u128;
    (bytes * rows as u128 / source_rows as u128) as usize
}

thread_local! {
    static SCOPED_CACHE: RefCell<Option<Rc<ScopedCache>>> = const { RefCell::new(None) };
}

pub(crate) static CACHE: Cache = Cache;

pub(crate) struct Cache;

struct CacheScope;

impl Drop for CacheScope {
    fn drop(&mut self) {
        let cache = SCOPED_CACHE.with(|current| current.borrow_mut().take());
        drop(cache);
    }
}

impl Cache {
    pub(crate) fn with_scope<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let outermost = SCOPED_CACHE.with(|current| {
            let mut current = current.borrow_mut();
            if current.is_some() {
                false
            } else {
                *current = Some(Rc::new(ScopedCache::default()));
                true
            }
        });
        let _scope = outermost.then(|| CacheScope);
        operation()
    }

    fn current(&self) -> Rc<ScopedCache> {
        SCOPED_CACHE
            .with(|current| current.borrow().clone())
            .unwrap_or_else(|| Rc::new(ScopedCache::default()))
    }
}

type MaskIdentity = (
    usize,
    usize,
    usize,
    usize,
    Option<(usize, usize, usize, usize)>,
);
type CombinedMasks =
    HashMap<(MaskIdentity, MaskIdentity), (BooleanArray, BooleanArray, BooleanArray)>;

fn mask_identity(mask: &BooleanArray) -> MaskIdentity {
    let values = mask.values();
    let buffer = values.inner();
    (
        buffer.data_ptr().as_ptr() as usize,
        buffer.ptr_offset(),
        values.offset(),
        mask.len(),
        mask.nulls().map(|nulls| {
            let buffer = nulls.buffer();
            (
                buffer.data_ptr().as_ptr() as usize,
                buffer.ptr_offset(),
                nulls.inner().offset(),
                nulls.len(),
            )
        }),
    )
}

pub(crate) fn compose_selection(
    source: &BooleanArray,
    selection: &BooleanArray,
) -> Result<BooleanArray> {
    CACHE.current().compose_masks(source, selection)
}

#[derive(Default)]
struct ScopedCache {
    masks: RefCell<CombinedMasks>,
    mask_indices: RefCell<HashMap<MaskIdentity, (BooleanArray, Int32Array)>>,
    identities: RefCell<HashMap<(IndicesIdentity, usize), (Int32Array, bool)>>,
    indices: RefCell<HashMap<(IndicesIdentity, IndicesIdentity), Int32Array>>,
    retained_indices: RefCell<Vec<Int32Array>>,
}

impl ScopedCache {
    fn mask_indices(&self, selection: &BooleanArray) -> Result<Int32Array> {
        let identity = mask_identity(selection);
        if let Some((_, indices)) = self.mask_indices.borrow().get(&identity) {
            return Ok(indices.clone());
        }
        let indices = filter_indices(selection);
        self.mask_indices
            .borrow_mut()
            .insert(identity, (selection.clone(), indices.clone()));
        Ok(indices)
    }

    fn compose_masks(
        &self,
        source: &BooleanArray,
        selection: &BooleanArray,
    ) -> Result<BooleanArray> {
        if selection.true_count() == selection.len() {
            return Ok(source.clone());
        }
        let identity = (mask_identity(source), mask_identity(selection));
        if let Some((_, _, combined)) = self.masks.borrow().get(&identity) {
            return Ok(combined.clone());
        }
        let mut builder = BooleanBufferBuilder::new(source.len());
        builder.append_n(source.len(), false);
        let mut selected = selection.iter();
        for (row, keep) in source.iter().enumerate() {
            if keep == Some(true) && selected.next() == Some(Some(true)) {
                builder.set_bit(row, true);
            }
        }
        let combined = BooleanArray::new(builder.finish(), None);
        self.masks.borrow_mut().insert(
            identity,
            (source.clone(), selection.clone(), combined.clone()),
        );
        Ok(combined)
    }

    fn is_identity(&self, indices: &Int32Array, len: usize) -> bool {
        let index = (indices_identity(indices), len);
        if let Some((_, identity)) = self.identities.borrow().get(&index) {
            return *identity;
        }
        let identity = indices.len() == len
            && indices.null_count() == 0
            && indices
                .values()
                .iter()
                .enumerate()
                .all(|(row, &index)| index >= 0 && index as usize == row);
        self.identities
            .borrow_mut()
            .insert(index, (indices.clone(), identity));
        identity
    }

    fn select(&self, source_indices: &Int32Array, indices: &Int32Array) -> Result<Int32Array> {
        if self.is_identity(indices, source_indices.len()) {
            return Ok(source_indices.clone());
        }
        let identity = (indices_identity(source_indices), indices_identity(indices));
        if let Some(selected) = self.indices.borrow().get(&identity) {
            return Ok(selected.clone());
        }
        let selected = take(source_indices, indices, None)?;
        let selected = selected
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("taking Int32Array indices returns an Int32Array")
            .clone();
        self.retained_indices
            .borrow_mut()
            .extend([source_indices.clone(), indices.clone()]);
        self.indices.borrow_mut().insert(identity, selected.clone());
        Ok(selected)
    }
}

pub fn column(array: ArrayRef) -> LazyColumnRef {
    Arc::new(array)
}

fn filter_indices(predicate: &BooleanArray) -> Int32Array {
    Int32Array::from(
        predicate
            .iter()
            .enumerate()
            .filter(|(_, value)| *value == Some(true))
            .map(|(index, _)| index as i32)
            .collect::<Vec<_>>(),
    )
}

pub fn take_columns(cols: &[LazyColumnRef], indices: &Int32Array) -> Result<Vec<ArrayRef>> {
    let take_all = || {
        let mut selected = HashMap::<usize, ArrayRef>::new();
        cols.iter()
            .map(|col| {
                let identity = Arc::as_ptr(col) as *const () as usize;
                if let Some(array) = selected.get(&identity) {
                    return Ok(array.clone());
                }
                let array = col.take(indices)?;
                selected.insert(identity, array.clone());
                Ok(array)
            })
            .collect()
    };
    if cols.iter().all(|col| col.is_materialized()) {
        // eager takes of materialized columns need no lazy-column caching
        take_all()
    } else {
        CACHE.with_scope(take_all)
    }
}

pub fn take_lazy_columns(
    cols: &[LazyColumnRef],
    indices: &Int32Array,
) -> Result<Vec<LazyColumnRef>> {
    CACHE.with_scope(|| take_lazy_columns_shared(cols, indices))
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct IndicesIdentity {
    values_pointer: usize,
    values_offset: usize,
    len: usize,
    nulls: Option<(usize, usize, usize, usize)>,
}

fn indices_identity(indices: &Int32Array) -> IndicesIdentity {
    let values = indices.values().inner();
    IndicesIdentity {
        values_pointer: values.data_ptr().as_ptr() as usize,
        values_offset: values.ptr_offset(),
        len: indices.len(),
        nulls: indices.nulls().map(|nulls| {
            let buffer = nulls.buffer();
            (
                buffer.data_ptr().as_ptr() as usize,
                buffer.ptr_offset(),
                nulls.inner().offset(),
                nulls.len(),
            )
        }),
    }
}

fn take_lazy_columns_shared(
    cols: &[LazyColumnRef],
    indices: &Int32Array,
) -> Result<Vec<LazyColumnRef>> {
    CACHE.with_scope(|| {
        let cache = CACHE.current();
        let mut selected = HashMap::<usize, LazyColumnRef>::new();
        cols.iter()
            .map(|col| {
                if cache.is_identity(indices, col.len()) {
                    return Ok(col.clone());
                }
                let identity = Arc::as_ptr(col) as *const () as usize;
                if let Some(col) = selected.get(&identity) {
                    return Ok(col.clone());
                }
                let output = col.take_lazy(indices)?;
                selected.insert(identity, output.clone());
                Ok(output)
            })
            .collect()
    })
}

pub trait LazyBatch: Send + Sync {
    fn is_materialized(&self) -> bool {
        (0..self.schema().fields().len()).all(|index| {
            self.get_column(index)
                .is_ok_and(|column| column.is_materialized())
        })
    }
    fn schema(&self) -> SchemaRef;
    fn num_rows(&self) -> usize;
    fn get_column(&self, index: usize) -> Result<LazyColumnRef>;
    fn take(&self, projection: Option<&[usize]>, indices: &Int32Array) -> Result<LazyBatchRef> {
        CACHE.with_scope(|| {
            if CACHE.current().is_identity(indices, self.num_rows()) {
                let projection = projection.map_or_else(
                    || (0..self.schema().fields().len()).collect::<Vec<_>>(),
                    |projection| projection.to_vec(),
                );
                return self.project(&projection);
            }
            let nullable = indices.null_count() != 0;
            let schema = self.schema();
            let projected;
            let projection = match projection {
                Some(projection) => projection,
                None => {
                    projected = (0..schema.fields().len()).collect::<Vec<_>>();
                    &projected
                }
            };
            let mut schema = schema.project(projection)?;
            if nullable {
                schema = arrow::datatypes::Schema::new_with_metadata(
                    schema
                        .fields()
                        .iter()
                        .map(|field| field.as_ref().clone().with_nullable(true))
                        .collect::<Vec<_>>(),
                    schema.metadata().clone(),
                );
            }
            let schema = Arc::new(schema);
            let columns = projection
                .iter()
                .map(|&index| self.get_column(index))
                .collect::<Result<Vec<_>>>()?;
            let columns = take_lazy_columns_shared(&columns, indices)?;
            ColumnBatch::try_new(schema, columns, indices.len())
        })
    }
    fn filter(&self, predicate: &BooleanArray) -> Result<LazyBatchRef> {
        CACHE.with_scope(|| {
            let mut filtered = HashMap::<usize, LazyColumnRef>::new();
            let columns = (0..self.schema().fields().len())
                .map(|index| {
                    let col = self.get_column(index)?;
                    let identity = Arc::as_ptr(&col) as *const () as usize;
                    if let Some(col) = filtered.get(&identity) {
                        return Ok(col.clone());
                    }
                    let output = if predicate.true_count() == predicate.len() {
                        col
                    } else {
                        col.filter(predicate)?
                    };
                    filtered.insert(identity, output.clone());
                    Ok(output)
                })
                .collect::<Result<Vec<_>>>()?;
            ColumnBatch::try_new(self.schema(), columns, predicate.true_count())
        })
    }
    fn project(&self, projection: &[usize]) -> Result<LazyBatchRef> {
        let schema = Arc::new(self.schema().project(projection)?);
        let columns = projection
            .iter()
            .map(|&index| self.get_column(index))
            .collect::<Result<Vec<_>>>()?;
        ColumnBatch::try_new(schema, columns, self.num_rows())
    }
    fn materialize(&self) -> Result<RecordBatch> {
        CACHE.with_scope(|| {
            let columns = (0..self.schema().fields().len())
                .map(|index| self.get_column(index)?.materialize())
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new_with_options(
                self.schema(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(self.num_rows())),
            )?)
        })
    }
    fn get_estimated_materialized_mem_size(&self) -> usize {
        (0..self.schema().fields().len())
            .filter_map(|index| self.get_column(index).ok())
            .fold(0usize, |bytes, col| {
                bytes.saturating_add(col.get_estimated_materialized_mem_size())
            })
    }
}

impl LazyBatch for RecordBatch {
    fn schema(&self) -> SchemaRef {
        RecordBatch::schema(self)
    }
    fn num_rows(&self) -> usize {
        RecordBatch::num_rows(self)
    }
    fn get_column(&self, index: usize) -> Result<LazyColumnRef> {
        Ok(column(self.columns()[index].clone()))
    }
    fn materialize(&self) -> Result<RecordBatch> {
        Ok(self.clone())
    }
}

pub(crate) struct ColumnBatch {
    schema: SchemaRef,
    columns: Vec<LazyColumnRef>,
    num_rows: usize,
}

impl ColumnBatch {
    pub fn try_new(
        schema: SchemaRef,
        columns: Vec<LazyColumnRef>,
        num_rows: usize,
    ) -> Result<LazyBatchRef> {
        // wrap lazy columns with memoized materialization so each column is
        // computed at most once no matter how many passes touch this batch;
        // shared columns share a single memo cell
        let mut once_columns = HashMap::<usize, LazyColumnRef>::new();
        let columns = columns
            .into_iter()
            .map(|column| {
                if column.is_materialized() || column.is_once() {
                    return column;
                }
                let key = Arc::as_ptr(&column) as *const () as usize;
                once_columns
                    .entry(key)
                    .or_insert_with(|| Arc::new(OnceColumn::new(column)))
                    .clone()
            })
            .collect();
        Ok(Arc::new(Self {
            schema,
            columns,
            num_rows,
        }))
    }
}

impl LazyBatch for ColumnBatch {
    fn is_materialized(&self) -> bool {
        self.columns.iter().all(|column| column.is_materialized())
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn num_rows(&self) -> usize {
        self.num_rows
    }
    fn get_column(&self, index: usize) -> Result<LazyColumnRef> {
        Ok(self.columns[index].clone())
    }
}

pub fn concat_batches_lazy(schema: SchemaRef, batches: &[LazyBatchRef]) -> Result<LazyBatchRef> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    if batches.is_empty() {
        return Ok(Arc::new(RecordBatch::new_empty(schema)));
    }
    let mut row_offsets = vec![0usize];
    for batch in batches {
        let rows = row_offsets
            .last()
            .expect("concat row offsets")
            .checked_add(batch.num_rows())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "concat batch row count overflow".to_owned(),
                )
            })?;
        row_offsets.push(rows);
    }
    let num_rows = *row_offsets.last().expect("concat row offsets");
    let row_offsets = Arc::new(row_offsets);
    let columns = (0..schema.fields().len())
        .map(|index| {
            let parts = batches
                .iter()
                .map(|batch| batch.get_column(index))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(ConcatColumn {
                parts,
                row_offsets: row_offsets.clone(),
            }) as LazyColumnRef)
        })
        .collect::<Result<Vec<_>>>()?;
    ColumnBatch::try_new(schema, columns, num_rows)
}

pub fn concat_batches(schema: SchemaRef, batches: &[LazyBatchRef]) -> Result<LazyBatchRef> {
    CACHE.with_scope(|| {
        let mut num_rows = 0usize;
        for batch in batches {
            num_rows += batch.num_rows();
        }
        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }
        if batches.is_empty() {
            return Ok(Arc::new(RecordBatch::new_empty(schema)));
        }
        let columns = (0..schema.fields().len())
            .map(|index| {
                let cols = batches
                    .iter()
                    .map(|batch| batch.get_column(index))
                    .collect::<Result<Vec<_>>>()?;
                cols[0].concat(&cols[1..])
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?) as LazyBatchRef)
    })
}

pub fn evaluate_columns(batch: &dyn LazyBatch, exprs: &[PhysicalExprRef]) -> Result<Vec<ArrayRef>> {
    let (exprs, projection) = prune_columns(exprs)?;
    let projected = batch.project(&projection)?.materialize()?;
    exprs
        .iter()
        .map(|expr| expr.evaluate(&projected)?.into_array(batch.num_rows()))
        .collect()
}

pub trait LazyBatchStream: Stream<Item = Result<LazyBatchRef>> {
    fn schema(&self) -> SchemaRef;
}

pub(crate) struct LazyBatchStreamAdapter<S> {
    schema: SchemaRef,
    stream: Pin<Box<S>>,
}

impl<S> LazyBatchStreamAdapter<S> {
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self {
            schema,
            stream: Box::pin(stream),
        }
    }
}

impl<S: Stream<Item = Result<LazyBatchRef>>> Stream for LazyBatchStreamAdapter<S> {
    type Item = Result<LazyBatchRef>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl<S: Stream<Item = Result<LazyBatchRef>>> LazyBatchStream for LazyBatchStreamAdapter<S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub trait RecordBatchStreamInput: Stream<Item = Result<RecordBatch>> + Send + 'static {
    fn schema(&self) -> SchemaRef;
}

impl<S: RecordBatchStream + Send + ?Sized + 'static> RecordBatchStreamInput for Pin<Box<S>> {
    fn schema(&self) -> SchemaRef {
        self.as_ref().get_ref().schema()
    }
}

impl<S: Stream<Item = Result<RecordBatch>> + Send + 'static> RecordBatchStreamInput
    for RecordBatchStreamAdapter<S>
{
    fn schema(&self) -> SchemaRef {
        RecordBatchStream::schema(self)
    }
}

pub fn wrap_stream<S: RecordBatchStreamInput>(stream: S) -> SendableLazyBatchStream {
    let schema = stream.schema();
    Box::pin(LazyBatchStreamAdapter::new(
        schema,
        Box::pin(stream).map(|batch| batch.map(|batch| Arc::new(batch) as LazyBatchRef)),
    ))
}

pub fn materialize_stream(stream: SendableLazyBatchStream) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        stream.schema(),
        stream.map(|batch| batch.and_then(|batch| batch.materialize())),
    ))
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{AsArray, DictionaryArray, StringArray},
        datatypes::{Field, Int32Type, Schema},
    };

    use super::*;

    fn batch(array: ArrayRef) -> Result<LazyBatchRef> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.as_ref().data_type().clone(),
            true,
        )]));
        let rows = array.as_ref().len();
        ColumnBatch::try_new(schema, vec![column(array)], rows)
    }

    #[test]
    fn take_null_indices_relaxes_all_columns_without_projection() -> Result<()> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int32, false)],
            [("source".to_owned(), "take-test".to_owned())].into(),
        ));
        let input: LazyBatchRef = Arc::new(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        )?);
        let indices = Int32Array::from(vec![Some(1), None, Some(0)]);
        let selected = input.take(None, &indices)?;
        assert!(selected.schema().field(0).is_nullable());
        assert_eq!(selected.schema().metadata(), schema.metadata());
        assert_eq!(
            selected.materialize()?,
            input.take(Some(&[0]), &indices)?.materialize()?
        );
        assert_eq!(
            selected.materialize()?.column(0).as_ref(),
            &Int32Array::from(vec![Some(20), None, Some(10)])
        );
        Ok(())
    }

    #[test]
    fn lazy_concat_preserves_sources_until_selected() -> Result<()> {
        let source: ArrayRef = Arc::new(StringArray::from(vec!["a", "long payload", "c"]));
        let first = batch(source.clone())?;
        let second = first.take(None, &Int32Array::from(vec![2, 0]))?;
        let empty = first.take(None, &Int32Array::from(Vec::<i32>::new()))?;
        let joined = concat_batches_lazy(
            first.schema(),
            &[empty.clone(), first.clone(), empty, second],
        )?;
        assert!(!joined.is_materialized());
        let count = Arc::strong_count(&source);
        let indices = Int32Array::from(vec![Some(4), None, Some(1), Some(3), Some(1)]);
        let selected = joined.take(None, &indices)?;
        assert!(Arc::strong_count(&source) <= count);
        assert!(!joined.is_materialized());
        let mask = BooleanArray::from(vec![false, true, false, true, true]);
        assert_eq!(
            joined.filter(&mask)?.materialize()?.column(0).as_ref(),
            &StringArray::from(vec!["long payload", "c", "a"])
        );
        assert_eq!(
            joined.get_column(0)?.materialize_selection(&mask)?.as_ref(),
            &StringArray::from(vec!["long payload", "c", "a"])
        );
        assert!(joined.take(None, &Int32Array::from(vec![-1])).is_err());
        assert!(joined.take(None, &Int32Array::from(vec![5])).is_err());
        drop(joined);
        assert_eq!(
            selected.materialize()?.column(0).as_ref(),
            &StringArray::from(vec![
                Some("a"),
                None,
                Some("long payload"),
                Some("c"),
                Some("long payload")
            ])
        );
        assert!(Arc::ptr_eq(
            &first,
            &concat_batches_lazy(first.schema(), std::slice::from_ref(&first))?
        ));
        let zero_columns = first.project(&[])?;
        let joined =
            concat_batches_lazy(zero_columns.schema(), &[zero_columns.clone(), zero_columns])?;
        assert_eq!(joined.materialize()?.num_rows(), 6);
        Ok(())
    }

    #[test]
    fn evaluate_columns_materializes_only_required_columns() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let selected = column(source).take_lazy(&Int32Array::from(vec![2, 0]))?;
        let untouched = column(Arc::new(StringArray::from(vec!["a", "b", "c"])))
            .take_lazy(&Int32Array::from(vec![2, 0]))?;
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ],
            [("source".to_owned(), "lazy-test".to_owned())].into(),
        ));
        let batch = ColumnBatch::try_new(schema.clone(), vec![selected, untouched], 2)?;
        let exprs: Vec<PhysicalExprRef> = vec![Arc::new(
            datafusion::physical_expr::expressions::Column::new("key", 0),
        )];
        let keys = evaluate_columns(batch.as_ref(), &exprs)?;
        assert_eq!(keys[0].as_ref(), &Int32Array::from(vec![30, 10]));
        assert!(batch.get_column(0)?.is_materialized());
        assert!(!batch.get_column(1)?.is_materialized());
        let empty = batch.project(&[])?;
        assert_eq!(empty.schema().metadata(), schema.metadata());
        assert_eq!(empty.materialize()?.num_rows(), 2);
        assert!(evaluate_columns(empty.as_ref(), &[])?.is_empty());
        assert!(!batch.get_column(1)?.is_materialized());
        Ok(())
    }

    #[test]
    fn filtered_masks_compose_take_and_estimate_through_interface() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(30),
            Some(40),
            Some(50),
        ]));
        let bytes = ArraySize::get_array_mem_size(source.as_ref());
        let plain = column(source.clone());
        let mask = BooleanArray::from(vec![Some(true), None, Some(true), Some(false), Some(true)]);
        let filtered = plain.filter(&mask)?;
        assert_eq!(filtered.len(), 3);
        assert!(!filtered.has_nulls());
        assert_eq!(
            filtered.get_estimated_materialized_mem_size(),
            bytes * 3 / 5
        );
        let next = BooleanArray::from(vec![None, Some(true), Some(true)]);
        let twice = filtered.filter(&next)?;
        assert_eq!(
            twice.materialize()?.as_ref(),
            &Int32Array::from(vec![30, 50])
        );
        assert_eq!(
            filtered.materialize_selection(&next)?.to_data(),
            twice.materialize()?.to_data()
        );
        let indices = Int32Array::from(vec![Some(2), None, Some(0), Some(2)]);
        let expected = Int32Array::from(vec![Some(50), None, Some(10), Some(50)]);
        assert_eq!(filtered.take(&indices)?.as_ref(), &expected);
        assert_eq!(
            filtered.take_lazy(&indices)?.materialize()?.as_ref(),
            &expected
        );
        assert!(Arc::ptr_eq(
            &source,
            &plain
                .filter(&BooleanArray::from(vec![true; 5]))?
                .materialize()?
        ));
        assert!(Arc::ptr_eq(
            &source,
            &plain.materialize_selection(&BooleanArray::from(vec![true; 5]))?
        ));
        assert_eq!(
            filtered
                .filter(&BooleanArray::from(vec![false; 3]))?
                .get_estimated_materialized_mem_size(),
            0
        );
        assert_eq!(
            column(new_empty_array(&DataType::Int32))
                .filter(&BooleanArray::from(vec![] as Vec<bool>))?
                .get_estimated_materialized_mem_size(),
            0
        );
        Ok(())
    }

    #[test]
    fn filtered_concat_and_indexed_selection_match_arrow() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(40)]));
        let plain = column(source.clone());
        let filtered = plain.filter(&BooleanArray::from(vec![true, true, false, true]))?;
        let other = column(Arc::new(Int32Array::from(vec![50, 60])))
            .filter(&BooleanArray::from(vec![false, true]))?;
        for cols in [
            vec![filtered.clone(), filtered.clone()],
            vec![filtered.clone(), other],
            vec![filtered.clone(), plain.clone()],
        ] {
            let eager = cols
                .iter()
                .map(|col| col.materialize())
                .collect::<Result<Vec<_>>>()?;
            let expected = arrow::compute::concat(
                &eager.iter().map(|array| array.as_ref()).collect::<Vec<_>>(),
            )?;
            let joined = CACHE.with_scope(|| {
                let joined = cols[0].concat(&cols[1..])?;
                assert!(CACHE.current().mask_indices.borrow().is_empty());
                assert!(CACHE.current().indices.borrow().is_empty());
                Ok(joined)
            })?;
            assert_eq!(joined.to_data(), expected.to_data());
            let joined = column(joined);
            for col in [
                joined.clone(),
                joined.take_lazy(&Int32Array::from(vec![Some(3), None, Some(0), Some(1)]))?,
            ] {
                let selection =
                    BooleanArray::from_iter((0..col.len()).map(|row| Some(row % 2 == 0)));
                assert_eq!(
                    col.materialize_selection(&selection)?.to_data(),
                    arrow::compute::filter(col.materialize()?.as_ref(), &selection)?.to_data()
                );
            }
        }
        CACHE.with_scope(|| {
            let old = BooleanArray::from(vec![false, true, true, false]);
            let new = BooleanArray::from(vec![false, true]);
            let first = compose_selection(&old, &new)?;
            let second = compose_selection(&old.clone(), &new.clone())?;
            assert_eq!(
                first.values().inner().as_ptr(),
                second.values().inner().as_ptr()
            );
            let sliced_old = BooleanArray::from(vec![true, false, true, true, false]).slice(1, 4);
            assert_eq!(compose_selection(&sliced_old, &new)?, first);
            Ok(())
        })
    }

    #[test]
    fn plain_columns_estimate_existing_array_size() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            Arc::new(StringArray::from(vec![
                Some("small"),
                None,
                Some("longer value"),
            ])),
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["a", "long value"])),
            )),
            Arc::new(arrow::array::StructArray::from(vec![(
                Arc::new(Field::new("child", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
            )])),
        ];
        for array in arrays {
            for array in [array.clone(), array.as_ref().slice(1, 1)] {
                assert_eq!(
                    column(array.clone()).get_estimated_materialized_mem_size(),
                    ArraySize::get_array_mem_size(array.as_ref())
                );
            }
        }
    }

    #[test]
    fn indexed_columns_estimate_output_rows() -> Result<()> {
        let source: ArrayRef = Arc::new(StringArray::from(vec!["a", "longer value", "bbb"]));
        let bytes = ArraySize::get_array_mem_size(source.as_ref());
        let plain = column(source);
        for indices in [
            Int32Array::from(vec![2]),
            Int32Array::from(vec![2, 2, 0, 0, 1]),
            Int32Array::from(vec![Some(2), None]),
            Int32Array::from(vec![None, None]),
        ] {
            let selected = plain.take_lazy(&indices)?;
            assert_eq!(
                selected.get_estimated_materialized_mem_size(),
                bytes * indices.len() / 3
            );
        }
        let identity = plain.take_lazy(&Int32Array::from(vec![0, 1, 2]))?;
        assert_eq!(identity.get_estimated_materialized_mem_size(), bytes);
        Ok(())
    }

    #[test]
    fn batches_estimate_each_duplicate_output_column() -> Result<()> {
        let input = batch(Arc::new(Int32Array::from(vec![10, 20, 30])))?;
        let bytes = input.get_column(0)?.get_estimated_materialized_mem_size();
        let duplicate = input.project(&[0, 0])?;
        assert!(Arc::ptr_eq(
            &duplicate.get_column(0)?,
            &duplicate.get_column(1)?
        ));
        assert_eq!(duplicate.get_estimated_materialized_mem_size(), bytes * 2);
        let selected = duplicate.take(None, &Int32Array::from(vec![2, 0]))?;
        assert_eq!(
            selected.get_estimated_materialized_mem_size(),
            (bytes * 2 / 3) * 2
        );
        let materialized = duplicate.materialize()?;
        assert_eq!(
            LazyBatch::get_estimated_materialized_mem_size(&materialized),
            bytes * 2
        );
        assert_eq!(input.project(&[])?.get_estimated_materialized_mem_size(), 0);
        Ok(())
    }

    #[test]
    fn zero_source_rows_estimate_zero_for_null_outputs() -> Result<()> {
        for data_type in [DataType::Int32, DataType::Utf8, DataType::Null] {
            let empty = column(new_empty_array(&data_type));
            let joined = empty.concat(std::slice::from_ref(&empty)).map(column)?;
            assert_eq!(
                joined.get_estimated_materialized_mem_size(),
                ArraySize::get_array_mem_size(joined.materialize()?.as_ref())
            );
            for col in [empty, joined] {
                let selected = col.take_lazy(&Int32Array::from(vec![None, None]))?;
                assert_eq!(selected.get_estimated_materialized_mem_size(), 0);
                assert_eq!(selected.materialize()?.logical_null_count(), 2);
            }
        }
        Ok(())
    }

    #[test]
    fn nonempty_sources_estimate_zero_for_empty_outputs() -> Result<()> {
        let plain = column(Arc::new(Int32Array::from(vec![10, 20, 30])));
        let indexed = plain.take_lazy(&Int32Array::from(vec![2, 0]))?;
        let composite = plain.concat(std::slice::from_ref(&plain)).map(column)?;
        for col in [plain, indexed, composite] {
            assert_eq!(
                col.take_lazy(&Int32Array::from(Vec::<i32>::new()))?
                    .get_estimated_materialized_mem_size(),
                0
            );
        }
        Ok(())
    }

    #[test]
    fn repeated_outputs_scale_estimate_beyond_source_size() -> Result<()> {
        let source: ArrayRef = Arc::new(StringArray::from(vec!["payload", "longer payload"]));
        let bytes = ArraySize::get_array_mem_size(source.as_ref());
        let plain = column(source);
        let indexed = plain.take_lazy(&Int32Array::from(vec![1]))?;
        let composite = plain.concat(std::slice::from_ref(&plain)).map(column)?;
        let indices = Int32Array::from(vec![0; 100_000]);
        let concatenated_bytes = composite.get_estimated_materialized_mem_size();
        for (col, source_bytes, source_rows) in
            [(indexed, bytes, 2), (composite, concatenated_bytes, 4)]
        {
            let selected = col.take_lazy(&indices)?;
            assert_eq!(selected.len(), indices.len());
            assert_eq!(
                selected.get_estimated_materialized_mem_size(),
                source_bytes * indices.len() / source_rows
            );
            assert!(selected.get_estimated_materialized_mem_size() > source_bytes);
        }
        Ok(())
    }

    #[test]
    fn take_results_for_plain_indexed_and_concatenated_columns() -> Result<()> {
        let values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            )?),
            Arc::new(arrow::array::StructArray::from(vec![(
                Arc::new(Field::new("child", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
            )])),
            Arc::new(arrow::array::NullArray::new(3)),
        ];
        for source in values {
            let plain = column(source.clone());
            let identity = Int32Array::from(vec![0, 1, 2]);
            assert!(Arc::ptr_eq(
                &source,
                &plain.take_lazy(&identity)?.materialize()?
            ));
            let shared = take_lazy_columns(
                &[plain.clone(), plain.clone()],
                &Int32Array::from(vec![0, 1, 2]),
            )?;
            assert!(Arc::ptr_eq(&shared[0], &plain));
            assert!(Arc::ptr_eq(&shared[0], &shared[1]));
            let indexed = plain.take_lazy(&Int32Array::from(vec![Some(2), None, Some(0)]))?;
            let concatenated = plain.concat(std::slice::from_ref(&plain)).map(column)?;
            let input = batch(source.clone())?;
            let lazy_concat =
                concat_batches_lazy(input.schema(), &[input.clone(), input])?.get_column(0)?;
            let selected =
                concatenated.take_lazy(&Int32Array::from(vec![Some(5), None, Some(1)]))?;
            for col in [plain, indexed, concatenated, lazy_concat, selected] {
                let materialized = col.materialize()?;
                for indices in [
                    Int32Array::from((0..col.len() as i32).collect::<Vec<_>>()),
                    Int32Array::from(vec![Some(2), None, Some(0), Some(2)]),
                    Int32Array::from(vec![None, None]),
                    Int32Array::from(Vec::<i32>::new()),
                    Int32Array::from(vec![99, 2, 1, 0]).slice(1, 3),
                ] {
                    let expected = take(materialized.as_ref(), &indices, None)?;
                    assert_eq!(col.take(&indices)?.to_data(), expected.to_data());
                    assert_eq!(
                        col.take_lazy(&indices)?.materialize()?.to_data(),
                        expected.to_data()
                    );
                }
                let rows = Int32Array::from(vec![Some(2), None, Some(0)]);
                let expected = take(materialized.as_ref(), &rows, None)?;
                let eager = take_columns(&[col.clone(), col.clone()], &rows)?;
                assert!(Arc::ptr_eq(&eager[0], &eager[1]));
                assert_eq!(eager[0].to_data(), expected.to_data());
            }
        }
        Ok(())
    }

    #[test]
    fn concat_results_for_plain_indexed_and_mixed_columns() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![40, 50]));
        let raw = column(a.clone())
            .concat(&[column(b.clone()), column(a.clone())])
            .map(column)?;
        let first = column(a.clone()).take_lazy(&Int32Array::from(vec![Some(2), None, Some(0)]))?;
        let indexed = first
            .concat(&[
                column(b.clone()).take_lazy(&Int32Array::from(vec![1, 0]))?,
                first.take_lazy(&Int32Array::from(vec![1, 2]))?,
            ])
            .map(column)?;
        let composite = column(a.clone())
            .concat(&[column(b.clone())])
            .map(column)?
            .take_lazy(&Int32Array::from(vec![Some(4), None, Some(0)]))?;
        let repeated = composite
            .concat(&[composite.take_lazy(&Int32Array::from(vec![1, 2]))?])
            .map(column)?;
        let reversed = column(b.clone())
            .concat(&[column(a.clone())])
            .map(column)?
            .take_lazy(&Int32Array::from(vec![Some(0), Some(4), None]))?;
        let merged = composite.concat(&[reversed]).map(column)?;
        for (col, expected) in [
            (
                raw,
                vec![
                    Some(10),
                    None,
                    Some(30),
                    Some(40),
                    Some(50),
                    Some(10),
                    None,
                    Some(30),
                ],
            ),
            (
                indexed,
                vec![Some(30), None, Some(10), Some(50), Some(40), None, Some(10)],
            ),
            (
                merged,
                vec![Some(50), None, Some(10), Some(40), Some(30), None],
            ),
            (repeated, vec![Some(50), None, Some(10), None, Some(10)]),
            (
                first
                    .concat(&[first.take_lazy(&Int32Array::from(vec![1, 2]))?])
                    .map(column)?,
                vec![Some(30), None, Some(10), None, Some(10)],
            ),
            (
                column(a.clone()).concat(&[]).map(column)?,
                vec![Some(10), None, Some(30)],
            ),
        ] {
            assert_eq!(
                col.materialize()?.as_primitive::<Int32Type>(),
                &Int32Array::from(expected)
            );
        }
        let mixed = first.concat(&[column(b), composite]).map(column)?;
        assert_eq!(
            mixed.materialize()?.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![
                Some(30),
                None,
                Some(10),
                Some(40),
                Some(50),
                Some(50),
                None,
                Some(10)
            ])
        );
        Ok(())
    }

    #[test]
    fn nullable_and_non_nullable_batches_preserve_values() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
        let input = batch(source)?;
        let selected = input.take(None, &Int32Array::from(vec![Some(2), None, Some(1)]))?;
        assert!(selected.get_column(0)?.has_nulls());
        assert_eq!(
            selected
                .materialize()?
                .column(0)
                .as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(30), None, None])
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input = ColumnBatch::try_new(
            schema,
            vec![column(Arc::new(Int32Array::from(vec![10, 20])))],
            2,
        )?;
        let selected = input.take(Some(&[0]), &Int32Array::from(vec![None, Some(0)]))?;
        assert!(selected.schema().field(0).is_nullable());
        assert_eq!(
            selected
                .materialize()?
                .column(0)
                .as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None, Some(10)])
        );
        let empty = input.take(None, &Int32Array::from(Vec::<i32>::new()))?;
        assert_eq!(empty.materialize()?.num_rows(), 0);
        assert!(!empty.get_column(0)?.has_nulls());
        let concatenated = concat_batches(input.schema(), &[input.clone(), input.clone()])?;
        assert_eq!(
            concatenated
                .materialize()?
                .column(0)
                .as_primitive::<Int32Type>(),
            &Int32Array::from(vec![10, 20, 10, 20])
        );
        assert!(Arc::ptr_eq(
            &concat_batches(input.schema(), std::slice::from_ref(&input))?,
            &input
        ));
        assert_eq!(
            concat_batches(input.schema(), &[])?
                .materialize()?
                .num_rows(),
            0
        );
        Ok(())
    }

    #[test]
    fn sliced_indices_and_null_bitmaps_match_arrow() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let indices = Int32Array::from(vec![0, 1, 2]);
        let bitmap = arrow::buffer::NullBuffer::from(vec![true, false, true, false]);
        let mappings = [
            indices.slice(0, 2),
            indices.slice(1, 2),
            Int32Array::new(indices.values().clone(), Some(bitmap.slice(0, 3))).slice(0, 2),
            Int32Array::new(indices.values().clone(), Some(bitmap.slice(1, 3))).slice(0, 2),
        ];
        for layout in [
            vec![0, 1, 3],
            vec![0, 2, 3],
            vec![0, 1, 2, 3],
            vec![0, 0, 2, 2, 3],
            vec![0, 0, 0, 2, 2, 3, 3, 3],
        ] {
            let columns = layout
                .windows(2)
                .map(|pair| column(source.as_ref().slice(pair[0], pair[1] - pair[0])))
                .collect::<Vec<_>>();
            let joined = columns[0].concat(&columns[1..]).map(column)?;
            for mapping in &mappings {
                let selected = joined.take_lazy(mapping)?;
                let expected = take(source.as_ref(), mapping, None)?;
                assert_eq!(selected.materialize()?.to_data(), expected.to_data());
                assert_eq!(selected.has_nulls(), expected.logical_null_count() != 0);
                let indices = Int32Array::from(vec![1, 0]);
                let expected = take(expected.as_ref(), &indices, None)?;
                assert_eq!(selected.take(&indices)?.to_data(), expected.to_data());
                assert_eq!(
                    selected.take_lazy(&indices)?.materialize()?.to_data(),
                    expected.to_data()
                );
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
            Field::new("different_layout", DataType::Int32, true),
        ]));
        let mapping = Int32Array::from(vec![Some(2), None, Some(0), Some(1)]);
        let sources: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            Arc::new(Int32Array::from(vec![40, 50, 60])),
            Arc::new(Int32Array::from(vec![70, 80, 90])),
        ];
        let layouts = [
            vec![0, 0, 1, 1, 3, 3],
            vec![0, 0, 1, 1, 3, 3],
            vec![0, 2, 3],
        ];
        let columns = sources
            .iter()
            .zip(layouts)
            .map(|(source, layout)| {
                let columns = layout
                    .windows(2)
                    .map(|pair| column(source.as_ref().slice(pair[0], pair[1] - pair[0])))
                    .collect::<Vec<_>>();
                columns[0]
                    .concat(&columns[1..])
                    .map(column)?
                    .take_lazy(&mapping)
            })
            .collect::<Result<Vec<_>>>()?;
        let selected = ColumnBatch::try_new(schema.clone(), columns, mapping.len())?;
        let joined = concat_batches(schema, &[selected.clone(), selected])?;
        let materialized = joined.materialize()?;
        for (column, source) in materialized.columns().iter().zip(sources) {
            let expected = take(source.as_ref(), &mapping, None)?;
            let expected = arrow::compute::concat(&[expected.as_ref(), expected.as_ref()])?;
            assert_eq!(column.to_data(), expected.to_data());
        }
        Ok(())
    }

    #[test]
    fn scoped_takes_share_mask_indices() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
        let mask = BooleanArray::from(vec![true, false, true, true]);
        CACHE.with_scope(|| {
            let first = filtered_column(source.clone(), mask.clone())?;
            let second = filtered_column(source.clone(), mask.clone())?;
            first.take_lazy(&Int32Array::from(vec![0, 1]))?;
            second.take_lazy(&Int32Array::from(vec![1]))?;
            // one mask scan serves every take in the scope
            assert_eq!(CACHE.current().mask_indices.borrow().len(), 1);
            Ok(())
        })
    }

    #[test]
    fn empty_columns_and_huge_row_spaces() -> Result<()> {
        for data_type in [DataType::Int32, DataType::Utf8, DataType::Null] {
            let empty = column(new_empty_array(&data_type));
            let joined = empty.concat(std::slice::from_ref(&empty)).map(column)?;
            assert_eq!(joined.materialize()?.as_ref().data_type(), &data_type);
            assert_eq!(joined.materialize()?.as_ref().len(), 0);
            assert_eq!(
                joined
                    .take_lazy(&Int32Array::from(vec![None, None]))?
                    .materialize()?
                    .logical_null_count(),
                2
            );
        }
        let huge = column(Arc::new(arrow::array::NullArray::new(
            i32::MAX as usize + 2,
        )));
        let small = column(Arc::new(arrow::array::NullArray::new(2)));
        let first = huge.take_lazy(&Int32Array::from(vec![Some(0), None]))?;
        let output = first
            .concat(&[small.take_lazy(&Int32Array::from(vec![1]))?])
            .map(column)?;
        assert_eq!(output.materialize()?.logical_null_count(), 3);
        assert!(output.get_estimated_materialized_mem_size() < 1024);
        Ok(())
    }

    #[test]
    fn test_memoized_materialize_replaces_lazy_source() -> Result<()> {
        let source: ArrayRef = Arc::new(Int32Array::from_iter_values(0..100i32));
        let weak = Arc::downgrade(&source);
        let selection = BooleanArray::from_iter((0..100).map(|i| Some(i % 2 == 0)));
        let column = filtered_column(source.clone(), selection)?;
        drop(source);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let lazy_batch = ColumnBatch::try_new(schema, vec![column], 50)?;

        // first pass materializes through a projected batch, like evaluate_columns
        let projected = lazy_batch.project(&[0])?.materialize()?;
        assert_eq!(projected.num_rows(), 50);
        assert!(
            weak.upgrade().is_none(),
            "lazy source should be released after the first materialization"
        );

        // second pass is served from the cached array
        let array = lazy_batch.get_column(0)?.materialize()?;
        assert_eq!(Array::len(array.as_ref()), 50);
        assert_eq!(&array, projected.column(0));
        Ok(())
    }
}
