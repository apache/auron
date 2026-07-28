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
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::{BitAnd, BitOr, BitXor},
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{df_execution_err, downcast_any};

use crate::{
    agg::{
        Agg,
        acc::{AccColumnRef, AccPrimColumn, create_acc_generic_column},
        agg::IdxSelection,
    },
    idx_for_zipped,
};

pub type AggBitAnd = AggBitwise<AggBitAndParams>;
pub type AggBitOr = AggBitwise<AggBitOrParams>;
pub type AggBitXor = AggBitwise<AggBitXorParams>;

/// Native implementation of Spark's bit_and / bit_or / bit_xor aggregates.
///
/// These only accept integral inputs. The accumulator is a single column of the
/// same type as the input: the first non-null value initializes the slot and
/// every subsequent value is folded in with the bitwise operator. Because the
/// operators are associative and commutative, the result is independent of the
/// visiting/merge order, and null inputs are simply skipped (an all-null group
/// yields null).
pub struct AggBitwise<P: AggBitwiseParams> {
    child: PhysicalExprRef,
    data_type: DataType,
    acc_array_data_types: Vec<DataType>,
    _phantom: PhantomData<P>,
}

impl<P: AggBitwiseParams> AggBitwise<P> {
    pub fn try_new(child: PhysicalExprRef, data_type: DataType) -> Result<Self> {
        match &data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {}
            other => df_execution_err!("{} only supports integral types, got {other:?}", P::NAME)?,
        }
        let acc_array_data_types = vec![data_type.clone()];
        Ok(Self {
            child,
            data_type,
            acc_array_data_types,
            _phantom: Default::default(),
        })
    }
}

impl<P: AggBitwiseParams> Debug for AggBitwise<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", P::NAME, self.child)
    }
}

impl<P: AggBitwiseParams> Agg for AggBitwise<P> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.data_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        create_acc_generic_column(self.data_type.clone(), num_rows)
    }

    fn acc_array_data_types(&self) -> &[DataType] {
        &self.acc_array_data_types
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let partial_arg = &partial_args[0];
        accs.ensure_size(acc_idx);

        macro_rules! handle_int {
            ($array_ty:ty, $native:ty) => {{
                let partial_arg = downcast_any!(partial_arg, $array_ty)?;
                let accs = downcast_any!(accs, mut AccPrimColumn<$native>)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if partial_arg.is_valid(partial_arg_idx) {
                            let partial_value = partial_arg.value(partial_arg_idx);
                            accs.update_value(acc_idx, partial_value, |v| P::op(v, partial_value));
                        }
                    }
                }
            }};
        }

        match &self.data_type {
            DataType::Int8 => handle_int!(Int8Array, i8),
            DataType::Int16 => handle_int!(Int16Array, i16),
            DataType::Int32 => handle_int!(Int32Array, i32),
            DataType::Int64 => handle_int!(Int64Array, i64),
            other => df_execution_err!("{} only supports integral types, got {other:?}", P::NAME)?,
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        accs.ensure_size(acc_idx);

        macro_rules! handle_int {
            ($native:ty) => {{
                let accs = downcast_any!(accs, mut AccPrimColumn<$native>)?;
                let merging_accs = downcast_any!(merging_accs, mut AccPrimColumn<$native>)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if let Some(merging_value) = merging_accs.value(merging_acc_idx) {
                            accs.update_value(acc_idx, merging_value, |v| P::op(v, merging_value));
                        }
                    }
                }
            }};
        }

        match &self.data_type {
            DataType::Int8 => handle_int!(i8),
            DataType::Int16 => handle_int!(i16),
            DataType::Int32 => handle_int!(i32),
            DataType::Int64 => handle_int!(i64),
            other => df_execution_err!("{} only supports integral types, got {other:?}", P::NAME)?,
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        Ok(accs.freeze_to_arrays(acc_idx)?[0].clone())
    }
}

pub trait AggBitwiseParams: 'static + Send + Sync {
    const NAME: &'static str;
    fn op<T>(a: T, b: T) -> T
    where
        T: BitAnd<Output = T> + BitOr<Output = T> + BitXor<Output = T>;
}

pub struct AggBitAndParams;
pub struct AggBitOrParams;
pub struct AggBitXorParams;

impl AggBitwiseParams for AggBitAndParams {
    const NAME: &'static str = "bit_and";
    fn op<T>(a: T, b: T) -> T
    where
        T: BitAnd<Output = T> + BitOr<Output = T> + BitXor<Output = T>,
    {
        a & b
    }
}

impl AggBitwiseParams for AggBitOrParams {
    const NAME: &'static str = "bit_or";
    fn op<T>(a: T, b: T) -> T
    where
        T: BitAnd<Output = T> + BitOr<Output = T> + BitXor<Output = T>,
    {
        a | b
    }
}

impl AggBitwiseParams for AggBitXorParams {
    const NAME: &'static str = "bit_xor";
    fn op<T>(a: T, b: T) -> T
    where
        T: BitAnd<Output = T> + BitOr<Output = T> + BitXor<Output = T>,
    {
        a ^ b
    }
}
