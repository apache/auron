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
    cmp::Ordering,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{downcast_any, scalar_value::compacted_scalar_value_from_array};

use crate::{
    agg::{
        Agg,
        acc::{
            AccBooleanColumn, AccBytes, AccBytesColumn, AccColumn, AccColumnRef, AccPrimColumn,
            AccScalarValueColumn, create_acc_generic_column,
        },
        agg::IdxSelection,
    },
    idx_for_zipped,
};

pub type AggMax = AggMaxMin<AggMaxParams>;
pub type AggMin = AggMaxMin<AggMinParams>;

fn spark_float_cmp<T: num::Float>(left: T, right: T) -> Ordering {
    // Spark places all NaNs last and treats signed zeros as equal.
    if left == right || (left.is_nan() && right.is_nan()) {
        Ordering::Equal
    } else if left.is_nan() {
        Ordering::Greater
    } else if right.is_nan() || left < right {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

macro_rules! compare_primitive {
    (spark, $left:expr, $right:expr) => {
        Some(spark_float_cmp($left, $right))
    };
    (partial, $left:expr, $right:expr) => {
        $left.partial_cmp(&$right)
    };
}

pub struct AggMaxMin<P: AggMaxMinParams> {
    child: PhysicalExprRef,
    data_type: DataType,
    acc_array_data_types: Vec<DataType>,
    _phantom: PhantomData<P>,
}

impl<P: AggMaxMinParams> AggMaxMin<P> {
    pub fn try_new(child: PhysicalExprRef, data_type: DataType) -> Result<Self> {
        let acc_array_data_types = vec![data_type.clone()];
        Ok(Self {
            child,
            data_type,
            acc_array_data_types,
            _phantom: Default::default(),
        })
    }
}

impl<P: AggMaxMinParams> Debug for AggMaxMin<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", P::NAME, self.child)
    }
}

impl<P: AggMaxMinParams> Agg for AggMaxMin<P> {
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

        macro_rules! handle_primitive {
            ($array:expr, $compare:ident) => {{
                let partial_arg = $array;
                let accs = downcast_any!(accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                     ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                         if partial_arg.is_valid(partial_arg_idx) {
                            let partial_value = partial_arg.value(partial_arg_idx);
                            accs.update_value(acc_idx, partial_value, |v| {
                                let ord = compare_primitive!($compare, v, partial_value);
                                if ord == Some(P::ORD) || ord == Some(Ordering::Equal) {
                                     v
                                 } else {
                                     partial_value
                                 }
                             });
                         }
                     }
                }
            }};
        }

        macro_rules! handle_boolean {
            ($array:expr) => {{
                let partial_arg = $array;
                let accs = downcast_any!(accs, mut AccBooleanColumn)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if partial_arg.is_valid(partial_arg_idx) {
                            let partial_value = partial_arg.value(partial_arg_idx);
                            accs.update_value(acc_idx, partial_value, |v| {
                                if v.partial_cmp(&partial_value) == Some(P::ORD) {
                                    v
                                } else {
                                    partial_value
                                }
                            });
                        }
                    }
                }
            }};
        }
        macro_rules! handle_bytes {
            ($array:expr) => {{
                let partial_arg = $array;
                let accs = downcast_any!(accs, mut AccBytesColumn)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if !partial_arg.is_valid(partial_arg_idx) {
                            continue;
                        }
                        let partial_value: &[u8] = partial_arg.value(partial_arg_idx).as_ref();
                        if let Some(w) = accs.value(acc_idx) && w.as_ref().partial_cmp(partial_value.as_ref()) == Some(P::ORD) {
                            continue;
                        }
                        accs.set_value(acc_idx, Some(AccBytes::from(partial_value)));
                    }
                }
            }};
        }

        match partial_arg.data_type() {
            DataType::Float32 => {
                handle_primitive!(downcast_any!(partial_arg, Float32Array)?, spark)
            }
            DataType::Float64 => {
                handle_primitive!(downcast_any!(partial_arg, Float64Array)?, spark)
            }
            _ => downcast_primitive_array! {
                partial_arg => handle_primitive!(partial_arg, partial),
                DataType::Boolean => handle_boolean!(downcast_any!(partial_arg, BooleanArray)?),
                DataType::Binary => handle_bytes!(downcast_any!(partial_arg, BinaryArray)?),
                DataType::Utf8 => handle_bytes!(downcast_any!(partial_arg, StringArray)?),
                DataType::Null => {}
                _ => {
                    let accs = downcast_any!(accs, mut AccScalarValueColumn)?;
                    idx_for_zipped! {
                        ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                            if partial_args[0].is_valid(partial_arg_idx) {
                                let partial_arg_scalar = compacted_scalar_value_from_array(
                                    &partial_args[0],
                                    partial_arg_idx,
                                )?;
                                let acc_scalar = accs.value(acc_idx);
                                if !acc_scalar.is_null() && acc_scalar.partial_cmp(&partial_arg_scalar) == Some(P::ORD) {
                                    continue;
                                }
                                accs.set_value(acc_idx, partial_arg_scalar);
                            }
                        }
                    }
                }
            },
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

        macro_rules! handle_primitive {
            ($ty:ty) => {
                handle_primitive!($ty, partial)
            };
            ($ty:ty, $compare:ident) => {{
                type TNative = <$ty as ArrowPrimitiveType>::Native;
                let accs = downcast_any!(accs, mut AccPrimColumn<TNative>)?;
                let merging_accs = downcast_any!(merging_accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if let Some(merging_value) = merging_accs.value(merging_acc_idx) {
                            accs.update_value(acc_idx, merging_value, |v| {
                                let ord = compare_primitive!($compare, v, merging_value);
                                if ord == Some(P::ORD) || ord == Some(Ordering::Equal) {
                                    v
                                } else {
                                    merging_value
                                }
                            })
                        }
                    }
                }
            }}
        }

        macro_rules! handle_boolean {
            () => {{
                let accs = downcast_any!(accs, mut AccBooleanColumn)?;
                let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        let accs = downcast_any!(accs, mut AccBooleanColumn)?;
                        let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
                        if let Some(merging_value) = merging_accs.value(merging_acc_idx) {
                            accs.update_value(acc_idx, merging_value, |v| {
                                if v.partial_cmp(&merging_value) == Some(P::ORD) {
                                    v
                                } else {
                                    merging_value
                                }
                            })
                        }
                    }
                }
            }};
        }

        macro_rules! handle_bytes {
            () => {{
                let accs = downcast_any!(accs, mut AccBytesColumn)?;
                let merging_accs = downcast_any!(merging_accs, mut AccBytesColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        let merging_value = merging_accs.take_value(merging_acc_idx);
                        if let Some(merging_value) = merging_value {
                            if let Some(w) = accs.value(acc_idx) {
                                if w.partial_cmp(&merging_value) == Some(P::ORD) {
                                    continue;
                                }
                            }
                            accs.set_value(acc_idx, Some(merging_value));
                        }
                    }
                }
            }};
        }
        match &self.data_type {
            DataType::Float32 => handle_primitive!(Float32Type, spark),
            DataType::Float64 => handle_primitive!(Float64Type, spark),
            _ => downcast_primitive! {
                (&self.data_type) => (handle_primitive),
                DataType::Boolean => handle_boolean!(),
                DataType::Utf8 | DataType::Binary => handle_bytes!(),
                DataType::Null => {},
                _ => {
                    let accs = downcast_any!(accs, mut AccScalarValueColumn)?;
                    let merging_accs = downcast_any!(merging_accs, mut AccScalarValueColumn)?;
                    idx_for_zipped! {
                        ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                            let merging_value = merging_accs.take_value(merging_acc_idx);
                            if merging_value.is_null() {
                                continue;
                            }
                            let w = accs.value(acc_idx);
                            if !w.is_null() && w.partial_cmp(&merging_value) == Some(P::ORD) {
                                continue;
                            }
                            accs.set_value(acc_idx, merging_value);
                        }
                    }
                }
            },
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        Ok(accs.freeze_to_arrays(acc_idx)?[0].clone())
    }
}

pub trait AggMaxMinParams: 'static + Send + Sync {
    const NAME: &'static str;
    const ORD: Ordering;
}

pub struct AggMaxParams;
pub struct AggMinParams;

impl AggMaxMinParams for AggMaxParams {
    const NAME: &'static str = "max";
    const ORD: Ordering = Ordering::Greater;
}

impl AggMaxMinParams for AggMinParams {
    const NAME: &'static str = "min";
    const ORD: Ordering = Ordering::Less;
}

#[cfg(test)]
mod tests {
    use datafusion::{common::ScalarValue, physical_expr::expressions::Literal};

    use super::*;

    fn result(agg: &dyn Agg, acc: &mut AccColumnRef) -> Result<f64> {
        let array = agg.final_merge(acc, IdxSelection::Single(0))?;
        Ok(downcast_any!(array, Float64Array)?.value(0))
    }

    #[test]
    fn float_ordering() {
        assert_eq!(spark_float_cmp(0.0_f64, -0.0), Ordering::Equal);
        assert_eq!(spark_float_cmp(f64::NAN, f64::NAN), Ordering::Equal);
        assert_eq!(
            spark_float_cmp(f64::from_bits(0xfff8_0000_0000_0001), f64::INFINITY),
            Ordering::Greater
        );
        assert_eq!(spark_float_cmp(0.0_f32, -0.0), Ordering::Equal);
        assert_eq!(
            spark_float_cmp(f32::from_bits(0xffc0_0001), f32::INFINITY),
            Ordering::Greater
        );
    }

    #[test]
    fn float_max_min_update_and_merge() -> Result<()> {
        let child: PhysicalExprRef = Arc::new(Literal::new(ScalarValue::Float64(None)));
        let max = AggMax::try_new(child.clone(), DataType::Float64)?;
        let min = AggMin::try_new(child, DataType::Float64)?;
        let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);

        let mut max_acc = max.create_acc_column(1);
        let max_values: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::INFINITY,
            negative_nan,
            f64::MAX,
        ]));
        max.partial_update(
            &mut max_acc,
            IdxSelection::Indices(&[0, 0, 0]),
            &[max_values],
            IdxSelection::Range(0, 3),
        )?;
        let mut max_merged = max.create_acc_column(1);
        max.partial_update(
            &mut max_merged,
            IdxSelection::Single(0),
            &[Arc::new(Float64Array::from(vec![f64::INFINITY]))],
            IdxSelection::Single(0),
        )?;
        max.partial_merge(
            &mut max_merged,
            IdxSelection::Single(0),
            &mut max_acc,
            IdxSelection::Single(0),
        )?;
        assert!(result(&max, &mut max_merged)?.is_nan());

        let mut min_acc = min.create_acc_column(1);
        let min_values: ArrayRef = Arc::new(Float64Array::from(vec![
            -1.0,
            f64::NEG_INFINITY,
            negative_nan,
        ]));
        min.partial_update(
            &mut min_acc,
            IdxSelection::Indices(&[0, 0, 0]),
            &[min_values],
            IdxSelection::Range(0, 3),
        )?;
        let mut min_merged = min.create_acc_column(1);
        min.partial_update(
            &mut min_merged,
            IdxSelection::Single(0),
            &[Arc::new(Float64Array::from(vec![f64::NAN]))],
            IdxSelection::Single(0),
        )?;
        min.partial_merge(
            &mut min_acc,
            IdxSelection::Single(0),
            &mut min_merged,
            IdxSelection::Single(0),
        )?;
        assert_eq!(result(&min, &mut min_acc)?, f64::NEG_INFINITY);

        let mut max_zero = max.create_acc_column(1);
        max.partial_update(
            &mut max_zero,
            IdxSelection::Indices(&[0, 0]),
            &[Arc::new(Float64Array::from(vec![-0.0, 0.0]))],
            IdxSelection::Range(0, 2),
        )?;
        assert_eq!(result(&max, &mut max_zero)?.to_bits(), (-0.0_f64).to_bits());

        let mut min_zero = min.create_acc_column(1);
        min.partial_update(
            &mut min_zero,
            IdxSelection::Indices(&[0, 0]),
            &[Arc::new(Float64Array::from(vec![0.0, -0.0]))],
            IdxSelection::Range(0, 2),
        )?;
        assert_eq!(result(&min, &mut min_zero)?.to_bits(), 0.0_f64.to_bits());
        Ok(())
    }
}
