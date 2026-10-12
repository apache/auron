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
            ($array:expr) => {{
                let partial_arg = $array;
                let accs = downcast_any!(accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                     ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                         if partial_arg.is_valid(partial_arg_idx) {
                             let partial_value = partial_arg.value(partial_arg_idx);
                             accs.update_value(acc_idx, partial_value, |v| {
                                 // Arrow's comparison orders NaN above ordinary values.
                                 if v.partial_cmp(&partial_value)
                                     .unwrap_or_else(|| v.compare(partial_value)) == P::ORD {
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

        downcast_primitive_array! {
            partial_arg => handle_primitive!(partial_arg),
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
            ($ty:ty) => {{
                type TNative = <$ty as ArrowPrimitiveType>::Native;
                let accs = downcast_any!(accs, mut AccPrimColumn<TNative>)?;
                let merging_accs = downcast_any!(merging_accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if let Some(merging_value) = merging_accs.value(merging_acc_idx) {
                            accs.update_value(acc_idx, merging_value, |v| {
                                if v.partial_cmp(&merging_value)
                                    .unwrap_or_else(|| v.compare(merging_value)) == P::ORD {
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
        downcast_primitive! {
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
    use datafusion::{common::ScalarValue, physical_expr::expressions::Column};

    use super::*;

    fn check_min_max_results(
        input: ArrayRef,
        expected_min: ScalarValue,
        expected_max: ScalarValue,
    ) -> Result<()> {
        let data_type = input.data_type().clone();
        let child: PhysicalExprRef = Arc::new(Column::new("v", 0));
        let min: Arc<dyn Agg> = Arc::new(AggMin::try_new(child.clone(), data_type.clone())?);
        let max: Arc<dyn Agg> = Arc::new(AggMax::try_new(child, data_type)?);
        for (agg, expected) in [(min, expected_min), (max, expected_max)] {
            for merge in [false, true] {
                let mut acc = agg.create_acc_column(1);
                for index in 0..input.len() {
                    let mut partial = agg.create_acc_column(1);
                    agg.partial_update(
                        if merge { &mut partial } else { &mut acc },
                        IdxSelection::Single(0),
                        &[input.clone()],
                        IdxSelection::Single(index),
                    )?;
                    if merge {
                        agg.partial_merge(
                            &mut acc,
                            IdxSelection::Single(0),
                            &mut partial,
                            IdxSelection::Single(0),
                        )?;
                    }
                }
                let result = agg.final_merge(&mut acc, IdxSelection::Single(0))?;
                let actual = ScalarValue::try_from_array(result.as_ref(), 0)?;
                match (&actual, &expected) {
                    (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(e)))
                        if e.is_nan() =>
                    {
                        assert!(a.is_nan());
                    }
                    (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(e)))
                        if e.is_nan() =>
                    {
                        assert!(a.is_nan());
                    }
                    _ => assert_eq!(actual, expected),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn float_min_max_nan_update_and_merge() -> Result<()> {
        let nan = f64::NAN;
        let inf = f64::INFINITY;
        for data_type in [DataType::Float32, DataType::Float64] {
            for (values, min, max) in [
                (vec![Some(nan), None, Some(1.0)], Some(1.0), Some(nan)),
                (vec![Some(1.0), None, Some(nan)], Some(1.0), Some(nan)),
                (vec![Some(-nan), Some(1.0)], Some(1.0), Some(nan)),
                (vec![Some(nan), Some(-nan)], Some(nan), Some(nan)),
                (vec![None, None], None, None),
                (vec![Some(inf), Some(-inf)], Some(-inf), Some(inf)),
                (vec![Some(0.0), Some(-0.0)], Some(-0.0), Some(-0.0)),
            ] {
                let input = arrow::compute::cast(&Float64Array::from(values), &data_type)?;
                let expected =
                    arrow::compute::cast(&Float64Array::from(vec![min, max]), &data_type)?;
                check_min_max_results(
                    input,
                    ScalarValue::try_from_array(expected.as_ref(), 0)?,
                    ScalarValue::try_from_array(expected.as_ref(), 1)?,
                )?;
            }
        }
        Ok(())
    }
}
