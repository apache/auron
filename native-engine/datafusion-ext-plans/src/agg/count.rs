// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::downcast_any;

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef, AccPrimColumn},
        agg::{Agg, IdxSelection},
    },
    idx_for_zipped,
};

pub struct AggCount {
    children: Vec<PhysicalExprRef>,
    data_type: DataType,
}

impl AggCount {
    pub fn try_new(children: Vec<PhysicalExprRef>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);
        Ok(Self {
            children,
            data_type,
        })
    }
}

impl Debug for AggCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count({:?})", self.children)
    }
}

impl Agg for AggCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<PhysicalExprRef> {
        self.children.clone()
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs.clone(),
            self.data_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn create_acc_column(&self, num_rows: usize) -> Box<dyn AccColumn> {
        Box::new(AccPrimColumn::<i64>::new(num_rows, DataType::Int64))
    }

    fn acc_array_data_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccPrimColumn<i64>)?;
        accs.ensure_size(acc_idx);

        idx_for_zipped! {
            ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                let add = partial_args
                    .iter()
                    .all(|arg| arg.is_valid(partial_arg_idx)) as i64;
                accs.set_value(acc_idx, Some(accs.value(acc_idx).unwrap_or(0) + add));
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
        let accs = downcast_any!(accs, mut AccPrimColumn<i64>)?;
        let merging_accs = downcast_any!(merging_accs, mut AccPrimColumn<i64>)?;
        accs.ensure_size(acc_idx);

        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                let v = match (accs.value(acc_idx), merging_accs.value(merging_acc_idx)) {
                    (Some(a), Some(b)) => Some(a + b),
                    (Some(a), _) => Some(a),
                    (_, Some(b)) => Some(b),
                    _ => Some(0),
                };
                accs.set_value(acc_idx, v);
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        Ok(accs.freeze_to_arrays(acc_idx)?[0].clone())
    }
}
