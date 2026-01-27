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
    io::{Read, Write},
    sync::Arc,
};

use arrow::{
    array::*,
    datatypes::{DataType, *},
};
use auron_memmgr::spill::{SpillCompressedReader, SpillCompressedWriter};
use bitvec::{bitvec, vec::BitVec};
use byteorder::WriteBytesExt;
use datafusion::common::{Result, ScalarValue, utils::proxy::VecAllocExt};
use datafusion_ext_commons::{
    SliceAsRawBytes, UninitializedInit, df_execution_err, downcast_any,
    io::{read_len, read_scalar, write_len, write_scalar},
    scalar_value::scalar_value_heap_mem_size,
};
use smallvec::SmallVec;

use crate::{agg::agg::IdxSelection, idx_for, idx_with_iter};

pub trait AccColumn: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn resize(&mut self, len: usize);
    fn shrink_to_fit(&mut self);
    fn num_records(&self) -> usize;
    fn mem_used(&self) -> usize;
    fn freeze_to_arrays(&mut self, idx: IdxSelection<'_>) -> Result<Vec<ArrayRef>>;
    fn unfreeze_from_arrays(&mut self, arrays: &[ArrayRef]) -> Result<()>;
    fn spill(&mut self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()>;
    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()>;

    fn ensure_size(&mut self, idx: IdxSelection<'_>) {
        let idx_max_value = match idx {
            IdxSelection::Single(v) => v,
            IdxSelection::Indices(v) => v.iter().copied().max().unwrap_or(0),
            IdxSelection::IndicesU32(v) => v.iter().copied().max().unwrap_or(0) as usize,
            IdxSelection::Range(_begin, end) => end,
        };
        if idx_max_value >= self.num_records() {
            self.resize(idx_max_value + 1);
        }
    }
}

pub type AccColumnRef = Box<dyn AccColumn>;

pub type AccBytes = SmallVec<u8, 24>;
const _ACC_BYTES_SIZE_CHECKER: [(); 32] = [(); size_of::<AccBytes>()];

pub struct AccTable {
    cols: Vec<AccColumnRef>,
}

impl AccTable {
    pub fn new(cols: Vec<AccColumnRef>, num_records: usize) -> Self {
        assert!(cols.iter().all(|c| c.num_records() == num_records));
        Self { cols }
    }

    pub fn cols(&self) -> &[AccColumnRef] {
        &self.cols
    }

    pub fn cols_mut(&mut self) -> &mut [AccColumnRef] {
        &mut self.cols
    }

    pub fn resize(&mut self, num_records: usize) {
        self.cols.iter_mut().for_each(|c| c.resize(num_records));
    }

    pub fn shrink_to_fit(&mut self) {
        self.cols.iter_mut().for_each(|c| c.shrink_to_fit());
    }

    pub fn mem_size(&self) -> usize {
        self.cols.iter().map(|c| c.mem_used()).sum()
    }
}

pub struct AccBooleanColumn {
    valids: BitVec,
    values: BitVec,
}

impl AccBooleanColumn {
    pub fn new(num_records: usize) -> Self {
        Self {
            valids: bitvec![0; num_records],
            values: bitvec![0; num_records],
        }
    }

    pub fn value(&self, idx: usize) -> Option<bool> {
        if self.valids[idx] {
            Some(self.values[idx])
        } else {
            None
        }
    }

    pub fn set_value(&mut self, idx: usize, value: Option<bool>) {
        if let Some(value) = value {
            self.values.set(idx, value);
            self.valids.set(idx, true);
        } else {
            self.valids.set(idx, false);
        }
    }

    pub fn update_value(&mut self, idx: usize, default_value: bool, update: impl Fn(bool) -> bool) {
        if self.valids[idx] {
            let value = self.values[idx];
            self.values.set(idx, update(value));
        } else {
            self.values.set(idx, default_value);
            self.valids.set(idx, true);
        }
    }

    pub fn to_array(&self, idx: IdxSelection<'_>) -> Result<ArrayRef> {
        idx_with_iter!((idx @ idx) => {
            Ok(Arc::new(BooleanArray::from_iter(
                idx.map(|i| self.valids[i].then_some(self.values[i]))
            )))
        })
    }
}

impl AccColumn for AccBooleanColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        self.valids.resize(len, false);
        self.values.resize(len, false);
    }

    fn shrink_to_fit(&mut self) {
        self.valids.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.values.len()
    }

    fn mem_used(&self) -> usize {
        self.num_records() / 4 // 2 bits for each value
    }

    fn freeze_to_arrays(&mut self, idx: IdxSelection<'_>) -> Result<Vec<ArrayRef>> {
        let array = self.to_array(idx)?;
        Ok(vec![array])
    }

    fn unfreeze_from_arrays(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        let array = downcast_any!(&arrays[0], BooleanArray)?;
        self.values.clear();
        self.valids.clear();

        // fill values
        for i in 0..array.len() {
            self.values.push(array.value(i));
        }

        // fill valids
        if let Some(nb) = array.nulls() {
            for bit in nb {
                self.valids.push(bit);
            }
        }
        self.valids.resize(self.values.len(), true);
        Ok(())
    }

    fn spill(&mut self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        let mut buf = vec![];

        idx_for! {
             (idx in idx) => {
                 if self.valids[idx] {
                    buf.push(1 + self.values[idx] as u8);
                } else {
                    buf.push(0);
                }
             }
        }
        w.write_all(&buf)?;
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        self.resize(num_rows);
        let mut buf = Vec::uninitialized_init(num_rows);
        r.read_exact(&mut buf)?;
        for (i, v) in buf.into_iter().enumerate() {
            if v == 0 {
                self.valids.set(i, false);
            } else {
                self.valids.set(i, true);
                self.values.set(i, v - 1 != 0);
            }
        }
        Ok(())
    }
}

pub struct AccPrimColumn<T: ArrowNativeType> {
    values: Vec<T>,
    valids: BitVec,
    dt: DataType,
}

impl<T: ArrowNativeType> AccPrimColumn<T> {
    pub fn new(num_records: usize, dt: DataType) -> Self {
        Self {
            values: vec![T::default(); num_records],
            valids: bitvec![0; num_records],
            dt,
        }
    }

    pub fn value(&self, idx: usize) -> Option<T> {
        if self.valids[idx] {
            Some(self.values[idx])
        } else {
            None
        }
    }

    pub fn set_value(&mut self, idx: usize, value: Option<T>) {
        if let Some(value) = value {
            self.values[idx] = value;
            self.valids.set(idx, true);
        } else {
            self.valids.set(idx, false);
        }
    }

    pub fn update_value(&mut self, idx: usize, default_value: T, update: impl Fn(T) -> T) {
        if self.valids[idx] {
            self.values[idx] = update(self.values[idx]);
        } else {
            self.values[idx] = default_value;
            self.valids.set(idx, true);
        }
    }

    pub fn to_array(&self, dt: &DataType, idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let array: ArrayRef;

        macro_rules! primitive_helper {
            ($ty:ty) => {{
                type TNative = <$ty as ArrowPrimitiveType>::Native;
                let self_prim = downcast_any!(self, AccPrimColumn<TNative>)?;
                idx_with_iter!((idx @ idx) => {
                    array = Arc::new(PrimitiveArray::<$ty>::from_iter(
                        idx.map(|i| self_prim.valids[i].then_some(self_prim.values[i]))
                    ));
                })
            }};
        }
        downcast_primitive! {
            dt => (primitive_helper),
            other => return df_execution_err!("expected primitive type, got {other:?}"),
        }

        if let Ok(decimal_array) = downcast_any!(array, Decimal128Array) {
            return Ok(Arc::new(decimal_array.clone().with_data_type(dt.clone())));
        }
        Ok(array)
    }
}

impl<T: ArrowNativeType> AccColumn for AccPrimColumn<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        self.values.resize(len, T::default());
        self.valids.resize(len, false);
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.valids.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.values.len()
    }

    fn mem_used(&self) -> usize {
        self.values.allocated_size() + (self.valids.capacity() + 7) / 8
    }

    fn freeze_to_arrays(&mut self, idx: IdxSelection<'_>) -> Result<Vec<ArrayRef>> {
        let array = self.to_array(&self.dt, idx)?;
        Ok(vec![array])
    }

    fn unfreeze_from_arrays(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        let array_data = arrays[0].to_data();
        self.resize(0);

        // fill values
        let buffer = array_data.buffer::<T>(0);
        self.values = buffer[array_data.offset()..][..array_data.len()].to_vec();

        // fill valids
        if let Some(nb) = array_data.nulls() {
            for bit in nb {
                self.valids.push(bit);
            }
        }
        self.valids.resize(self.values.len(), true);
        Ok(())
    }

    fn spill(&mut self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        // write valids
        let mut bits: BitVec<u8> = BitVec::with_capacity(idx.len());
        idx_for! {
             (idx in idx) => {
                 bits.push(self.valids[idx]);
             }
        }
        let num_valids = bits.count_ones();
        write_len(num_valids, w)?;
        w.write_all(bits.as_raw_slice())?;

        // write values
        let mut values = Vec::with_capacity(num_valids);
        idx_for! {
            (idx in idx) => {
                if self.valids[idx] {
                   values.push(self.values[idx]);
                }
            }
        }
        w.write_all(values.as_raw_bytes())?;
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        let num_valids = read_len(r)?;

        // read valids
        let mut bits: BitVec<u8> = BitVec::repeat(false, num_rows);
        r.read_exact(bits.as_raw_mut_slice())?;
        self.valids.clear();
        self.valids.extend_from_bitslice(bits.as_bitslice());

        // read values
        self.values.resize(num_rows, T::default());
        let mut read_values: Vec<T> = Vec::uninitialized_init(num_valids);
        let mut read_value_pos = 0;
        r.read_exact(read_values.as_raw_bytes_mut())?;
        for i in self.valids.iter_ones() {
            self.values[i] = read_values[read_value_pos];
            read_value_pos += 1;
        }
        Ok(())
    }
}

pub struct AccBytesColumn {
    items: Vec<Option<AccBytes>>,
    heap_mem_used: usize,
    dt: DataType,
}

impl AccBytesColumn {
    pub fn new(num_records: usize, dt: DataType) -> Self {
        Self {
            items: vec![None; num_records],
            heap_mem_used: 0,
            dt,
        }
    }

    pub fn value(&self, idx: usize) -> Option<&AccBytes> {
        self.items[idx].as_ref()
    }

    pub fn take_value(&mut self, idx: usize) -> Option<AccBytes> {
        self.heap_mem_used -= self.item_heap_mem_used(idx);
        std::mem::take(&mut self.items[idx])
    }

    pub fn set_value(&mut self, idx: usize, value: Option<AccBytes>) {
        self.heap_mem_used -= self.item_heap_mem_used(idx);
        self.items[idx] = value;
        self.heap_mem_used += self.item_heap_mem_used(idx);
    }

    fn to_array(&self, idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let binary;

        idx_with_iter!((idx @ idx) => {
            binary = BinaryArray::from_iter(idx.map(|i| self.items[i].as_ref()));
        });
        match &self.dt {
            DataType::Utf8 => Ok(make_array(
                binary
                    .to_data()
                    .into_builder()
                    .data_type(DataType::Utf8)
                    .build()?,
            )),
            DataType::Binary => Ok(Arc::new(binary)),
            dt => df_execution_err!("expected string or binary type, got {dt:?}"),
        }
    }

    fn item_heap_mem_used(&self, idx: usize) -> usize {
        if let Some(v) = &self.items[idx]
            && v.spilled()
        {
            v.capacity()
        } else {
            0
        }
    }

    fn refresh_heap_mem_used(&mut self) {
        self.heap_mem_used = 0;
        for item in &self.items {
            if let Some(v) = item {
                if v.spilled() {
                    self.heap_mem_used += v.capacity();
                }
            }
        }
    }

    fn save_value(&self, idx: usize, w: &mut impl Write) -> Result<()> {
        if let Some(v) = &self.items[idx] {
            write_len(1 + v.len(), w)?;
            w.write_all(v)?;
        } else {
            w.write_u8(0)?;
        }
        Ok(())
    }

    fn load_value(&mut self, r: &mut impl Read) -> Result<()> {
        let read_len = read_len(r)?;
        if read_len == 0 {
            self.items.push(None);
        } else {
            let len = read_len - 1;
            let mut bytes = AccBytes::uninitialized_init(len);
            r.read_exact(bytes.as_mut())?;
            self.items.push(Some(bytes));
        }
        Ok(())
    }
}

impl AccColumn for AccBytesColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        if len > self.items.len() {
            self.items.resize(len, Default::default());
        } else {
            for idx in len..self.items.len() {
                self.heap_mem_used -= self.item_heap_mem_used(idx);
            }
            self.items.truncate(len);
        }
    }

    fn shrink_to_fit(&mut self) {
        self.items.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.items.len()
    }

    fn mem_used(&self) -> usize {
        self.heap_mem_used + self.items.allocated_size()
    }

    fn freeze_to_arrays(&mut self, idx: IdxSelection<'_>) -> Result<Vec<ArrayRef>> {
        let array = self.to_array(idx)?;
        Ok(vec![array])
    }

    fn unfreeze_from_arrays(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        let array_data = arrays[0].to_data();
        self.items.clear();

        // fill values
        let offset_buffer = array_data.buffer::<i32>(0);
        let data_buffer = array_data.buffer::<u8>(1);
        for i in 0..array_data.len() {
            if array_data.is_valid(i) {
                let offset_begin = offset_buffer[i] as usize;
                let offset_end = offset_buffer[i + 1] as usize;
                self.items.push(Some(AccBytes::from_slice(
                    &data_buffer[offset_begin..offset_end],
                )));
            } else {
                self.items.push(None);
            }
        }
        self.refresh_heap_mem_used();
        Ok(())
    }

    fn spill(&mut self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                self.save_value(idx, w)?;
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        for _ in 0..num_rows {
            self.load_value(r)?;
        }
        self.refresh_heap_mem_used();
        Ok(())
    }
}

pub struct AccScalarValueColumn {
    items: Vec<ScalarValue>,
    dt: DataType,
    null_value: ScalarValue,
    heap_mem_used: usize,
}

impl AccScalarValueColumn {
    pub fn new(dt: DataType, num_rows: usize) -> Self {
        let null_value = ScalarValue::try_from(&dt).expect("unsupported data type");
        Self {
            items: (0..num_rows).map(|_| null_value.clone()).collect(),
            dt,
            null_value,
            heap_mem_used: 0,
        }
    }

    pub fn to_array(&mut self, idx: IdxSelection<'_>) -> Result<ArrayRef> {
        idx_with_iter!((idx @ idx) => {
            ScalarValue::iter_to_array(idx.map(|i| {
                std::mem::replace(&mut self.items[i], self.null_value.clone())
            }))
        })
    }

    pub fn value(&self, idx: usize) -> &ScalarValue {
        &self.items[idx]
    }

    pub fn take_value(&mut self, idx: usize) -> ScalarValue {
        self.heap_mem_used -= scalar_value_heap_mem_size(&self.items[idx]);
        std::mem::replace(&mut self.items[idx], self.null_value.clone())
    }

    pub fn set_value(&mut self, idx: usize, value: ScalarValue) {
        self.heap_mem_used -= scalar_value_heap_mem_size(&self.items[idx]);
        self.items[idx] = value;
        self.heap_mem_used += scalar_value_heap_mem_size(&self.items[idx]);
    }
}

impl AccColumn for AccScalarValueColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        if len > self.items.len() {
            self.items.resize_with(len, || self.null_value.clone());
        } else {
            for idx in len..self.items.len() {
                self.heap_mem_used -= scalar_value_heap_mem_size(&self.items[idx]);
            }
            self.items.truncate(len);
        }
    }

    fn shrink_to_fit(&mut self) {
        self.items.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.items.len()
    }

    fn mem_used(&self) -> usize {
        self.heap_mem_used + self.items.allocated_size()
    }

    fn freeze_to_arrays(&mut self, idx: IdxSelection<'_>) -> Result<Vec<ArrayRef>> {
        let array = self.to_array(idx)?; // data type is not used
        Ok(vec![array])
    }

    fn unfreeze_from_arrays(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        let array = &arrays[0];

        self.items.clear();
        self.heap_mem_used = 0;

        for i in 0..array.len() {
            let scalar = ScalarValue::try_from_array(array, i)?;
            self.heap_mem_used += scalar_value_heap_mem_size(&scalar);
            self.items.push(scalar);
        }
        Ok(())
    }

    fn spill(&mut self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                let scalar = self.take_value(idx);
                write_scalar(&scalar, true, w)?;
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        self.items.clear();
        self.heap_mem_used = 0;

        for _ in 0..num_rows {
            let scalar = read_scalar(r, &self.dt, true)?;
            self.heap_mem_used += scalar_value_heap_mem_size(&scalar);
            self.items.push(scalar);
        }
        Ok(())
    }
}

pub fn create_acc_generic_column(dt: DataType, num_rows: usize) -> AccColumnRef {
    macro_rules! primitive_helper {
        ($t:ty) => {
            Box::new(AccPrimColumn::<<$t as ArrowPrimitiveType>::Native>::new(
                num_rows, dt,
            ))
        };
    }
    downcast_primitive! {
        dt => (primitive_helper),
        DataType::Boolean => Box::new(AccBooleanColumn::new(num_rows)),
        DataType::Utf8 | DataType::Binary => Box::new(AccBytesColumn::new(num_rows, dt)),
        other => Box::new(AccScalarValueColumn::new(other, num_rows)),
    }
}
