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
    io::{Cursor, Read, Write},
    mem::{size_of, size_of_val},
};

use arrow::datatypes::DataType;
use datafusion::{
    common::{Result, ScalarValue},
    parquet::data_type::AsBytes,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any,
    io::{read_bytes_slice, read_len, read_scalar, write_len, write_scalar},
    slim_bytes::SlimBytes,
};
use hashbrown::HashSet;
use slimmer_box::SlimmerBox;
use smallvec::SmallVec;

pub type DynVal = Option<Box<dyn AggDynValue>>;

const ACC_STORE_BLOCK_SIZE: usize = 65536;

pub struct AccStore {
    initial: OwnedAccumStateRow,
    num_accs: usize,
    fixed_store: Vec<Vec<u8>>,
    dyn_store: Vec<Vec<DynVal>>,
}

impl AccStore {
    pub fn new(initial: OwnedAccumStateRow) -> Self {
        Self {
            initial,
            num_accs: 0,
            fixed_store: vec![],
            dyn_store: vec![],
        }
    }

    pub fn clear(&mut self) {
        self.num_accs = 0;
        self.fixed_store.iter_mut().for_each(|s| s.clear());
        self.dyn_store.iter_mut().for_each(|s| s.clear());
    }

    pub fn clear_and_free(&mut self) {
        self.num_accs = 0;
        self.fixed_store = vec![];
        self.dyn_store = vec![];
    }

    pub fn mem_size(&self) -> usize {
        self.num_accs * (self.fixed_len() + self.dyns_len() * size_of::<DynVal>() + 32)
    }

    pub fn new_acc(&mut self) -> u32 {
        let initial = unsafe {
            // safety: ignore borrow checker
            std::mem::transmute::<_, &OwnedAccumStateRow>(&self.initial)
        };
        self.new_acc_from(initial)
    }

    pub fn new_acc_from(&mut self, acc: &impl AccumStateRow) -> u32 {
        let idx = self.num_accs;
        self.num_accs += 1;
        if self.num_required_blocks() >= self.fixed_store.len() {
            // add a new block
            // reserve a whole block to avoid reallocation
            self.fixed_store
                .push(Vec::with_capacity(ACC_STORE_BLOCK_SIZE * self.fixed_len()));
            self.dyn_store
                .push(Vec::with_capacity(ACC_STORE_BLOCK_SIZE * self.dyns_len()));
        }
        let idx1 = idx / ACC_STORE_BLOCK_SIZE;
        self.fixed_store[idx1].extend_from_slice(acc.fixed());
        self.dyn_store[idx1].extend(
            acc.dyns()
                .iter()
                .map(|v| v.as_ref().map(|v| v.clone_boxed())),
        );
        idx as u32
    }

    pub fn get(&self, idx: u32) -> RefAccumStateRow {
        let idx1 = idx as usize / ACC_STORE_BLOCK_SIZE;
        let idx2 = idx as usize % ACC_STORE_BLOCK_SIZE;
        let fixed_ptr = self.fixed_store[idx1][idx2 * self.fixed_len()..].as_ptr() as *mut u8;
        let dyns_ptr = self.dyn_store[idx1][idx2 * self.dyns_len()..].as_ptr() as *mut DynVal;
        unsafe {
            // safety: skip borrow/mutable checking
            RefAccumStateRow {
                fixed: std::slice::from_raw_parts_mut(fixed_ptr, self.fixed_len()),
                dyns: std::slice::from_raw_parts_mut(dyns_ptr, self.dyns_len()),
            }
        }
    }

    fn num_required_blocks(&self) -> usize {
        (self.num_accs + ACC_STORE_BLOCK_SIZE - 1) / ACC_STORE_BLOCK_SIZE
    }

    fn fixed_len(&self) -> usize {
        self.initial.fixed.len()
    }

    fn dyns_len(&self) -> usize {
        self.initial.dyns.len()
    }
}

pub struct OwnedAccumStateRow {
    fixed: SlimBytes,
    dyns: SlimmerBox<[DynVal]>,
}

impl OwnedAccumStateRow {
    pub fn as_mut<'a>(&'a mut self) -> RefAccumStateRow<'a> {
        RefAccumStateRow {
            fixed: &mut self.fixed,
            dyns: &mut self.dyns,
        }
    }
}

impl Clone for OwnedAccumStateRow {
    fn clone(&self) -> Self {
        Self {
            fixed: self.fixed.clone(),
            dyns: SlimmerBox::from_box(
                self.dyns
                    .iter()
                    .map(|v| v.as_ref().map(|x| x.clone_boxed()))
                    .collect::<Box<[DynVal]>>(),
            ),
        }
    }
}

impl AccumStateRow for OwnedAccumStateRow {
    fn fixed(&self) -> &[u8] {
        &self.fixed
    }

    fn fixed_mut(&mut self) -> &mut [u8] {
        &mut self.fixed
    }

    fn dyns(&self) -> &[DynVal] {
        &self.dyns
    }

    fn dyns_mut(&mut self) -> &mut [DynVal] {
        &mut self.dyns
    }
}

pub struct RefAccumStateRow<'a> {
    fixed: &'a mut [u8],
    dyns: &'a mut [DynVal],
}

impl<'a> AccumStateRow for RefAccumStateRow<'a> {
    fn fixed(&self) -> &[u8] {
        self.fixed
    }

    fn fixed_mut(&mut self) -> &mut [u8] {
        self.fixed
    }

    fn dyns(&self) -> &[DynVal] {
        self.dyns
    }

    fn dyns_mut(&mut self) -> &mut [DynVal] {
        self.dyns
    }
}

pub trait AccumStateRow {
    fn fixed(&self) -> &[u8];
    fn fixed_mut(&mut self) -> &mut [u8];
    fn dyns(&self) -> &[DynVal];
    fn dyns_mut(&mut self) -> &mut [DynVal];

    fn mem_size(&self) -> usize {
        let dyns_mem_size = self
            .dyns()
            .iter()
            .map(|v| size_of_val(v) + v.as_ref().map(|x| x.mem_size()).unwrap_or_default())
            .sum::<usize>();
        self.fixed().len() + dyns_mem_size
    }

    fn is_fixed_valid(&self, addr: AccumStateValAddr) -> bool {
        let idx = addr.fixed_valid_idx();
        self.fixed()[self.fixed().len() - 1 - idx / 8] & (1 << (idx % 8)) != 0
    }

    fn set_fixed_valid(&mut self, addr: AccumStateValAddr, valid: bool) {
        let idx = addr.fixed_valid_idx();
        let fixed_len = self.fixed().len();
        self.fixed_mut()[fixed_len - 1 - idx / 8] |= (valid as u8) << (idx % 8);
    }

    fn fixed_value<T: Sized + Copy>(&self, addr: AccumStateValAddr) -> T {
        let offset = addr.fixed_offset();
        let tptr = self.fixed()[offset..][..size_of::<T>()].as_ptr() as *const T;
        unsafe { std::ptr::read_unaligned(tptr) }
    }

    fn set_fixed_value<T: Sized + Copy>(&mut self, addr: AccumStateValAddr, v: T) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed_mut()[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe {
            std::ptr::write_unaligned(tptr, v);
        }
    }

    fn update_fixed_value<T: Sized + Copy>(
        &mut self,
        addr: AccumStateValAddr,
        updater: impl Fn(T) -> T,
    ) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed_mut()[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe { std::ptr::write_unaligned(tptr, updater(std::ptr::read_unaligned(tptr))) }
    }

    fn dyn_value(&mut self, addr: AccumStateValAddr) -> &DynVal {
        &self.dyns_mut()[addr.dyn_idx()]
    }

    fn dyn_value_mut(&mut self, addr: AccumStateValAddr) -> &mut DynVal {
        &mut self.dyns_mut()[addr.dyn_idx()]
    }

    fn load(&mut self, mut r: impl Read, dyn_loders: &[LoadFn]) -> Result<()> {
        r.read_exact(&mut self.fixed_mut())?;
        let dyns = self.dyns_mut();
        if !dyns.is_empty() {
            let mut reader = LoadReader(Box::new(r));
            for (v, load) in dyns.iter_mut().zip(dyn_loders) {
                *v = load(&mut reader)?;
            }
        }
        Ok(())
    }

    fn load_from_bytes(&mut self, bytes: &[u8], dyn_loaders: &[LoadFn]) -> Result<()> {
        self.load(Cursor::new(bytes), dyn_loaders)
    }

    fn save(&mut self, mut w: impl Write, dyn_savers: &[SaveFn]) -> Result<()> {
        w.write_all(&self.fixed())?;
        let dyns = self.dyns_mut();
        if !dyns.is_empty() {
            let mut writer = SaveWriter(Box::new(&mut w));
            for (v, save) in dyns.iter_mut().zip(dyn_savers) {
                save(&mut writer, std::mem::take(v))?;
            }
        }
        Ok(())
    }

    fn save_to_bytes(&mut self, dyn_savers: &[SaveFn]) -> Result<SlimBytes> {
        let mut bytes = vec![];
        self.save(&mut bytes, dyn_savers)?;
        Ok(bytes.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccumInitialValue {
    Scalar(ScalarValue),
    DynList(DataType),
    DynSet(DataType),
}

pub fn create_acc_from_initial_value(
    values: &[AccumInitialValue],
) -> Result<(OwnedAccumStateRow, Box<[AccumStateValAddr]>)> {
    let mut fixed_count = 0;
    let mut fixed_valids = vec![];
    let mut fixed: Vec<u8> = vec![];
    let mut dyns: Vec<DynVal> = vec![];
    let mut addrs: Vec<AccumStateValAddr> = vec![];

    macro_rules! handle_fixed {
        ($v:expr, $nbytes:expr) => {{
            addrs.push(AccumStateValAddr::new_fixed(fixed_count, fixed.len()));
            if fixed_count % 8 == 0 {
                fixed_valids.push(0);
            }
            match $v {
                Some(v) => {
                    fixed_valids[fixed_count / 8] |= 1 << (fixed_count % 8);
                    fixed.extend(v.to_ne_bytes());
                }
                None => {
                    fixed.extend(&[0; $nbytes]);
                }
            }
            fixed_count += 1;
        }};
    }
    for value in values {
        match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => handle_fixed!(None::<u8>, 0),
                ScalarValue::Boolean(v) => handle_fixed!(v.map(|x| x as u8), 1),
                ScalarValue::Float32(v) => handle_fixed!(v, 4),
                ScalarValue::Float64(v) => handle_fixed!(v, 8),
                ScalarValue::Decimal128(v, ..) => handle_fixed!(v, 16),
                ScalarValue::Int8(v) => handle_fixed!(v, 1),
                ScalarValue::Int16(v) => handle_fixed!(v, 2),
                ScalarValue::Int32(v) => handle_fixed!(v, 4),
                ScalarValue::Int64(v) => handle_fixed!(v, 8),
                ScalarValue::UInt8(v) => handle_fixed!(v, 1),
                ScalarValue::UInt16(v) => handle_fixed!(v, 2),
                ScalarValue::UInt32(v) => handle_fixed!(v, 4),
                ScalarValue::UInt64(v) => handle_fixed!(v, 8),
                ScalarValue::Date32(v) => handle_fixed!(v, 4),
                ScalarValue::Date64(v) => handle_fixed!(v, 8),
                ScalarValue::TimestampSecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampMillisecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampMicrosecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampNanosecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::Utf8(v) => {
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(v.as_ref().map(|s| {
                        addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                        let v: Box<dyn AggDynValue> = Box::new(AggDynStr::from_str(s));
                        v
                    }));
                }
                ScalarValue::Binary(v) => {
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(v.as_ref().map(|s| {
                        addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                        let v: Box<dyn AggDynValue> = Box::new(AggDynBinary::from_slice(s));
                        v
                    }));
                }
                other => {
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(match other {
                        v if v.is_null() => None,
                        v => Some(Box::new(AggDynScalar::new(v.clone()))),
                    });
                }
            },
            AccumInitialValue::DynList(_dt) => {
                addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                dyns.push(Some(Box::new(AggDynList::default())));
            }
            AccumInitialValue::DynSet(_dt) => {
                addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                dyns.push(Some(Box::new(AggDynSet::default())));
            }
        }
    }

    // reverse fixed_valids and append it to fixed, so no need to change addrs
    fixed_valids.reverse();
    fixed.extend(fixed_valids);

    let acc = OwnedAccumStateRow {
        fixed: fixed.into(),
        dyns: SlimmerBox::from_box(dyns.into()),
    };
    Ok((acc, addrs.into()))
}

pub struct LoadReader<'a>(pub Box<dyn Read + 'a>);
pub struct SaveWriter<'a>(pub Box<dyn Write + 'a>);
pub type LoadFn = Box<dyn Fn(&mut LoadReader) -> Result<DynVal> + Send + Sync>;
pub type SaveFn = Box<dyn Fn(&mut SaveWriter, DynVal) -> Result<()> + Send + Sync>;

pub fn create_dyn_loaders_from_initial_value(values: &[AccumInitialValue]) -> Result<Vec<LoadFn>> {
    let mut loaders: Vec<LoadFn> = vec![];
    for value in values {
        let loader: LoadFn = match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => continue,
                ScalarValue::Boolean(_) => continue,
                ScalarValue::Float32(_) => continue,
                ScalarValue::Float64(_) => continue,
                ScalarValue::Decimal128(_, ..) => continue,
                ScalarValue::Int8(_) => continue,
                ScalarValue::Int16(_) => continue,
                ScalarValue::Int32(_) => continue,
                ScalarValue::Int64(_) => continue,
                ScalarValue::UInt8(_) => continue,
                ScalarValue::UInt16(_) => continue,
                ScalarValue::UInt32(_) => continue,
                ScalarValue::UInt64(_) => continue,
                ScalarValue::Date32(_) => continue,
                ScalarValue::Date64(_) => continue,
                ScalarValue::TimestampSecond(..) => continue,
                ScalarValue::TimestampMillisecond(..) => continue,
                ScalarValue::TimestampMicrosecond(..) => continue,
                ScalarValue::TimestampNanosecond(..) => continue,
                ScalarValue::Utf8(_) => Box::new(|r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let s = read_bytes_slice(&mut r.0, n - 1)?;
                            let v = String::from_utf8_lossy(&s);
                            Some(Box::new(AggDynStr::from_str(&v)))
                        }
                    })
                }),
                ScalarValue::Binary(_) => Box::new(|r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let v = read_bytes_slice(&mut r.0, n - 1)?;
                            Some(Box::new(AggDynBinary::new(SlimBytes::from(v))))
                        }
                    })
                }),
                other => {
                    let dt = other.get_datatype();
                    Box::new(move |r: &mut LoadReader| {
                        Ok(Some(Box::new(AggDynScalar::new(read_scalar(
                            &mut r.0, &dt,
                        )?))))
                    })
                }
            },
            AccumInitialValue::DynList(dt) => {
                let dt = dt.clone();
                Box::new(move |r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let data_len = n - 1;
                            let mut load_vec: SmallVec<[ScalarValue; 4]> = SmallVec::new();
                            for _i in 0..data_len {
                                load_vec.push(read_scalar(&mut r.0, &dt)?);
                            }
                            Some(Box::new(AggDynList { values: load_vec }))
                        }
                    })
                })
            }
            AccumInitialValue::DynSet(dt) => {
                let dt = dt.clone();
                Box::new(move |r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let vec_len = n - 1;
                            let mut scalar_vec: SmallVec<[ScalarValue; 4]> = SmallVec::new();
                            for _i in 0..vec_len {
                                scalar_vec.push(read_scalar(&mut r.0, &dt)?);
                            }
                            Some(Box::new(AggDynSet {
                                values: OptimizedSet::SmallVec(scalar_vec),
                            }))
                        }
                    })
                })
            }
        };
        loaders.push(loader);
    }
    Ok(loaders)
}

pub fn create_dyn_savers_from_initial_value(values: &[AccumInitialValue]) -> Result<Vec<SaveFn>> {
    let mut savers: Vec<SaveFn> = vec![];
    for value in values {
        let saver = match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => continue,
                ScalarValue::Boolean(_) => continue,
                ScalarValue::Float32(_) => continue,
                ScalarValue::Float64(_) => continue,
                ScalarValue::Decimal128(_, ..) => continue,
                ScalarValue::Int8(_) => continue,
                ScalarValue::Int16(_) => continue,
                ScalarValue::Int32(_) => continue,
                ScalarValue::Int64(_) => continue,
                ScalarValue::UInt8(_) => continue,
                ScalarValue::UInt16(_) => continue,
                ScalarValue::UInt32(_) => continue,
                ScalarValue::UInt64(_) => continue,
                ScalarValue::Date32(_) => continue,
                ScalarValue::Date64(_) => continue,
                ScalarValue::TimestampSecond(..) => continue,
                ScalarValue::TimestampMillisecond(..) => continue,
                ScalarValue::TimestampMicrosecond(..) => continue,
                ScalarValue::TimestampNanosecond(..) => continue,
                ScalarValue::Utf8(_) => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        match v {
                            None => write_len(0, &mut w.0)?,
                            Some(v) => {
                                let s = downcast_any!(v, AggDynStr)?;
                                write_len(s.value().as_bytes().len() + 1, &mut w.0)?;
                                w.0.write_all(s.value().as_bytes())?;
                            }
                        }
                        Ok(())
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
                ScalarValue::Binary(_) => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        match v {
                            None => write_len(0, &mut w.0)?,
                            Some(v) => {
                                let s = downcast_any!(v, AggDynBinary)?;
                                write_len(s.value().as_bytes().len() + 1, &mut w.0)?;
                                w.0.write_all(s.value().as_bytes())?;
                            }
                        }
                        Ok(())
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
                _other => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        if let Some(v) = v {
                            write_scalar(&downcast_any!(v, AggDynScalar)?.value, &mut w.0)
                        } else {
                            write_scalar(&ScalarValue::Int32(None), &mut w.0)
                        }
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
            },
            AccumInitialValue::DynList(_dt) => {
                fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                    match v {
                        None => write_len(0, &mut w.0)?,
                        Some(v) => {
                            let list = v
                                .as_any_boxed()
                                .downcast::<AggDynList>()
                                .or_else(|_| df_execution_err!("error downcasting to AggDynList"))?
                                .into_values();
                            write_len(list.len() + 1, &mut w.0)?;
                            for v in list {
                                write_scalar(&v, &mut w.0)?;
                            }
                        }
                    }
                    Ok(())
                }
                let f: SaveFn = Box::new(f);
                f
            }
            AccumInitialValue::DynSet(_dt) => {
                fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                    match v {
                        None => write_len(0, &mut w.0)?,
                        Some(v) => {
                            let set = v
                                .as_any_boxed()
                                .downcast::<AggDynSet>()
                                .or_else(|_| df_execution_err!("error downcasting to AggDynSet"))?
                                .into_values();

                            match set {
                                OptimizedSet::SmallVec(vec) => {
                                    write_len(vec.len() + 1, &mut w.0)?;
                                    for v in vec {
                                        write_scalar(&v, &mut w.0)?;
                                    }
                                }
                                OptimizedSet::Set(set) => {
                                    write_len(set.len() + 1, &mut w.0)?;
                                    for v in set {
                                        write_scalar(&v, &mut w.0)?;
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                }
                let f: SaveFn = Box::new(f);
                f
            }
        };
        savers.push(saver);
    }
    Ok(savers)
}

#[allow(clippy::borrowed_box)]
pub trait AggDynValue: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;
    fn mem_size(&self) -> usize;
    fn clone_boxed(&self) -> Box<dyn AggDynValue>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynScalar {
    pub value: ScalarValue,
}

#[allow(clippy::borrowed_box)]
impl AggDynScalar {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    pub fn into_value(self) -> ScalarValue {
        self.value
    }
}

impl AggDynValue for AggDynScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value.size() - size_of_val(&self.value)
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynBinary {
    pub value: SlimBytes,
}

#[allow(clippy::borrowed_box)]
impl AggDynBinary {
    pub fn new(value: SlimBytes) -> Self {
        Self { value }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Self::new(SlimBytes::from(slice))
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub fn into_value(self) -> SlimBytes {
        self.value
    }
}

impl AggDynValue for AggDynBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value().as_bytes().len()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynStr {
    value: SlimmerBox<str>,
}

#[allow(clippy::borrowed_box)]
impl AggDynStr {
    pub fn new(value: SlimmerBox<str>) -> Self {
        Self { value }
    }

    pub fn from_str(v: &str) -> Self {
        Self::new(SlimmerBox::from_box(v.to_owned().into()))
    }

    pub fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub fn into_value(self) -> SlimmerBox<str> {
        self.value
    }
}

impl AggDynValue for AggDynStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value().as_bytes().len()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynList {
    pub values: SmallVec<[ScalarValue; 4]>,
}

impl AggDynList {
    pub fn append(&mut self, value: ScalarValue) {
        self.values.push(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.values.append(&mut other.values);
    }

    pub fn values(&self) -> &[ScalarValue] {
        self.values.as_slice()
    }

    pub fn into_values(self) -> SmallVec<[ScalarValue; 4]> {
        self.values
    }
}

impl AggDynValue for AggDynList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        let spilled_size = if self.values.spilled() {
            self.values.capacity() * (1 + size_of::<ScalarValue>())
        } else {
            0
        };
        let mem_size = size_of::<Self>()
            + self
                .values
                .iter()
                .map(|sv| sv.size() - size_of_val(sv))
                .sum::<usize>()
            + spilled_size;
        mem_size
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynSet {
    pub values: OptimizedSet,
}

impl AggDynSet {
    pub fn append(&mut self, value: ScalarValue) {
        match &mut self.values {
            OptimizedSet::SmallVec(vec) => {
                if vec.len() < vec.inline_size() {
                    vec.push(value);
                } else {
                    let mut value_set = HashSet::from_iter(std::mem::take(vec).into_iter());
                    value_set.insert(value);
                    self.values = OptimizedSet::Set(value_set);
                }
            }
            OptimizedSet::Set(value_set) => {
                value_set.insert(value);
            }
        }
    }

    pub fn merge(&mut self, other: &mut Self) {
        match (&mut self.values, &mut other.values) {
            (OptimizedSet::SmallVec(vec1), OptimizedSet::SmallVec(vec2)) => {
                if vec1.len() + vec2.len() <= vec1.inline_size() {
                    vec1.append(vec2);
                } else {
                    let new_set = HashSet::from_iter(
                        std::mem::take(vec1).into_iter().chain(std::mem::take(vec2)),
                    );
                    self.values = OptimizedSet::Set(new_set);
                }
            }
            (OptimizedSet::SmallVec(vec), OptimizedSet::Set(set)) => {
                set.extend(std::mem::take(vec).into_iter());
                self.values = OptimizedSet::Set(std::mem::take(set));
            }
            (OptimizedSet::Set(set), OptimizedSet::SmallVec(vec)) => {
                set.extend(std::mem::take(vec).into_iter());
            }
            (OptimizedSet::Set(set1), OptimizedSet::Set(set2)) => {
                set1.extend(std::mem::take(set2).into_iter());
            }
        }
    }

    pub fn values(&self) -> &OptimizedSet {
        &self.values
    }

    pub fn into_values(self) -> OptimizedSet {
        self.values
    }
}

impl AggDynValue for AggDynSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.values.mem_size() - size_of_val(&self.values)
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum OptimizedSet {
    SmallVec(SmallVec<[ScalarValue; 4]>),
    Set(HashSet<ScalarValue>),
}

impl Default for OptimizedSet {
    fn default() -> Self {
        OptimizedSet::SmallVec(SmallVec::default())
    }
}

impl OptimizedSet {
    fn mem_size(&self) -> usize {
        match self {
            OptimizedSet::SmallVec(vec) => {
                size_of::<Self>()
                    + vec
                        .iter()
                        .map(|sv| sv.size() - size_of_val(sv))
                        .sum::<usize>()
            }
            OptimizedSet::Set(hash_set) => {
                size_of::<Self>()
                    + hash_set.capacity() * size_of::<ScalarValue>()
                    + hash_set
                        .iter()
                        .map(|sv| sv.size() - size_of_val(sv))
                        .sum::<usize>()
            }
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct AccumStateValAddr(u64);

impl AccumStateValAddr {
    #[inline]
    fn new_fixed(valid_idx: usize, offset: usize) -> Self {
        Self((valid_idx as u64) << 32 | (offset as u64))
    }

    #[inline]
    fn new_dyn(idx: usize) -> Self {
        Self((idx as u64) | 0x8000_0000_0000_0000)
    }
    #[inline]
    fn fixed_offset(&self) -> usize {
        (self.0 & 0x0000_0000_ffff_ffff) as usize
    }

    #[inline]
    fn fixed_valid_idx(&self) -> usize {
        ((self.0 & 0x7fff_ffff_0000_0000) >> 32) as usize
    }

    #[inline]
    fn dyn_idx(&self) -> usize {
        (self.0 & 0x7fff_ffff_ffff_ffff) as usize
    }
}

#[cfg(test)]
mod test {
    use std::{io::Cursor, sync::Arc};

    use arrow::datatypes::{DataType, Field, Fields};
    use datafusion::common::{Result, ScalarValue};
    use datafusion_ext_commons::downcast_any;
    use smallvec::SmallVec;

    use crate::agg::acc::{
        create_acc_from_initial_value, create_dyn_loaders_from_initial_value,
        create_dyn_savers_from_initial_value, AccumInitialValue, AccumStateRow, AggDynList,
        AggDynSet, AggDynStr, LoadReader, OptimizedSet, SaveWriter,
    };

    #[test]
    fn test_dyn_list() {
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let l0 = ScalarValue::List(
            Some(vec![
                ScalarValue::from(1i32),
                ScalarValue::from(2i32),
                ScalarValue::from(3i32),
            ]),
            Arc::new(Field::new("item", DataType::Int32, true)),
        );

        let l1 = ScalarValue::List(
            Some(vec![ScalarValue::from(4i32), ScalarValue::Int32(None)]),
            Arc::new(Field::new("item", DataType::Int32, true)),
        );

        let l2 = ScalarValue::List(None, Arc::new(Field::new("item", DataType::Int32, true)));

        let loaders = create_dyn_loaders_from_initial_value(&[AccumInitialValue::DynList(
            DataType::List(list_field.clone()),
        )])
        .unwrap();
        let savers = create_dyn_savers_from_initial_value(&[AccumInitialValue::DynList(
            DataType::List(list_field.clone()),
        )])
        .unwrap();
        let mut dyn_list = AggDynList::default();
        dyn_list.append(l0.clone());
        dyn_list.append(l1.clone());
        dyn_list.append(l2.clone());

        let mut buf = vec![];
        savers[0](
            &mut SaveWriter(Box::new(&mut buf)),
            Some(Box::new(dyn_list)),
        )
        .unwrap();

        let dyn_list = loaders[0](&mut LoadReader(Box::new(Cursor::new(&buf)))).unwrap();
        assert_eq!(
            downcast_any!(dyn_list.unwrap(), AggDynList)
                .unwrap()
                .values(),
            &[l0.clone(), l1.clone(), l2.clone(),]
        );
    }

    #[test]
    fn test_dyn_set() {
        let fields_b = Fields::from(vec![
            Field::new("ba", DataType::UInt64, true),
            Field::new("bb", DataType::UInt64, true),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Struct(fields_b.clone()), true),
        ]);
        let scalars = vec![
            ScalarValue::Struct(None, fields.clone()),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(None),
                    ScalarValue::Struct(None, fields_b.clone()),
                ]),
                fields.clone(),
            ),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(None),
                    ScalarValue::Struct(
                        Some(vec![ScalarValue::UInt64(None), ScalarValue::UInt64(None)]),
                        fields_b.clone(),
                    ),
                ]),
                fields.clone(),
            ),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(Some(1)),
                    ScalarValue::Struct(
                        Some(vec![
                            ScalarValue::UInt64(Some(2)),
                            ScalarValue::UInt64(Some(3)),
                        ]),
                        fields_b,
                    ),
                ]),
                fields.clone(),
            ),
        ];

        let loaders = create_dyn_loaders_from_initial_value(&[AccumInitialValue::DynSet(
            DataType::Struct(fields.clone()),
        )])
        .unwrap();
        let savers = create_dyn_savers_from_initial_value(&[AccumInitialValue::DynSet(
            DataType::Struct(fields.clone()),
        )])
        .unwrap();
        let mut dyn_set = AggDynSet::default();
        dyn_set.append(scalars[0].clone());
        dyn_set.append(scalars[1].clone());
        dyn_set.append(scalars[3].clone());

        let mut buf = vec![];
        savers[0](&mut SaveWriter(Box::new(&mut buf)), Some(Box::new(dyn_set))).unwrap();

        let dyn_set = loaders[0](&mut LoadReader(Box::new(Cursor::new(&buf)))).unwrap();

        let right_set: SmallVec<[ScalarValue; 4]> = SmallVec::from_iter(
            vec![scalars[0].clone(), scalars[1].clone(), scalars[3].clone()].into_iter(),
        );
        let right = OptimizedSet::SmallVec(right_set);
        assert_eq!(
            downcast_any!(dyn_set.unwrap(), AggDynSet).unwrap().values(),
            &right
        );
    }

    #[test]
    fn test_acc() {
        let data_types = vec![
            DataType::Null,
            DataType::Int32,
            DataType::Int64,
            DataType::Utf8,
        ];
        let scalars = data_types
            .iter()
            .map(|dt: &DataType| Ok(AccumInitialValue::Scalar(dt.clone().try_into()?)))
            .collect::<Result<Vec<AccumInitialValue>>>()
            .unwrap();

        let (mut acc, addrs) = create_acc_from_initial_value(&scalars).unwrap();
        let dyn_loaders = create_dyn_loaders_from_initial_value(&scalars).unwrap();
        let dyn_savers = create_dyn_savers_from_initial_value(&scalars).unwrap();
        assert!(!acc.is_fixed_valid(addrs[0]));
        assert!(!acc.is_fixed_valid(addrs[1]));
        assert!(!acc.is_fixed_valid(addrs[2]));

        // set values
        let mut acc_valued = acc.clone();
        acc_valued.set_fixed_value(addrs[1], 123456789_i32);
        acc_valued.set_fixed_value(addrs[2], 1234567890123456789_i64);
        acc_valued.set_fixed_valid(addrs[1], true);
        acc_valued.set_fixed_valid(addrs[2], true);
        *acc_valued.dyn_value_mut(addrs[3]) = Some(Box::new(AggDynStr::from_str("test")));

        // save + load
        let bytes = acc_valued.save_to_bytes(&dyn_savers).unwrap();
        acc.load_from_bytes(&bytes, &dyn_loaders).unwrap();

        assert!(!acc.is_fixed_valid(addrs[0]));
        assert!(acc.is_fixed_valid(addrs[1]));
        assert!(acc.is_fixed_valid(addrs[2]));
        assert_eq!(acc.fixed_value::<i32>(addrs[1]), 123456789_i32);
        assert_eq!(acc.fixed_value::<i64>(addrs[2]), 1234567890123456789_i64);
        assert_eq!(
            downcast_any!(acc.dyn_value(addrs[3]).as_ref().unwrap(), AggDynStr)
                .unwrap()
                .value(),
            "test",
        );
    }
}
