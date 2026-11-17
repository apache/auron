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

//! Functionality used both on logical and physical plans

use arrow::{
    array::*,
    datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, DataType, Int8Type, Int16Type, Int32Type,
        Int64Type, TimeUnit,
    },
};

use crate::hash::{mur::spark_compatible_murmur3_hash, xxhash::spark_compatible_xxhash64_hash};

pub fn create_murmur3_hashes(len: usize, arrays: &[ArrayRef], seed: i32) -> Vec<i32> {
    create_hashes_impl::<true, _>(len, arrays, seed, |data: &[u8], seed: i32| {
        spark_compatible_murmur3_hash(data, seed)
    })
}

pub fn create_xxhash64_hashes(len: usize, arrays: &[ArrayRef], seed: i64) -> Vec<i64> {
    create_hashes_impl::<true, _>(len, arrays, seed, |data: &[u8], seed: i64| {
        spark_compatible_xxhash64_hash(data, seed)
    })
}

#[inline]
fn float32_bits<const SPARK: bool>(value: f32) -> u32 {
    let bits = value.to_bits();
    if SPARK && bits & 0x7fff_ffff > 0x7f80_0000 {
        0x7fc0_0000
    } else {
        bits
    }
}

#[inline]
fn float64_bits<const SPARK: bool>(value: f64) -> u64 {
    let bits = value.to_bits();
    if SPARK && bits & 0x7fff_ffff_ffff_ffff > 0x7ff0_0000_0000_0000 {
        0x7ff8_0000_0000_0000
    } else {
        bits
    }
}

#[inline]
fn hash_decimal<const SPARK: bool, T: num::PrimInt>(
    value: i128,
    precision: u8,
    seed: T,
    h: impl Fn(&[u8], T) -> T,
) -> T {
    if !SPARK {
        return h(&value.to_le_bytes(), seed);
    }
    if precision <= 18 {
        return h(&(value as i64).to_le_bytes(), seed);
    }
    // BigInteger.toByteArray(): minimal signed big-endian two's complement.
    let bytes = value.to_be_bytes();
    let sign_bits = (value ^ (value >> 127)).leading_zeros();
    let start = ((sign_bits - 1) / 8) as usize;
    h(&bytes[start..], seed)
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[inline]
pub fn create_hashes<T: num::PrimInt>(
    len: usize,
    arrays: &[ArrayRef],
    seed: T,
    h: impl Fn(&[u8], T) -> T + Copy,
) -> Vec<T> {
    // Internal join hashes use their own byte representation.
    create_hashes_impl::<false, _>(len, arrays, seed, h)
}

#[inline]
fn create_hashes_impl<const SPARK: bool, T: num::PrimInt>(
    len: usize,
    arrays: &[ArrayRef],
    seed: T,
    h: impl Fn(&[u8], T) -> T + Copy,
) -> Vec<T> {
    let mut hash_buffer = vec![seed; len];
    for col in arrays.iter() {
        hash_array::<SPARK, _>(col, &mut hash_buffer, h);
    }
    hash_buffer
}

#[allow(clippy::panic)] // Temporarily allow panic to refactor to Result later
#[inline]
fn hash_array<const SPARK: bool, T: num::PrimInt>(
    array: &ArrayRef,
    hashes_buffer: &mut [T],
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    assert_eq!(array.len(), hashes_buffer.len());

    macro_rules! hash_array {
        ($array_type:ident, $column:ident, $hashes:ident, $h:expr) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");
            if array.null_count() == 0 {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $h(&array.value(i).as_ref(), *hash);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $h(&array.value(i).as_ref(), *hash);
                    }
                }
            }
        };
    }

    macro_rules! hash_array_primitive {
        ($array_type:ident, $column:ident, $ty:ident, $hashes:ident, $h:expr $(, $map:expr)?) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");
            let values = array.values();

            if array.null_count() == 0 {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    let value = *value as $ty;
                    $(let value = ($map)(value);)?
                    *hash = $h(value.to_le_bytes().as_ref(), *hash);
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        let value = *value as $ty;
                        $(let value = ($map)(value);)?
                        *hash = $h(value.to_le_bytes().as_ref(), *hash);
                    }
                }
            }
        };
    }

    macro_rules! hash_array_decimal {
        ($array_type:ident, $column:ident, $hashes:ident, $h:expr) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");

            if array.null_count() == 0 {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = hash_decimal::<SPARK, _>(array.value(i), array.precision(), *hash, $h);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash =
                            hash_decimal::<SPARK, _>(array.value(i), array.precision(), *hash, $h);
                    }
                }
            }
        };
    }

    match array.data_type() {
        DataType::Null => {}
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Expected a BooleanArray");
            if array.null_count() == 0 {
                for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                    *hash = h(
                        (if array.value(i) { 1u32 } else { 0u32 })
                            .to_le_bytes()
                            .as_ref(),
                        *hash,
                    );
                }
            } else {
                for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = h(
                            (if array.value(i) { 1u32 } else { 0u32 })
                                .to_le_bytes()
                                .as_ref(),
                            *hash,
                        );
                    }
                }
            }
        }
        DataType::Int8 => {
            hash_array_primitive!(Int8Array, array, i32, hashes_buffer, h);
        }
        DataType::Int16 => {
            hash_array_primitive!(Int16Array, array, i32, hashes_buffer, h);
        }
        DataType::Int32 => {
            hash_array_primitive!(Int32Array, array, i32, hashes_buffer, h);
        }
        DataType::Int64 => {
            hash_array_primitive!(Int64Array, array, i64, hashes_buffer, h);
        }
        DataType::Float32 => {
            hash_array_primitive!(
                Float32Array,
                array,
                f32,
                hashes_buffer,
                h,
                float32_bits::<SPARK>
            );
        }
        DataType::Float64 => {
            hash_array_primitive!(
                Float64Array,
                array,
                f64,
                hashes_buffer,
                h,
                float64_bits::<SPARK>
            );
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            hash_array_primitive!(TimestampSecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            hash_array_primitive!(TimestampMillisecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            hash_array_primitive!(TimestampMicrosecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            hash_array_primitive!(TimestampNanosecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Date32 => {
            hash_array_primitive!(Date32Array, array, i32, hashes_buffer, h);
        }
        DataType::Date64 => {
            hash_array_primitive!(Date64Array, array, i64, hashes_buffer, h);
        }
        DataType::Binary => {
            hash_array!(BinaryArray, array, hashes_buffer, h);
        }
        DataType::LargeBinary => {
            hash_array!(LargeBinaryArray, array, hashes_buffer, h);
        }
        DataType::Utf8 => {
            hash_array!(StringArray, array, hashes_buffer, h);
        }
        DataType::LargeUtf8 => {
            hash_array!(LargeStringArray, array, hashes_buffer, h);
        }
        DataType::Decimal128(..) => {
            hash_array_decimal!(Decimal128Array, array, hashes_buffer, h);
        }
        DataType::Dictionary(index_type, _) => match index_type.as_ref() {
            DataType::Int8 => {
                create_hashes_dictionary::<SPARK, Int8Type, _>(array, hashes_buffer, h)
            }
            DataType::Int16 => {
                create_hashes_dictionary::<SPARK, Int16Type, _>(array, hashes_buffer, h)
            }
            DataType::Int32 => {
                create_hashes_dictionary::<SPARK, Int32Type, _>(array, hashes_buffer, h)
            }
            DataType::Int64 => {
                create_hashes_dictionary::<SPARK, Int64Type, _>(array, hashes_buffer, h)
            }
            other => panic!("Unsupported dictionary type in hasher hashing: {other}"),
        },
        _ => {
            for idx in 0..array.len() {
                hash_one::<SPARK, _>(array, idx, &mut hashes_buffer[idx], h);
            }
        }
    }
}

/// Hash the values in a dictionary array
#[inline]
fn create_hashes_dictionary<const SPARK: bool, K: ArrowDictionaryKeyType, T: num::PrimInt>(
    array: &ArrayRef,
    hashes_buffer: &mut [T],
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    let dict_array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .expect("Expected a DictionaryArray");

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = dict_array.values();
    for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
        if let Some(key) = key {
            hash_one::<SPARK, _>(dict_values, key.as_usize(), hash, h);
        }
    }
}

#[allow(clippy::panic)] // Temporarily allow panic to refactor to Result later
fn hash_one<const SPARK: bool, T: num::PrimInt>(
    col: &ArrayRef,
    idx: usize,
    hash: &mut T,
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    macro_rules! hash_one_primitive {
        ($array_type:ident, $column:ident, $ty:ident, $hash:ident, $idx:ident, $h:expr $(, $map:expr)?) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");
            let value = array.value($idx as usize) as $ty;
            $(let value = ($map)(value);)?
            *$hash = $h(value.to_le_bytes().as_ref(), *$hash);
        };
    }

    macro_rules! hash_one_binary {
        ($array_type:ident, $column:ident, $hash:ident, $idx:ident, $h:expr) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");
            *$hash = $h(&array.value($idx as usize).as_ref(), *$hash);
        };
    }

    macro_rules! hash_one_decimal {
        ($array_type:ident, $column:ident, $hash:ident, $idx:ident, $h:expr) => {
            let array = $column
                .as_any()
                .downcast_ref::<$array_type>()
                .expect("downcast to expected array type failed");
            *$hash = hash_decimal::<SPARK, _>(array.value($idx), array.precision(), *$hash, $h);
        };
    }

    if col.is_valid(idx) {
        match col.data_type() {
            DataType::Null => {}
            DataType::Boolean => {
                let array = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Expected a BooleanArray");
                *hash = h(
                    (if array.value(idx) { 1u32 } else { 0u32 })
                        .to_le_bytes()
                        .as_ref(),
                    *hash,
                );
            }
            DataType::Int8 => {
                hash_one_primitive!(Int8Array, col, i32, hash, idx, h);
            }
            DataType::Int16 => {
                hash_one_primitive!(Int16Array, col, i32, hash, idx, h);
            }
            DataType::Int32 => {
                hash_one_primitive!(Int32Array, col, i32, hash, idx, h);
            }
            DataType::Int64 => {
                hash_one_primitive!(Int64Array, col, i64, hash, idx, h);
            }
            DataType::Float32 => {
                hash_one_primitive!(Float32Array, col, f32, hash, idx, h, float32_bits::<SPARK>);
            }
            DataType::Float64 => {
                hash_one_primitive!(Float64Array, col, f64, hash, idx, h, float64_bits::<SPARK>);
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                hash_one_primitive!(TimestampSecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_one_primitive!(TimestampMillisecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_one_primitive!(TimestampMicrosecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_one_primitive!(TimestampNanosecondArray, col, i64, hash, idx, h);
            }
            DataType::Date32 => {
                hash_one_primitive!(Date32Array, col, i32, hash, idx, h);
            }
            DataType::Date64 => {
                hash_one_primitive!(Date64Array, col, i64, hash, idx, h);
            }
            DataType::Binary => {
                hash_one_binary!(BinaryArray, col, hash, idx, h);
            }
            DataType::LargeBinary => {
                hash_one_binary!(LargeBinaryArray, col, hash, idx, h);
            }
            DataType::Utf8 => {
                hash_one_binary!(StringArray, col, hash, idx, h);
            }
            DataType::LargeUtf8 => {
                hash_one_binary!(LargeStringArray, col, hash, idx, h);
            }
            DataType::Decimal128(..) => {
                hash_one_decimal!(Decimal128Array, col, hash, idx, h);
            }
            DataType::List(..) => {
                let list_array = col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected a ListArray");
                let value_array = list_array.value(idx);
                for i in 0..value_array.len() {
                    hash_one::<SPARK, _>(&value_array, i, hash, h);
                }
            }
            DataType::Map(..) => {
                let map_array = col
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .expect("Expected a MapArray");
                let kv_array = map_array.value(idx);
                let key_array = kv_array.column(0);
                let value_array = kv_array.column(1);
                for i in 0..kv_array.len() {
                    hash_one::<SPARK, _>(key_array, i, hash, h);
                    hash_one::<SPARK, _>(value_array, i, hash, h);
                }
            }
            DataType::Struct(_) => {
                let struct_array = col
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Expected a StructArray");
                for col in struct_array.columns() {
                    hash_one::<SPARK, _>(col, idx, hash, h);
                }
            }
            other => panic!("Unsupported data type in hasher: {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{
            Array, ArrayData, ArrayRef, Int8Array, Int32Array, Int64Array, MapArray, StringArray,
            StructArray, UInt32Array, make_array,
        },
        buffer::Buffer,
        datatypes::{DataType, Field, ToByteSlice},
    };

    use super::*;

    #[test]
    fn test_list() {
        let mut hashes_buffer = [42; 4];
        for hash in hashes_buffer.iter_mut() {
            *hash = spark_compatible_murmur3_hash(5_i32.to_le_bytes(), *hash);
        }
    }

    #[test]
    fn test_i8() {
        let i = Arc::new(Int8Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i8::MAX),
            Some(i8::MIN),
        ])) as ArrayRef;
        let hashes = create_murmur3_hashes(5, &[i], 42);

        // generated with Spark Murmur3_x86_32
        let expected: Vec<i32> = [
            0xdea578e3_u32,
            0x379fae8f,
            0xa0590e3d,
            0x43b4d8ed,
            0x422a1365,
        ]
        .into_iter()
        .map(|v| v as i32)
        .collect();
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_i32() {
        let i = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[i], 42);
        assert_eq!(hashes, vec![-559580957]);

        let j = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[j], 42);
        assert_eq!(hashes, vec![1765031574]);

        let m = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[m], 42);
        assert_eq!(hashes, vec![-1823081949]);

        let n = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[n], 42);
        assert_eq!(hashes, vec![-397064898]);
    }

    #[test]
    fn test_i64() {
        let i = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
        ])) as ArrayRef;

        // generated with Murmur3Hash(Seq(Literal(1L)), 42).eval() since Spark is tested
        let hashes = create_murmur3_hashes(5, std::slice::from_ref(&i), 42);
        let expected: Vec<i32> = [
            0x99f0149d_u32,
            0x9c67b85d,
            0xc8008529,
            0xa05b5d7b,
            0xcd1e64fb,
        ]
        .into_iter()
        .map(|v| v as i32)
        .collect();
        assert_eq!(hashes, expected);

        // generated with XxHash64(Seq(Literal(1L)), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_xxhash64_hashes(5, std::slice::from_ref(&i), 42);
        let expected = vec![
            -7001672635703045582,
            -5252525462095825812,
            3858142552250413010,
            -3246596055638297850,
            -8619748838626508300,
        ];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_str() {
        let i = Arc::new(StringArray::from(vec!["hello", "bar", "", "😁", "天地"])) as ArrayRef;

        // generated with Murmur3Hash(Seq(Literal("")), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_murmur3_hashes(5, std::slice::from_ref(&i), 42);
        let expected: Vec<i32> = [3286402344_u32, 2486176763, 142593372, 885025535, 2395000894]
            .into_iter()
            .map(|v| v as i32)
            .collect();
        assert_eq!(hashes, expected);

        // generated with XxHash64(Seq(Literal("")), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_xxhash64_hashes(5, std::slice::from_ref(&i), 42);
        let expected = vec![
            -4367754540140381902,
            -1798770879548125814,
            -7444071767201028348,
            -6337236088984028203,
            -235771157374669727,
        ];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_list_array() -> Result<(), Box<dyn Error>> {
        // Create inner array data: [1, 2, 3, 4, 5, 6]
        let value_data = ArrayData::builder(DataType::Int32)
            .len(6)
            .add_buffer(Buffer::from_slice_ref(
                [1i32, 2, 3, 4, 5, 6].to_byte_slice(),
            ))
            .build()?;

        // Create offset array to define list boundaries: [[1, 2], [3, 4, 5], [6]]
        let list_data_type = DataType::new_list(DataType::Int32, false);
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref([0i32, 2, 5, 6].to_byte_slice()))
            .add_child_data(value_data)
            .build()?;

        let list_array = ListArray::from(list_data);
        let array_ref = Arc::new(list_array) as ArrayRef;

        // Test Murmur3 hash
        let hashes = create_murmur3_hashes(3, std::slice::from_ref(&array_ref), 42);
        assert_eq!(hashes, vec![-222940379, -374492525, -331964951]);
        Ok(())
    }

    #[test]
    fn test_map_array() -> Result<(), Box<dyn Error>> {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                [0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice(),
            ))
            .build()?;
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                [0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()?;

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from_slice_ref([0, 3, 6, 8].to_byte_slice());

        let keys_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, true));
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()?;
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(3, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[2]);
        assert_eq!(2, map_array.value_length(2));

        let key_array = Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef;
        let value_array =
            Arc::new(UInt32Array::from(vec![None, Some(10u32), Some(20)])) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (keys_field.clone(), key_array),
            (values_field.clone(), value_array),
        ]);
        assert_eq!(
            struct_array,
            StructArray::from(map_array.value(0).into_data())
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected a StructArray")
        );
        for i in 0..3 {
            assert!(map_array.is_valid(i));
            assert!(!map_array.is_null(i));
        }

        // Now test with a non-zero offset
        let map_data = ArrayData::builder(map_array.data_type().clone())
            .len(2)
            .offset(1)
            .add_buffer(map_array.to_data().buffers()[0].clone())
            .add_child_data(map_array.to_data().child_data()[0].clone())
            .build()?;
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(2, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[1]);
        assert_eq!(2, map_array.value_length(1));

        let key_array = Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef;
        let value_array = Arc::new(UInt32Array::from(vec![None, Some(40), None])) as ArrayRef;
        let struct_array =
            StructArray::from(vec![(keys_field, key_array), (values_field, value_array)]);
        assert_eq!(
            &struct_array,
            map_array
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected a StructArray")
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected a StructArray")
        );
        Ok(())
    }
}

#[cfg(test)]
mod spark_value_encoding_tests {
    use std::sync::Arc;

    use arrow::{
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{Field, Float32Type, Float64Type},
    };

    use super::*;

    // Goldens from actual Spark 3.0.3, 3.2.0 and 3.5.8 Catalyst
    // Murmur3HashFunction/XxHash64Function; these cases agree in all versions.
    fn assert_hashes(array: ArrayRef, murmur: &[i32], xxhash: &[i64]) {
        assert_eq!(
            create_murmur3_hashes(array.len(), std::slice::from_ref(&array), 42),
            murmur
        );
        assert_eq!(
            create_xxhash64_hashes(array.len(), std::slice::from_ref(&array), 42),
            xxhash
        );
        // Single-row slices also exercise the no-null fast paths.
        for i in 0..array.len() {
            let row = array.slice(i, 1);
            assert_eq!(
                create_murmur3_hashes(1, std::slice::from_ref(&row), 42),
                [murmur[i]]
            );
            assert_eq!(create_xxhash64_hashes(1, &[row], 42), [xxhash[i]]);
        }
        if array.len() > 2 {
            let sliced = array.slice(1, array.len() - 2);
            assert_eq!(
                create_murmur3_hashes(sliced.len(), std::slice::from_ref(&sliced), 42),
                murmur[1..murmur.len() - 1]
            );
            assert_eq!(
                create_xxhash64_hashes(sliced.len(), &[sliced], 42),
                xxhash[1..xxhash.len() - 1]
            );
        }
    }

    #[test]
    fn spark_hash_nan_arrays_and_nested_values() {
        let floats: Vec<_> = [
            0, 0x7fc00000, 0x7fc00001, 0xffc00000, 0x7f800001, 0x3f800000, 0x7f800000, 0xff800000,
        ]
        .into_iter()
        .map(|v| Some(f32::from_bits(v)))
        .chain([None])
        .collect();
        let doubles: Vec<_> = [
            0,
            0x7ff8000000000000,
            0x7ff8000000000001,
            0xfff8000000000000,
            0x7ff0000000000001,
            0x3ff0000000000000,
            0x7ff0000000000000,
            0xfff0000000000000,
        ]
        .into_iter()
        .map(|v| Some(f64::from_bits(v)))
        .chain([None])
        .collect();
        assert_hashes(
            Arc::new(Float32Array::from(floats.clone())),
            &[
                933211791, -349261430, -349261430, -349261430, -349261430, -466301895, 2026854605,
                427440766, 42,
            ],
            &[
                3614696996920510707,
                2692338816207849720,
                2692338816207849720,
                2692338816207849720,
                2692338816207849720,
                700633588856507837,
                -5940311692336719973,
                -7580553461823983095,
                42,
            ],
        );
        assert_hashes(
            Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
                floats
                    .into_iter()
                    .map(|v| v.map(|v| vec![Some(v), None, Some(1.0)])),
            )),
            &[
                -1948681258,
                1120934142,
                1120934142,
                1120934142,
                1120934142,
                1090151794,
                1639636676,
                -717692674,
                42,
            ],
            &[
                -4188506254282232552,
                3507257904945349037,
                3507257904945349037,
                3507257904945349037,
                3507257904945349037,
                7045592171476072005,
                -3050372117123012898,
                7967751882884668089,
                42,
            ],
        );
        assert_hashes(
            Arc::new(Float64Array::from(doubles.clone())),
            &[
                -1670924195,
                -1281358385,
                -1281358385,
                -1281358385,
                -1281358385,
                -460888942,
                833680482,
                461104036,
                42,
            ],
            &[
                -5252525462095825812,
                -3127944061524951246,
                -3127944061524951246,
                -3127944061524951246,
                -3127944061524951246,
                -2162451265447482029,
                5810986238603807492,
                5326262080505358431,
                42,
            ],
        );
        assert_hashes(
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                doubles
                    .into_iter()
                    .map(|v| v.map(|v| vec![Some(v), None, Some(1.0)])),
            )),
            &[
                1610320073, -712254141, -712254141, -712254141, -712254141, 146095046, -770461293,
                147740909, 42,
            ],
            &[
                -1822463583027973292,
                6518213585619870252,
                6518213585619870252,
                6518213585619870252,
                6518213585619870252,
                382911295564016611,
                -6561926802706257703,
                8050832243374139752,
                42,
            ],
        );
    }

    #[test]
    fn spark_hash_decimal_precision_and_signed_byte_boundaries() {
        let values = vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(127),
            Some(128),
            Some(-128),
            Some(-129),
            Some(32767),
            Some(32768),
            Some(-32768),
            Some(-32769),
            None,
        ];
        for precision in [9, 18] {
            assert_hashes(
                Arc::new(
                    Decimal128Array::from(values.clone())
                        .with_precision_and_scale(precision, 2)
                        .expect("test decimal precision and scale should be valid"),
                ),
                &[
                    -1670924195,
                    -1712319331,
                    -939490007,
                    -351318305,
                    -1794377518,
                    684229342,
                    -1627571634,
                    -860381306,
                    -1812886946,
                    -584221512,
                    1183113035,
                    42,
                ],
                &[
                    -5252525462095825812,
                    -7001672635703045582,
                    3858142552250413010,
                    4269007880567061913,
                    -4045576785621496215,
                    -7035553742189352530,
                    -1087258766029903377,
                    -1269405870795497628,
                    -438320791795300563,
                    -6137856927263092174,
                    464625387341390294,
                    42,
                ],
            );
        }
        for precision in [19, 38] {
            assert_hashes(
                Arc::new(
                    Decimal128Array::from(values.clone())
                        .with_precision_and_scale(precision, 2)
                        .expect("test decimal precision and scale should be valid"),
                ),
                &[
                    -783713497,
                    -386724586,
                    1398487324,
                    1185089389,
                    -544401882,
                    775851899,
                    -771458971,
                    -1459034263,
                    -99371362,
                    -774221049,
                    2014060947,
                    42,
                ],
                &[
                    -8959994473701255385,
                    6668291691252061002,
                    -4006032525457443936,
                    -5998106174972408993,
                    6715097930120473301,
                    -7898411661632158123,
                    -6459515886037897956,
                    -5198520329571243073,
                    7486961118722643011,
                    -4443336017607688062,
                    816881880154201779,
                    42,
                ],
            );
        }
        let values = vec![
            Some(9223372036854775807),
            Some(9223372036854775808),
            Some(-9223372036854775808),
            Some(-9223372036854775809),
            Some(18446744073709551616),
            Some(-18446744073709551617),
            Some(99999999999999999999999999999999999999),
            Some(-99999999999999999999999999999999999999),
            None,
        ];
        assert_hashes(
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 2)
                    .expect("test decimal precision and scale should be valid"),
            ),
            &[
                -1627571634,
                1479175736,
                -1794377518,
                -1667168926,
                1074617613,
                -627024645,
                -817514053,
                1400911110,
                42,
            ],
            &[
                -1087258766029903377,
                2657851292443253618,
                -4045576785621496215,
                3800235373674817761,
                7007687479959933989,
                -6582748315522549911,
                -47190729175993179,
                -2254039905620870768,
                42,
            ],
        );
    }

    #[test]
    fn spark_hash_decimal_nested_nulls_and_dictionary() {
        let item = Arc::new(Field::new("item", DataType::Decimal128(38, 2), true));
        let min_decimal = -99999999999999999999999999999999999999_i128;
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(128),
                Some(-129),
                None,
                Some(777),
                Some(min_decimal),
            ])
            .with_precision_and_scale(38, 2)
            .expect("test decimal precision and scale should be valid"),
        );
        let lists: ArrayRef = Arc::new(ListArray::new(
            item,
            OffsetBuffer::new(vec![0, 3, 4, 4, 5].into()),
            values,
            Some(NullBuffer::from(vec![true, false, true, true])),
        ));
        assert_hashes(
            lists.clone(),
            &[-1086779959, 42, 42, 1400911110],
            &[-8147968042812628407, 42, 42, -2254039905620870768],
        );
        let fields = vec![
            Field::new("tag", DataType::Int32, true),
            Field::new("value", DataType::Decimal128(38, 2), true),
            Field::new("values", lists.data_type().clone(), true),
        ]
        .into();
        let structures: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(123), None, Some(-1)])),
                Arc::new(
                    Decimal128Array::from(vec![Some(128), Some(123), None, Some(min_decimal)])
                        .with_precision_and_scale(38, 2)
                        .expect("test decimal precision and scale should be valid"),
                ),
                lists,
            ],
            Some(NullBuffer::from(vec![true, false, true, true])),
        ));
        assert_hashes(
            structures,
            &[2006883997, 42, 42, -1255420180],
            &[-810710873250454100, 42, 42, 7494781972098717872],
        );
        let dictionary_values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(128), Some(-129), None])
                .with_precision_and_scale(19, 2)
                .expect("test decimal precision and scale should be valid"),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(1), None, Some(0), Some(2)]),
                dictionary_values,
            )
            .expect("test dictionary keys should be valid"),
        );
        assert_hashes(
            dictionary,
            &[-771458971, 42, -544401882, 42],
            &[-6459515886037897956, 42, 6715097930120473301, 42],
        );
    }

    #[test]
    fn internal_join_hash_encoding_is_unchanged() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::from_bits(0x7ff8000000000001),
            -0.0,
        ]));
        assert_eq!(
            create_hashes(2, &[values], 42, |v, h| spark_compatible_murmur3_hash(v, h)),
            vec![274393586, -853646085]
        );
        for precision in [18, 38] {
            let decimal: ArrayRef = Arc::new(
                Decimal128Array::from(vec![1])
                    .with_precision_and_scale(precision, 2)
                    .expect("test decimal precision and scale should be valid"),
            );
            assert_eq!(
                create_hashes(1, &[decimal], 42, |v, h| spark_compatible_murmur3_hash(
                    v, h
                )),
                vec![-680163996]
            );
        }
    }
}
