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

use arrow::{
    array::{Array, ArrayData, ArrayRef, UInt64Array, make_array, new_empty_array},
    compute::take,
    datatypes::DataType,
    ffi::FFI_ArrowArray,
};

pub fn export_ffi_array(array: &dyn Array) -> FFI_ArrowArray {
    FFI_ArrowArray::new(&align_array_data_offset(array.to_data()))
}

pub fn align_array_data_offset(mut data: ArrayData) -> ArrayData {
    if needs_local_normalization(&data) {
        let array = make_array(data);
        data = if array.is_empty() {
            new_empty_array(array.data_type()).to_data()
        } else {
            let indices = UInt64Array::from_iter_values(0..array.len() as u64);
            take(array.as_ref(), &indices, None)
                .expect("valid array indices for FFI export")
                .to_data()
        };
    }
    if data.child_data().iter().any(needs_normalization) {
        let children = data
            .child_data()
            .iter()
            .cloned()
            .map(align_array_data_offset)
            .collect();
        data = data
            .into_builder()
            .child_data(children)
            .build()
            .expect("valid normalized child arrays");
    }
    data
}

fn needs_local_normalization(data: &ArrayData) -> bool {
    if data.offset() != 0 || data.nulls().is_some_and(|nulls| nulls.offset() != 0) {
        return true;
    }
    // Empty arrays may have no offset entries.
    match data.data_type() {
        DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Map(..) => {
            let offsets = data.buffers()[0].typed_data::<i32>();
            offsets.first().is_some_and(|&offset| offset != 0)
        }
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
            let offsets = data.buffers()[0].typed_data::<i64>();
            offsets.first().is_some_and(|&offset| offset != 0)
        }
        _ => false,
    }
}

fn needs_normalization(data: &ArrayData) -> bool {
    needs_local_normalization(data) || data.child_data().iter().any(needs_normalization)
}

#[cfg(test)]
fn has_nonzero_offset(data: &ArrayData) -> bool {
    data.offset() != 0 || data.child_data().iter().any(has_nonzero_offset)
}

pub fn align_array_offset(array: ArrayRef) -> ArrayRef {
    let data = array.to_data();
    if !needs_normalization(&data) {
        return array;
    }
    make_array(align_array_data_offset(data))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, ArrayData, AsArray, BinaryArray, Int32Array, RunArray, StructArray, make_array,
        },
        buffer::ScalarBuffer,
        datatypes::{DataType, Field, Fields, Int32Type},
    };

    use super::*;

    fn view_with_offset(data: &ArrayData, offset: usize, len: usize) -> ArrayData {
        ArrayData::try_new(
            data.data_type().clone(),
            len,
            None,
            offset,
            data.buffers().to_vec(),
            data.child_data().to_vec(),
        )
        .expect("valid test array")
    }

    fn assert_normalized_ffi(array: &FFI_ArrowArray, data_type: &DataType) {
        assert_eq!(array.offset(), 0, "{data_type:?}");
        match data_type {
            DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Map(..) => {
                assert_eq!(unsafe { *array.buffer(1).cast::<i32>() }, 0);
            }
            DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
                assert_eq!(unsafe { *array.buffer(1).cast::<i64>() }, 0);
            }
            _ => {}
        }
        match data_type {
            DataType::Struct(fields) => {
                for (index, field) in fields.iter().enumerate() {
                    assert_normalized_ffi(array.child(index), field.data_type());
                }
            }
            DataType::Union(fields, _) => {
                for (index, (_, field)) in fields.iter().enumerate() {
                    assert_normalized_ffi(array.child(index), field.data_type());
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => {
                assert_normalized_ffi(array.child(0), field.data_type());
            }
            DataType::Dictionary(_, values) => {
                assert_normalized_ffi(array.dictionary().expect("valid test array"), values);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                assert_normalized_ffi(array.child(0), run_ends.data_type());
                assert_normalized_ffi(array.child(1), values.data_type());
            }
            _ => {}
        }
    }

    #[test]
    fn recursively_normalizes_ffi_for_struct_list_struct_slices() {
        use arrow::{
            array::{BooleanArray, ListArray, StringArray},
            buffer::OffsetBuffer,
        };

        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("prefix"),
            Some("20260924"),
            None,
            Some("20260917"),
            Some("20260910"),
            Some("suffix"),
        ]));
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
        ]));
        let values = StructArray::from(vec![
            (Arc::new(Field::new("date", DataType::Utf8, true)), strings),
            (
                Arc::new(Field::new("flag", DataType::Boolean, true)),
                booleans,
            ),
        ]);
        let list = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 5, 6])),
            Arc::new(values),
            None,
        );
        let outer = StructArray::from(vec![(
            Arc::new(Field::new("items", list.data_type().clone(), true)),
            Arc::new(list) as ArrayRef,
        )]);
        for array in [
            outer.slice(1, 2),
            outer.slice(1, 3).slice(1, 1),
            outer.slice(2, 0),
        ] {
            let normalized = align_array_data_offset(array.to_data());
            assert!(!needs_normalization(&normalized));
            assert_eq!(normalized, array.to_data());
            let ffi = export_ffi_array(&array);
            assert_normalized_ffi(&ffi, array.data_type());
            let imported =
                unsafe { arrow::ffi::from_ffi_and_data_type(ffi, array.data_type().clone()) }
                    .expect("valid test array");
            assert_eq!(imported, array.to_data());
        }
    }

    #[test]
    fn recursively_normalizes_dictionary_values_inside_struct() {
        use arrow::array::{DictionaryArray, StringArray};

        let values: ArrayRef =
            Arc::new(StringArray::from(vec!["prefix", "20260924", "20260917"]).slice(1, 2));
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            values,
        )
        .expect("valid test array");
        let outer = StructArray::from(vec![(
            Arc::new(Field::new("date", dictionary.data_type().clone(), true)),
            Arc::new(dictionary) as ArrayRef,
        )])
        .slice(1, 2);
        let ffi = export_ffi_array(&outer);
        assert_normalized_ffi(&ffi, outer.data_type());
        let imported =
            unsafe { arrow::ffi::from_ffi_and_data_type(ffi, outer.data_type().clone()) }
                .expect("valid test array");
        assert_eq!(imported, outer.to_data());
    }

    #[test]
    fn keeps_offset_zero_array_unchanged() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let data = align_array_data_offset(array.to_data());
        assert_eq!(data.offset(), 0);
        assert_eq!(
            make_array(data).as_primitive::<Int32Type>().values(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn accepts_empty_offset_buffers() {
        use arrow::buffer::Buffer;

        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        ));
        for data_type in [
            DataType::Utf8,
            DataType::Binary,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::List(item.clone()),
            DataType::LargeList(item),
            DataType::Map(entries, false),
        ] {
            let empty = ArrayData::new_empty(&data_type);
            let mut buffers = empty.buffers().to_vec();
            buffers[0] = Buffer::from(Vec::<i64>::new());
            let data = empty
                .into_builder()
                .buffers(buffers)
                .build()
                .expect("valid test array");
            data.validate_full().expect("valid test array");
            assert!(align_array_data_offset(data.clone()).ptr_eq(&data));

            // Exercise recursive detection on the same raw empty buffer.
            let nested = ArrayData::builder(DataType::Struct(Fields::from(vec![Field::new(
                "empty", data_type, true,
            )])))
            .len(0)
            .add_child_data(data.clone())
            .build()
            .expect("valid test array");
            assert!(align_array_data_offset(nested.clone()).ptr_eq(&nested));

            let array = make_array(data);
            let ffi = export_ffi_array(array.as_ref());
            assert_eq!(ffi.len(), 0);
            let imported =
                unsafe { arrow::ffi::from_ffi_and_data_type(ffi, array.data_type().clone()) }
                    .expect("valid test array");
            imported.validate_full().expect("valid test array");
        }
    }

    #[test]
    fn normalizes_primitive_array_offset() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let viewed = view_with_offset(&array.to_data(), 2, 2);
        assert_eq!(viewed.offset(), 2);

        let original_buffer = viewed.buffers()[0].clone();
        let normalized = align_array_data_offset(viewed);
        assert_eq!(normalized.offset(), 0);
        assert_ne!(normalized.buffers()[0].as_ptr(), original_buffer.as_ptr());
        let normalized_array = make_array(normalized);
        let values = normalized_array.as_primitive::<Int32Type>();
        assert_eq!(values.values(), &[3, 4]);
    }

    #[test]
    fn normalizes_binary_array_offset() {
        let array = BinaryArray::from(vec![
            Some(b"a".as_ref()),
            Some(b"bb".as_ref()),
            Some(b"ccc".as_ref()),
        ]);
        let viewed = view_with_offset(&array.to_data(), 1, 2);
        assert_eq!(viewed.offset(), 1);

        let normalized = align_array_data_offset(viewed);
        assert_eq!(normalized.offset(), 0);
        let normalized_array = make_array(normalized);
        let values = normalized_array.as_binary::<i32>();
        assert_eq!(values.value(0), b"bb");
        assert_eq!(values.value(1), b"ccc");
    }

    #[test]
    fn normalizes_sliced_strings_and_binary_with_copied_values() {
        use arrow::array::{LargeBinaryArray, LargeStringArray, StringArray};

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("前缀"),
                Some("20260924"),
                None,
                Some("20260917"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("前缀"),
                Some("20260924"),
                None,
                Some("20260917"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(b"prefix".as_ref()),
                Some(b"20260924".as_ref()),
                None,
                Some(b"20260917".as_ref()),
            ])),
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"prefix".as_ref()),
                Some(b"20260924".as_ref()),
                None,
                Some(b"20260917".as_ref()),
            ])),
        ];
        for array in arrays {
            let sliced = array.slice(1, 3);
            let original = sliced.to_data();
            let normalized = align_array_data_offset(original.clone());
            normalized.validate_full().expect("valid test array");
            assert_eq!(normalized.offset(), 0);
            assert_eq!(normalized.nulls().expect("valid test array").offset(), 0);
            match array.data_type() {
                DataType::Utf8 | DataType::Binary => {
                    assert_eq!(normalized.buffers()[0].typed_data::<i32>()[0], 0)
                }
                _ => assert_eq!(normalized.buffers()[0].typed_data::<i64>()[0], 0),
            }
            assert_eq!(make_array(normalized.clone()).to_data(), original);
            assert_ne!(
                normalized.buffers()[1].as_ptr(),
                original.buffers()[1].as_ptr()
            );
            let ffi = export_ffi_array(sliced.as_ref());
            let imported =
                unsafe { arrow::ffi::from_ffi_and_data_type(ffi, array.data_type().clone()) }
                    .expect("valid test array");
            assert_eq!(imported, original);
        }
    }

    #[test]
    fn normalizes_boolean_offsets_and_nulls() {
        use arrow::array::BooleanArray;

        let array =
            BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(false)]);
        let sliced = array.slice(1, 3);
        assert_ne!(sliced.to_data().offset(), 0);
        let normalized = align_array_data_offset(sliced.to_data());
        assert_eq!(normalized.offset(), 0);
        assert_eq!(normalized.nulls().expect("valid test array").offset(), 0);
        assert_eq!(make_array(normalized).to_data(), sliced.to_data());
        let ffi = export_ffi_array(&sliced);
        assert_eq!(ffi.offset(), 0);
    }

    #[test]
    fn normalizes_nested_list_and_boolean_offsets() {
        use arrow::array::{BooleanArray, ListArray};

        let values = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ]));
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            arrow::buffer::OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
            values,
            None,
        );
        let sliced = list.slice(1, 2);
        let normalized = align_array_data_offset(sliced.to_data());
        normalized.validate_full().expect("valid test array");
        assert!(!has_nonzero_offset(&normalized));
        assert_eq!(normalized.buffers()[0].typed_data::<i32>(), &[0, 2, 3]);
        assert_eq!(make_array(normalized).to_data(), sliced.to_data());
    }

    #[test]
    fn normalizes_sliced_run_array() {
        use arrow::array::StringArray;

        let run_ends = Int32Array::from(vec![3, 5, 8]);
        let values = StringArray::from(vec!["a", "b", "c"]);
        let array = RunArray::<Int32Type>::try_new(&run_ends, &values).expect("valid test array");
        let sliced = array.slice(2, 4);
        let normalized = align_array_data_offset(sliced.to_data());
        normalized.validate_full().expect("valid test array");
        assert!(!has_nonzero_offset(&normalized));
        let normalized = RunArray::<Int32Type>::from(normalized);
        assert_eq!(normalized.run_ends().values(), &[1, 3, 4]);
        assert_eq!(normalized.values().to_data(), values.to_data());
        assert_eq!(normalized.len(), 4);
    }

    #[test]
    fn preserves_unsliced_array_and_buffer_identity() {
        use arrow::array::StringArray;

        let array: ArrayRef = Arc::new(StringArray::from(vec!["20260924", "20260917"]));
        assert!(Arc::ptr_eq(&array, &align_array_offset(array.clone())));
        let data = array.to_data();
        let normalized = align_array_data_offset(data.clone());
        for (original, normalized) in data.buffers().iter().zip(normalized.buffers()) {
            assert_eq!(original.as_ptr(), normalized.as_ptr());
        }
    }

    #[test]
    fn normalizes_nested_dictionary_and_fixed_size_offsets() {
        use arrow::array::{
            BooleanArray, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
        };

        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, true, false,
        ]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1, 0]),
                booleans.slice(1, 2),
            )
            .expect("valid test array"),
        );
        let fixed_binary: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter([b"aa", b"bb", b"cc"].into_iter())
                .expect("valid test array"),
        );
        let fixed_list: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, false)),
            2,
            booleans,
            None,
        ));
        for array in [dictionary, fixed_binary, fixed_list] {
            let sliced = array.slice(1, 2);
            let normalized = align_array_data_offset(sliced.to_data());
            normalized.validate_full().expect("valid test array");
            assert!(!needs_normalization(&normalized));
            assert_eq!(make_array(normalized).to_data(), sliced.to_data());
        }
    }

    #[test]
    fn normalizes_struct_array_offset() {
        let ints = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, false)),
            ints as ArrayRef,
        )]);
        let viewed = view_with_offset(&struct_array.to_data(), 1, 2);
        assert_eq!(viewed.offset(), 1);

        let normalized = align_array_data_offset(viewed);
        assert_eq!(normalized.offset(), 0);
        let normalized_array = make_array(normalized);
        let normalized_struct = normalized_array.as_struct();
        let col = normalized_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(col.values(), &[2, 3]);
    }

    #[test]
    fn export_ffi_array_normalizes_offset() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let viewed = make_array(view_with_offset(&array.to_data(), 2, 2));
        let ffi_array = export_ffi_array(viewed.as_ref());
        assert_eq!(ffi_array.offset(), 0);
        assert_eq!(ffi_array.len(), 2);
    }

    #[test]
    fn normalizes_nested_child_offset_even_when_top_level_offset_is_zero() {
        let ints = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let child_viewed = view_with_offset(&ints.to_data(), 2, 2);
        assert_eq!(child_viewed.offset(), 2);

        let fields = Fields::from(vec![Field::new("a", DataType::Int32, false)]);
        let top = ArrayData::try_new(
            DataType::Struct(fields),
            2,
            None,
            0,
            vec![],
            vec![child_viewed],
        )
        .expect("valid test array");
        assert_eq!(top.offset(), 0);
        assert!(has_nonzero_offset(&top));

        let normalized = align_array_data_offset(top);
        assert!(!has_nonzero_offset(&normalized));
        let normalized_array = make_array(normalized);
        let normalized_struct = normalized_array.as_struct();
        let col = normalized_struct.column(0).as_primitive::<Int32Type>();
        assert_eq!(col.values(), &[3, 4]);
    }
}
