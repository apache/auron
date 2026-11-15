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
    array::{RecordBatch, RecordBatchOptions},
    datatypes::SchemaRef,
};
use datafusion::{common::Result, scalar::ScalarValue};

/// auron: splice partition column values (constant for the whole file) back
/// into the output batch at the positions indicated by `full_projection`,
/// since `mapped` only contains the file columns (in the order of
/// `full_projection`'s file-column entries).
pub fn splice_partition_columns(
    mapped: RecordBatch,
    full_projection: &[usize],
    num_file_schema_fields: usize,
    partition_values: &[ScalarValue],
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = mapped.num_rows();
    let mut file_col_pos = 0;
    let cols = full_projection
        .iter()
        .map(|&idx| {
            if idx < num_file_schema_fields {
                let col = mapped.column(file_col_pos).clone();
                file_col_pos += 1;
                Ok(col)
            } else {
                let p = idx - num_file_schema_fields;
                partition_values[p].to_array_of_size(num_rows)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        output_schema,
        cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{common::Result, scalar::ScalarValue};

    use super::{RecordBatchOptions, splice_partition_columns};

    #[test]
    fn test_empty_projection_preserves_row_count() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        for num_rows in [0, 1, 1024] {
            let mapped = RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )?;
            let out = splice_partition_columns(mapped, &[], 1, &[], schema.clone())?;
            assert_eq!(out.num_columns(), 0);
            assert_eq!(out.num_rows(), num_rows);
            assert_eq!(out.schema(), schema);
        }
        Ok(())
    }

    #[test]
    fn test_partition_only_projection_preserves_row_count() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Utf8, true),
            Field::new("hh", DataType::Int32, true),
        ]));
        let partition_values = vec![
            ScalarValue::Utf8(Some("2024-01-01".to_string())),
            ScalarValue::Int32(None),
        ];
        for num_rows in [0, 1, 1024] {
            let mapped = RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )?;
            let out = splice_partition_columns(
                mapped,
                &[1, 2],
                1,
                &partition_values,
                output_schema.clone(),
            )?;
            assert_eq!(out.num_columns(), 2);
            assert_eq!(out.num_rows(), num_rows);
            let dt = out
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray");
            assert_eq!(dt.len(), num_rows);
            assert!(dt.iter().all(|value| value == Some("2024-01-01")));
            assert_eq!(out.column(1).null_count(), num_rows);
        }
        Ok(())
    }

    #[test]
    fn test_no_reorder() -> Result<()> {
        // file columns: a(Utf8), b(Utf8); partition columns: dt(Utf8)
        // query selects all columns in natural order: [a, b, dt]
        let mapped_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let mapped = RecordBatch::try_new(
            mapped_schema,
            vec![
                Arc::new(StringArray::from(vec!["a0", "a1"])),
                Arc::new(StringArray::from(vec!["b0", "b1"])),
            ],
        )?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("dt", DataType::Utf8, true),
        ]));

        let full_projection = vec![0, 1, 2];
        let partition_values = vec![ScalarValue::Utf8(Some("2024-01-01".to_string()))];

        let out = splice_partition_columns(
            mapped,
            &full_projection,
            2,
            &partition_values,
            output_schema,
        )?;

        assert_eq!(out.num_columns(), 3);
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray")
                .value(0),
            "a0"
        );
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("expected StringArray")
                .value(1),
            "b1"
        );
        let dt = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(dt.value(0), "2024-01-01");
        assert_eq!(dt.value(1), "2024-01-01");
        Ok(())
    }

    #[test]
    fn test_pushed_down_projection_with_reorder_and_filter_column() -> Result<()> {
        // table schema: a(Utf8,0), b(Utf8,1), online_join_params(Utf8,2),
        // d(Int32,3); partition cols: dt(Utf8,4), hh(Utf8,5)
        // query: SELECT b, online_join_params, dt FROM t WHERE dt='X'
        // => pushed projection (ordered_column_indices) = [1, 2, 4]
        let mapped_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("online_join_params", DataType::Utf8, true),
        ]));
        let mapped = RecordBatch::try_new(
            mapped_schema,
            vec![
                Arc::new(StringArray::from(vec!["b0", "b1"])),
                Arc::new(StringArray::from(vec!["json0", "json1"])),
            ],
        )?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("online_join_params", DataType::Utf8, true),
            Field::new("dt", DataType::Utf8, true),
        ]));

        let full_projection = vec![1, 2, 4];
        let num_file_schema_fields = 4;
        // partition_values ordered as [dt, hh], matching table_partition_cols order
        let partition_values = vec![
            ScalarValue::Utf8(Some("X".to_string())),
            ScalarValue::Utf8(Some("ignored_hh".to_string())),
        ];

        let out = splice_partition_columns(
            mapped,
            &full_projection,
            num_file_schema_fields,
            &partition_values,
            output_schema,
        )?;

        assert_eq!(out.num_columns(), 3);
        let col1 = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(col1.value(0), "json0");
        assert_eq!(col1.value(1), "json1");
        let col2 = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(col2.value(0), "X");
        assert_eq!(col2.value(1), "X");
        Ok(())
    }

    #[test]
    fn test_partition_column_interleaved_before_file_columns() -> Result<()> {
        // projection order: [dt(partition), a(file,0), b(file,1)]
        // i.e. partition column appears FIRST in projection order, before
        // file columns - this can legitimately happen since
        // ordered_column_indices() follows the pushed-down projection's
        // expression order, not natural schema order.
        let mapped_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let mapped = RecordBatch::try_new(
            mapped_schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![30, 40])),
            ],
        )?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let num_file_schema_fields = 2;
        let full_projection = vec![2, 0, 1]; // dt, a, b
        let partition_values = vec![ScalarValue::Utf8(Some("P".to_string()))];

        let out = splice_partition_columns(
            mapped,
            &full_projection,
            num_file_schema_fields,
            &partition_values,
            output_schema,
        )?;

        assert_eq!(out.num_columns(), 3);
        let dt = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(dt.value(0), "P");
        let a = out
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array");
        assert_eq!(a.value(0), 10);
        assert_eq!(a.value(1), 20);
        let b = out
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array");
        assert_eq!(b.value(0), 30);
        assert_eq!(b.value(1), 40);
        Ok(())
    }

    #[test]
    fn test_multiple_partition_cols_not_all_projected() -> Result<()> {
        // table: a(Utf8,0), b(Utf8,1); partition cols: year(Utf8), month(Utf8), day(Utf8)
        // query: SELECT a, day FROM t  (only 'day' partition col projected,
        // not year/month) => pushed projection = [0, 4] (a=0, day is 3rd
        // partition col => global index = num_file_schema_fields(2)+2=4)
        let mapped_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let mapped =
            RecordBatch::try_new(mapped_schema, vec![Arc::new(StringArray::from(vec!["a0"]))])?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("day", DataType::Utf8, true),
        ]));

        let num_file_schema_fields = 2;
        let full_projection = vec![0, 4];
        // partition_values ordered as [year, month, day]
        let partition_values = vec![
            ScalarValue::Utf8(Some("2024".to_string())),
            ScalarValue::Utf8(Some("01".to_string())),
            ScalarValue::Utf8(Some("15".to_string())),
        ];

        let out = splice_partition_columns(
            mapped,
            &full_projection,
            num_file_schema_fields,
            &partition_values,
            output_schema,
        )?;

        assert_eq!(out.num_columns(), 2);
        let a = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(a.value(0), "a0");
        let day = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(day.value(0), "15");
        Ok(())
    }
}
