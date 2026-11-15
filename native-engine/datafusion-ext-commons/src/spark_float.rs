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

use std::{cmp::Ordering, sync::Arc};

use arrow::{
    array::{ArrayRef, Float32Array, Float64Array},
    compute::kernels::arity::unary,
    datatypes::{DataType, Float32Type, Float64Type},
};
use datafusion::common::Result;

use crate::downcast_any;

pub fn spark_float_cmp<T: num::Float>(left: T, right: T) -> Ordering {
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

/// Replaces every NaN bit pattern with the canonical NaN, like Spark does when
/// hashing floats. Signed zeros stay distinct. Non-float arrays are returned
/// unchanged.
pub fn canonicalize_nans(values: &ArrayRef) -> Result<ArrayRef> {
    Ok(match values.data_type() {
        DataType::Float32 => Arc::new(unary::<_, _, Float32Type>(
            downcast_any!(values, Float32Array)?,
            |v| if v.is_nan() { f32::NAN } else { v },
        )),
        DataType::Float64 => Arc::new(unary::<_, _, Float64Type>(
            downcast_any!(values, Float64Array)?,
            |v| if v.is_nan() { f64::NAN } else { v },
        )),
        _ => Arc::clone(values),
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};

    use super::*;

    fn check_spark_float_cmp<T: num::Float + std::fmt::Debug>(nan: T) {
        let values = [
            (T::neg_infinity(), 0),
            (-T::one(), 1),
            (-T::zero(), 2),
            (T::zero(), 2),
            (T::one(), 3),
            (T::infinity(), 4),
            (T::nan(), 5),
            (-T::nan(), 5),
            (nan, 5),
            (-nan, 5),
        ];
        for (left, left_rank) in values {
            for (right, right_rank) in values {
                assert_eq!(
                    spark_float_cmp(left, right),
                    left_rank.cmp(&right_rank),
                    "{left:?} vs {right:?}"
                );
            }
        }
    }

    #[test]
    fn test_spark_float_cmp_f32() {
        check_spark_float_cmp(f32::from_bits(0x7fc0_0001));
    }

    #[test]
    fn test_spark_float_cmp_f64() {
        check_spark_float_cmp(f64::from_bits(0x7ff8_0000_0000_0001));
    }

    #[test]
    fn test_canonicalize_nans() -> Result<()> {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(-0.0),
            None,
            Some(1.5),
        ]));
        let result = canonicalize_nans(&values)?;
        let result = downcast_any!(&result, Float64Array)?;
        assert_eq!(result.value(0).to_bits(), f64::NAN.to_bits());
        assert_eq!(result.value(1).to_bits(), (-0.0f64).to_bits());
        assert!(result.is_null(2));
        assert_eq!(result.value(3), 1.5);

        let floats: ArrayRef = Arc::new(Float32Array::from(vec![f32::from_bits(0x7fc0_0001)]));
        let result = canonicalize_nans(&floats)?;
        assert_eq!(
            downcast_any!(&result, Float32Array)?.value(0).to_bits(),
            f32::NAN.to_bits()
        );

        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert!(Arc::ptr_eq(&canonicalize_nans(&ints)?, &ints));
        Ok(())
    }
}
