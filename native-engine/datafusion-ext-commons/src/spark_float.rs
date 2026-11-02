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

use std::cmp::Ordering;

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

#[cfg(test)]
mod tests {
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
}
