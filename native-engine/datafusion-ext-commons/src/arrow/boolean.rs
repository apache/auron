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

use arrow::array::{Array, BooleanArray};

/// Returns a BooleanArray where nulls are converted to `false` and the result
/// has no null bitmap (all values are valid).
#[inline]
pub fn nulls_to_false(is_boolean: &BooleanArray) -> BooleanArray {
    match is_boolean.nulls() {
        Some(nulls) => {
            let is_not_null = nulls.inner();
            BooleanArray::new(is_boolean.values() & is_not_null, None)
        }
        None => is_boolean.clone(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray};

    use super::nulls_to_false;

    #[test]
    fn converts_nulls_to_false() {
        let input = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let output = nulls_to_false(&input);

        assert!(output.nulls().is_none());

        let got: Vec<Option<bool>> = output.iter().collect();
        let expected = vec![Some(true), Some(false), Some(false)];
        assert_eq!(got, expected);
    }

    #[test]
    fn preserves_when_no_nulls() {
        let input = BooleanArray::from(vec![Some(false), Some(true)]);
        let output = nulls_to_false(&input);

        assert!(output.nulls().is_none());
        let got: Vec<Option<bool>> = output.iter().collect();
        let expected = vec![Some(false), Some(true)];
        assert_eq!(got, expected);
    }
}
