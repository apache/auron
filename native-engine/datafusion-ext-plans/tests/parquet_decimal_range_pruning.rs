// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{cell::Cell, collections::HashSet, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, Decimal128Array},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    common::{Column, Result, ScalarValue},
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr,
        expressions::{BinaryExpr, Column as PhysicalColumn, Literal},
        simplifier::PhysicalExprSimplifier,
    },
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};

struct DecimalStatistics {
    values: ArrayRef,
    dictionary_reads: Cell<usize>,
    bloom_probes: Cell<usize>,
}

impl PruningStatistics for DecimalStatistics {
    fn min_values(&self, _: &Column) -> Option<ArrayRef> {
        Some(Arc::clone(&self.values))
    }

    fn max_values(&self, _: &Column) -> Option<ArrayRef> {
        Some(Arc::clone(&self.values))
    }

    fn num_containers(&self) -> usize {
        self.values.len()
    }

    fn null_counts(&self, _: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        self.bloom_probes.set(self.bloom_probes.get() + 1);
        None
    }

    fn dictionary_values(&self, _: &Column) -> Option<ArrayRef> {
        self.dictionary_reads.set(self.dictionary_reads.get() + 1);
        None
    }
}

#[test]
fn decimal_equality_as_range_uses_only_min_max() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(18, 2),
        true,
    )]));
    let values = [-100, 0, 100, 500];
    let statistics = DecimalStatistics {
        values: Arc::new(Decimal128Array::from(values.to_vec()).with_precision_and_scale(18, 2)?),
        dictionary_reads: Cell::new(0),
        bloom_probes: Cell::new(0),
    };
    for value in values {
        let column: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("d", 0));
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(value), 18, 2)));
        let lower = Arc::new(BinaryExpr::new(
            Arc::clone(&column),
            Operator::GtEq,
            Arc::clone(&literal),
        ));
        let upper = Arc::new(BinaryExpr::new(column, Operator::LtEq, literal));
        let expression = Arc::new(BinaryExpr::new(lower, Operator::And, upper));
        let expression = PhysicalExprSimplifier::new(&schema).simplify(expression)?;
        let predicate = PruningPredicate::try_new(expression, Arc::clone(&schema))?;
        assert!(predicate.literal_guarantees().is_empty());
        assert_eq!(
            predicate.prune(&statistics)?,
            values.map(|candidate| candidate == value)
        );
        assert_eq!(statistics.dictionary_reads.get(), 0);
        assert_eq!(statistics.bloom_probes.get(), 0);
    }
    Ok(())
}
