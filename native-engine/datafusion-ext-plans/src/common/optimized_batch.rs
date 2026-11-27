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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    datatypes::SchemaRef,
    row::{RowConverter, Rows},
};
use datafusion::{
    common::Result, execution::SendableRecordBatchStream, physical_expr::PhysicalSortExpr,
};
use futures::{Stream, StreamExt};

use super::lazy::{
    LazyBatchRef, LazyBatchStreamAdapter, SendableLazyBatchStream, materialize_stream,
};

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptimizedProps {
    projection: Option<Vec<usize>>,
    input_stats: bool,
    key_rows: Option<Vec<PhysicalSortExpr>>,
}

impl ExecuteOptimizedProps {
    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn input_stats(&self) -> bool {
        self.input_stats
    }

    pub fn key_rows(&self) -> Option<&[PhysicalSortExpr]> {
        self.key_rows.as_deref()
    }

    pub fn with_input_stats(mut self, input_stats: bool) -> Self {
        self.input_stats = input_stats;
        self
    }

    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        self.with_optional_projection(Some(projection))
    }

    pub fn with_optional_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_key_rows(self, key_rows: Vec<PhysicalSortExpr>) -> Self {
        self.with_optional_key_rows(Some(key_rows))
    }

    pub fn with_optional_key_rows(mut self, key_rows: Option<Vec<PhysicalSortExpr>>) -> Self {
        self.key_rows = key_rows;
        self
    }
}

#[derive(Debug, Clone)]
pub struct KeyRows {
    pub rows: Arc<Rows>,
    pub rows_converter: Arc<RowConverter>,
}

pub struct OptimizedBatch {
    batch: LazyBatchRef,
    key_rows: Option<KeyRows>,
}

impl OptimizedBatch {
    pub fn new(batch: LazyBatchRef, key_rows: Option<KeyRows>) -> Self {
        Self { batch, key_rows }
    }

    pub fn as_lazy_batch(&self) -> &LazyBatchRef {
        &self.batch
    }

    pub fn key_rows(&self) -> Option<&KeyRows> {
        self.key_rows.as_ref()
    }

    pub fn into_lazy_batch(self) -> LazyBatchRef {
        self.batch
    }
}

pub trait OptimizedBatchStream: Stream<Item = Result<OptimizedBatch>> {
    fn schema(&self) -> SchemaRef;
    fn keys(&self) -> Option<&[PhysicalSortExpr]>;
}

pub type SendableOptimizedBatchStream = Pin<Box<dyn OptimizedBatchStream + Send>>;

pub struct OptimizedBatchStreamAdapter {
    stream: Pin<Box<dyn Stream<Item = Result<OptimizedBatch>> + Send>>,
    schema: SchemaRef,
    keys: Option<Vec<PhysicalSortExpr>>,
}

impl OptimizedBatchStreamAdapter {
    pub fn new(
        stream: impl Stream<Item = Result<OptimizedBatch>> + Send + 'static,
        schema: SchemaRef,
        keys: Option<Vec<PhysicalSortExpr>>,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            schema,
            keys,
        }
    }
}

impl Stream for OptimizedBatchStreamAdapter {
    type Item = Result<OptimizedBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl OptimizedBatchStream for OptimizedBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn keys(&self) -> Option<&[PhysicalSortExpr]> {
        self.keys.as_deref()
    }
}

pub fn into_lazy_stream(stream: SendableOptimizedBatchStream) -> SendableLazyBatchStream {
    Box::pin(LazyBatchStreamAdapter::new(
        stream.schema(),
        stream.map(|batch| batch.map(OptimizedBatch::into_lazy_batch)),
    ))
}

pub fn materialize_optimized_stream(
    stream: SendableOptimizedBatchStream,
) -> SendableRecordBatchStream {
    materialize_stream(into_lazy_stream(stream))
}

#[cfg(test)]
mod tests {
    use arrow::{array::RecordBatch, datatypes::Schema};
    use datafusion::common::DataFusionError;
    use futures::TryStreamExt;

    use super::*;

    #[test]
    fn execute_optimized_props_accessors_and_builders() {
        let props = ExecuteOptimizedProps::default();
        assert_eq!(props.projection(), None);
        assert!(!props.input_stats());
        assert_eq!(props.key_rows(), None);

        let keys = vec![PhysicalSortExpr {
            expr: Arc::new(datafusion::physical_expr::expressions::Column::new(
                "key", 0,
            )),
            options: arrow::compute::SortOptions::default(),
        }];
        let props = props
            .with_input_stats(true)
            .with_projection(vec![1, 0, 1])
            .with_key_rows(keys.clone());
        assert_eq!(props.projection(), Some([1, 0, 1].as_slice()));
        assert!(props.input_stats());
        assert_eq!(props.key_rows(), Some(keys.as_slice()));

        let props = props.with_projection(vec![]).with_key_rows(vec![]);
        assert_eq!(props.projection(), Some([].as_slice()));
        assert_eq!(props.key_rows(), Some([].as_slice()));
        assert!(props.input_stats());

        let props = props
            .with_input_stats(false)
            .with_optional_projection(None)
            .with_optional_key_rows(None);
        assert_eq!(props.projection(), None);
        assert!(!props.input_stats());
        assert_eq!(props.key_rows(), None);
    }

    #[tokio::test]
    async fn into_lazy_stream_preserves_batch_identity_and_errors() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batch: LazyBatchRef = Arc::new(RecordBatch::new_empty(schema.clone()));
        let stream: SendableOptimizedBatchStream = Box::pin(OptimizedBatchStreamAdapter::new(
            futures::stream::iter(vec![
                Ok(OptimizedBatch::new(batch.clone(), None)),
                Err(DataFusionError::Execution("test error".to_owned())),
            ]),
            schema.clone(),
            None,
        ));
        assert_eq!(stream.keys(), None);
        let mut stream = into_lazy_stream(stream);
        assert_eq!(stream.schema(), schema);
        let output = stream.try_next().await?.expect("expected batch");
        assert!(Arc::ptr_eq(&batch, &output));
        assert!(
            matches!(stream.try_next().await, Err(DataFusionError::Execution(message)) if message == "test error")
        );
        assert!(stream.try_next().await?.is_none());
        Ok(())
    }

    #[test]
    fn optimized_batch_keeps_optional_key_rows_without_locking() -> Result<()> {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KeyRows>();
        assert_send_sync::<OptimizedBatch>();
        let converter = Arc::new(RowConverter::new(vec![])?);
        let rows = Arc::new(converter.empty_rows(0, 0));
        let batch: LazyBatchRef = Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty())));
        let output = OptimizedBatch::new(
            batch.clone(),
            Some(KeyRows {
                rows: rows.clone(),
                rows_converter: converter.clone(),
            }),
        );
        let keys = output.key_rows().expect("expected key rows");
        assert!(Arc::ptr_eq(&keys.rows, &rows));
        assert!(Arc::ptr_eq(&keys.rows_converter, &converter));
        assert!(Arc::ptr_eq(output.as_lazy_batch(), &batch));
        assert!(Arc::ptr_eq(&output.into_lazy_batch(), &batch));
        Ok(())
    }
}
