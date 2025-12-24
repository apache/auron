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

use std::sync::Arc;

use blaze_jni_bridge::is_task_running;
use datafusion::{
    common::DataFusionError, datasource::physical_plan::FileScanConfig, error::Result,
    execution::SendableRecordBatchStream,
};
use datafusion_ext_commons::df_execution_err;
use futures_util::StreamExt;

use crate::common::execution_context::ExecutionContext;

pub fn parallel_scan(
    exec_ctx: Arc<ExecutionContext>,
    file_scan_config: FileScanConfig,
    create_file_stream_fn: impl Fn(&FileScanConfig) -> Result<SendableRecordBatchStream>
    + Send
    + 'static,
    num_parallel_files: usize,
) -> Result<SendableRecordBatchStream> {
    let partition_files = file_scan_config
        .file_groups
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();

    if num_parallel_files <= 0 {
        return df_execution_err!("num_parallel_files must be positive, got {num_parallel_files}");
    }

    // no parallel
    if num_parallel_files == 1 {
        let mut file_stream = create_file_stream_fn(&file_scan_config)?;
        let stream = exec_ctx
            .clone()
            .output_with_sender("FileScan", move |sender| async move {
                let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
                let _timer = elapsed_compute.timer();
                sender.exclude_time(&elapsed_compute);

                while let Some(batch) = file_stream.next().await.transpose()? {
                    sender.send(batch).await;
                }
                Ok(())
            });
        return Ok(stream);
    }

    let stream = exec_ctx
        .clone()
        .output_with_sender("FileScan", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            let _timer = elapsed_compute.timer();
            sender.exclude_time(&elapsed_compute);

            let (file_streams_tx, mut file_streams_rx) =
                tokio::sync::mpsc::channel(num_parallel_files - 1);
            let exec_ctx_cloned = exec_ctx.clone();

            // create file streams for each file
            let mut file_streams: Vec<SendableRecordBatchStream> = partition_files
                .into_iter()
                .map(|partition_file| {
                    let mut file_scan_config = file_scan_config.clone();
                    let partition_id = exec_ctx_cloned.partition_id();
                    file_scan_config.file_groups = vec![vec![]; file_scan_config.file_groups.len()];
                    file_scan_config.file_groups[partition_id] = vec![partition_file];
                    create_file_stream_fn(&file_scan_config)
                })
                .collect::<Result<_>>()?;

            // read first batch without parallelism to avoid latency of later operators
            if !file_streams.is_empty() {
                if let Some(batch) = file_streams[0].next().await.transpose()? {
                    sender.send(batch).await;
                }
            }

            // read rest file streams in parallel
            let handle = tokio::spawn(async move {
                for mut file_stream in file_streams {
                    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                    let handle = tokio::spawn(async move {
                        while is_task_running()
                            && let Some(batch) = file_stream.next().await.transpose()?
                        {
                            tx.send(batch).await.or_else(|e| df_execution_err!("{e}"))?;
                        }
                        Ok::<_, DataFusionError>(())
                    });
                    let eager_stream = exec_ctx_cloned.output_with_sender(
                        "FileScan.File",
                        move |sender| async move {
                            while is_task_running()
                                && let Some(batch) = rx.recv().await
                            {
                                sender.send(batch).await;
                            }
                            handle.await.or_else(|e| df_execution_err!("{e}"))??;
                            Ok(())
                        },
                    );

                    file_streams_tx
                        .send(eager_stream)
                        .await
                        .or_else(|e| df_execution_err!("{e}"))?;
                }
                Ok::<_, DataFusionError>(())
            });

            while let Some(mut stream) = file_streams_rx.recv().await {
                while let Some(batch) = stream.next().await.transpose()? {
                    sender.send(batch).await;
                }
            }
            handle
                .await
                .or_else(|e| df_execution_err!("failed to join task handle: {e}"))??;
            Ok(())
        });
    Ok(stream)
}
