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

use auron_jni_bridge::{conf, conf::BooleanConf, is_task_running};
use datafusion::{
    common::DataFusionError,
    datasource::physical_plan::{FileOpener, FileScanConfig, FileStream, OnError},
    error::Result,
    execution::SendableRecordBatchStream,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_ext_commons::df_execution_err;
use futures_util::StreamExt;
use tokio::{sync::mpsc::Receiver, task::JoinSet};

use crate::common::execution_context::ExecutionContext;

pub fn parallel_scan<O: FileOpener + Send + 'static>(
    exec_ctx: Arc<ExecutionContext>,
    file_scan_config: FileScanConfig,
    create_opener_fn: impl Fn(&FileScanConfig) -> O + Send + 'static,
    num_parallel_files: usize,
) -> Result<SendableRecordBatchStream> {
    let partition_files = file_scan_config
        .file_groups
        .iter()
        .flat_map(|file_group| file_group.iter())
        .cloned()
        .collect::<Vec<_>>();

    let (parallel_files_tx, mut parallel_files_rx) = tokio::sync::mpsc::channel(num_parallel_files);
    let exec_ctx_cloned = exec_ctx.clone();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        for partition_file in partition_files {
            if !is_task_running() {
                break;
            }
            let mut file_scan_config = file_scan_config.clone();
            let partition_id = exec_ctx_cloned.partition_id();
            file_scan_config.file_groups =
                vec![FileGroup::default(); file_scan_config.file_groups.len()];
            file_scan_config.file_groups[partition_id] = FileGroup::new(vec![partition_file]);

            let mut file_stream = FileStream::new(
                &file_scan_config,
                partition_id,
                Arc::new(create_opener_fn(&file_scan_config)),
                exec_ctx_cloned.execution_plan_metrics(),
            )?;
            if conf::IGNORE_CORRUPTED_FILES.value()? {
                file_stream = file_stream.with_on_error(OnError::Skip);
            }
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let mut file_tasks = JoinSet::new();
            file_tasks.spawn(async move {
                while is_task_running()
                    && let Some(batch) = file_stream.next().await.transpose()?
                {
                    tx.send(batch)
                        .await
                        .or_else(|e| df_execution_err!("failed to send batch: {e}"))?;
                }
                Ok::<_, DataFusionError>(())
            });
            let eager_stream = exec_ctx_cloned.clone().output_with_sender(
                "FileScan.File",
                move |sender| async move {
                    while is_task_running()
                        && let Some(batch) = rx.recv().await
                    {
                        sender.send(batch).await;
                    }
                    finish_scan_tasks(rx, file_tasks, !is_task_running()).await
                },
            );

            parallel_files_tx
                .send(eager_stream)
                .await
                .or_else(|e| df_execution_err!("failed to send file stream: {e}"))?;
        }
        Ok::<_, DataFusionError>(())
    });

    let stream = exec_ctx
        .clone()
        .output_with_sender("FileScan", move |sender| async move {
            let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
            let _timer = elapsed_compute.timer();
            sender.exclude_time(&elapsed_compute);

            while is_task_running()
                && let Some(mut stream) = parallel_files_rx.recv().await
            {
                while is_task_running()
                    && let Some(batch) = stream.next().await.transpose()?
                {
                    sender.send(batch).await;
                }
            }
            finish_scan_tasks(parallel_files_rx, tasks, !is_task_running()).await
        });
    Ok(stream)
}

async fn finish_scan_tasks<T>(
    rx: Receiver<T>,
    mut tasks: JoinSet<Result<()>>,
    cancelled: bool,
) -> Result<()> {
    drop(rx);
    if cancelled {
        return Ok(());
    }
    while let Some(result) = tasks.join_next().await {
        result.or_else(|e| df_execution_err!("failed to join task handle: {e}"))??;
    }
    Ok(())
}
