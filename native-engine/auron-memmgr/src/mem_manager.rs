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

use std::sync::{Arc, Weak};

use async_trait::async_trait;
use bytesize::ByteSize;
use datafusion::common::Result;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    runtime_mem_manager::{
        ConsumerInfo, ConsumerStatus, ManagerStatus, MemDomain, RuntimeMemManager,
    },
    runtime_spill_coordinator::{
        RuntimeSpillCoordinator, RuntimeWatermarkEvaluator,
        update_consumer_mem_used_with_custom_updater,
    },
    system_stats::{get_mem_jvm_direct_used, get_proc_memory_limited, get_proc_memory_used},
};

pub const MIN_TRIGGER_SIZE: usize = 1 << 24;

const DEFAULT_WATERMARK_L1: f64 = 0.8;
const DEFAULT_WATERMARK_L2: f64 = 0.9;
const DEFAULT_NUM_MAX_SPILLING_CONSUMERS: usize = 2;

/// Host off-heap memory domain.
pub struct Mem;

pub type MemManager = RuntimeMemManager<Mem>;
pub type MemConsumerInfo = ConsumerInfo<Mem>;
pub type MemConsumerStatus = ConsumerStatus;
pub type MemManagerStatus = ManagerStatus<Mem>;
pub type MemSpillCoordinator = RuntimeSpillCoordinator<Mem>;

static MEM_MANAGER_CELL: OnceCell<Arc<RuntimeMemManager<Mem>>> = OnceCell::new();
static MEM_SPILL_COORDINATOR_CELL: OnceCell<RuntimeSpillCoordinator<Mem>> = OnceCell::new();

#[async_trait]
impl MemDomain for Mem {
    fn name() -> &'static str {
        "mem"
    }

    fn min_trigger_size() -> usize {
        MIN_TRIGGER_SIZE
    }

    fn watermark_config() -> (f64, f64) {
        use auron_jni_bridge::conf::{self, DoubleConf};

        let watermark_l1 = conf::MEM_SPILL_WATERMARK_L1
            .value()
            .unwrap_or(DEFAULT_WATERMARK_L1);
        let watermark_l2 = conf::MEM_SPILL_WATERMARK_L2
            .value()
            .unwrap_or(DEFAULT_WATERMARK_L2);
        validated_watermarks(watermark_l1, watermark_l2)
    }

    fn num_max_spilling_consumers() -> usize {
        use auron_jni_bridge::conf::{self, IntConf};

        let configured = conf::NUM_MAX_SPILLING_CONSUMERS
            .value()
            .unwrap_or(DEFAULT_NUM_MAX_SPILLING_CONSUMERS as i32);
        if configured > 0 {
            return configured as usize;
        }
        DEFAULT_NUM_MAX_SPILLING_CONSUMERS
    }

    fn manager_cell() -> &'static OnceCell<Arc<RuntimeMemManager<Mem>>> {
        &MEM_MANAGER_CELL
    }

    fn coordinator_cell() -> &'static OnceCell<RuntimeSpillCoordinator<Mem>> {
        &MEM_SPILL_COORDINATOR_CELL
    }
}

impl RuntimeWatermarkEvaluator<Mem> for Mem {
    fn physical_watermark() -> f64 {
        use auron_jni_bridge::conf::{self, DoubleConf};
        static PROCESS_MEMORY_FRACTION: OnceCell<f64> = OnceCell::new();
        let fraction = *PROCESS_MEMORY_FRACTION
            .get_or_init(|| conf::PROCESS_MEMORY_FRACTION.value().unwrap_or(1.0));
        let limit = (get_proc_memory_limited() as f64 * fraction) as usize;
        if limit > 0 && get_proc_memory_used() > limit {
            1.0
        } else {
            0.0
        }
    }

    fn logical_watermark() -> f64 {
        if !RuntimeMemManager::<Mem>::initialized() {
            return 0.0;
        }
        let mm = RuntimeMemManager::<Mem>::get();
        memory_ratio(
            mm.total_used().saturating_add(get_mem_jvm_direct_used()),
            mm.total,
        )
    }
}

impl RuntimeMemManager<Mem> {
    pub fn init(total: usize) {
        Self::init_with_total(total);
    }

    pub fn register_consumer(mut consumer: Arc<dyn MemConsumer>, spillable: bool) {
        let consumer_info = Self::register_consumer_info(consumer.name().to_owned(), spillable);

        // SAFETY: callers register before sharing the consumer with other tasks.
        unsafe {
            let consumer_mut = Arc::get_mut_unchecked(&mut consumer);
            consumer_mut.set_consumer_info(Arc::downgrade(&consumer_info));
        }
    }

    pub fn deregister_consumer(consumer: &dyn MemConsumer) {
        let consumer_info = consumer.consumer_info();

        // Remove the election before taking manager locks.
        MemSpillCoordinator::get().remove_consumer(&consumer_info);

        Self::deregister_consumer_info(consumer.name(), &consumer_info);
    }

    pub fn dump_status(&self) {
        let status = *self.status.lock();
        log::info!(
            "mem manager status: total: {}, mem_used: {}, jvm_direct: {}, proc resident: {}",
            ByteSize(self.total as u64),
            ByteSize(status.total_used as u64),
            ByteSize(get_mem_jvm_direct_used() as u64),
            ByteSize(get_proc_memory_used() as u64),
        );
        self.log_consumers();
    }
}

#[async_trait]
pub trait MemConsumer: Send + Sync {
    fn name(&self) -> &str;
    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>);
    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo>;

    fn consumer_info(&self) -> Arc<MemConsumerInfo> {
        self.get_consumer_info()
            .upgrade()
            .expect("consumer deregistered")
    }

    fn mem_used_percent(&self) -> f64 {
        let mm = MemManager::get();
        let total = mm.total;
        let mm_status = *mm.status.lock();

        let mem_unspillable = mm_status.total_used - mm_status.mem_spillables;
        let total_managed = total
            .saturating_sub(get_mem_jvm_direct_used())
            .saturating_sub(mem_unspillable);
        let mem_used = self.consumer_info().status.lock().mem_used;
        let consumer_mem_max = total_managed / mm_status.num_spillables.max(1);
        memory_ratio(mem_used, consumer_mem_max)
    }

    fn mem_used(&self) -> usize {
        self.consumer_info().status.lock().mem_used
    }

    fn set_spillable(&self, spillable: bool) {
        MemManager::set_consumer_spillable(&self.consumer_info(), spillable);
    }

    async fn update_mem_used(&self, new_used: usize) -> Result<()>
    where
        Self: Sized,
    {
        let consumer_info = self.consumer_info();
        update_consumer_mem_used_with_custom_updater(
            self.name(),
            &consumer_info,
            |consumer_status| {
                let old_used = std::mem::replace(&mut consumer_status.mem_used, new_used);
                (old_used, new_used)
            },
            false,
            || self.spill(),
        )
        .await
    }

    async fn update_mem_used_with_diff(&self, diff_used: isize) -> Result<()>
    where
        Self: Sized,
    {
        let consumer_info = self.consumer_info();
        update_consumer_mem_used_with_custom_updater(
            self.name(),
            &consumer_info,
            |consumer_status| {
                let old_used = consumer_status.mem_used;
                let new_used = if diff_used > 0 {
                    old_used.saturating_add(diff_used as usize)
                } else {
                    old_used.saturating_sub(diff_used.unsigned_abs())
                };
                consumer_status.mem_used = new_used;
                (old_used, new_used)
            },
            false,
            || self.spill(),
        )
        .await
    }

    async fn force_spill(&self) -> Result<()>
    where
        Self: Sized,
    {
        let consumer_info = self.consumer_info();
        update_consumer_mem_used_with_custom_updater(
            self.name(),
            &consumer_info,
            |consumer_status| {
                let used = consumer_status.mem_used;
                (used, used)
            },
            true,
            || self.spill(),
        )
        .await
    }

    async fn spill(&self) -> Result<()> {
        unimplemented!()
    }
}

fn validated_watermarks(l1: f64, l2: f64) -> (f64, f64) {
    if l1.is_finite() && l2.is_finite() && 0.0 < l1 && l1 <= l2 && l2 <= 1.0 {
        (l1, l2)
    } else {
        log::warn!("invalid memory spill watermarks ({l1}, {l2}); using defaults");
        (DEFAULT_WATERMARK_L1, DEFAULT_WATERMARK_L2)
    }
}

fn memory_ratio(used: usize, total: usize) -> f64 {
    if total == 0 && used == 0 {
        0.0
    } else {
        used as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::executor::block_on;

    use super::*;

    // The mem manager and coordinator are process singletons.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    // Reservations must exceed MIN_TRIGGER_SIZE (16MiB) to trigger automatic
    // spills.
    const MB: usize = 1 << 20;

    struct TestConsumer {
        info: Weak<MemConsumerInfo>,
        spills: AtomicUsize,
        release_per_spill: usize,
    }

    impl TestConsumer {
        fn register(release_per_spill: usize) -> Arc<Self> {
            Self::register_with_spillable(release_per_spill, true)
        }

        fn register_with_spillable(release_per_spill: usize, spillable: bool) -> Arc<Self> {
            MemManager::init(1_000 * MB);
            let consumer = Arc::new(Self {
                info: Weak::new(),
                spills: AtomicUsize::new(0),
                release_per_spill,
            });
            MemManager::register_consumer(consumer.clone(), spillable);
            consumer
        }
    }

    #[async_trait]
    impl MemConsumer for TestConsumer {
        fn name(&self) -> &str {
            "TestMemConsumer"
        }

        fn set_consumer_info(&mut self, info: Weak<MemConsumerInfo>) {
            self.info = info;
        }

        fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
            &self.info
        }

        async fn spill(&self) -> Result<()> {
            self.spills.fetch_add(1, Ordering::SeqCst);
            let remaining = self
                .consumer_info()
                .status
                .lock()
                .mem_used
                .saturating_sub(self.release_per_spill);
            // Decreased and unchanged reports during a spill must not recurse.
            self.update_mem_used(remaining).await?;
            self.update_mem_used(remaining).await
        }
    }

    impl Drop for TestConsumer {
        fn drop(&mut self) {
            MemManager::deregister_consumer(self);
        }
    }

    #[test]
    fn first_reservation_spills_without_recursing_on_reconciliation() -> Result<()> {
        let _guard = TEST_LOCK.lock();
        let consumer = TestConsumer::register(100 * MB);
        block_on(consumer.update_mem_used(1_000 * MB))?;
        assert_eq!(consumer.spills.load(Ordering::SeqCst), 1);
        assert_eq!(MemManager::get().total_used(), 900 * MB);
        drop(consumer);
        assert_eq!(MemManager::get().total_used(), 0);
        Ok(())
    }

    #[test]
    fn pinning_and_forced_spills_preserve_accounting() -> Result<()> {
        let _guard = TEST_LOCK.lock();
        let consumer = TestConsumer::register(usize::MAX);
        block_on(consumer.update_mem_used(200))?;

        consumer.set_spillable(false);
        assert_eq!(MemManager::get().status.lock().mem_spillables, 0);
        assert_eq!(MemManager::get().total_used(), 200);

        consumer.set_spillable(true);
        assert_eq!(MemManager::get().status.lock().mem_spillables, 200);
        block_on(consumer.force_spill())?;
        block_on(consumer.force_spill())?;
        assert_eq!(consumer.spills.load(Ordering::SeqCst), 2);
        assert_eq!(MemManager::get().total_used(), 0);
        assert_eq!(MemManager::get().status.lock().mem_spillables, 0);
        drop(consumer);
        assert_eq!(MemManager::get().total_used(), 0);
        Ok(())
    }
    #[test]
    fn validates_watermarks_and_zero_budget_ratios() {
        assert_eq!(validated_watermarks(0.5, 0.7), (0.5, 0.7));
        for (l1, l2) in [
            (0.9, 0.8),
            (f64::NAN, 0.9),
            (0.0, 0.9),
            (0.8, f64::INFINITY),
        ] {
            assert_eq!(validated_watermarks(l1, l2), (0.8, 0.9));
        }
        assert_eq!(memory_ratio(0, 0), 0.0);
        assert_eq!(memory_ratio(1, 0), f64::INFINITY);
    }

    #[test]
    fn concurrent_pinning_and_reporting_preserve_accounting() -> Result<()> {
        let _guard = TEST_LOCK.lock();
        let consumer = TestConsumer::register(usize::MAX);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..1000 {
                    consumer.set_spillable(false);
                    consumer.set_spillable(true);
                }
            });
            scope.spawn(|| {
                for used in 0..1000 {
                    block_on(consumer.update_mem_used(used)).expect("concurrent report");
                }
            });
        });
        assert_eq!(consumer.mem_used(), 999);
        let status = *MemManager::get().status.lock();
        assert_eq!(status.total_used, 999);
        assert_eq!(status.mem_spillables, 999);
        block_on(consumer.update_mem_used_with_diff(isize::MIN))?;
        assert_eq!(consumer.mem_used(), 0);
        drop(consumer);
        assert_eq!(MemManager::get().total_used(), 0);
        Ok(())
    }
}
