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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use auron_memmgr::{
    runtime_mem_manager::{ConsumerInfo, MemDomain, RuntimeMemManager},
    runtime_spill_coordinator::{
        RuntimeSpillCoordinator, RuntimeWatermarkEvaluator,
        update_consumer_mem_used_with_custom_updater,
    },
};
use futures::executor::block_on;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

struct TestDomain;
static TEST_LOCK: Mutex<()> = Mutex::new(());
static PRESSURE: AtomicUsize = AtomicUsize::new(100);
static INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);

fn test_lock() -> parking_lot::MutexGuard<'static, ()> {
    let lock = TEST_LOCK.lock();
    PRESSURE.store(100, Ordering::SeqCst);
    lock
}
#[async_trait]
impl MemDomain for TestDomain {
    fn on_manager_initialized() {
        INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
    }
    fn name() -> &'static str {
        "test"
    }
    fn watermark_config() -> (f64, f64) {
        (0.8, 0.9)
    }
    fn num_max_spilling_consumers() -> usize {
        1
    }
    fn manager_cell() -> &'static OnceCell<Arc<RuntimeMemManager<Self>>> {
        static CELL: OnceCell<Arc<RuntimeMemManager<TestDomain>>> = OnceCell::new();
        &CELL
    }
    fn coordinator_cell() -> &'static OnceCell<RuntimeSpillCoordinator<Self>> {
        static CELL: OnceCell<RuntimeSpillCoordinator<TestDomain>> = OnceCell::new();
        &CELL
    }
}
impl RuntimeWatermarkEvaluator<TestDomain> for TestDomain {
    fn physical_watermark() -> f64 {
        PRESSURE.load(Ordering::SeqCst) as f64 / 100.0
    }
    fn logical_watermark() -> f64 {
        0.0
    }
}
struct Consumer(Arc<ConsumerInfo<TestDomain>>);
impl Consumer {
    fn new(name: &str) -> Self {
        RuntimeMemManager::<TestDomain>::init_with_total(1000);
        Self(RuntimeMemManager::<TestDomain>::register_consumer_info(
            name.to_owned(),
            true,
        ))
    }
}
impl Drop for Consumer {
    fn drop(&mut self) {
        RuntimeSpillCoordinator::<TestDomain>::get().remove_consumer(&self.0);
        RuntimeMemManager::<TestDomain>::deregister_consumer_info(&self.0.name, &self.0);
    }
}

#[test]
fn initialization_hook_runs_once() {
    let _lock = test_lock();
    RuntimeMemManager::<TestDomain>::init_with_total(1000);
    RuntimeMemManager::<TestDomain>::init_with_total(2000);
    assert_eq!(INITIALIZATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(RuntimeMemManager::<TestDomain>::get().total, 1000);
}

#[test]
fn automatic_spill_triggers_do_spill_above_watermark() {
    let _lock = test_lock();
    let consumer = Consumer::new("auto");
    let spills = AtomicUsize::new(0);
    PRESSURE.store(85, Ordering::SeqCst);
    block_on(update_consumer_mem_used_with_custom_updater(
        "auto",
        &consumer.0,
        |status| {
            let old = std::mem::replace(&mut status.mem_used, 100);
            (old, 100)
        },
        || async {
            spills.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    ))
    .expect("automatic spill");
    assert_eq!(spills.load(Ordering::SeqCst), 1);
}
