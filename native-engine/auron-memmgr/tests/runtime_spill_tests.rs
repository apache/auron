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
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Context,
};

use async_trait::async_trait;
use auron_memmgr::{
    runtime_mem_manager::{ConsumerInfo, MemDomain, RuntimeMemManager},
    runtime_spill_coordinator::{
        RuntimeSpillCoordinator, RuntimeWatermarkEvaluator, SpillAction,
        update_consumer_mem_used_with_custom_updater,
    },
};
use futures::{FutureExt, channel::oneshot, executor::block_on, future::pending, task::noop_waker};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

struct TestDomain;
static TEST_LOCK: Mutex<()> = Mutex::new(());
static PRESSURE: AtomicUsize = AtomicUsize::new(100);
static INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static PREPARATION: Mutex<Option<oneshot::Receiver<()>>> = Mutex::new(None);

fn test_lock() -> parking_lot::MutexGuard<'static, ()> {
    let lock = TEST_LOCK.lock();
    PRESSURE.store(100, Ordering::SeqCst);
    lock
}
#[async_trait]
impl MemDomain for TestDomain {
    async fn on_before_spill(_consumer_name: &str) {
        let receiver = PREPARATION.lock().take();
        if let Some(receiver) = receiver {
            receiver.await.expect("finish spill preparation");
        }
    }

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
fn active_spill_counts_toward_concurrency_limit() {
    let _lock = test_lock();
    let small = Consumer::new("small");
    let large = Consumer::new("large");
    // Directly account reservations without triggering a spill.
    small.0.status.lock().mem_used = 100;
    large.0.status.lock().mem_used = 200;
    let mm = RuntimeMemManager::<TestDomain>::get();
    mm.status.lock().total_used = 300;
    mm.status.lock().mem_spillables = 300;
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    let mut active = Box::pin(coordinator.spill_in_action("small", &small.0, || pending()));
    let waker = noop_waker();
    assert!(
        active
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        matches!(
            coordinator.update_consumer_mem_used(&large.0),
            SpillAction::WaitAndRetry(_)
        ),
        "a selected consumer must not bypass an active spill"
    );
}
#[test]
fn growing_report_inside_spill_does_not_reenter_spill() {
    let _lock = test_lock();
    let consumer = Consumer::new("recursive");
    let nested_spills = AtomicUsize::new(0);
    block_on(update_consumer_mem_used_with_custom_updater(
        "recursive",
        &consumer.0,
        |status| {
            let old = std::mem::replace(&mut status.mem_used, 100);
            (old, 100)
        },
        false,
        || async {
            update_consumer_mem_used_with_custom_updater(
                "recursive",
                &consumer.0,
                |status| {
                    let old = std::mem::replace(&mut status.mem_used, 200);
                    (old, 200)
                },
                false,
                || async {
                    nested_spills.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        },
    ))
    .expect("usage report");
    assert_eq!(
        nested_spills.load(Ordering::SeqCst),
        0,
        "spill callbacks must not recursively spill their own consumer"
    );
}

fn account(consumer: &Consumer, used: usize) {
    let manager = RuntimeMemManager::<TestDomain>::get();
    let mut status = manager.status.lock();
    let mut consumer_status = consumer.0.status.lock();
    status.total_used = status.total_used - consumer_status.mem_used + used;
    if consumer_status.spillable {
        status.mem_spillables = status.mem_spillables - consumer_status.mem_used + used;
    }
    consumer_status.mem_used = used;
}

#[test]
fn cancellation_releases_slot_and_wakes_waiter() {
    let _lock = test_lock();
    let first = Consumer::new("first");
    let second = Consumer::new("second");
    account(&first, 100);
    account(&second, 200);
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    let mut active = Box::pin(coordinator.spill_in_action("first", &first.0, pending));
    assert!(active.as_mut().now_or_never().is_none());
    let waiter = match coordinator.update_consumer_mem_used(&second.0) {
        SpillAction::WaitAndRetry(receiver) => receiver,
        _ => unreachable!("expected waiter"),
    };
    drop(active);
    assert!(
        waiter
            .now_or_never()
            .expect("cancellation must wake waiter")
            .is_ok()
    );
    let completed = coordinator
        .spill_in_action("second", &second.0, || async { Ok(()) })
        .now_or_never();
    assert!(
        completed
            .expect("cancelled spill must release its slot")
            .is_ok()
    );
}

#[test]
fn concurrent_admission_cannot_exceed_limit() {
    let _lock = test_lock();
    let first = Consumer::new("first");
    let second = Consumer::new("second");
    account(&first, 100);
    account(&second, 200);
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    // Both callers can pass the advisory check before either callback starts.
    assert!(matches!(
        coordinator.update_consumer_mem_used(&first.0),
        SpillAction::DoSpill
    ));
    assert!(matches!(
        coordinator.update_consumer_mem_used(&second.0),
        SpillAction::DoSpill
    ));
    let mut active = Box::pin(coordinator.spill_in_action("first", &first.0, pending));
    assert!(active.as_mut().now_or_never().is_none());
    let started = AtomicUsize::new(0);
    let mut next = Box::pin(coordinator.spill_in_action("second", &second.0, || async {
        started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    assert!(next.as_mut().now_or_never().is_none());
    assert_eq!(started.load(Ordering::SeqCst), 0);
    drop(active);
    assert!(
        next.as_mut()
            .now_or_never()
            .expect("next spill starts after cancellation")
            .is_ok()
    );
    assert_eq!(started.load(Ordering::SeqCst), 1);
}

#[test]
fn pinning_waiting_consumer_skips_its_callback() {
    let _lock = test_lock();
    let first = Consumer::new("first");
    let second = Consumer::new("second");
    account(&first, 100);
    account(&second, 200);
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    let mut active = Box::pin(coordinator.spill_in_action("first", &first.0, pending));
    assert!(active.as_mut().now_or_never().is_none());
    let mut next = Box::pin(coordinator.spill_in_action("second", &second.0, || async {
        unreachable!("a pinned consumer must not spill")
    }));
    assert!(next.as_mut().now_or_never().is_none());
    RuntimeMemManager::<TestDomain>::set_consumer_spillable(&second.0, false);
    assert!(
        next.as_mut()
            .now_or_never()
            .expect("pinning wakes the reporter")
            .is_ok()
    );
    assert_eq!(
        RuntimeMemManager::<TestDomain>::get()
            .status
            .lock()
            .mem_spillables,
        100
    );
}

#[test]
fn releasing_memory_wakes_reporters_before_spill_finishes() {
    let _lock = test_lock();
    let first = Consumer::new("first");
    let second = Consumer::new("second");
    account(&first, 100);
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    let mut active = Box::pin(coordinator.spill_in_action("first", &first.0, pending));
    assert!(active.as_mut().now_or_never().is_none());
    let mut next = Box::pin(update_consumer_mem_used_with_custom_updater(
        "second",
        &second.0,
        |status| {
            let old = std::mem::replace(&mut status.mem_used, 200);
            (old, 200)
        },
        false,
        || async { unreachable!("pressure has already fallen") },
    ));
    assert!(next.as_mut().now_or_never().is_none());
    PRESSURE.store(50, Ordering::SeqCst);
    block_on(update_consumer_mem_used_with_custom_updater(
        "first",
        &first.0,
        |status| {
            let old = std::mem::replace(&mut status.mem_used, 0);
            (old, 0)
        },
        false,
        || async { unreachable!("releasing memory must not spill") },
    ))
    .expect("release");
    assert!(
        next.as_mut()
            .now_or_never()
            .expect("memory release wakes waiter")
            .is_ok()
    );
}

#[test]
fn low_watermark_elects_largest_and_respects_active_spill() {
    let _lock = test_lock();
    let small = Consumer::new("small");
    let large = Consumer::new("large");
    account(&small, 100);
    account(&large, 200);
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    PRESSURE.store(79, Ordering::SeqCst);
    assert!(matches!(
        coordinator.update_consumer_mem_used(&large.0),
        SpillAction::DoNotSpill
    ));
    PRESSURE.store(85, Ordering::SeqCst);
    assert!(matches!(
        coordinator.update_consumer_mem_used(&small.0),
        SpillAction::DoNotSpill
    ));
    assert!(matches!(
        coordinator.update_consumer_mem_used(&large.0),
        SpillAction::DoSpill
    ));
    let mut active = Box::pin(coordinator.spill_in_action("small", &small.0, pending));
    assert!(active.as_mut().now_or_never().is_none());
    assert!(matches!(
        coordinator.update_consumer_mem_used(&large.0),
        SpillAction::DoNotSpill
    ));
    drop(active);
    account(&large, 0);
    assert!(matches!(
        coordinator.update_consumer_mem_used(&small.0),
        SpillAction::DoSpill
    ));
}

#[test]
fn spill_error_releases_slot() {
    let _lock = test_lock();
    let first = Consumer::new("first");
    let second = Consumer::new("second");
    let coordinator = RuntimeSpillCoordinator::<TestDomain>::get();
    assert!(
        block_on(coordinator.spill_in_action("first", &first.0, || async {
            Err(datafusion::common::DataFusionError::Execution(
                "injected spill error".into(),
            ))
        }))
        .is_err()
    );
    assert!(
        coordinator
            .spill_in_action("second", &second.0, || async { Ok(()) })
            .now_or_never()
            .expect("failed spill must release slot")
            .is_ok()
    );
}

#[test]
fn initialization_hook_runs_once() {
    let _lock = test_lock();
    RuntimeMemManager::<TestDomain>::init_with_total(1000);
    RuntimeMemManager::<TestDomain>::init_with_total(2000);
    assert_eq!(INITIALIZATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(RuntimeMemManager::<TestDomain>::get().total, 1000);
}

fn spill_after_preparation(forced: bool, relieve_pressure: impl FnOnce(&Consumer)) -> usize {
    let consumer = Consumer::new("preparing");
    let spills = AtomicUsize::new(0);
    let (sender, receiver) = oneshot::channel();
    *PREPARATION.lock() = Some(receiver);
    let mut report = Box::pin(update_consumer_mem_used_with_custom_updater(
        "preparing",
        &consumer.0,
        |status| {
            let old = std::mem::replace(&mut status.mem_used, 100);
            (old, 100)
        },
        forced,
        || async {
            spills.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    ));
    assert!(report.as_mut().now_or_never().is_none());
    relieve_pressure(&consumer);
    sender.send(()).expect("resume spill preparation");
    report
        .as_mut()
        .now_or_never()
        .expect("report finishes")
        .expect("spill decision");
    spills.load(Ordering::SeqCst)
}

#[test]
fn automatic_spill_skips_empty_consumer_after_preparation() {
    let _lock = test_lock();
    assert_eq!(
        spill_after_preparation(false, |consumer| account(consumer, 0)),
        0
    );
}

#[test]
fn automatic_spill_rechecks_pressure_after_preparation() {
    let _lock = test_lock();
    assert_eq!(
        spill_after_preparation(false, |_| PRESSURE.store(50, Ordering::SeqCst)),
        0
    );
}

#[test]
fn forced_spill_bypasses_pressure_after_preparation() {
    let _lock = test_lock();
    assert_eq!(
        spill_after_preparation(true, |consumer| {
            account(consumer, 0);
            PRESSURE.store(50, Ordering::SeqCst);
        }),
        1
    );
}
