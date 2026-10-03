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

//! Automatic spills: none below L1, largest consumer between L1 and L2,
//! bounded concurrency at or above L2.

use std::{
    future::Future,
    sync::{Arc, Weak},
};

use bytesize::ByteSize;
use datafusion::common::Result;
use futures::channel::oneshot;
use parking_lot::Mutex;

use crate::runtime_mem_manager::{ConsumerInfo, ConsumerStatus, MemDomain, RuntimeMemManager};

pub enum SpillAction {
    DoSpill,
    DoNotSpill,
    WaitAndRetry(oneshot::Receiver<()>),
}

pub async fn wait_for_spill(receiver: oneshot::Receiver<()>) -> bool {
    receiver.await.is_ok()
}

struct RuntimeSpillCoordinatorState<D: MemDomain> {
    spilling_consumers: Vec<Weak<ConsumerInfo<D>>>,
    active_consumers: Vec<Weak<ConsumerInfo<D>>>,
    pending_notifiers: Vec<oneshot::Sender<()>>,
}

impl<D: MemDomain> Default for RuntimeSpillCoordinatorState<D> {
    fn default() -> Self {
        Self {
            spilling_consumers: Vec::new(),
            active_consumers: Vec::new(),
            pending_notifiers: Vec::new(),
        }
    }
}

pub struct RuntimeSpillCoordinator<D: MemDomain> {
    state: Mutex<RuntimeSpillCoordinatorState<D>>,
    watermark_l1: f64,
    watermark_l2: f64,
    num_max_spilling_consumers: usize,
}

impl<D: MemDomain + RuntimeWatermarkEvaluator<D>> RuntimeSpillCoordinator<D> {
    pub fn get() -> &'static RuntimeSpillCoordinator<D> {
        D::coordinator_cell().get_or_init(|| {
            let (watermark_l1, watermark_l2) = D::watermark_config();
            let num_max_spilling_consumers = D::num_max_spilling_consumers();

            assert!(
                watermark_l1.is_finite()
                    && watermark_l2.is_finite()
                    && 0.0 < watermark_l1
                    && watermark_l1 <= watermark_l2
                    && watermark_l2 <= 1.0,
                "spill watermarks must satisfy 0 < L1 <= L2 <= 1"
            );
            assert!(
                num_max_spilling_consumers > 0,
                "spill concurrency must be positive"
            );

            log::info!(
                "{} spill coordinator initialized: L1: {watermark_l1}, L2: {watermark_l2}, \
                 max spilling consumers: {num_max_spilling_consumers}",
                D::name(),
            );
            RuntimeSpillCoordinator {
                state: Mutex::new(RuntimeSpillCoordinatorState::default()),
                watermark_l1,
                watermark_l2,
                num_max_spilling_consumers,
            }
        })
    }

    /// Lock order: `coordinator.state -> {manager.consumers, manager.status}
    /// -> consumer.status`.
    pub fn update_consumer_mem_used(&self, consumer_info: &Arc<ConsumerInfo<D>>) -> SpillAction {
        let mut state = self.state.lock();

        state.spilling_consumers.retain(|elected| {
            elected.upgrade().is_some_and(|info| {
                let status = info.status.lock();
                status.spillable && status.mem_used > 0
            })
        });
        if state
            .active_consumers
            .iter()
            .any(|active| weak_ptr_eq(active, consumer_info))
        {
            return SpillAction::DoNotSpill;
        }
        let status = *consumer_info.status.lock();
        if !status.spillable || status.mem_used == 0 {
            return SpillAction::DoNotSpill;
        }
        let watermark = <D as RuntimeWatermarkEvaluator<D>>::watermark();
        if watermark < self.watermark_l1 {
            state.spilling_consumers.clear();
            return SpillAction::DoNotSpill;
        }
        if watermark < self.watermark_l2 {
            if !state.active_consumers.is_empty() {
                return SpillAction::DoNotSpill;
            }
            // Re-elect because the previous consumer may have shrunk.
            state.spilling_consumers.clear();
            elect_largest_spillable_consumer::<D>(&mut state.spilling_consumers);
            return if state
                .spilling_consumers
                .iter()
                .any(|elected| weak_ptr_eq(elected, consumer_info))
            {
                SpillAction::DoSpill
            } else {
                SpillAction::DoNotSpill
            };
        }
        if state.active_consumers.len() >= self.num_max_spilling_consumers {
            return Self::park_reporter(&mut state);
        }
        // Spill the reporter: an idle consumer cannot be triggered here.
        SpillAction::DoSpill
    }

    fn park_reporter(state: &mut RuntimeSpillCoordinatorState<D>) -> SpillAction {
        state
            .pending_notifiers
            .retain(|sender| !sender.is_canceled());
        let (sender, receiver) = oneshot::channel();
        state.pending_notifiers.push(sender);
        SpillAction::WaitAndRetry(receiver)
    }

    /// Forces a spill with a slot guard that wakes waiters on completion or
    /// cancellation.
    pub async fn spill_in_action<F, Fut>(
        &self,
        consumer_name: &str,
        consumer_info: &Arc<ConsumerInfo<D>>,
        do_spill: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.spill_with_mode(consumer_name, consumer_info, do_spill, true)
            .await
    }

    async fn spill_with_mode<F, Fut>(
        &self,
        consumer_name: &str,
        consumer_info: &Arc<ConsumerInfo<D>>,
        do_spill: F,
        forced: bool,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        // Reserve the slot under the same lock used for admission.
        let _guard = loop {
            let receiver = {
                let mut state = self.state.lock();
                let status = *consumer_info.status.lock();
                if !status.spillable
                    || (!forced && status.mem_used == 0)
                    || state
                        .active_consumers
                        .iter()
                        .any(|active| weak_ptr_eq(active, consumer_info))
                {
                    return Ok(());
                }
                let watermark = <D as RuntimeWatermarkEvaluator<D>>::watermark();
                if !forced && watermark < self.watermark_l1 {
                    return Ok(());
                }
                let limit = if watermark < self.watermark_l2 {
                    1
                } else {
                    self.num_max_spilling_consumers
                };
                if state.active_consumers.len() < limit {
                    state.active_consumers.push(Arc::downgrade(consumer_info));
                    break SpillInActionGuard {
                        coordinator: self,
                        consumer: Arc::downgrade(consumer_info),
                    };
                }
                match Self::park_reporter(&mut state) {
                    SpillAction::WaitAndRetry(receiver) => receiver,
                    _ => unreachable!("parking must return a waiter"),
                }
            };
            wait_for_spill(receiver).await;
        };

        if !consumer_info.status.lock().spillable {
            log::info!(
                "{} spill coordinator: {} was pinned before its spill started, skipping",
                D::name(),
                consumer_info.name,
            );
            return Ok(());
        }

        let mem_used = consumer_info.status.lock().mem_used;
        let unspillable = {
            let mm_status = *RuntimeMemManager::<D>::get().status.lock();
            mm_status
                .total_used
                .saturating_sub(mm_status.mem_spillables)
        };
        D::on_before_spill(consumer_name).await;
        let status = *consumer_info.status.lock();
        if !status.spillable
            || (!forced
                && (status.mem_used == 0
                    || <D as RuntimeWatermarkEvaluator<D>>::watermark() < self.watermark_l1))
        {
            return Ok(());
        }
        log::info!(
            "{} manager spilling {consumer_name} (consumer: {}), unspillable: {}, \
             logical watermark: {:.3}, physical watermark: {:.3}",
            D::name(),
            ByteSize(mem_used as u64),
            ByteSize(unspillable as u64),
            <D as RuntimeWatermarkEvaluator<D>>::logical_watermark(),
            <D as RuntimeWatermarkEvaluator<D>>::physical_watermark(),
        );
        do_spill().await
    }

    pub fn remove_consumer(&self, consumer_info: &Arc<ConsumerInfo<D>>) {
        let mut state = self.state.lock();
        state
            .spilling_consumers
            .retain(|elected| !weak_ptr_eq(elected, consumer_info));
    }
}

impl<D: MemDomain> RuntimeSpillCoordinator<D> {
    pub(crate) fn wake_waiters(&self) {
        let notifiers = std::mem::take(&mut self.state.lock().pending_notifiers);
        for notifier in notifiers {
            let _ = notifier.send(());
        }
    }
}

struct SpillInActionGuard<'a, D: MemDomain> {
    coordinator: &'a RuntimeSpillCoordinator<D>,
    consumer: Weak<ConsumerInfo<D>>,
}

impl<D: MemDomain> Drop for SpillInActionGuard<'_, D> {
    fn drop(&mut self) {
        let notifiers = {
            let mut state = self.coordinator.state.lock();
            state
                .active_consumers
                .retain(|active| !Weak::ptr_eq(active, &self.consumer));
            std::mem::take(&mut state.pending_notifiers)
        };
        for notifier in notifiers {
            let _ = notifier.send(());
        }
    }
}

fn elect_largest_spillable_consumer<D: MemDomain + RuntimeWatermarkEvaluator<D>>(
    elected: &mut Vec<Weak<ConsumerInfo<D>>>,
) {
    let mm = RuntimeMemManager::<D>::get();
    let consumers = mm.consumers.lock();

    let mut largest: Option<(usize, &Arc<ConsumerInfo<D>>)> = None;
    let mut num_spillable = 0usize;
    let mut mem_spillable = 0usize;
    let mut mem_unreclaimable = 0usize;
    for consumer_info in consumers.iter() {
        let status = *consumer_info.status.lock();
        if !status.spillable || status.mem_used == 0 {
            mem_unreclaimable += status.mem_used;
            continue;
        }
        num_spillable += 1;
        mem_spillable += status.mem_used;
        if elected
            .iter()
            .any(|already| weak_ptr_eq(already, consumer_info))
        {
            continue;
        }
        if largest.is_none_or(|(mem_used, _)| status.mem_used > mem_used) {
            largest = Some((status.mem_used, consumer_info));
        }
    }

    if let Some((mem_used, consumer_info)) = largest {
        log::info!(
            "{} spill coordinator elected {} (mem_used: {}) to spill; \
             spillable: {}/{} consumers holding {}, unreclaimable holding {}",
            D::name(),
            consumer_info.name,
            ByteSize(mem_used as u64),
            num_spillable,
            consumers.len(),
            ByteSize(mem_spillable as u64),
            ByteSize(mem_unreclaimable as u64),
        );
        elected.push(Arc::downgrade(consumer_info));
    } else {
        static LAST_WARN: Mutex<Option<std::time::Instant>> = Mutex::new(None);
        let mut last_warn = LAST_WARN.lock();
        let now = std::time::Instant::now();
        let should_warn = last_warn
            .is_none_or(|prev| now.duration_since(prev) >= std::time::Duration::from_secs(1));
        if should_warn {
            *last_warn = Some(now);
            drop(last_warn);
            log::warn!(
                "{} spill coordinator found NO spillable consumer to elect: \
                 {} consumers registered, {} spillable holding {}, unreclaimable holding {}, \
                 already elected: {}, logical watermark: {:.3}",
                D::name(),
                consumers.len(),
                num_spillable,
                ByteSize(mem_spillable as u64),
                ByteSize(mem_unreclaimable as u64),
                elected.len(),
                <D as RuntimeWatermarkEvaluator<D>>::logical_watermark(),
            );
        }
    }
}

fn weak_ptr_eq<D: MemDomain>(weak: &Weak<ConsumerInfo<D>>, strong: &Arc<ConsumerInfo<D>>) -> bool {
    std::ptr::eq(weak.as_ptr(), Arc::as_ptr(strong))
}

/// Combines logical usage with domain-specific physical pressure.
pub trait RuntimeWatermarkEvaluator<D: MemDomain> {
    fn physical_watermark() -> f64;
    fn logical_watermark() -> f64;

    fn watermark() -> f64 {
        Self::physical_watermark().max(Self::logical_watermark())
    }
}

pub async fn update_consumer_mem_used_with_custom_updater<D, F, Fut>(
    consumer_name: &str,
    consumer_info: &Arc<ConsumerInfo<D>>,
    updater: impl Fn(&mut ConsumerStatus) -> (usize, usize),
    forced: bool,
    do_spill: F,
) -> Result<()>
where
    D: MemDomain + RuntimeWatermarkEvaluator<D>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mm = RuntimeMemManager::<D>::get();

    {
        let mut status = mm.status.lock();
        let mut consumer_status = consumer_info.status.lock();

        let (old_used, new_used) = updater(&mut consumer_status);
        let spillable = consumer_status.spillable;
        let diff_used = new_used as isize - old_used as isize;
        assert!(
            !forced || spillable,
            "forced spilling an unspillable {} consumer",
            D::name()
        );

        status.update_total_used_with_diff(diff_used);

        if spillable {
            assert!(status.mem_spillables as isize + diff_used >= 0);
            status.mem_spillables = (status.mem_spillables as isize + diff_used) as usize;
        }
        drop(consumer_status);
        drop(status);

        if new_used < old_used {
            if let Some(coordinator) = D::coordinator_cell().get() {
                coordinator.wake_waiters();
            }
        }
        if !forced
            && (new_used == 0
                || !spillable
                || new_used <= old_used
                || new_used <= D::min_trigger_size())
        {
            return Ok(());
        }
    }

    if forced {
        return RuntimeSpillCoordinator::<D>::get()
            .spill_in_action(consumer_name, consumer_info, do_spill)
            .await;
    }

    spill_consumer_if_needed(consumer_name, consumer_info, do_spill).await
}

async fn spill_consumer_if_needed<D, F, Fut>(
    consumer_name: &str,
    consumer_info: &Arc<ConsumerInfo<D>>,
    do_spill: F,
) -> Result<()>
where
    D: MemDomain + RuntimeWatermarkEvaluator<D>,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let consumer_status = *consumer_info.status.lock();
    if !consumer_status.spillable || consumer_status.mem_used == 0 {
        return Ok(());
    }
    let coordinator = RuntimeSpillCoordinator::<D>::get();
    loop {
        match coordinator.update_consumer_mem_used(consumer_info) {
            SpillAction::DoNotSpill => return Ok(()),
            SpillAction::DoSpill => {
                return coordinator
                    .spill_with_mode(consumer_name, consumer_info, do_spill, false)
                    .await;
            }
            SpillAction::WaitAndRetry(receiver) => {
                wait_for_spill(receiver).await;
            }
        }
    }
}
