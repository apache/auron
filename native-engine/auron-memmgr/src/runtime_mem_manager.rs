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
    marker::PhantomData,
    sync::{Arc, Once},
};

use async_trait::async_trait;
use bytesize::ByteSize;
use once_cell::sync::OnceCell;
use parking_lot::{Condvar, Mutex};

use crate::runtime_spill_coordinator::RuntimeSpillCoordinator;

/// Domain-specific memory accounting and spill policy.
#[async_trait]
pub trait MemDomain: Send + Sync + Sized + 'static {
    fn name() -> &'static str;

    /// Minimum reservation for automatic spill checks.
    fn min_trigger_size() -> usize {
        0
    }

    /// Ratios satisfying `0 < L1 <= L2 <= 1`.
    fn watermark_config() -> (f64, f64);

    /// Maximum active spills at or above L2.
    fn num_max_spilling_consumers() -> usize;

    /// Called before each spill; may await an async precondition.
    async fn on_before_spill(_consumer_name: &str) {}

    /// Called once after the manager singleton is created.
    fn on_manager_initialized() {}

    fn manager_cell() -> &'static OnceCell<Arc<RuntimeMemManager<Self>>>;

    fn coordinator_cell() -> &'static OnceCell<RuntimeSpillCoordinator<Self>>;
}

pub struct ConsumerInfo<D: MemDomain> {
    pub name: String,
    pub status: Mutex<ConsumerStatus>,
    _marker: PhantomData<D>,
}

impl<D: MemDomain> std::fmt::Debug for ConsumerInfo<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerInfo")
            .field("name", &self.name)
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConsumerStatus {
    pub mem_used: usize,
    pub spillable: bool,
}

pub struct ManagerStatus<D: MemDomain> {
    pub num_consumers: usize,
    pub total_used: usize,
    pub num_spillables: usize,
    pub mem_spillables: usize,
    _marker: PhantomData<D>,
}

impl<D: MemDomain> Default for ManagerStatus<D> {
    fn default() -> Self {
        Self {
            num_consumers: 0,
            total_used: 0,
            num_spillables: 0,
            mem_spillables: 0,
            _marker: PhantomData,
        }
    }
}

impl<D: MemDomain> Clone for ManagerStatus<D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D: MemDomain> Copy for ManagerStatus<D> {}

impl<D: MemDomain> ManagerStatus<D> {
    pub fn update_total_used_with_diff(&mut self, diff_used: isize) -> usize {
        assert!(self.total_used as isize + diff_used >= 0);

        let new_used = (self.total_used as isize + diff_used) as usize;
        let old_used = std::mem::replace(&mut self.total_used, new_used);

        if new_used < old_used {
            RuntimeMemManager::<D>::get().cv.notify_all();
        }
        new_used
    }
}

pub struct RuntimeMemManager<D: MemDomain> {
    pub total: usize,
    pub consumers: Mutex<Vec<Arc<ConsumerInfo<D>>>>,
    pub status: Mutex<ManagerStatus<D>>,
    pub cv: Condvar,
    initialized_hook: Once,
}

impl<D: MemDomain> RuntimeMemManager<D> {
    pub fn init_with_total(total: usize) {
        D::manager_cell().get_or_init(|| {
            log::info!(
                "{} manager initialized with total memory: {}",
                D::name(),
                ByteSize(total as u64),
            );
            Arc::new(RuntimeMemManager {
                total,
                consumers: Mutex::default(),
                status: Mutex::new(ManagerStatus::default()),
                cv: Condvar::default(),
                initialized_hook: Once::new(),
            })
        });
        Self::get()
            .initialized_hook
            .call_once(D::on_manager_initialized);
    }

    pub fn initialized() -> bool {
        D::manager_cell().get().is_some()
    }

    pub fn get() -> &'static RuntimeMemManager<D> {
        D::manager_cell()
            .get()
            .expect("memory domain manager not initialized")
    }

    pub fn num_consumers(&self) -> usize {
        self.consumers.lock().len()
    }

    pub fn total_used(&self) -> usize {
        self.status.lock().total_used
    }

    pub fn mem_used_percent(&self) -> f64 {
        let used = self.total_used();
        if self.total == 0 {
            if used == 0 { 0.0 } else { f64::INFINITY }
        } else {
            used as f64 / self.total as f64
        }
    }

    pub fn register_consumer_info(name: String, spillable: bool) -> Arc<ConsumerInfo<D>> {
        let consumer_info = Arc::new(ConsumerInfo {
            name,
            status: Mutex::new(ConsumerStatus {
                mem_used: 0,
                spillable,
            }),
            _marker: PhantomData,
        });
        log::info!(
            "{} manager registering consumer: {}",
            D::name(),
            consumer_info.name
        );

        let mm = Self::get();
        let mut consumers = mm.consumers.lock();
        let mut status = mm.status.lock();
        consumers.push(consumer_info.clone());
        status.num_consumers += 1;
        if spillable {
            status.num_spillables += 1;
        }
        consumer_info
    }

    pub fn deregister_consumer_info(name: &str, consumer_info: &Arc<ConsumerInfo<D>>) {
        let mm = Self::get();
        let mut consumers = mm.consumers.lock();
        let mut status = mm.status.lock();
        let consumer_status = consumer_info.status.lock();

        assert!(status.total_used >= consumer_status.mem_used);
        status.num_consumers -= 1;
        status.update_total_used_with_diff(-(consumer_status.mem_used as isize));

        if consumer_status.spillable {
            assert!(status.mem_spillables >= consumer_status.mem_used);
            status.num_spillables -= 1;
            status.mem_spillables -= consumer_status.mem_used;
        }
        drop(consumer_status);

        for i in 0..consumers.len() {
            if Arc::ptr_eq(&consumers[i], consumer_info) {
                log::info!("{} manager deregistered consumer: {name}", D::name());
                consumers.swap_remove(i);
                drop(status);
                drop(consumers);
                if let Some(coordinator) = D::coordinator_cell().get() {
                    coordinator.wake_waiters();
                }
                return;
            }
        }
        unreachable!("deregistering non-registered {} memory consumer", D::name());
    }

    pub fn set_consumer_spillable(consumer_info: &Arc<ConsumerInfo<D>>, spillable: bool) {
        let mm = Self::get();
        let mut status = mm.status.lock();
        let mut consumer = consumer_info.status.lock();
        if consumer.spillable != spillable {
            if spillable {
                status.num_spillables += 1;
                status.mem_spillables += consumer.mem_used;
            } else {
                status.num_spillables -= 1;
                status.mem_spillables -= consumer.mem_used;
            }
            consumer.spillable = spillable;
        }
        drop(consumer);
        drop(status);
        if let Some(coordinator) = D::coordinator_cell().get() {
            coordinator.wake_waiters();
        }
    }

    pub fn log_consumers(&self) {
        for consumer in &*self.consumers.lock() {
            let consumer_status = consumer.status.lock();
            log::info!(
                "* {} consumer: {}, spillable: {}, mem_used: {}",
                D::name(),
                consumer.name,
                consumer_status.spillable,
                ByteSize(consumer_status.mem_used as u64),
            );
        }
    }
}
