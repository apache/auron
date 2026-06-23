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

use std::{cell::UnsafeCell, sync::Arc};

use arrow::array::ArrayBuilder;
use datafusion::common::DataFusionError;

#[derive(Clone, Debug)]
pub(crate) struct SharedArrayBuilder {
    inner: Arc<UnsafeCell<dyn ArrayBuilder>>,
}

unsafe impl Send for SharedArrayBuilder {}
unsafe impl Sync for SharedArrayBuilder {}

impl SharedArrayBuilder {
    pub fn new(builder: impl ArrayBuilder) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(builder)),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_dyn_mut(&self) -> &mut dyn ArrayBuilder {
        // safety: get value from UnsafeCell
        unsafe { &mut *self.inner.get() }
    }

    pub(crate) fn get_mut<T: ArrayBuilder>(
        &self,
    ) -> datafusion::common::Result<SharedArrayBuilderHolder<T>> {
        SharedArrayBuilderHolder::try_new(self.clone())
    }

    pub(crate) fn len(&self) -> usize {
        self.get_dyn_mut().len()
    }
}
pub struct SharedArrayBuilderHolder<T: ArrayBuilder> {
    _shared: SharedArrayBuilder,
    holder: UnsafeCell<&'static mut T>, // bypass lifetime checking
}

unsafe impl<T: ArrayBuilder> Send for SharedArrayBuilderHolder<T> {}
unsafe impl<T: ArrayBuilder> Sync for SharedArrayBuilderHolder<T> {}

impl<T: ArrayBuilder> SharedArrayBuilderHolder<T> {
    fn try_new(shared: SharedArrayBuilder) -> datafusion::common::Result<Self> {
        let shared_cloned = shared.clone();
        let borrowed = shared.get_dyn_mut();
        // println!("The generic type parameter is: {}", type_name::<T>());
        let holder = borrowed.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "failed to downcast array builder {shared_cloned:?}",
            ))
        })?;
        Ok(Self {
            _shared: shared_cloned,
            holder: unsafe {
                // safety: holder has the same lifetime as shared, this bypasses lifetime
                // checking
                UnsafeCell::new(std::mem::transmute::<_, &'static mut T>(holder))
            },
        })
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_mut(&self) -> &mut T {
        // safety: bypass mutability checking
        unsafe { *self.holder.get() }
    }
}
