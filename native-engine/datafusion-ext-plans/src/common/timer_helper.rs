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

use std::{
    future::Future,
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use datafusion::physical_plan::{
    metrics::{ExecutionPlanMetricsSet, MetricValue, Time},
    Metric,
};
use futures::{future::BoxFuture, FutureExt};

pub trait TimerHelper {
    fn with_timer<T>(&self, f: impl FnOnce() -> T) -> T;
    fn with_timer_async<'a, T>(&'a self, f: impl Future<Output = T> + Send + 'a) -> BoxFuture<T>;

    fn exclude_timer<T>(&self, f: impl FnOnce() -> T) -> T;
    fn exclude_timer_async<'a, T>(&'a self, f: impl Future<Output = T> + Send + 'a)
        -> BoxFuture<T>;

    fn duration(&self) -> Duration;
    fn sub_duration(&self, duration: Duration);

    fn wrap_writer<W: Write>(&self, w: W) -> TimedWriter<W>;
}

impl TimerHelper for Time {
    fn with_timer<T>(&self, f: impl FnOnce() -> T) -> T {
        let _timer = self.timer();
        f()
    }

    fn with_timer_async<'a, T>(&'a self, f: impl Future<Output = T> + Send + 'a) -> BoxFuture<T> {
        let time = self.clone();
        let start_time = Instant::now();
        f.inspect(move |_| time.add_duration(start_time.elapsed()))
            .boxed()
    }

    fn exclude_timer<T>(&self, f: impl FnOnce() -> T) -> T {
        let start_time = Instant::now();
        let t = f();
        self.sub_duration(start_time.elapsed());
        t
    }

    fn exclude_timer_async<'a, T>(
        &'a self,
        f: impl Future<Output = T> + Send + 'a,
    ) -> BoxFuture<T> {
        let time = self.clone();
        let start_time = Instant::now();
        f.inspect(move |_| time.sub_duration(start_time.elapsed()))
            .boxed()
    }

    fn duration(&self) -> Duration {
        Duration::from_nanos(self.value() as u64)
    }

    fn sub_duration(&self, duration: Duration) {
        pub struct XTime {
            pub nanos: Arc<AtomicUsize>,
        }
        assert_eq!(size_of::<Time>(), size_of::<XTime>());

        let xtime = unsafe {
            // safety: access private nanos field
            &*(self as *const Time as *const XTime)
        };
        xtime.nanos.fetch_sub(duration.as_nanos() as usize, Relaxed);
    }

    fn wrap_writer<W: Write>(&self, w: W) -> TimedWriter<W> {
        TimedWriter(w, self.clone())
    }
}

pub struct TimedWriter<W: Write>(pub W, Time);

impl<W: Write> Write for TimedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.1.with_timer(|| self.0.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.1.with_timer(|| self.0.flush())
    }
}

pub trait RegisterTimer {
    fn register_timer(&self, name: &str, partition: usize) -> Time;
}

impl RegisterTimer for ExecutionPlanMetricsSet {
    fn register_timer(&self, name: &str, partition: usize) -> Time {
        let time = Time::new();
        self.register(Arc::new(Metric::new(
            MetricValue::Time {
                name: name.to_owned().into(),
                time: time.clone(),
            },
            Some(partition),
        )));
        time
    }
}
