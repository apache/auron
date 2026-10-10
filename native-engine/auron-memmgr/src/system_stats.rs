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

use auron_jni_bridge::{is_jni_bridge_inited, jni_call_static};

pub(crate) fn get_mem_jvm_direct_used() -> usize {
    if is_jni_bridge_inited() {
        jni_call_static!(JniBridge.getDirectMemoryUsed() -> i64).unwrap_or_default() as usize
    } else {
        0
    }
}

pub(crate) fn get_proc_memory_limited() -> usize {
    if is_jni_bridge_inited() {
        jni_call_static!(JniBridge.getTotalMemoryLimited() -> i64).unwrap_or_default() as usize
    } else {
        0
    }
}

pub(crate) fn get_proc_memory_used() -> usize {
    #[cfg(target_os = "linux")]
    fn get_vmrss_used() -> usize {
        use procfs::{ProcResult, process::Process};

        fn get_vmrss_used_impl() -> ProcResult<usize> {
            let self_proc = Process::myself()?;
            let statm = self_proc.statm()?;
            Ok(statm.resident as usize * procfs::page_size() as usize)
        }
        get_vmrss_used_impl().unwrap_or(0)
    }

    #[cfg(not(target_os = "linux"))]
    fn get_vmrss_used() -> usize {
        0
    }
    get_vmrss_used()
}
