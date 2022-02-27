use std::cell::RefCell;
use std::sync::Arc;

use datafusion::datasource::object_store::ObjectStoreRegistry;
use hdfs_object_store::HDFSSingleFileObjectStore;

pub mod hdfs_object_store; // note: can be changed to priv once plan transforming is removed
pub mod jni_bridge;
pub mod shuffle_reader_exec;
pub mod shuffle_writer_exec;
pub mod util;

mod batch_buffer;

lazy_static::lazy_static! {
    static ref OBJECT_STORE_REGISTRY: ObjectStoreRegistry = {
        let osr = ObjectStoreRegistry::default();
        let hdfs_object_store = Arc::new(HDFSSingleFileObjectStore);
        osr.register_store("hdfs".to_owned(), hdfs_object_store.clone());
        osr.register_store("viewfs".to_owned(), hdfs_object_store.clone());
        osr
    };
}

pub fn global_object_store_registry() -> &'static ObjectStoreRegistry {
    &OBJECT_STORE_REGISTRY
}

// set_job_id/get_job_id should only be used in main thread
thread_local! {
    static JOB_ID: RefCell<String> = RefCell::default()
}

pub fn set_job_id(job_id: &str) {
    JOB_ID.with(|thread_local_job_id| {
        let mut job_id_ref = thread_local_job_id.borrow_mut();
        job_id_ref.clear();
        job_id_ref.push_str(job_id);
    });
}

pub fn get_job_id() -> String {
    JOB_ID.with(|thread_local_job_id| thread_local_job_id.borrow().clone())
}
