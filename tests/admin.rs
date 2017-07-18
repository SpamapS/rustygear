extern crate bytes;
extern crate futures;
extern crate rustygear;

use std::sync::Arc;

use bytes::Bytes;

use rustygear::admin::admin_command_status;
use rustygear::constants::*;
use rustygear::job::Job;
use rustygear::queues::{HandleJobStorage, SharedJobStorage};
use rustygear::worker::{Worker, SharedWorkers, Wake};

#[test]
fn admin_command_status_1job() {
    let j = Job::new(Bytes::from("f"),
                     Bytes::from("u"),
                     Bytes::new(),
                     Bytes::from("h"));
    let mut w = Worker::new();
    w.can_do(Bytes::from("f"));
    let mut storage = SharedJobStorage::new_job_storage();
    let mut workers = SharedWorkers::new_workers();
    storage.add_job(Arc::new(j), PRIORITY_NORMAL, None);
    workers.sleep(&mut w, 1);
    let packet = admin_command_status(storage, workers);
    assert_eq!(b"f\t1\t0\t1\n.\n", &packet.data[..])
}

#[test]
fn admin_command_status_empty() {
    let storage = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();
    let packet = admin_command_status(storage, workers);
    assert_eq!(b".\n", &packet.data[..]);
}
