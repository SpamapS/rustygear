extern crate bytes;
extern crate futures;
extern crate tokio_proto;
extern crate rustygear;

use std::sync::Arc;

use bytes::Bytes;
use futures::{Async, Stream};
use tokio_proto::streaming::Message;

use rustygear::admin::admin_command_status;
use rustygear::constants::*;
use rustygear::job::Job;
use rustygear::queues::{HandleJobStorage, SharedJobStorage};
use rustygear::worker::{Worker, SharedWorkers, Wake};

#[test]
fn admin_command_status_1job() {
    let j = Job::new(Bytes::from("f"), vec![b'u'], Vec::new(), vec![b'h']);
    let mut w = Worker::new();
    w.can_do(Bytes::from("f"));
    let mut storage = SharedJobStorage::new_job_storage();
    let mut workers = SharedWorkers::new_workers();
    storage.add_job(Arc::new(j), PRIORITY_NORMAL, None);
    workers.sleep(&mut w, 1);
    match admin_command_status(storage, workers) {
        Message::WithoutBody(_) => panic!("Wrong type, expected WithoutBody"),
        Message::WithBody(_, mut body) => {
            match body.poll() {
                Ok(Async::Ready(Some(chunk))) => assert_eq!(b"f\t1\t0\t1\n.\n", &chunk[..]),
                _ => panic!("Unexpected response to poll!"),
            }
        }
    }
}

#[test]
fn admin_command_status_empty() {
    let storage = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();
    match admin_command_status(storage, workers) {
        Message::WithoutBody(_) => panic!("Wrong type, expected WithoutBody"),
        Message::WithBody(_, mut body) => {
            match body.poll() {
                Ok(Async::Ready(Some(chunk))) => assert_eq!(b".\n", &chunk[..]),
                _ => panic!("Unexpected result when calling poll"),
            }
        }
    }
}
