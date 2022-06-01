extern crate bytes;
extern crate futures;
extern crate rustygear;
extern crate rustygeard;

use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

use bytes::Bytes;

use rustygear::constants::*;
use rustygear::job::Job;

use rustygeard::admin::{admin_command_status, admin_command_unknown, admin_command_workers};
use rustygeard::queues::{HandleJobStorage, SharedJobStorage};
use rustygeard::worker::{SharedWorkers, Wake, Worker};
use rustygeard::service::WorkersByConnId;

#[test]
fn admin_command_status_1job() {
    let j = Job::new(
        Bytes::from("f"),
        Bytes::from("u"),
        Bytes::new(),
        Bytes::from("h"),
    );
    let mut w = Worker::new("127.0.0.1:37337".parse().unwrap(), Bytes::from("client1"));
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

#[test]
fn admin_command_workers_empty() {
    let workers_by_conn_id = Arc::new(Mutex::new(BTreeMap::new()));
    let packet = admin_command_workers(workers_by_conn_id);
    assert_eq!(b".\n", &packet.data[..]);
}

#[test]
fn admin_command_workers_with2() {
    let workers_by_conn_id: WorkersByConnId = Arc::new(Mutex::new(BTreeMap::new()));
    {
        let mut wbci = workers_by_conn_id.lock().unwrap();
        let worker = Arc::new(Mutex::new(Worker::new("127.0.0.1:37337".parse().unwrap(), Bytes::from("hacker1"))));
        {
            let mut worker = worker.lock().unwrap();
            worker.can_do(Bytes::from("hack"));
        }
        wbci.insert(10, worker);
        let worker = Arc::new(Mutex::new(Worker::new("127.0.0.1:33333".parse().unwrap(), Bytes::from("-"))));
        wbci.insert(11, worker);
    }
    let packet = admin_command_workers(workers_by_conn_id);
    let response = String::from_utf8(packet.data.to_vec()).unwrap();
    let expected = String::from("10 127.0.0.1:37337 hacker1 : hack\n11 127.0.0.1:33333 - :\n.\n");
    assert_eq!(expected, response);
}

#[test]
fn admin_command_unknown_t() {
    let packet = admin_command_unknown();
    let response = String::from_utf8(packet.data.to_vec()).unwrap();
    let expected = String::from("ERR UNKNOWN_COMMAND Unknown+server+command\n");
    assert_eq!(expected, response);
}
