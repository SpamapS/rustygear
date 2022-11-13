extern crate bytes;
extern crate futures;
extern crate rustygear;
extern crate rustygeard;

use std::collections::{BTreeMap, HashMap};
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc::channel;

use rustygear::constants::*;
use rustygear::job::Job;

use rustygeard::admin::*;
use rustygeard::queues::{HandleJobStorage, SharedJobStorage};
use rustygeard::service::{GearmanService, WorkersByConnId};
use rustygeard::worker::{SharedWorkers, Wake, Worker};

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
        let worker = Arc::new(Mutex::new(Worker::new(
            "127.0.0.1:37337".parse().unwrap(),
            Bytes::from("hacker1"),
        )));
        {
            let mut worker = worker.lock().unwrap();
            worker.can_do(Bytes::from("hack"));
        }
        wbci.insert(10, worker);
        let worker = Arc::new(Mutex::new(Worker::new(
            "127.0.0.1:33333".parse().unwrap(),
            Bytes::from("-"),
        )));
        wbci.insert(11, worker);
    }
    let packet = admin_command_workers(workers_by_conn_id);
    let response = String::from_utf8(packet.data.to_vec()).unwrap();
    let expected = String::from("10 127.0.0.1:37337 hacker1 : hack\n11 127.0.0.1:33333 - :\n.\n");
    assert_eq!(expected, response);
}

#[test]
fn admin_command_priority_status_empty() {
    let storage = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();
    let packet = admin_command_priority_status(storage, workers);
    assert_eq!(b".\n", &packet.data[..]);
}

#[test]
fn admin_command_priority_status_priority_jobs() {
    let mut storage = SharedJobStorage::new_job_storage();
    for priority in vec![PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH] {
        for i in 0..2 {
            let job = Job::new(
                Bytes::from("func"),
                Bytes::from(format!("unique{}", i)),
                Bytes::new(),
                Bytes::from(format!("H:localhost:{}", i)),
            );
            storage.add_job(Arc::new(job), priority, None);
        }
    }
    let mut w = Worker::new("127.0.0.1:37337".parse().unwrap(), Bytes::from("client1"));
    w.can_do(Bytes::from("func"));
    let mut workers = SharedWorkers::new_workers();
    workers.sleep(&mut w, 1);
    let packet = admin_command_priority_status(storage, workers);
    assert_eq!(
        "func\t2\t2\t2\t1\n.\n",
        String::from_utf8(packet.data[..].to_vec()).unwrap()
    )
}

#[test]
fn admin_command_unknown_t() {
    let packet = admin_command_unknown();
    let response = String::from_utf8(packet.data.to_vec()).unwrap();
    let expected = String::from("ERR UNKNOWN_COMMAND Unknown+server+command\n");
    assert_eq!(expected, response);
}

#[tokio::test]
async fn admin_command_shutdown_t() {
    let queues = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();
    let job_count = Arc::new(AtomicUsize::new(0));
    let senders_by_conn_id = Arc::new(Mutex::new(HashMap::new()));
    let workers_by_conn_id = Arc::new(Mutex::new(BTreeMap::new()));
    let job_waiters = Arc::new(Mutex::new(HashMap::new()));
    let service_addr: SocketAddrV6 = "[::1]:10000".parse().unwrap();
    let peer_addr: SocketAddrV6 = "[::1]:11000".parse().unwrap();
    let (shut_tx, mut shut_rx) = channel(4);
    let service = GearmanService::new(
        1,
        queues,
        workers,
        job_count,
        senders_by_conn_id,
        workers_by_conn_id,
        job_waiters,
        service_addr.into(),
        shut_tx,
    );
    let packet = admin_command_shutdown(&service, &peer_addr.into());
    let response = String::from_utf8(packet.data.to_vec()).unwrap();
    let expected = String::from("BYE\n");
    assert!(shut_rx.recv().await.is_some());
    assert_eq!(expected, response);
}
