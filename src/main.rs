#[macro_use]
extern crate log;
extern crate env_logger;
extern crate mio;
extern crate tokio_proto;

extern crate rustygear;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_proto::TcpServer;

use rustygear::queues::*;
use rustygear::worker::*;
use rustygear::proto::GearmanProto;
use rustygear::service::GearmanService;

fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse().unwrap();

    let curr_conn_id = AtomicUsize::new(0);
    let queues = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();
    let job_count = Arc::new(AtomicUsize::new(0));
    let connections: Arc<Mutex<HashMap<usize, Arc<GearmanService>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let server = TcpServer::new(GearmanProto, address);
    server.serve(move || {
        let conn_id = curr_conn_id.fetch_add(1, Ordering::Relaxed);
        let service = Arc::new(GearmanService::new(curr_conn_id.fetch_add(1, Ordering::Relaxed),
                                                   queues.clone(),
                                                   workers.clone(),
                                                   connections.clone(),
                                                   job_count.clone()));
        let mut connections = connections.lock().unwrap();
        connections.insert(conn_id, service.clone());
        Ok(service)
    })
}
