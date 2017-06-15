#[macro_use]
extern crate log;
extern crate env_logger;
extern crate mio;
extern crate tokio_proto;

extern crate rustygear;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
//use mio::tcp::*;
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
    let workers =  SharedWorkers::new_workers();
    let job_count = Arc::new(Mutex::new(AtomicUsize::new(0)));
    let server = TcpServer::new(GearmanProto, address);
    server.serve(move || {
        Ok(GearmanService::new(
                curr_conn_id.fetch_add(1, Ordering::Relaxed),
                queues.clone(),
                workers.clone(),
                job_count.clone(),
                )
            )
    })
}
