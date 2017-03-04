#[macro_use]
extern crate log;
extern crate env_logger;
extern crate mio;

extern crate rustygear;

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use mio::tcp::*;

use rustygear::queues::*;
use rustygear::worker::*;
use rustygear::server::*;

fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse::<SocketAddr>().unwrap();
    let server_socket = TcpListener::bind(&address).unwrap();

    let queues = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();

    let job_count = AtomicUsize::new(0);

    let mut server = GearmanServer::new(server_socket, queues.clone(), workers.clone(), job_count);

    server.poll();
}
