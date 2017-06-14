#[macro_use]
extern crate log;
extern crate env_logger;
extern crate mio;
extern crate tokio_proto;

extern crate rustygear;

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
//use mio::tcp::*;
use tokio_proto::TcpServer;

use rustygear::queues::*;
use rustygear::worker::*;
use rustygear::server::*;
use rustygear::proto::GearmanProto;
use rustygear::service::GearmanService;

fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse().unwrap();

    let server = TcpServer::new(GearmanProto, address);
    server.serve(|| Ok(GearmanService))
    /*
    let queues = SharedJobStorage::new_job_storage();
    let workers = SharedWorkers::new_workers();

    let job_count = AtomicUsize::new(0);

    let mut server = GearmanServer::new(server_socket, queues.clone(), workers.clone(), job_count);

    server.poll();
*/
}
