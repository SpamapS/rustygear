#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rustygear;
extern crate tokio_core;


use rustygear::server::GearmanServer;
use rustygear::queues::*;
use rustygear::worker::*;
use rustygear::proto::GearmanProto;
use rustygear::service::GearmanService;

fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse().unwrap();
    GearmanServer::run(address);
}
