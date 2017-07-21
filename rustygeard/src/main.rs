#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rustygear;

use rustygear::server::GearmanServer;

fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse().unwrap();
    GearmanServer::run(address);
}
