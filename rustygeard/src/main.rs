#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rustygear;
extern crate rustygeard;

use rustygeard::server::GearmanServer;
use clap::{Arg, App};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    let matches = App::new("rustygeard")
        .version(VERSION)
        .about("See http://gearman.org/protocol")
        .arg(Arg::with_name("listen")
            .short("L")
            .long("listen")
            .value_name("Address:port")
            .help("Server will listen on this address")
            .takes_value(true))
        .get_matches();

    let listen = matches.value_of("listen").unwrap_or("0.0.0.0:4730");
    env_logger::init();

    info!("Binding to {}", listen);
    let address = listen.parse().unwrap();
    GearmanServer::run(address);
}
