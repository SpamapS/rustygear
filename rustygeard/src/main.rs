#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rustygear;
extern crate rustygeard;

use clap::{command, Arg};
use rustygeard::server::GearmanServer;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    let matches = command!()
        .version(VERSION)
        .about("See http://gearman.org/protocol")
        .arg(
            Arg::new("listen")
                .short('L')
                .long("listen")
                .value_name("Address:port")
                .help("Server will listen on this address")
                .default_value("0.0.0.0:4730")
                .num_args(1),
        )
        .get_matches();

    let listen = matches
        .get_one::<String>("listen")
        .expect("default_value should ensure this");
    env_logger::init();

    info!("Binding to {}", listen);
    let address = listen.parse().unwrap();
    if let Err(e) = GearmanServer::run(address) {
        eprintln!("Error: {}", e);
        std::process::exit(-1)
    }
}
