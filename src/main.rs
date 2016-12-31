#[macro_use] extern crate log;
extern crate byteorder;
extern crate bytes;
extern crate env_logger;
extern crate mio;
extern crate uuid;

use std::net::SocketAddr;
use mio::*;
use mio::deprecated::EventLoop;
use mio::tcp::*;

pub mod constants;
pub mod job;
pub mod packet;
pub mod queues;
use queues::*;
pub mod worker;
use worker::*;
pub mod server;
use server::*;


fn main() {
    env_logger::init().unwrap();

    info!("Binding to 0.0.0.0:4730");
    let address = "0.0.0.0:4730".parse::<SocketAddr>().unwrap();
    let server_socket = TcpListener::bind(&address).unwrap();
    let mut event_loop = EventLoop::new().unwrap();

    let queues = JobQueues::new_queues();
    let workers = SharedWorkers::new_workers();

    let mut server = GearmanServer::new(server_socket, queues.clone(), workers.clone());


    event_loop.register(&server.socket,
                        Token(0),
                        Ready::readable(),
                        PollOpt::edge()).unwrap();

    event_loop.run(&mut server).unwrap();
}
