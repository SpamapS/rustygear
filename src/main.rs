extern crate byteorder;
extern crate bytes;
extern crate mio;
extern crate uuid;

use std::collections::HashMap;
use std::net::SocketAddr;
use mio::*;
use mio::deprecated::EventLoop;
use mio::tcp::*;

pub mod constants;
use constants::*;
pub mod job;
use job::*;
pub mod packet;
use packet::*;
pub mod queues;
use queues::*;
pub mod worker;
use worker::*;
pub mod server;
use server::*;


fn main() {
    let address = "0.0.0.0:4730".parse::<SocketAddr>().unwrap();
    let server_socket = TcpListener::bind(&address).unwrap();
    let mut event_loop = EventLoop::new().unwrap();

    let mut queues = QueueHolder::new();

    let mut server = GearmanServer::new(server_socket, queues.clone());


    event_loop.register(&server.socket,
                        Token(0),
                        Ready::readable(),
                        PollOpt::edge()).unwrap();

    event_loop.run(&mut server).unwrap();
}
