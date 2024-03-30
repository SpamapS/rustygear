use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use std::panic::catch_unwind;
use std::time::Duration;

use crate::server::GearmanServer;

pub fn start_test_server() -> Option<SocketAddr> {
    let mut server_addr: Option<SocketAddr> = None;
    for port in 30000..40000 {
        let addr: SocketAddr = format!("[::1]:{port}").parse().unwrap();
        let (tx, rx): (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        let serv = std::thread::spawn(move || {
            match catch_unwind(move || GearmanServer::run(addr.clone())) {
                Ok(_) => unreachable!(),
                Err(e) => {
                    println!("Server paniced: {:?}", e);
                    tx.send(true).unwrap();
                },
            };
        });
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(_e) => {},
            Ok(_failed) => {
                println!("Failed to listen on port {}", port);
                serv.join().expect("Server thread did not panic or exit");
                continue;
            },
        };
        println!("Server started!");
        TcpStream::connect(&addr).unwrap();
        println!("we connected");
        server_addr = Some(addr);
        break;
    }
    return server_addr;
}
