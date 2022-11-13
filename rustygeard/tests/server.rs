use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use std::panic::catch_unwind;
use std::time::Duration;

use rustygeard::server::GearmanServer;


#[test]
fn test_server_starts() {
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
        match(rx.recv_timeout(Duration::from_millis(50))) {
            Err(e) => {},
            Ok(_failed) => {
                println!("Failed to listen on port {}", port);
                serv.join().expect("Server thread did not panic or exit");
                continue;
            },
        };
        println!("Server started!");
        let mut c = TcpStream::connect(&addr).unwrap();
        println!("we connected");
        break;
    }
}