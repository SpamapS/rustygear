use std::net::{SocketAddr, TcpStream};
use std::io::Write;
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use std::panic::catch_unwind;
use std::time::Duration;

use crate::server::GearmanServer;

pub struct ServerGuard {
    addr: SocketAddr,
    admin_socket: TcpStream
}

impl ServerGuard {
    pub fn connect(addr: SocketAddr) -> ServerGuard {
        ServerGuard {
            addr: addr,
            admin_socket: TcpStream::connect(addr).expect("Could not connect"),
        }
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

static SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.admin_socket.write_all(SHUTDOWN_COMMAND).expect("could not send shutdown");
    }
}

pub fn start_test_server() -> Option<ServerGuard> {
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
        return Some(ServerGuard::connect(addr));
    }
    return None;
}