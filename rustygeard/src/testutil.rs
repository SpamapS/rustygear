use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::time::Duration;

use rustygear::client::Client;

use crate::server::GearmanServer;

pub struct ServerGuard {
    addr: SocketAddr,
    admin_socket: TcpStream,
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
static RUNNING_SERVERS: AtomicUsize = AtomicUsize::new(0);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.admin_socket
            .write_all(SHUTDOWN_COMMAND)
            .expect("could not send shutdown");
    }
}

pub fn start_test_server() -> Option<ServerGuard> {
    let server_offset = RUNNING_SERVERS.fetch_add(1, Ordering::Relaxed) + 30000;
    if server_offset > 40000 {
        panic!("Cannot run more than 10000 test servers");
    }
    let _ = env_logger::builder().is_test(true).try_init();
    for port in server_offset..40000 {
        let addr: SocketAddr = format!("[::1]:{port}").parse().unwrap();
        let (tx, rx): (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        let serv = std::thread::spawn(move || {
            match GearmanServer::run(addr.clone(), None) {
                Ok(_) => info!("Server exited cleanly."),
                Err(e) => {
                    error!("Server failed: {:?}", e);
                    tx.send(true).unwrap();
                }
            };
        });
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(_e) => {}
            Ok(_failed) => {
                warn!("Failed to listen on port {}", port);
                serv.join().expect("Server thread did not panic or exit");
                continue;
            }
        };
        info!("Server started!");
        return Some(ServerGuard::connect(addr));
    }
    return None;
}

pub async fn connect(addr: &SocketAddr) -> Client {
    connect_with_client_id(addr, "tests").await
}

pub async fn connect_with_client_id(addr: &SocketAddr, client_id: &'static str) -> Client {
    let client = Client::new().add_server(&addr.to_string());
    client
        .set_client_id(client_id)
        .connect()
        .await
        .expect("Failed to connect to server")
}

pub async fn worker(addr: &SocketAddr) -> Client {
    connect(addr)
        .await
        .can_do("testfunc", |workerjob| {
            Ok(format!(
                "worker saw {} with unique [{}]",
                String::from_utf8_lossy(workerjob.payload()),
                String::from_utf8_lossy(workerjob.unique())
            )
            .into_bytes())
        })
        .await
        .expect("CAN_DO should work")
}
