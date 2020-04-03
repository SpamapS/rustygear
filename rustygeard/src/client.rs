use std::sync::{Arc, Mutex};
use std::net::SocketAddr;

use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;

type Hostname = String;

pub struct Client {
    servers: Vec<Hostname>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
    connected: Vec<bool>,
}

impl Client {
    pub fn new() -> Client {
        Client {
            servers: Vec::new(),
            conns: Arc::new(Mutex::new(Vec::new())),
            connected: Vec::new(),
        }
    }

    pub fn add_server(mut self, server: &str) -> Self {
        self.servers.push(Hostname::from(server));
        self.connected.push(false);
        self
    }

    pub async fn connect(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        /* Returns the client after having attempted to connect to all servers. */
        let mut i = 0;
        let (mut tx, mut rx) = channel(self.servers.len());
        for is_conn in self.connected.iter() {
            if !is_conn {
                let server: &str = self.servers.get(i).unwrap();
                let addr = server.parse::<SocketAddr>()?;
                tx.send((i, TcpStream::connect(addr).await?)).await?
            }
            i = i + 1;
        }
        while let Some(result) = rx.recv().await {
            let offset = result.0;
            let conn = result.1;
            self.conns.lock().unwrap().insert(offset, conn);
            self.connected[offset] = true;
        }
        Ok(self)
    }

    /* TODO
    pub async fn submit(function: &str, payload: Vec<u8>) {
        let conns = self.conns.lock().unwrap();
        /* Pick the conn later */
        conn = conns[0];
        
    }
    */
}
