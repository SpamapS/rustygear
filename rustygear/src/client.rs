use std::io;

use hash_ring::HashRing;
use tokio::net::TcpStream;
use futures::Future;

struct Server {
    host: String,
    port: u16,
}

impl ToString for Server {
    fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub struct Client {
    ring: HashRing<Server>,
    sockets: Vec<TcpStream>,
}

impl Client {
    fn connect(&mut self, server: Server) -> Future<Item=usize, Error=io::Error> {
        TcpStream::connect(&(&server.host, server.port)).and_then(|sock| {
            self.sockets.append(sock);
            self.sockets.length() - 1
        }
    }
}
