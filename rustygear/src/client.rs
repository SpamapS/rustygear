extern crate hash_ring;
extern crate tokio;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::{AddrParseError, SocketAddr};
use std::sync::{Arc, Mutex};
use std::io;
use std::fmt;

use futures::sync::mpsc::{Receiver, Sender};
use futures::future;
use futures::Future;

use hash_ring::HashRing;
use tokio::net::TcpStream;

use crate::codec::Packet;

type ResponseFuture<I, E> = Box<Future<Item = I, Error = E>>;

#[test]
fn test_basic_client() {
    let mut c = Client::new();
    c.add_server("::1", None);
    //let response = c.echo("foo");
    //assert_eq!(response, "foo");
    c.remove_server("::1", None);
}


#[derive(Clone, Eq)]
struct ServerNode {
    host: String,
    port: u16,
}

impl fmt::Display for ServerNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl Hash for ServerNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.port.hash(state);
    }
}

impl PartialEq for ServerNode {
    fn eq(&self, other: &ServerNode) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl ServerNode {
    fn new(server: &Server) -> ServerNode {
        ServerNode {
            host: server.host.clone(),
            port: server.port.clone(),
        }
    }
}

struct Server {
    host: String,
    port: u16,
    addr: SocketAddr,
    socket: Arc<Mutex<Option<Arc<TcpStream>>>>,
}

const DEFAULT_PORT: u16 = 4730;

impl PartialEq for Server {
    fn eq(&self, other: &Server) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl Eq for Server {}

impl Server {
    fn new(host: &String, port: u16, addr: SocketAddr) -> Server {
        Server {
            host: host.clone(),
            port: port,
            addr: addr,
            socket: Arc::new(Mutex::new(None)),
        }
    }

    fn connect(&mut self) -> ResponseFuture<Arc<TcpStream>, io::Error> {
        let sock_ptr = self.socket.clone();
        let sock_now = sock_ptr.lock().unwrap();
        let sock_ptr2 = self.socket.clone();
        match *sock_now {
            None => {
                Box::new(TcpStream::connect(&self.addr).and_then(move |sock| {
                    let asock = Arc::new(sock);
                    let mut sock = sock_ptr2.lock().unwrap();
                    *sock = Some(asock.clone());
                    Ok(asock)
                }))
            },
            Some(ref sock) => Box::new(future::ok(sock.clone())),
        }
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", &self.host, self.port)
    }
}

struct Client {
    server_ring: HashRing<ServerNode>,
    servers: HashMap<ServerNode, Server>,
}

impl Client {
    fn new() -> Client {
        Client {
            server_ring: HashRing::new(Vec::new(), 1),
            servers: HashMap::new(),
        }
    }

    fn add_server<S: Into<String>>(&mut self, host: S, port: Option<u16>) -> Result<(), io::Error>
    where S: fmt::Display {
        let port = port.unwrap_or(DEFAULT_PORT);
        let addr = match format!("{}:{}", host, port).parse() {
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!("{}", &e))),
            Ok(addr) => addr,
        };
        let serv = Server::new(&host.into(), port, addr);
        let serv_node = ServerNode::new(&serv);
        self.server_ring.add_node(&serv_node);
        self.servers.insert(serv_node, serv);
        Ok(())
    }

    fn remove_server<S: Into<String>>(&mut self, host: S, port: Option<u16>) -> Result<(), io::Error>
    where S: fmt::Display {
        let port = port.unwrap_or(DEFAULT_PORT);
        let addr = match format!("{}:{}", host, port).parse() {
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!("{}", &e))),
            Ok(addr) => addr,
        };
        let serv = Server::new(&host.into(), port, addr);
        let serv_node = ServerNode::new(&serv);
        self.server_ring.remove_node(&serv_node);
        self.servers.remove(&serv_node);
        Ok(())
    }
}
