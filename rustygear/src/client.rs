extern crate hash_ring;
extern crate tokio;
extern crate tokio_io;
extern crate tokio_core;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::{AddrParseError, SocketAddr};
use std::sync::{Arc, Mutex};
use std::io;
use std::fmt;

use futures::sync::mpsc::{Receiver, Sender};
use futures::sink::Sink;
use futures::stream::Stream;
use futures::stream;
use futures::future;
use futures::Future;

use hash_ring::HashRing;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use tokio_codec::Framed;
use tokio::io::write_all;
use bytes::{Bytes, BytesMut};

use crate::client::tokio_io::AsyncRead;
use crate::codec::{Packet, PacketCodec, PacketMagic};
use crate::constants::*;

type ResponseFuture<I, E> = Box<Future<Item = I, Error = E>>;
type FramedGearman = Framed<TcpStream, PacketCodec>;

#[test]
fn test_basic_client() {
    let mut c = Client::new();
    {
        let result = c.add_server("::1", None);
        assert!(result.is_err(), "Add server failed.");
    }
    //let response = c.echo("foo");
    //assert_eq!(response, "foo");
    assert!(c.remove_server("::1", None).is_err(), "Remove server failed.");
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
    core: Core,
}

const READ_BUF_SIZE: usize = 8192;

impl Client {
    fn new() -> Client {
        Client {
            server_ring: HashRing::new(Vec::new(), 1),
            servers: HashMap::new(),
            readbuf: Arc::new(Mutex::new(BytesMut::with_capacity(READ_BUF_SIZE))),
            core: Core::new().unwrap(),
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

    fn connect(&mut self, server: &Server) -> ResponseFuture<usize, io::Error> {
        Box::new(TcpStream::connect(&server.addr, &self.core.handle()).and_then(move |sock| {
            self.sockets.append(Framed::new(sock, PacketCodec{}));
            self.sockets.length() - 1
        }))
    }

    fn choose_connection<S: Into<String>>(&mut self, unique: S) -> ResponseFuture<usize, io::Error>
    {
        match self.server_ring.get_node(unique.into()) {
            None => Box::new(future::err(io::Error::new(io::ErrorKind::Other, "Server missing from hash ring!"))),
            Some(ref serv_node) => match self.servers.get_mut(&serv_node) {
                None => Box::new(future::err(io::Error::new(io::ErrorKind::Other, "Server missing from hash map!"))),
                Some(server) => server.connect(),
            }
        }
    }

    fn echo<B: Into<Bytes>>(&mut self, data: B) -> impl Future<Item = Bytes, Error = io::Error> 
    where std::string::String: std::convert::From<B> {
        let data: Bytes = data.into();
        let unique: String = String::from_utf8_lossy(&data).to_string();
        self.choose_connection(unique).and_then(|packet_io| {
            let p = Packet {
                magic: PacketMagic::REQ,
                ptype: ECHO_REQ,
                psize: data.len() as u32,
                data: data
            };
            let packet_io = packet_io.clone();
            packet_io.send(p)
        }).and_then(|packet_io| {
            packet_io.into_future().map_err(|(e, _)| e)
        }).and_then(|(p, _)| {
            match p {
                None => Err(io::Error::new(io::ErrorKind::Other, "Server closed connection!")),
                Some(p) => match p.ptype {
                    ECHO_RES => Ok(p.data),
                    _ => Err(io::Error::new(io::ErrorKind::Other, "Got unepxected response to echo!")),
                },
            }
        })
    }
}
