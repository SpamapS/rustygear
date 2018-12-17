use std::hash::{Hash, Hasher};
extern crate hash_ring;
use client::hash_ring::HashRing;
use std::fmt;

#[test]
fn test_basic_client() {
    let mut c = Client::new();
    c.add_server("::1", None);
    //let response = c.echo("foo");
    //assert_eq!(response, "foo");
    c.remove_server("::1", None);
}

#[derive(PartialEq, Eq, Clone)]
enum ConnectionState {
    INIT,
    CONNECTING,
    CONNECTED,
}

#[derive(PartialEq, Clone)]
struct Server {
    host: String,
    port: u16,
    state: ConnectionState,
}

impl Server {
    fn new(host: &String, port: Option<u16>) -> Server {
        Server {
            host: host.clone(),
            port: port.unwrap_or(11211),
            state: ConnectionState::INIT,
        }
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", &self.host, self.port)
    }
}

impl Hash for Server {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.port.hash(state);
    }
}

struct Client {
    servers: HashRing<Server>,
}

impl Client {
    fn new() -> Client {
        Client {
            servers: HashRing::new(Vec::new(), 1),
        }
    }

    fn add_server<S: Into<String>>(&mut self, host: S, port: Option<u16>) {
       self.servers.add_node(&Server::new(&host.into(), port));
    }

    fn remove_server<S: Into<String>>(&mut self, host: S, port: Option<u16>) {
       self.servers.remove_node(&Server::new(&host.into(), port));
    }
}
