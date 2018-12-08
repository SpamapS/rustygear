use std::hash::{Hash, Hasher};
use std::collections::HashSet;

#[test]
fn test_basic_client() {
    let mut c = Client::new();
    c.add_server("::1");
    //let response = c.echo("foo");
    //assert_eq!(response, "foo");
}

#[derive(PartialEq, Eq)]
enum ConnectionState {
    INIT,
    CONNECTING,
    CONNECTED,
}

#[derive(PartialEq, Eq)]
struct Server {
    hostname: String,
    state: ConnectionState,
}

impl Server {
    fn new(hostname: &String) -> Server {
        Server {
            hostname: hostname.clone(),
            state: ConnectionState::INIT,
        }
    }
}

impl Hash for Server {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hostname.hash(state);
    }
}

struct Client {
    servers: HashSet<Server>,
}

impl Client {
    fn new() -> Client {
        Client {
            servers: HashSet::new(),
        }
    }

    fn add_server<S: Into<String>>(&mut self, hostname: S) {
       self.servers.insert(Server::new(&hostname.into()));
    }
}
