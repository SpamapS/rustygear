/// You'll want to use this client as such:
///
/// let mut client = Client::new();
/// client.add_server("203.0.113.5");
/// client.add_server("198.51.100.9");
/// let mut client = client.connect().wait();
/// let job = ClientJob::new(Bytes::from("sort"), Bytes::from("a\nc\nb\n"), None);
/// let answer = client.submit_job(job).wait();
///
///

use std::io;
use std::str;
use std::collections::HashMap;
use std::net::ToSocketAddrs;

use uuid::Uuid;

use bytes::Bytes;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use futures::{BoxFuture, Future, Poll, Async, Sink, Stream};
use futures::sync::mpsc::{channel, Receiver};

pub const MAX_FAIL_RETRIES: u8 = 3;

pub struct ClientJob {
    fname: Bytes,
    unique: Bytes,
    data: Bytes,
    handle: Option<Bytes>,
}


impl ClientJob {
    pub fn new(fname: Bytes, data: Bytes, unique: Option<Bytes>) -> ClientJob {
        let unique = match unique {
            None => Bytes::from(Uuid::new_v4().hyphenated().to_string()),
            Some(unique) => unique,
        };
        ClientJob {
            fname: fname,
            unique: unique,
            data: data,
            handle: None,
        }
    }
}

pub struct Client {
    client_id: Option<Bytes>,
    servers: Vec<Bytes>,
    sockets: HashMap<Bytes, TcpStream>,
    errors: Vec<(Bytes, io::Error)>,
}

pub trait AddServer<T> {
    fn add_server(&mut self, server: T);
}

impl<T> AddServer<T> for Client
where
    T: Into<Bytes>,
{
    fn add_server(&mut self, server: T) {
        self.servers.push(server.into())
    }
}

impl Client {
    pub fn new(client_id: Option<Bytes>) -> Client {
        Client {
            client_id: client_id,
            servers: Vec::new(),
            sockets: HashMap::new(),
            errors: Vec::new(),
        }
    }

    pub fn servers(&self) -> &Vec<Bytes> {
        &self.servers
    }

    pub fn sockets(&self) -> &HashMap<Bytes, TcpStream> {
        &self.sockets
    }

    pub fn errors(&self) -> &Vec<(Bytes, io::Error)> {
        &self.errors
    }

    /// Returns a future which takes ownership of the Client while connecting all servers
    /// After they've all been attempted, the future resolves to the Client object, ready to be
    /// used for interacting with the server.
    pub fn connect(self, handle: Handle) -> BoxFuture<Client, io::Error> {
        if self.servers.len() == 0 {
            panic!("connect called with no servers");
        }
        // Channel for connection results
        let (tx, rx) = channel(64);
        // connect any inactive servers
        for server in self.servers.iter() {
            // XXX loop through these, don't just take first one every time
            let server_addr = str::from_utf8(server)
                .unwrap()
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap();
            let server = server.clone(); // so future can be 'static
            let tx = tx.clone();
            handle.spawn(TcpStream::connect(&server_addr, &handle).then(
                move |result| {
                    tx.send((server, result)).map(|_| ()).map_err(|_| ())
                },
            ));
        }
        rx.fold(self, |mut client, (server, result)| {
            match result {
                Err(e) => client.errors.push((server, e)),
                Ok(socket) => { client.sockets.insert(server, socket); },
            }
            Ok(client)
        })
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Receiver Error"))
        .boxed()
    }
}
