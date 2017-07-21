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
use std::fmt;
use std::str;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use bytes::{Bytes, BytesMut, BufMut};
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use futures::{BoxFuture, Future, Sink, Stream};
use futures::future;
use futures::sync::mpsc;
use futures::sync::oneshot;
use hash_ring::HashRing;

use constants::*;
use codec::{Packet, PacketCodec};

pub const MAX_UNHANDLED_REPLIES: usize = 1024;
/// See: https://docs.rs/hash_ring/0.0.5/hash_ring/struct.HashRing.html
pub const HASH_RING_REPLICAS: isize = 8;

pub struct ClientJob {
    fname: Bytes,
    unique: Bytes,
    data: Bytes,
    handle: Option<Arc<Bytes>>,
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

    pub fn set_handle(&mut self, handle: Arc<Bytes>) {
        self.handle = Some(handle);
    }

    pub fn handle(&self) -> Option<Arc<Bytes>> {
        self.handle.clone()
    }
}

pub struct Client {
    client_id: Option<Bytes>,
    servers: Vec<Bytes>,
    sockets: Arc<Mutex<HashMap<Bytes, TcpStream>>>,
    errors: Vec<(Bytes, io::Error)>,
    hash_ring: HashRing<ServerBytes>,
}

pub trait AddServer<T> {
    fn add_server(&mut self, server: T);
}

struct ServerBytes(pub Bytes);

impl fmt::Display for ServerBytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self.0[..])
    }
}

impl Clone for ServerBytes {
    fn clone(&self) -> ServerBytes {
        ServerBytes(self.0.clone())
    }
}

impl<T> AddServer<T> for Client
where
    T: Into<Bytes>,
{
    fn add_server(&mut self, server: T) {
        self.servers.push(server.into());
        let server = self.servers.last().unwrap();
        let server = ServerBytes(server.clone());
        self.hash_ring.add_node(&server);
    }
}

pub trait GetServer<T> {
    fn get_server(&self, server: &T) -> &Bytes;
}

impl<T> GetServer<T> for Client
where
    T: Into<Bytes> + Clone,
{
    fn get_server(&self, server: &T) -> &Bytes {
        let k: Bytes = server.clone().into();
        let k = ServerBytes(k).to_string();
        let sb = self.hash_ring.get_node(&k).expect("Must add servers first");
        &sb.0
    }
}

impl Client {
    pub fn new(client_id: Option<Bytes>) -> Client {
        Client {
            client_id: client_id,
            servers: Vec::new(),
            sockets: Arc::new(Mutex::new(HashMap::new())),
            errors: Vec::new(),
            hash_ring: HashRing::new(Vec::new(), HASH_RING_REPLICAS),
        }
    }

    pub fn servers(&self) -> &Vec<Bytes> {
        &self.servers
    }

    pub fn sockets(&self) -> Arc<Mutex<HashMap<Bytes, TcpStream>>> {
        self.sockets.clone()
    }

    pub fn errors(&self) -> &Vec<(Bytes, io::Error)> {
        &self.errors
    }

    /// Submits the job to the remote server, returns future that resolves to job
    /// with handle filled in
    pub fn submit_job(&mut self, job: &ClientJob) -> BoxFuture<ClientJob, io::Error> {
        let return_sockets = self.sockets.clone();
        let mut sockets = self.sockets.lock().unwrap();
        if sockets.len() < 1 {
            return future::err(io::Error::new(
                io::ErrorKind::Other,
                "No sockets to send on",
            )).boxed();
        }
        // Header + nulls + fields
        let mut body = BytesMut::with_capacity(
            12 + PTYPES[SUBMIT_JOB as usize].nargs as usize + job.fname.len() +
                job.unique.len() + job.data.len(),
        );
        body.put_slice(&job.fname[..]);
        body.put_u8(b'\0');
        body.put_slice(&job.unique[..]);
        body.put_u8(b'\0');
        body.put_slice(&job.data[..]);
        let body = body.freeze();
        let packet = Packet::new_req(SUBMIT_JOB, body);
        let server: Bytes = self.get_server(&job.unique).clone();
        // Temporarily give it to the future
        let socket = sockets.remove::<Bytes>(&server).expect(
            "Cannot send concurrently to same server",
        );
        let socket = socket.framed(PacketCodec);
        let (tx, rx) = oneshot::channel::<ClientJob>();
        socket
            .send(packet)
            .and_then(move |socket| {
                let socket = socket.into_inner();
                let mut sockets = return_sockets.lock().unwrap();
                sockets.insert(server.clone(), socket);
                rx.map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "Handle receiver cancelled")
                })
            })
            .boxed()
    }

    /// Returns a future which takes ownership of the Client while connecting all servers
    /// After they've all been attempted, the future resolves to the Client object, ready to be
    /// used for interacting with the server.
    pub fn connect(self, handle: Handle) -> BoxFuture<Client, io::Error> {
        if self.servers.len() == 0 {
            panic!("connect called with no servers");
        }
        // Channel for connection results
        let (tx, rx) = mpsc::channel(64);
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
                Ok(socket) => {
                    let mut sockets = client.sockets.lock().unwrap();
                    sockets.insert(server, socket);
                }
            }
            Ok(client)
        }).map_err(|_| io::Error::new(io::ErrorKind::Other, "Receiver Error"))
            .boxed()
    }
}
