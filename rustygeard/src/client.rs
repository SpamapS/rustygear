use core::task::{Context, Poll};

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut, BufMut};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::Future;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

use tower_service::Service;
use uuid::Uuid;

use crate::service::{new_req, next_field, no_response};
use rustygear::codec::{Packet, PacketCodec};
use rustygear::constants::*;

type Hostname = String;

pub struct Client {
    servers: Vec<Hostname>,
    conns: Arc<Mutex<Vec<Arc<Mutex<ClientHandler>>>>>,
    connected: Vec<bool>,
    client_id: Option<Bytes>,
    echo_tx: mpsc::Sender<Bytes>,
    echo_rx: mpsc::Receiver<Bytes>,
    job_created_tx: mpsc::Sender<Bytes>,
    job_created_rx: mpsc::Receiver<Bytes>,
}

struct ClientHandler {
    client_id: Option<Bytes>,
    sink_tx: mpsc::Sender<Packet>,
    echo_tx: mpsc::Sender<Bytes>,
    job_created_tx: mpsc::Sender<Bytes>,
}

pub struct ClientJob {
    handle: Bytes,
    response_tx: mpsc::Sender<Bytes>,
    response_rx: mpsc::Receiver<Bytes>,
}

impl ClientJob {
    pub fn handle(&self) -> &Bytes {
        &self.handle
    }
    pub async fn response(&mut self) -> Result<Bytes, io::Error> {
        Ok(self.response_rx.recv().await.unwrap())
    }
}

impl Service<Packet> for ClientHandler {
    type Response = Packet;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        /* XXX: This should implement backpressure */
        trace!("cx = {:?}", cx);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Packet) -> Self::Future {
        debug!("[{:?}] Got a req {:?}", self.client_id, req);
        let res = match req.ptype {
            //NOOP => self.handle_noop(),
            JOB_CREATED => self.handle_job_created(&req),
            //NO_JOB => self.handle_no_job(&req),
            //JOB_ASSIGN => self.handle_job_assign(&req),
            ECHO_RES => self.handle_echo_res(&req),
            ERROR => self.handle_error(&req),
            STATUS_RES | STATUS_RES_UNIQUE => self.handle_status_res(&req),
            OPTION_RES => self.handle_option_res(&req),
            WORK_DATA | WORK_STATUS | WORK_WARNING | WORK_FAIL | WORK_EXCEPTION => {
                self.handle_work_update(&req)
            }
            //JOB_ASSIGN_UNIQ => self.handle_job_assign_uniq(&req),
            //JOB_ASSIGN_ALL => self.handle_job_assign_all(&req),
            _ => {
                error!("Unimplemented: {:?} processing packet", req);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Invalid packet type {}", req.ptype),
                ))
            }
        };
        let fut = async { res };
        Box::pin(fut)
    }
}

impl Client {
    pub fn new() -> Client {
        let (tx, rx) = mpsc::channel(100); // XXX this is lame
        let (txj, rxj) = mpsc::channel(100); // XXX this is lame
        Client {
            servers: Vec::new(),
            conns: Arc::new(Mutex::new(Vec::new())),
            connected: Vec::new(),
            client_id: None,
            echo_tx: tx,
            echo_rx: rx,
            job_created_tx: txj,
            job_created_rx: rxj,
        }
    }

    pub fn add_server(mut self, server: &str) -> Self {
        self.servers.push(Hostname::from(server));
        self.connected.push(false);
        self
    }

    pub fn set_client_id(mut self, client_id: &'static str) -> Self {
        self.client_id = Some(Bytes::from(client_id));
        self
    }

    pub async fn connect(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        /* Returns the client after having attempted to connect to all servers. */
        trace!("connecting");
        let mut i = 0;
        let mut connects = Vec::new();
        for is_conn in self.connected.iter() {
            if !is_conn {
                let server: &str = self.servers.get(i).unwrap();
                let addr = server.parse::<SocketAddr>()?;
                trace!("really connecting: i={} addr={:?}", i, addr);
                connects.push(runtime::Handle::current().spawn(async move { (i, TcpStream::connect(addr).await) }));
            }
            i = i + 1;
        }
        for connect in connects.iter_mut() {
            let connect = connect.await?;
            let offset = connect.0;
            let conn = connect.1?;
            trace!("connected: offset={} server={}", offset, self.servers[offset]);
            let pc = PacketCodec {};
            let (mut sink, mut stream) = pc.framed(conn).split();
            let (tx, mut rx) = mpsc::channel(100); // XXX pick a good value or const
            let tx = tx.clone();
            let tx2 = tx.clone();
            let handler = Arc::new(Mutex::new(ClientHandler::new(&self.client_id, self.echo_tx.clone(), tx2, self.job_created_tx.clone())));
            self.connected[offset] = true;
            self.conns.lock().unwrap().insert(offset, handler.clone());
            let reader = async move {
                let mut tx = tx.clone();
                while let Some(frame) = stream.next().await {
                    let response = {
                        let handler = handler.clone();
                        let mut handler = handler.lock().unwrap();
                        handler.call(frame.unwrap())
                    };
                    let response = response.await;
                    if let Err(e) = response {
                        error!("conn dropped?: {}", e);
                        return
                    }
                    if let Err(_) = tx.send(response.unwrap()).await {
                        error!("receiver dropped")
                    }
                }
            };
            let writer = async move {
                while let Some(packet) = rx.next().await {
                    trace!("Sending {:?}", &packet);
                    if let Err(_) = sink.send(packet).await {
                        error!("Connection ({}) dropped", offset);
                    }
                }
            };
            runtime::Handle::current().spawn(reader);
            runtime::Handle::current().spawn(writer);
        }
        trace!("connected all");
        Ok(self)
    }

    pub async fn echo(&mut self, payload: &[u8]) -> Result<(), io::Error> {
        let payload = payload.clone();
        let packet = new_req(ECHO_REQ, Bytes::copy_from_slice(payload));
        if let Some(conn) = self.conns.lock().unwrap().get_mut(0) {
            let mut conn = conn.lock().unwrap();
            if let Err(_) = conn.sink_tx.send(packet).await {
                error!("Receiver dropped")
            }
        }
        debug!("Waiting for echo response");
        match self.echo_rx.recv().await {
            Some(res) => info!("echo received: {:?}", res),
            None => info!("echo channel closed"),
        };
        Ok(())
    }

    pub async fn submit(&mut self, function: &str, payload: &[u8]) -> Result<ClientJob, io::Error> {
        let mut conns = self.conns.lock().unwrap();
        /* Pick the conn later */
        match conns.get_mut(0) {
            Some(conn) => {
                let unique = format!("{}", Uuid::new_v4());
                let mut data = BytesMut::with_capacity(
                    2 + function.len() + unique.len() + payload.len()); // 2 for nulls
                data.extend(function.bytes());
                data.put_u8(b'\0');
                data.extend(unique.bytes());
                data.put_u8(b'\0');
                data.extend(payload);
                let packet = new_req(SUBMIT_JOB, data.freeze());
                let mut conn = conn.lock().unwrap();
                if let Err(e) = conn.sink_tx.send(packet).await {
                    error!("Receiver dropped");
                    Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)))
                } else {
                    if let Some(handle) = self.job_created_rx.recv().await {
                        let (tx, rx) = mpsc::channel(100); // XXX lamer
                        Ok(ClientJob { handle: handle, response_tx: tx, response_rx: rx })
                    } else {
                        Err(io::Error::new(io::ErrorKind::Other, "No job created!"))
                    }
                }
            }
            None => {
                error!("No connections on which to submit.");
                Err(io::Error::new(io::ErrorKind::Other, "No Connections on which to submit"))
            }
        }
    }
}

impl ClientHandler {

    fn new(client_id: &Option<Bytes>, echo_tx: mpsc::Sender<Bytes>, sink_tx: mpsc::Sender<Packet>, job_created_tx: mpsc::Sender<Bytes>) -> ClientHandler {
        ClientHandler {
            client_id: client_id.clone(),
            echo_tx: echo_tx,
            sink_tx: sink_tx,
            job_created_tx: job_created_tx,
        }
    }

    fn handle_echo_res(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        info!("Echo response received: {:?}", req.data);
        let mut tx = self.echo_tx.clone();
        let data = req.data.clone();
        runtime::Handle::current().spawn(async move { tx.send(data).await });
        Ok(no_response())
    }

    fn handle_job_created(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        info!("Job Created: {:?}", req);
        let mut tx = self.job_created_tx.clone();
        let handle = req.data.clone();
        runtime::Handle::current().spawn(async move { tx.send(handle).await });
        Ok(no_response())
    }

    fn handle_error(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        /*
        if let Some(error_call) = self.error_call {
            error_call(req.data)
        }
        */
        Ok(no_response())
    }

    fn handle_status_res(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut req = req.clone();
        let handle = next_field(&mut req.data).unwrap();
        let known = next_field(&mut req.data).unwrap();
        let running = next_field(&mut req.data).unwrap();
        let numerator = next_field(&mut req.data).unwrap();
        let denominator = next_field(&mut req.data).unwrap();
        /*
        if req.ptype == STATUS_RES_UNIQUE {
            let waiting = next_field(req.data).unwrap();
            if let Some(status_unique_call) = self.status_unique_call {
                status_unique_call(handle, known, running, numerator, denominator, waiting)
            }
        } else if let Some(status_call) = self.status_call {
            status_call(handle, known, running, numerator, denominator)
        }
        */
        Ok(no_response())
    }

    fn handle_option_res(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        /*
        if let Some(option_call) = self.option_call {
            option_call(req.data)
        }
        */
        Ok(no_response())
    }

    fn handle_work_update(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        /*
        if let Some(work_update_call) = self.work_update_call {
            work_update_call(req)
        }
        */
        Ok(no_response())
    }
}
