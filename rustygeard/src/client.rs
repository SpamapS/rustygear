use core::task::{Context, Poll};

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::Future;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

use tower_service::Service;

use crate::service::{new_req, next_field, no_response};
use rustygear::codec::{Packet, PacketCodec};
use rustygear::constants::*;

type Hostname = String;

pub struct Client {
    servers: Vec<Hostname>,
    conns: Arc<Mutex<Vec<mpsc::Sender<Result<Packet, io::Error>>>>>,
    connected: Vec<bool>,
    client_id: Option<Bytes>,
    echo_tx: Option<mpsc::Sender<Bytes>>,
}

impl Service<Packet> for Client {
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
        Client {
            servers: Vec::new(),
            conns: Arc::new(Mutex::new(Vec::new())),
            connected: Vec::new(),
            client_id: None,
            echo_tx: None,
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

    pub async fn connect(self) -> Result<Arc<Mutex<Self>>, Box<dyn std::error::Error>> {
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
        let aclient = Arc::new(Mutex::new(self));
        for connect in connects.iter_mut() {
            let aclient = aclient.clone();
            let connect = connect.await?;
            let offset = connect.0;
            let conn = connect.1?;
            trace!("connected: offset={}", offset);
            let pc = PacketCodec {};
            let (mut sink, mut stream) = pc.framed(conn).split();
            let (tx, mut rx) = mpsc::channel(100); // XXX pick a good value or const
            let tx = tx.clone();
            let tx2 = tx.clone();
            {
                let mut client = aclient.lock().unwrap();
                client.connected[offset] = true;
                client.conns.lock().unwrap().insert(offset, tx2);
            }
            let reader = async move {
                let mut tx = tx.clone();
                while let Some(frame) = stream.next().await {
                    let response = {
                        let aclient3 = aclient.clone();
                        let mut aclient = aclient3.lock().unwrap();
                        aclient.call(frame.unwrap())
                    };
                    let response = response.await;
                    if let Err(_) = tx.send(response).await {
                        error!("receiver dropped")
                    }
                }
            };
            let writer = async move {
                while let Some(packet) = rx.next().await {
                    trace!("Sending {:?}", &packet);
                    if let Err(_) = packet {
                        error!("Receiver failed");
                    }
                    if let Err(_) = sink.send(packet.unwrap()).await {
                        error!("Connection ({}) dropped", offset);
                    }
                }
            };
            runtime::Handle::current().spawn(reader);
            runtime::Handle::current().spawn(writer);
        }
        trace!("connected all");
        Ok(aclient)
    }

    pub async fn echo(&mut self, payload: &[u8]) -> Result<(), io::Error> {
        let payload = payload.clone();
        let (tx, mut rx) = mpsc::channel(1);
        self.echo_tx = Some(tx);
        let packet = new_req(ECHO_REQ, Bytes::copy_from_slice(payload));
        if let Some(conn) = self.conns.lock().unwrap().get_mut(0) {
            if let Err(_) = conn.send(Ok(packet)).await {
                error!("Receiver dropped")
            }
        }
        debug!("Waiting for echo response");
        match rx.recv().await {
            Some(res) => info!("echo received: {:?}", res),
            None => info!("echo channel closed"),
        };
        Ok(())
    }

    /* TODO
    pub async fn submit(function: &str, payload: Vec<u8>) {
        let conns = self.conns.lock().unwrap();
        /* Pick the conn later */
        conn = conns[0];

    }
    */

    fn handle_echo_res(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        info!("Echo response received: {:?}", req.data);
        if let Some(echo_tx) = &self.echo_tx {
            let mut tx = echo_tx.clone();
            let data = req.data.clone();
            runtime::Handle::current().spawn(async move { tx.send(data).await });
            Ok(no_response())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Rogue echo response received {:?}", req.data),
            ))
        }
    }

    fn handle_job_created(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        info!("Job Created: {:?}", req);
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
