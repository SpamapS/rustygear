/*
 * Copyright 2020 Clint Byrum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::cmp::min;

use bytes::{BufMut, Bytes, BytesMut};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{delay_for, Duration};
use tokio_util::codec::Decoder;

use hashring::HashRing;
use uuid::Uuid;

use crate::codec::{Packet, PacketCodec, PacketMagic};
use crate::constants::*;
use crate::util::{bytes2bool, new_req, new_res, next_field, no_response};

type Hostname = String;

const MAX_BACKOFF_SECONDS: u64 = 15;

#[derive(Debug)]
/// Used for passing job completion stats to clients
pub struct JobStatus {
    handle: Bytes,
    known: bool,
    running: bool,
    numerator: u32,
    denominator: u32,
    waiting: u32,
}

/// Client for interacting with Gearman service
///
/// Both workers and clients will use this as the top-level object to communicate
/// with a gearman server. [Client.new] should produce a functioning structure which
/// should then be configured as needed. It will not do anything useful until after
/// [Client.connect] has been called.
///
/// See examples/client.rs and examples/worker.rs for information on how to use it.
pub struct Client {
    servers: Vec<Hostname>,
    hash_ring: HashRing<Hostname>,
    conns: Arc<Mutex<HashMap<Hostname, Arc<Mutex<ClientHandler>>>>>,
    connected: Vec<bool>,
    client_id: Option<Bytes>,
    senders_by_handle: Arc<Mutex<HashMap<Bytes, Sender<WorkUpdate>>>>,
    jobs_tx_by_func: Arc<Mutex<HashMap<Vec<u8>, Sender<WorkerJob>>>>,
    echo_tx: Sender<Bytes>,
    echo_rx: Receiver<Bytes>,
    job_created_tx: Sender<Bytes>,
    job_created_rx: Receiver<Bytes>,
    status_res_tx: Sender<JobStatus>,
    status_res_rx: Receiver<JobStatus>,
    error_tx: Sender<(Bytes, Bytes)>,
    error_rx: Receiver<(Bytes, Bytes)>,
    worker_job_tx: Sender<WorkerJob>,
    worker_job_rx: Receiver<WorkerJob>,
}

/// Each individual connection has one of these for handling packets
struct ClientHandler {
    client_id: Option<Bytes>,
    senders_by_handle: Arc<Mutex<HashMap<Bytes, Sender<WorkUpdate>>>>,
    jobs_tx_by_func: Arc<Mutex<HashMap<Vec<u8>, Sender<WorkerJob>>>>,
    sink_tx: Sender<Packet>,
    echo_tx: Sender<Bytes>,
    job_created_tx: Sender<Bytes>,
    status_res_tx: Sender<JobStatus>,
    error_tx: Sender<(Bytes, Bytes)>,
    worker_job_tx: Sender<WorkerJob>,
}

/// Return object for submit_ functions.
pub struct ClientJob {
    handle: Bytes,
    response_rx: Receiver<WorkUpdate>,
}

/// Passed to workers
///
/// The sink_tx property of this structure can be used to send raw packets
/// to the gearman server from workers, although this is not known to work
/// generically as of this writing.
pub struct WorkerJob {
    handle: Bytes,
    function: Bytes,
    payload: Bytes,
    sink_tx: Sender<Packet>,
}

#[derive(Debug)]
/// Logical representation of the data workers may send back to clients
pub enum WorkUpdate {
    Complete {
        handle: Bytes,
        payload: Bytes,
    },
    Data {
        handle: Bytes,
        payload: Bytes,
    },
    Warning {
        handle: Bytes,
        payload: Bytes,
    },
    Exception {
        handle: Bytes,
        payload: Bytes,
    },
    Status {
        handle: Bytes,
        numerator: usize,
        denominator: usize,
    },
    Fail(Bytes),
}

async fn send_packet(conn: Arc<Mutex<ClientHandler>>, packet: Packet) -> Result<(), io::Error> {
    let mut sink_tx = conn.lock().unwrap().sink_tx.clone();
    if let Err(e) = sink_tx.send(packet).await {
        error!("Receiver dropped");
        return Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)));
    }
    Ok(())
}

impl ClientJob {
    fn new(handle: Bytes, response_rx: Receiver<WorkUpdate>) -> ClientJob {
        ClientJob {
            handle: handle,
            response_rx: response_rx,
        }
    }

    /// returns the job handle
    pub fn handle(&self) -> &Bytes {
        &self.handle
    }

    /// Should only return when the worker has sent data or completed the job. Errors if used on background jobs.
    ///
    /// Use this in clients to wait for a response on a job that was submitted. This will block
    /// forever or error if used on a background job.
    pub async fn response(&mut self) -> Result<WorkUpdate, io::Error> {
        Ok(self.response_rx.recv().await.unwrap())
    }
}

impl WorkerJob {
    pub fn handle(&self) -> &[u8] {
        self.handle.as_ref()
    }
    pub fn function(&self) -> &[u8] {
        self.function.as_ref()
    }
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    /// Sends a WORK_STATUS
    ///
    /// This will send a WORK_STATUS packet to the server, and can be called from a worker,
    /// although that worker may need to manage its own runtime, and as of this writing, this
    /// method may not be functional.
    ///
    /// See examples/worker.rs for an idea of how it may work.
    pub async fn work_status(&mut self, numerator: u32, denominator: u32) -> Result<(), io::Error> {
        let numerator = format!("{}", numerator);
        let denominator = format!("{}", denominator);
        let mut payload = BytesMut::with_capacity(
            2 + self.handle.len() + numerator.as_bytes().len() + denominator.as_bytes().len(),
        );
        payload.extend(self.handle.clone());
        payload.put_u8(b'\0');
        payload.extend(numerator.as_bytes());
        payload.put_u8(b'\0');
        payload.extend(denominator.as_bytes());
        let packet = new_res(WORK_STATUS, payload.freeze());
        self.send_packet(packet).await
    }

    async fn send_packet(&mut self, packet: Packet) -> Result<(), io::Error> {
        match self.sink_tx.send(packet).await {
            Err(_) => Err(io::Error::new(io::ErrorKind::Other, "Connection closed")),
            Ok(_) => Ok(()),
        }
    }

    /// Sends a WORK_FAIL
    ///
    /// This method is typically called by the [Client.work] method upon return
    /// of an error from the assigned closure.
    pub async fn work_fail(&mut self) -> Result<(), io::Error> {
        let packet = new_res(WORK_FAIL, self.handle.clone());
        self.send_packet(packet).await
    }

    /// Sends a WORK_COMPLETE
    ///
    /// This method is typically called by the [Client.work] method upon return of
    /// the assigned closure.
    pub async fn work_complete(&mut self, response: Vec<u8>) -> Result<(), io::Error> {
        let mut payload = BytesMut::with_capacity(self.handle.len() + 1 + self.payload.len());
        payload.extend(self.handle.clone());
        payload.put_u8(b'\0');
        payload.extend(response);
        let packet = new_res(WORK_COMPLETE, payload.freeze());
        self.send_packet(packet).await
    }
}

impl Client {
    pub fn new() -> Client {
        let (tx, rx) = channel(100); // XXX this is lame
        let (txj, rxj) = channel(100); // XXX this is lame
        let (txs, rxs) = channel(100); // XXX this is lame
        let (txe, rxe) = channel(100); // XXX this is lame
        let (txw, rxw) = channel(100);
        Client {
            servers: Vec::new(),
            hash_ring: HashRing::new(),
            conns: Arc::new(Mutex::new(HashMap::new())),
            connected: Vec::new(),
            client_id: None,
            senders_by_handle: Arc::new(Mutex::new(HashMap::new())),
            jobs_tx_by_func: Arc::new(Mutex::new(HashMap::new())),
            echo_tx: tx,
            echo_rx: rx,
            job_created_tx: txj,
            job_created_rx: rxj,
            status_res_tx: txs,
            status_res_rx: rxs,
            error_tx: txe,
            error_rx: rxe,
            worker_job_tx: txw,
            worker_job_rx: rxw,
        }
    }

    /// Add a server to the client. This does not initiate anything, it just configures the client.
    ///
    /// As of this writing, only one server at a time is supported.
    pub fn add_server(mut self, server: &str) -> Self {
        let server = Hostname::from(server);
        self.servers.push(server.clone());
        self.hash_ring.add(server);
        self.connected.push(false);
        self
    }

    /// Configures the client ID for this client
    pub fn set_client_id(mut self, client_id: &'static str) -> Self {
        self.client_id = Some(Bytes::from(client_id));
        self
    }

    /// Attempts to connect to all servers added via [Client.add_server]
    ///
    /// This spawns a thread which will not only connect to all servers but
    /// will also reconnect to those servers on errors.
    /// 
    /// It follows the builder pattern and returns the client after having
    /// started to connect to all servers.
    ///
    /// TODO: give a way to block on conns and detect failures
    pub async fn connect(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        trace!("connecting");
        let (conn_tx, mut conn_rx): (Sender<Hostname>, Receiver<Hostname>) = channel(10);
        let conn_tx_w = conn_tx.clone();
        let mut conn_tx_r = conn_tx.clone();
        let client_id = self.client_id.clone();
        let senders_by_handle = self.senders_by_handle.clone();
        let jobs_tx_by_func = self.jobs_tx_by_func.clone();
        let echo_tx = self.echo_tx.clone();
        let job_created_tx = self.job_created_tx.clone();
        let status_res_tx = self.status_res_tx.clone();
        let error_tx = self.error_tx.clone();
        let worker_job_tx = self.worker_job_tx.clone();
        let conns = self.conns.clone();
        runtime::Handle::current().spawn(async move {
            let mut backoffs: HashMap<Hostname, u64> = HashMap::new();
            while let Some(server) = conn_rx.recv().await {
                let addr = server.parse::<SocketAddr>().unwrap();
                trace!("really connecting: server={} addr={:?}", server, addr);
                let conn = match TcpStream::connect(addr).await {
                    Ok(conn) => {
                        backoffs.insert(server.clone(), 1); // Reset on success
                        conn
                    },
                    Err(e) => {
                        let backoff = match backoffs.get(&server) {
                            None => 1,
                            Some(backoff) => *backoff,
                        };
                        backoffs.insert(server.clone(), min(MAX_BACKOFF_SECONDS, backoff*2));
                        error!("Connection error: {}, sleeping {} seconds and retrying", e, backoff);
                        let mut conn_tx = conn_tx.clone();
                        runtime::Handle::current().spawn( async move {
                            let delay = Duration::new(backoff, 0);
                            delay_for(delay).await;
                            conn_tx.send(server).await.unwrap();
                        });
                        continue
                    }
                };
                trace!(
                    "connected: server={} addr={:?}",
                    server, addr
                );
                let pc = PacketCodec {};
                let (mut sink, mut stream) = pc.framed(conn).split();
                if let Some(ref client_id) = client_id {
                    let req = new_req(SET_CLIENT_ID, client_id.clone());
                    if let Err(e) = sink.send(req).await {
                        error!("Server ({}) send error: {}", server, e);
                        let mut conn_tx = conn_tx.clone();
                        conn_tx.send(server).await.expect("Connection thread failed");
                        continue
                    }
                }
                let (tx, mut rx) = channel(100); // XXX pick a good value or const
                let tx = tx.clone();
                let tx2 = tx.clone();
                let handler = Arc::new(Mutex::new(ClientHandler::new(
                    &client_id,
                    senders_by_handle.clone(),
                    jobs_tx_by_func.clone(),
                    echo_tx.clone(),
                    tx2,
                    job_created_tx.clone(),
                    status_res_tx.clone(),
                    error_tx.clone(),
                    worker_job_tx.clone(),
                )));
                let conns = conns.clone();
                conns.lock().unwrap().insert(server.clone(), handler.clone());
                let mut conn_tx = conn_tx.clone();
                let server = server.clone();
                let server_w = server.clone();
                let reader = async move {
                    let mut tx = tx.clone();
                    while let Some(Ok(frame)) = stream.next().await {
                        if frame.magic == PacketMagic::EOF {
                            info!("disconnected, reconnecting");
                            conn_tx.send(server).await.unwrap();
                            return
                        }
                        trace!("Frame read: {:?}", frame);
                        let response = {
                            let handler = handler.clone();
                            debug!("Locking handler");
                            let mut handler = handler.lock().unwrap();
                            debug!("Locked handler");
                            handler.call(frame)
                        };
                        if let Err(e) = response {
                            error!("conn dropped?: {}", e);
                            return;
                        }
                        if let Err(_) = tx.send(response.unwrap()).await {
                            error!("receiver dropped")
                        }
                    }
                };
                let server = server_w;
                let mut conn_tx_w = conn_tx_w.clone();
                let writer = async move {
                    while let Some(packet) = rx.next().await {
                        trace!("Sending {:?}", &packet);
                        if let Err(_) = sink.send(packet).await {
                            error!("Connection ({}) dropped", server);
                            conn_tx_w.send(server).await.unwrap();
                            return
                        }
                    }
                };
                runtime::Handle::current().spawn(reader);
                runtime::Handle::current().spawn(writer);
            }
        });
        // We don't need this list to stay intact since we'll never reference it again
        for server in self.servers.drain(..) {
            conn_tx_r.send(server).await?;
        }
        Ok(self)
    }

    /// Sends an ECHO_REQ to the server, a good way to confirm the connection is alive
    ///
    /// Returns an error if there aren't any connected servers
    ///
    /// XXX: waits forever if an echo does not come back
    pub async fn echo(&mut self, payload: &[u8]) -> Result<(), io::Error> {
        let payload = payload.clone();
        let packet = new_req(ECHO_REQ, Bytes::copy_from_slice(payload));
        let mut conns = self.conns.lock().unwrap();
        for (_, conn) in conns.iter_mut() {
            send_packet(conn.clone(), packet.clone()).await?;
            debug!("Waiting for echo response");
            match self.echo_rx.recv().await {
                Some(res) => info!("echo received: {:?}", res),
                None => info!("echo channel closed"),
            };
        }
        Ok(())
    }

    /// Submits a foreground job. The see [ClientJob.response] for how to see the response from the
    /// worker.
    pub async fn submit(&mut self, function: &str, payload: &[u8]) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB, function, None, payload).await
    }

    /// Submits a background job. The [ClientJob] returned won't be able to use the
    /// [ClientJob.response] method because the server will never send packets for it.
    pub async fn submit_background(
        &mut self,
        function: &str,
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB_BG, function, None, payload).await
    }

    async fn direct_submit(
        &mut self,
        ptype: u32,
        function: &str,
        unique: Option<&str>,
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        let unique = match unique {
            None => format!("{}", Uuid::new_v4()),
            Some(unique) => unique.to_string(),
        };
        let conn = {
            let conns = self.conns.lock().unwrap();
            let server = self.hash_ring.get(&unique).unwrap();
            if let Some(conn) = conns.get(server) {
                conn.clone()
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "No connections for submitting jobs.",
                ));
            }
        };
        let conn = conn.clone();
        let mut data = BytesMut::with_capacity(2 + function.len() + unique.len() + payload.len()); // 2 for nulls
        data.extend(function.bytes());
        data.put_u8(b'\0');
        data.extend(unique.bytes());
        data.put_u8(b'\0');
        data.extend(payload);
        let packet = new_req(ptype, data.freeze());
        {
            let conn = conn.clone();
            send_packet(conn, packet).await?;
            /* Really important that conn be unlocked here to unblock res processing */
        }
        if let Some(handle) = self.job_created_rx.recv().await {
            let (tx, rx) = channel(100); // XXX lamer
            let mut response_by_handle = self.senders_by_handle.lock().unwrap();
            response_by_handle.insert(handle.clone(), tx.clone());
            Ok(ClientJob::new(handle, rx))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "No job created!"))
        }
    }

    /// Sends a GET_STATUS packet and then returns the STATUS_RES in a [JobStatus]
    ///
    /// Because this handle may exist on any server, we have to send to all servers
    /// We will return the first known status we get back.
    pub async fn get_status(&mut self, handle: &[u8]) -> Result<JobStatus, io::Error> {
        let mut conns = self.conns.lock().unwrap();
        let mut payload = BytesMut::with_capacity(handle.len());
        payload.extend(handle);
        let status_req = new_req(GET_STATUS, payload.freeze());
        for (_, conn) in conns.iter_mut() {
            send_packet(conn.clone(), status_req.clone()).await?;
            if let Some(status_res) = self.status_res_rx.recv().await {
                return Ok(status_res)
            }
        };
        Err(io::Error::new(io::ErrorKind::Other, "No status to report!"))
    }

    /// Sends a CAN_DO on every connection and registers a callback for it
    ///
    /// This informs the gearman server(s) of what "functions" your worker can perform,
    /// and it takes a closure which will be passed a mutable reference to jobs assigned
    /// to it. The function should return a vector of bytes to signal completion, that
    /// will trigger a WORK_COMPLETE packet to the server with the contents of the returned
    /// vector as the payload. If it returns an error, this will trigger a WORK_FAIL packet.
    ///
    /// See examples/worker.rs for more information.
    ///
    pub async fn can_do<F>(self, function: &str, mut func: F) -> Result<Self, io::Error>
    where
        F: FnMut(&mut WorkerJob) -> Result<Vec<u8>, io::Error> + Send + 'static,
    {
        let (tx, mut rx) = channel(100); // Some day we'll use this param right
        {
            let mut conns = self.conns.lock().unwrap();
            for (_, conn) in conns.iter_mut() {
                {
                    let conn = conn.lock().unwrap();
                    let mut jobs_tx_by_func = conn.jobs_tx_by_func.lock().unwrap();
                    let mut k = Vec::with_capacity(function.len());
                    k.extend_from_slice(function.as_bytes());
                    // Same tx for all jobs, the jobs themselves will have a response conn ref
                    jobs_tx_by_func.entry(k).or_insert(tx.clone());
                }
                let mut payload = BytesMut::with_capacity(function.len());
                payload.extend(function.bytes());
                let can_do = new_req(CAN_DO, payload.freeze());
                send_packet(conn.clone(), can_do).await?;
            }
        }
        runtime::Handle::current().spawn(async move {
            while let Some(mut job) = rx.recv().await {
                match func(&mut job) {
                    Err(_) => {
                        if let Err(e) = job.work_fail().await {
                            warn!("Failed to send WORK_FAIL {}", e);
                        }
                    }
                    Ok(response) => {
                        if let Err(e) = job.work_complete(response).await {
                            warn!("Failed to send WORK_COMPLETE {}", e);
                        }
                    }
                }
            }
        });
        Ok(self)
    }

    /// Run the assigned jobs through can_do functions until an error happens
    ///
    /// After you have set up all functions your worker can do via the
    /// [Client.can_do] method, call this function to begin working. It will
    /// not return unless there is an unexpected error.
    ///
    /// See examples/worker.rs for more information on how to use it.
    pub async fn work(mut self) -> Result<(), io::Error> {
        loop {
            let job = self.worker_job_rx.try_recv();
            let job = match job {
                Err(TryRecvError::Empty) => {
                    let conns = self.conns.lock().unwrap();
                    for (_, conn) in conns.iter() {
                        let packet = new_req(GRAB_JOB, Bytes::new());
                        send_packet(conn.clone(), packet).await?;
                    }
                    match self.worker_job_rx.recv().await {
                        Some(job) => job,
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Worker job tx are all dropped",
                            ))
                        }
                    }
                }
                Err(TryRecvError::Closed) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Worker job tx are all dropped",
                    ))
                }
                Ok(job) => job,
            };
            let mut jobs_tx_by_func = self.jobs_tx_by_func.lock().unwrap();
            let tx = match jobs_tx_by_func.get_mut(job.function()) {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Received job for unregistered function: {:?}",
                            job.function()
                        ),
                    ))
                }
                Some(tx) => tx,
            };
            if let Err(_) = tx.send(job).await {
                warn!("Ignored a job for an unregistered function"); // XXX We can do much, much better
            }
        }
    }

    /// Gets a single error that might have come from the server. The tuple returned is (code,
    /// message)
    pub async fn error(&mut self) -> Result<Option<(Bytes, Bytes)>, io::Error> {
        Ok(self.error_rx.recv().await)
    }
}

impl ClientHandler {
    fn new(
        client_id: &Option<Bytes>,
        senders_by_handle: Arc<Mutex<HashMap<Bytes, Sender<WorkUpdate>>>>,
        jobs_tx_by_func: Arc<Mutex<HashMap<Vec<u8>, Sender<WorkerJob>>>>,
        echo_tx: Sender<Bytes>,
        sink_tx: Sender<Packet>,
        job_created_tx: Sender<Bytes>,
        status_res_tx: Sender<JobStatus>,
        error_tx: Sender<(Bytes, Bytes)>,
        worker_job_tx: Sender<WorkerJob>,
    ) -> ClientHandler {
        ClientHandler {
            client_id: client_id.clone(),
            senders_by_handle: senders_by_handle,
            jobs_tx_by_func: jobs_tx_by_func,
            echo_tx: echo_tx,
            sink_tx: sink_tx,
            job_created_tx: job_created_tx,
            status_res_tx: status_res_tx,
            error_tx: error_tx,
            worker_job_tx: worker_job_tx,
        }
    }

    fn call(&mut self, req: Packet) -> Result<Packet, io::Error> {
        debug!("[{:?}] Got a req {:?}", self.client_id, req);
        match req.ptype {
            NOOP => self.handle_noop(),
            JOB_CREATED => self.handle_job_created(&req),
            NO_JOB => self.handle_no_job(),
            JOB_ASSIGN => self.handle_job_assign(&req),
            ECHO_RES => self.handle_echo_res(&req),
            ERROR => self.handle_error(&req),
            STATUS_RES | STATUS_RES_UNIQUE => self.handle_status_res(&req),
            OPTION_RES => self.handle_option_res(&req),
            WORK_COMPLETE | WORK_DATA | WORK_STATUS | WORK_WARNING | WORK_FAIL | WORK_EXCEPTION => {
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
        }
    }

    fn handle_noop(&self) -> Result<Packet, io::Error> {
        Ok(new_req(GRAB_JOB, Bytes::new()))
    }

    fn handle_no_job(&self) -> Result<Packet, io::Error> {
        Ok(new_req(PRE_SLEEP, Bytes::new()))
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
        let mut data = req.data.clone();
        let code = next_field(&mut data);
        let text = next_field(&mut data);
        let mut tx = self.error_tx.clone();
        runtime::Handle::current().spawn(async move { tx.send((code, text)).await });
        Ok(no_response())
    }

    fn handle_status_res(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut req = req.clone();
        let mut js = JobStatus {
            handle: next_field(&mut req.data),
            known: bytes2bool(&next_field(&mut req.data)),
            running: bytes2bool(&next_field(&mut req.data)),
            numerator: String::from_utf8(next_field(&mut req.data).to_vec())
                .unwrap()
                .parse()
                .unwrap(), // XXX we can do better
            denominator: String::from_utf8(next_field(&mut req.data).to_vec())
                .unwrap()
                .parse()
                .unwrap(),
            waiting: 0,
        };
        if req.ptype == STATUS_RES_UNIQUE {
            js.waiting = String::from_utf8(next_field(&mut req.data).to_vec())
                .unwrap()
                .parse()
                .unwrap();
        }
        let mut tx = self.status_res_tx.clone();
        runtime::Handle::current().spawn(async move { tx.send(js).await });
        Ok(no_response())
    }

    fn handle_option_res(&mut self, _req: &Packet) -> Result<Packet, io::Error> {
        /*
        if let Some(option_call) = self.option_call {
            option_call(req.data)
        }
        */
        Ok(no_response())
    }

    fn handle_work_update(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut data = req.data.clone();
        let handle = next_field(&mut data);
        let payload = next_field(&mut data);
        let work_update = {
            let handle = handle.clone();
            match req.ptype {
                WORK_DATA => WorkUpdate::Data {
                    handle: handle,
                    payload: payload,
                },
                WORK_COMPLETE => WorkUpdate::Complete {
                    handle: handle,
                    payload: payload,
                },
                WORK_WARNING => WorkUpdate::Warning {
                    handle: handle,
                    payload: payload,
                },
                WORK_EXCEPTION => WorkUpdate::Exception {
                    handle: handle,
                    payload: payload,
                },
                WORK_FAIL => WorkUpdate::Fail(handle),
                WORK_STATUS => {
                    let numerator: usize = String::from_utf8((&payload).to_vec())
                        .unwrap()
                        .parse()
                        .unwrap();
                    let denominator: usize = String::from_utf8(next_field(&mut data).to_vec())
                        .unwrap()
                        .parse()
                        .unwrap();
                    WorkUpdate::Status {
                        handle: handle,
                        numerator: numerator,
                        denominator: denominator,
                    }
                }
                _ => unreachable!("handle_work_status called with wrong ptype: {:?}", req),
            }
        };
        let senders_by_handle = self.senders_by_handle.lock().unwrap();
        if let Some(tx) = senders_by_handle.get(&handle) {
            let mut tx = tx.clone();
            runtime::Handle::current().spawn(async move { tx.send(work_update).await });
        } else {
            error!("Received work for unknown job: {:?}", handle);
        };
        Ok(no_response())
    }

    fn handle_job_assign(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut data = req.data.clone();
        let handle = next_field(&mut data);
        let function = next_field(&mut data);
        let payload = next_field(&mut data);
        let job = WorkerJob {
            handle: handle,
            function: function,
            payload: payload,
            sink_tx: self.sink_tx.clone(),
        };
        let mut tx = self.worker_job_tx.clone();
        runtime::Handle::current().spawn(async move { tx.send(job).await });
        Ok(no_response())
    }
}
