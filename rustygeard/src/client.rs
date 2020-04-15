use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::Decoder;

use uuid::Uuid;

use crate::service::{new_req, new_res, next_field, no_response};
use crate::util::bytes2bool;
use rustygear::codec::{Packet, PacketCodec};
use rustygear::constants::*;

type Hostname = String;

#[derive(Debug)]
pub struct JobStatus {
    handle: Bytes,
    known: bool,
    running: bool,
    numerator: u32,
    denominator: u32,
    waiting: u32,
}

/// Client for submitting and tracking jobs
pub struct Client {
    servers: Vec<Hostname>,
    conns: Arc<Mutex<Vec<Arc<Mutex<ClientHandler>>>>>,
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
    conn: Option<Arc<Mutex<ClientHandler>>>,
}

/// Passed to workers
pub struct WorkerJob {
    handle: Bytes,
    function: Bytes,
    payload: Bytes,
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

async fn send_packet(
    conn: Arc<Mutex<ClientHandler>>,
    packet: Packet,
) -> Result<(), io::Error> {
    let mut sink_tx = conn.lock().unwrap().sink_tx.clone();
    if let Err(e) = sink_tx.send(packet).await {
        error!("Receiver dropped");
        return Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)));
    }
    Ok(())
}

impl ClientJob {
    fn new(
        handle: Bytes,
        response_rx: Receiver<WorkUpdate>,
        conn: Option<Arc<Mutex<ClientHandler>>>,
    ) -> ClientJob {
        ClientJob {
            handle: handle,
            response_rx: response_rx,
            conn: conn,
        }
    }

    /// returns the job handle
    pub fn handle(&self) -> &Bytes {
        &self.handle
    }
    /// Should only return when the worker has sent data or completed the job. Errors if used on background jobs.
    pub async fn response(&mut self) -> Result<WorkUpdate, io::Error> {
        Ok(self.response_rx.recv().await.unwrap())
    }

    async fn send_packet(&mut self, packet: Packet) -> Result<(), io::Error> {
        if let Some(conn) = &self.conn {
            send_packet(conn.clone(), packet).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "No connection on which to send WORK_FAIL",
            ))
        }
    }

    /// Sends a WORK_FAIL
    pub async fn work_fail(&mut self) -> Result<(), io::Error> {
        let packet = new_res(WORK_FAIL, self.handle.clone());
        self.send_packet(packet).await
    }

    /// Sends a WORK_STATUS
    pub async fn work_status(&mut self, numerator: u32, denominator: u32) -> Result<(), io::Error> {
        let numerator = format!("{}", numerator);
        let denominator = format!("{}", denominator);
        let mut payload = BytesMut::with_capacity(2 + self.handle.len() + numerator.as_bytes().len() + denominator.as_bytes().len());
        payload.extend(self.handle.clone());
        payload.put_u8(b'\0');
        payload.extend(numerator.as_bytes());
        payload.put_u8(b'\0');
        payload.extend(denominator.as_bytes());
        let packet = new_res(WORK_STATUS, payload.freeze());
        self.send_packet(packet).await
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
            conns: Arc::new(Mutex::new(Vec::new())),
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
    pub fn add_server(mut self, server: &str) -> Self {
        self.servers.push(Hostname::from(server));
        self.connected.push(false);
        self
    }

    /// Configures the client ID for this client
    pub fn set_client_id(mut self, client_id: &'static str) -> Self {
        self.client_id = Some(Bytes::from(client_id));
        self
    }

    /// Attempts to connect to all servers
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
                connects.push(
                    runtime::Handle::current()
                        .spawn(async move { (i, TcpStream::connect(addr).await) }),
                );
            }
            i = i + 1;
        }
        for connect in connects.iter_mut() {
            let connect = connect.await?;
            let offset = connect.0;
            let conn = connect.1?;
            trace!(
                "connected: offset={} server={}",
                offset,
                self.servers[offset]
            );
            let pc = PacketCodec {};
            let (mut sink, mut stream) = pc.framed(conn).split();
            let (tx, mut rx) = channel(100); // XXX pick a good value or const
            let tx = tx.clone();
            let tx2 = tx.clone();
            let handler = Arc::new(Mutex::new(ClientHandler::new(
                &self.client_id,
                self.senders_by_handle.clone(),
                self.jobs_tx_by_func.clone(),
                self.echo_tx.clone(),
                tx2,
                self.job_created_tx.clone(),
                self.status_res_tx.clone(),
                self.error_tx.clone(),
                self.worker_job_tx.clone(),
            )));
            self.connected[offset] = true;
            self.conns.lock().unwrap().insert(offset, handler.clone());
            let reader = async move {
                let mut tx = tx.clone();
                while let Some(frame) = stream.next().await {
                    trace!("Frame read: {:?}", frame);
                    let response = {
                        let handler = handler.clone();
                        debug!("Locking handler");
                        let mut handler = handler.lock().unwrap();
                        debug!("Locked handler");
                        handler.call(frame.unwrap())
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

    /// Sends an ECHO_REQ to the server, a good way to confirm the connection is alive
    ///
    /// Returns an error if there aren't any connected servers, or no ECHO_RES comes back
    pub async fn echo(&mut self, payload: &[u8]) -> Result<(), io::Error> {
        let payload = payload.clone();
        let packet = new_req(ECHO_REQ, Bytes::copy_from_slice(payload));
        let conn: Arc<Mutex<ClientHandler>> = {
            if let Some(conn) = self.conns.lock().unwrap().get_mut(0) {
                conn.clone()
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "No connections for echo!",
                ));
            }
        };
        send_packet(conn, packet).await?;
        debug!("Waiting for echo response");
        match self.echo_rx.recv().await {
            Some(res) => info!("echo received: {:?}", res),
            None => info!("echo channel closed"),
        };
        Ok(())
    }

    /// Submits a foreground job. The see [ClientJob.response] for how to see the response from the
    /// worker.
    pub async fn submit(&mut self, function: &str, payload: &[u8]) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB, function, payload).await
    }

    /// Submits a background job. The [ClientJob] returned won't be able to use the
    /// [ClientJob.response] method because the server will never send packets for it.
    pub async fn submit_background(
        &mut self,
        function: &str,
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB_BG, function, payload).await
    }

    async fn direct_submit(
        &mut self,
        ptype: u32,
        function: &str,
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        let conn = {
            let mut conns = self.conns.lock().unwrap();
            if let Some(conn) = conns.get_mut(0) {
                conn.clone()
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "No connections for submitting jobs.",
                ));
            }
        };
        /* Pick the conn later */
        let conn = conn.clone();
        let unique = format!("{}", Uuid::new_v4());
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
            Ok(ClientJob::new(handle, rx, Some(conn.clone())))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "No job created!"))
        }
    }

    /// Sends a GET_STATUS packet and then returns the STATUS_RES in a [JobStatus]
    pub async fn get_status(&mut self, handle: &[u8]) -> Result<JobStatus, io::Error> {
        let conn: Arc<Mutex<ClientHandler>> = {
            let mut conns = self.conns.lock().unwrap();
            conns.get_mut(0).unwrap().clone()
        };
        let mut payload = BytesMut::with_capacity(handle.len());
        payload.extend(handle);
        let status_req = new_req(GET_STATUS, payload.freeze());
        send_packet(conn, status_req).await?;
        if let Some(status_res) = self.status_res_rx.recv().await {
            Ok(status_res)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "No status to report!"))
        }
    }

    /// Sends a CAN_DO and registers a callback for it
    pub async fn can_do<F>(self, function: &str, mut func: F) -> Result<Self, io::Error>
    where
        F: FnMut(&mut WorkerJob) -> Result<(), io::Error> + Send + 'static,
    {
        let conn: Arc<Mutex<ClientHandler>> = {
            let mut conns = self.conns.lock().unwrap();
            conns.get_mut(0).unwrap().clone()
        };
        let (tx, mut rx) = channel(100); // Some day we'll use this param right
        {
            let conn = conn.lock().unwrap();
            let mut jobs_tx_by_func = conn.jobs_tx_by_func.lock().unwrap();
            {
                let mut k = Vec::with_capacity(function.len());
                k.extend_from_slice(function.as_bytes());
                if let Some(_) = jobs_tx_by_func.get(&k) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("function {} already registered.", function),
                    ));
                }
            }
            let mut k = Vec::with_capacity(function.len());
            k.extend_from_slice(function.as_bytes());
            jobs_tx_by_func.insert(k, tx.clone());
        }
        let mut payload = BytesMut::with_capacity(function.len());
        payload.extend(function.bytes());
        let can_do = new_req(CAN_DO, payload.freeze());
        send_packet(conn, can_do).await?;
        runtime::Handle::current().spawn(async move {
            while let Some(mut job) = rx.recv().await {
                if let Err(_) = func(&mut job) {
                    debug!("NOT IMPLEMENTED NO ERROR HANDLING FOR FUNCTIONS");
                }
            }
        });
        Ok(self)
    }

    pub async fn work(mut self) -> Result<(), io::Error> {
        while let Some(job) = self.worker_job_rx.recv().await {
            let mut jobs_tx_by_func = self.jobs_tx_by_func.lock().unwrap();
            let tx = match jobs_tx_by_func.get_mut(job.function()) {
                None => return Err(io::Error::new(io::ErrorKind::Other, format!("Received job for unregistered function: {:?}", job.function()))),
                Some(tx) => tx,
            };
            if let Err(_) = tx.send(job).await {
                warn!("Ignored a job for an unregistered function"); // XXX We can do much, much better
            }
        }
        Ok(())
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
            //NOOP => self.handle_noop(),
            JOB_CREATED => self.handle_job_created(&req),
            //NO_JOB => self.handle_no_job(&req),
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
        };
        let mut tx = self.worker_job_tx.clone();
        runtime::Handle::current().spawn(async move {
            tx.send(job).await
        });
        Ok(no_response())
    }
}
