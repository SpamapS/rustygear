use core::fmt;
use std::fmt::Display;
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
use std::io;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio::time::sleep;
use tokio_util::codec::Decoder;

use uuid::Uuid;

use crate::clientdata::ClientData;
use crate::codec::{Packet, PacketCodec};
use crate::conn::{Connections, ConnHandler};
use crate::constants::*;
use crate::util::{new_req, new_res};

type Hostname = String;

const CLIENT_CHANNEL_BOUND_SIZE: usize = 100;
const RECONNECT_BACKOFF: Duration = Duration::from_millis(30000);

#[derive(Debug)]
/// Used for passing job completion stats to clients
pub struct JobStatus {
    pub handle: Bytes,
    pub known: bool,
    pub running: bool,
    pub numerator: u32,
    pub denominator: u32,
    pub waiting: u32,
}

impl Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobStatus {{ handle: {}, known: {}, running: {}", String::from_utf8_lossy(&self.handle), self.known, self.running)?;
        if self.denominator > 0 {
            write!(f, " {}/{}", self.numerator, self.denominator)?;
        }
        write!(f, " {} waiters }}", self.waiting)
    }
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
    streams: Arc<Mutex<Vec<Option<TcpStream>>>>,
    conns: Arc<Mutex<Connections>>,
    client_id: Option<Bytes>,
    client_data: ClientData,
}

/// Return object for submit_ functions.
#[derive(Debug)]
pub struct ClientJob {
    handle: Bytes,
    response_rx: Receiver<WorkUpdate>,
}

impl fmt::Display for ClientJob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(
            f,
            "ClientJob[{}]",
            String::from_utf8(self.handle.to_vec()).unwrap()
        )
    }
}

/// Passed to workers
///
/// The sink_tx property of this structure can be used to send raw packets
/// to the gearman server from workers, although this is not known to work
/// generically as of this writing.
pub struct WorkerJob {
    pub handle: Bytes,
    pub function: Bytes,
    pub payload: Bytes,
    pub(crate) sink_tx: Sender<Packet>,
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
        if let Some(workupdate) = self.response_rx.recv().await {
            Ok(workupdate)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Nothing to receive."))
        }
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
        let packet = new_req(WORK_COMPLETE, payload.freeze());
        self.send_packet(packet).await
    }
}

impl Client {
    pub fn new() -> Client {
        Client {
            servers: Vec::new(),
            streams: Arc::new(Mutex::new(Vec::new())),
            conns: Arc::new(Mutex::new(Connections::new())),
            client_id: None,
            client_data: ClientData::new(),
        }
    }

    /// Add a server to the client. This does not initiate anything, it just configures the client.
    ///
    pub fn add_server(mut self, server: &str) -> Self {
        self.servers.push(Hostname::from(server));
        self.streams.lock().unwrap().push(None);
        self
    }

    /// Configures the client ID for this client
    pub fn set_client_id(mut self, client_id: &'static str) -> Self {
        self.client_id = Some(Bytes::from(client_id));
        self
    }

    /// Attempts to connect to all servers added via [Client.add_server]
    pub async fn connect(self) -> Result<Self, Box<dyn std::error::Error>> {
        /* Returns the client after having attempted to connect to all servers. */
        trace!("connecting");
        let (ctx, mut crx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        // Start connector thread which reads offsets from conns, writes to conns vec, and sends offset
        let (ctdtx, mut ctdrx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        let ctx_conn = ctx.clone();
        let ctx2 = ctx.clone();
        let wait_for_all_streams = self.streams.clone();
        let connector_servers = self.servers.clone();
        let connector_streams = self.streams.clone();
        let client_id = self.client_id.clone();
        let handler_client_data = self.client_data.clone();
        let connector_conns = self.conns.clone();
        let connector = async move {
            trace!("Connector thread starting");
            loop {
                let offset: Option<usize> = crx.recv().await;
                match offset {
                    None => {
                        debug!("Shutting down connector thread.");
                        return;
                    },
                    Some(offset) => {
                        match connector_servers.get(offset) {
                            None => warn!("Invalid connection offset {} sent to connector thread, ignoring.", offset),
                            Some(server) => {
                                let server: &str = server;
                                let addr = server.to_socket_addrs().unwrap().next().unwrap();
                                trace!("really connecting: i={} addr={:?}", offset, addr);
                                match TcpStream::connect(addr).await {
                                    Err(e) => {
                                        error!("Couldn't connect to {} [{}], will retry after {:?}", server, e, RECONNECT_BACKOFF);
                                        let ctx_conn = ctx_conn.clone();
                                        // Retry in BACKOFF seconds -- TODO: keep track and do exponential
                                        runtime::Handle::current().spawn(
                                            async move {
                                                sleep(RECONNECT_BACKOFF).await;
                                                ctx_conn.send(offset).await});
                                    },
                                    Ok(stream) => {
                                        info!("Connected to {}", server);
                                        connector_streams.lock().unwrap()[offset] = Some(stream);
                                        let pc = PacketCodec {};
                                        if let None = connector_streams.lock().unwrap()[offset] {
                                            warn!("Received offset of disconnected server, ignoring");
                                            continue;
                                        }
                                        let stream = std::mem::replace(&mut connector_streams.lock().unwrap()[offset], None);
                                        let (mut sink, mut stream) = pc.framed(stream.unwrap()).split();
                                        if let Some(ref client_id) = client_id {
                                            let req = new_req(SET_CLIENT_ID, client_id.clone());
                                            if let Err(e) = sink.send(req).await {
                                                debug!("Connection {:?} can't send packets. ({:?})", sink, e);
                                                continue;
                                            }
                                        }
                                        let (tx, mut rx) = channel(CLIENT_CHANNEL_BOUND_SIZE); // XXX pick a good value or const
                                        let tx = tx.clone();
                                        let tx2 = tx.clone();
                                        let handler = ConnHandler::new(
                                            &client_id,
                                            tx2,
                                            handler_client_data.clone(),
                                        );
                                        trace!("Inserting at {}", offset);
                                        connector_conns.lock().unwrap().insert(offset, handler.clone());
                                        trace!("Inserted at {}", offset);
                                        let reader_streams = connector_streams.clone();
                                        let reader_ctx = ctx2.clone();
                                        let reader = async move {
                                            let tx = tx.clone();
                                            while let Some(frame) = stream.next().await {
                                                trace!("Frame read: {:?}", frame);
                                                let response = {
                                                    let handler = handler.clone();
                                                    debug!("Locking handler");
                                                    let mut handler = handler;
                                                    debug!("Locked handler");
                                                    handler.call(frame.unwrap())
                                                };
                                                if let Err(e) = response {
                                                    error!("conn dropped?: {}", e);
                                                    break;
                                                }
                                                if let Err(_) = tx.send(response.unwrap()).await {
                                                    error!("receiver dropped")
                                                }
                                            }
                                            reader_streams.lock().unwrap()[offset] = None;
                                            if let Err(e) = reader_ctx.send(offset).await {
                                                error!("Can't send to connector, aborting! {}", e);
                                            }
                                        };
                                        let writer_streams = connector_streams.clone();
                                        let writer = async move {
                                            while let Some(packet) = rx.recv().await {
                                                trace!("Sending {:?}", &packet);
                                                if let Err(_) = sink.send(packet).await {
                                                    error!("Connection ({}) dropped", offset);
                                                    writer_streams.lock().unwrap()[offset] = None;
                                                }
                                            }
                                        };
                                        runtime::Handle::current().spawn(reader);
                                        runtime::Handle::current().spawn(writer);
                                        if let Err(e) = ctdtx.send(offset).await {
                                            // Connected channel is closed, shut it all down
                                            info!("Shutting down connector because connected channel returned error ({})", e);
                                            break;
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            }
        };
        runtime::Handle::current().spawn(connector);
        trace!("Connector thread started, initiating all connections");
        for (i, _) in self.servers.iter().enumerate() {
            ctx.send(i).await.expect("Connector RX lives");
        }
        let mut waiting_for = wait_for_all_streams.lock().unwrap().len();
        loop {
            info!("Waiting for {}", waiting_for);
            match ctdrx.recv().await {
                None => {
                    return Err(Box::new(io::Error::new(io::ErrorKind::Other, "Connector aborted")))
                },
                Some(_) => { waiting_for -= 1 }
            }
            if waiting_for <= 0 {
                break;
            }
        }
        debug!("connected all");
        Ok(self)
    }

    /// Sends an ECHO_REQ to the server, a good way to confirm the connection is alive
    ///
    /// Returns an error if there aren't any connected servers, or no ECHO_RES comes back
    pub async fn echo(&mut self, payload: &[u8]) -> Result<(), io::Error> {
        let packet = new_req(ECHO_REQ, Bytes::copy_from_slice(payload));
        if self.conns.lock().unwrap().len() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "No connections for echo!",
            ));
        }
        self.conns.lock().unwrap().get(0).unwrap().send_packet(packet).await?;
        debug!("Waiting for echo response");
        match self.client_data.receivers().echo_rx.recv().await {
            Some(res) => info!("echo received: {:?}", res),
            None => info!("echo channel closed"),
        };
        Ok(())
    }

    /// Submits a foreground job. The see [ClientJob.response] for how to see the response from the
    /// worker.
    pub async fn submit(&mut self, function: &str, payload: &[u8]) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB, function, payload, None)
            .await
    }

    pub async fn submit_unique(
        &mut self,
        function: &str,
        unique: &[u8],
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB, function, payload, Some(unique))
            .await
    }

    /// Submits a background job. The [ClientJob] returned won't be able to use the
    /// [ClientJob.response] method because the server will never send packets for it.
    pub async fn submit_background(
        &mut self,
        function: &str,
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB_BG, function, payload, None)
            .await
    }

    /// Submits a background job. The [ClientJob] returned won't be able to use the
    /// [ClientJob.response] method because the server will never send packets for it.
    pub async fn submit_background_unique(
        &mut self,
        function: &str,
        unique: &[u8],
        payload: &[u8],
    ) -> Result<ClientJob, io::Error> {
        self.direct_submit(SUBMIT_JOB_BG, function, payload, Some(unique))
            .await
    }

    async fn direct_submit(
        &mut self,
        ptype: u32,
        function: &str,
        payload: &[u8],
        unique: Option<&[u8]>,
    ) -> Result<ClientJob, io::Error> {
        let mut uuid_unique = BytesMut::new();
        let unique: &[u8] = match unique {
            None => {
                uuid_unique.extend(format!("{}", Uuid::new_v4()).bytes());
                &uuid_unique
            }
            Some(unique) => unique,
        };
        let mut data = BytesMut::with_capacity(2 + function.len() + unique.len() + payload.len()); // 2 for nulls
        data.extend(function.bytes());
        data.put_u8(b'\0');
        data.extend(unique);
        data.put_u8(b'\0');
        data.extend(payload);
        let packet = new_req(ptype, data.freeze());
        {
            let mut conns = self.conns.lock().unwrap();
            let conn  = match conns.get_hashed_conn(&unique.iter().map(|b| *b).collect()) {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "No connections for submitting jobs.",
                    ));
                },
                Some(conn) => conn
            };
            conn.send_packet(packet).await?;
            /* Really important that conn be unlocked here to unblock res processing */
        }
        let client_data = self.client_data.clone();
        let submit_result = if let Some(handle) = client_data.receivers().job_created_rx.recv().await {
            let (tx, rx) = channel(CLIENT_CHANNEL_BOUND_SIZE); // XXX lamer
            self.client_data.set_sender_by_handle(handle.clone(), tx.clone());
            Ok(ClientJob::new(handle, rx))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "No job created!"))
        };
        submit_result
    }

    /// Sends a GET_STATUS packet and then returns the STATUS_RES in a [JobStatus]
    pub async fn get_status(&mut self, handle: &[u8]) -> Result<JobStatus, io::Error> {
        // TODO: loop all conns or keep track of mapping?
        {
            let conns = self.conns.lock().unwrap();
            let conn = conns.get(0).expect("At least one conn");
            let mut payload = BytesMut::with_capacity(handle.len());
            payload.extend(handle);
            let status_req = new_req(GET_STATUS, payload.freeze());
            conn.send_packet(status_req).await?;
        }
        if let Some(status_res) = self.client_data.receivers().status_res_rx.recv().await {
            Ok(status_res)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "No status to report!"))
        }
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
    pub async fn can_do<F>(mut self, function: &str, func: F) -> Result<Self, io::Error>
    where
        F: FnMut(&mut WorkerJob) -> Result<Vec<u8>, io::Error> + Send + 'static,
    {
        let (tx, mut rx) = channel(CLIENT_CHANNEL_BOUND_SIZE); // Some day we'll use this param right
        {
            for (i, conn) in self.conns.lock().unwrap().iter_mut().filter_map(|c| c.to_owned()).enumerate() {
                {
                    let mut k = Vec::with_capacity(function.len());
                    k.extend_from_slice(function.as_bytes());
                    // Same tx for all jobs, the jobs themselves will have a response conn ref
                    self.client_data.set_jobs_tx_by_func(k, tx.clone());
                }
                let mut payload = BytesMut::with_capacity(function.len());
                payload.extend(function.bytes());
                let can_do = new_req(CAN_DO, payload.freeze());
                conn.send_packet(can_do).await?;
                info!("Sent CAN_DO({}) to {}", function, self.servers[i]);
            }
        }
        let func_arc = Arc::new(Mutex::new(func));
        runtime::Handle::current().spawn(async move {
            while let Some(mut job) = rx.recv().await {
                let func_clone = func_arc.clone();
                task::spawn_blocking(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let res = func_clone.lock().unwrap()(&mut job);
                    match res {
                        Err(_) => {
                            if let Err(e) = rt.block_on(job.work_fail()) {
                                warn!("Failed to send WORK_FAIL {}", e);
                            }
                        }
                        Ok(response) => {
                            if let Err(e) = rt.block_on(job.work_complete(response)) {
                                warn!("Failed to send WORK_COMPLETE {}", e);
                            }
                        }
                    };
                })
                .await
                .unwrap();
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
    pub async fn work(self) -> Result<(), io::Error> {
        loop {
            let job = self.client_data.receivers().worker_job_rx.try_recv();
            let job = match job {
                Err(TryRecvError::Empty) => {
                    for conn in self.conns.lock().unwrap().iter().filter_map(|c| c.to_owned()) {
                        let packet = new_req(GRAB_JOB, Bytes::new());
                        conn.send_packet(packet).await?;
                    }
                    match self.client_data.receivers().worker_job_rx.recv().await {
                        Some(job) => job,
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Worker job tx are all dropped",
                            ))
                        }
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Worker job tx are all dropped",
                    ))
                }
                Ok(job) => job,
            };
            let tx = match self.client_data.get_jobs_tx_by_func(&Vec::from(job.function())) {
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
        Ok(self.client_data.receivers().error_rx.recv().await)
    }
}