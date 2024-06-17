use std::{
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    io,
    slice::{Iter, IterMut},
};

use bytes::Bytes;
use hashring::HashRing;
use tokio::{runtime, sync::mpsc::Sender};

use crate::{
    client::{Hostname, JobStatus, WorkUpdate, WorkerJob},
    clientdata::ClientData,
    codec::Packet,
    constants::*,
    util::{bytes2bool, new_req, next_field, no_response},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ServerHandle {
    server: Hostname,
    handle: Bytes,
}

impl ServerHandle {
    pub fn new(server: Hostname, handle: Bytes) -> ServerHandle {
        ServerHandle { server, handle }
    }

    pub fn server(&self) -> &Hostname {
        &self.server
    }

    pub fn handle(&self) -> &Bytes {
        &self.handle
    }
}

impl Display for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

/// Each individual connection has one of these for handling packets
#[derive(Clone)]
pub struct ConnHandler {
    client_id: Option<Bytes>,
    server: Hostname,
    sink_tx: Sender<Packet>,
    client_data: ClientData,
    active: bool,
}

impl ConnHandler {
    pub fn new(
        client_id: &Option<Bytes>,
        server: Hostname,
        sink_tx: Sender<Packet>,
        client_data: ClientData,
        active: bool,
    ) -> ConnHandler {
        ConnHandler {
            client_id: client_id.clone(),
            server,
            sink_tx,
            client_data,
            active,
        }
    }

    pub fn server(&self) -> &Hostname {
        &self.server
    }

    pub fn is_active(&self) -> bool {
        return self.active;
    }

    pub fn set_active(&mut self, active: bool) {
        self.active = active
    }

    pub async fn send_packet(&self, packet: Packet) -> Result<(), Box<dyn Error>> {
        Ok(self.sink_tx.send(packet).await?)
    }

    pub fn call(&mut self, req: Packet) -> Result<Packet, Box<dyn std::error::Error>> {
        debug!("[{:?}] Got a req {:?}", self.client_id, req);
        match req.ptype {
            NOOP => self.handle_noop(),
            JOB_CREATED => Ok(self.handle_job_created(&req)),
            NO_JOB => Ok(self.handle_no_job()),
            JOB_ASSIGN => Ok(self.handle_job_assign(&req)),
            JOB_ASSIGN_UNIQ => Ok(self.handle_job_assign_uniq(&req)),
            ECHO_RES => Ok(self.handle_echo_res(&req)),
            ERROR => Ok(self.handle_error(&req)),
            STATUS_RES | STATUS_RES_UNIQUE => self.handle_status_res(&req),
            OPTION_RES => Ok(self.handle_option_res(&req)),
            WORK_COMPLETE | WORK_DATA | WORK_STATUS | WORK_WARNING | WORK_FAIL | WORK_EXCEPTION => {
                self.handle_work_update(&req)
            }
            //JOB_ASSIGN_ALL => self.handle_job_assign_all(&req),
            _ => {
                error!("Unimplemented: {:?} processing packet", req);
                Err(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid packet type {}", req.ptype),
                )))
            }
        }
    }

    fn handle_noop(&self) -> Result<Packet, Box<dyn Error>> {
        Ok(new_req(GRAB_JOB_UNIQ, Bytes::new()))
    }

    fn handle_no_job(&self) -> Packet {
        new_req(PRE_SLEEP, Bytes::new())
    }

    fn handle_echo_res(&mut self, req: &Packet) -> Packet {
        info!("Echo response received: {:?}", req.data);
        let tx = self.client_data.echo_tx();
        let data = req.data.clone();
        runtime::Handle::current().spawn(async move { tx.send(data).await });
        no_response()
    }

    fn handle_job_created(&mut self, req: &Packet) -> Packet {
        info!("Job Created: {:?}", req);
        let tx = self.client_data.job_created_tx();
        let handle = req.data.clone();
        let server = self.server.clone();
        runtime::Handle::current()
            .spawn(async move { tx.send(ServerHandle::new(server, handle)).await });
        no_response()
    }

    fn handle_error(&mut self, req: &Packet) -> Packet {
        let mut data = req.data.clone();
        let code = next_field(&mut data);
        let text = next_field(&mut data);
        let tx = self.client_data.error_tx();
        runtime::Handle::current().spawn(async move { tx.send((code, text)).await });
        no_response()
    }

    fn handle_status_res(&mut self, req: &Packet) -> Result<Packet, Box<dyn Error>> {
        let mut req = req.clone();
        let mut js = JobStatus {
            handle: next_field(&mut req.data),
            known: bytes2bool(&next_field(&mut req.data)),
            running: bytes2bool(&next_field(&mut req.data)),
            numerator: String::from_utf8(next_field(&mut req.data).to_vec())?.parse()?,
            denominator: String::from_utf8(next_field(&mut req.data).to_vec())?.parse()?,
            waiting: 0,
        };
        if req.ptype == STATUS_RES_UNIQUE {
            js.waiting = String::from_utf8(next_field(&mut req.data).to_vec())?.parse()?;
        }
        let tx = self.client_data.status_res_tx();
        runtime::Handle::current().spawn(async move { tx.send(js).await });
        Ok(no_response())
    }

    fn handle_option_res(&mut self, _req: &Packet) -> Packet {
        /*
        if let Some(option_call) = self.option_call {
            option_call(req.data)
        }
        */
        no_response()
    }

    fn handle_work_update(&mut self, req: &Packet) -> Result<Packet, Box<dyn Error>> {
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
                    let numerator: usize = String::from_utf8((&payload).to_vec())?.parse()?;
                    let denominator: usize =
                        String::from_utf8(next_field(&mut data).to_vec())?.parse()?;
                    WorkUpdate::Status {
                        handle: handle,
                        numerator: numerator,
                        denominator: denominator,
                    }
                }
                _ => unreachable!("handle_work_status called with wrong ptype: {:?}", req),
            }
        };
        let server_handle = ServerHandle::new(self.server().clone(), handle);
        if let Some(tx) = self.client_data.get_sender_by_handle(&server_handle) {
            runtime::Handle::current().spawn(async move { tx.send(work_update).await });
        } else {
            error!("Received {:?} for unknown job: {:?}", req, server_handle);
        };
        Ok(no_response())
    }

    fn handle_job_assign(&mut self, req: &Packet) -> Packet {
        let mut data = req.data.clone();
        let handle = next_field(&mut data);
        let function = next_field(&mut data);
        let payload = next_field(&mut data);
        let unique = Bytes::new();
        let job = WorkerJob {
            handle,
            function,
            payload,
            unique,
            sink_tx: self.sink_tx.clone(),
        };
        let tx = self.client_data.worker_job_tx();
        runtime::Handle::current().spawn(async move { tx.send(job).await });
        no_response()
    }

    fn handle_job_assign_uniq(&mut self, req: &Packet) -> Packet {
        let mut data = req.data.clone();
        let handle = next_field(&mut data);
        let function = next_field(&mut data);
        let unique = next_field(&mut data);
        let payload = next_field(&mut data);
        let job = WorkerJob {
            handle,
            function,
            payload,
            unique,
            sink_tx: self.sink_tx.clone(),
        };
        let tx = self.client_data.worker_job_tx();
        runtime::Handle::current().spawn(async move { tx.send(job).await });
        no_response()
    }
}

impl Debug for ConnHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // client_data is too big to display
        f.debug_struct("ConnHandler")
            .field("client_id", &self.client_id)
            .field("server", &self.server)
            .field("sink_tx", &self.sink_tx)
            .finish()
    }
}

pub struct Connections {
    conns: Vec<Option<ConnHandler>>,
    server_map: HashMap<Hostname, usize>,
    ring: HashRing<usize>,
}

impl Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ring = format!("HashRing(len={})", self.ring.len());
        f.debug_struct("Connections")
            .field("conns", &self.conns)
            .field("ring", &ring)
            .finish()
    }
}

/* This pairs a vector of ConnHandler's with a ring to select them by hash. This is used in the high-level client to implement predictable job sumission distribution. */
impl Connections {
    pub fn new() -> Connections {
        Connections {
            conns: Vec::new(),
            server_map: HashMap::new(),
            ring: HashRing::new(),
        }
    }

    pub fn insert(&mut self, offset: usize, handler: ConnHandler) {
        if offset > self.conns.len() {
            (0..offset).for_each(|_| self.conns.push(None))
        }
        self.server_map.insert(handler.server.clone(), offset);
        self.conns.insert(offset, Some(handler));
        self.ring.add(offset);
    }

    pub fn active_servers(&self) -> impl Iterator<Item = &Hostname> {
        self.conns.iter().filter_map(|conn| {
            conn.as_ref().and_then(|conn| {
                if conn.is_active() {
                    Some(conn.server())
                } else {
                    None
                }
            })
        })
    }

    pub fn deactivate_server(&mut self, server: &Hostname) {
        self.set_active_by_server(server, false)
    }

    pub fn activate_server(&mut self, server: &Hostname) {
        self.set_active_by_server(server, true)
    }

    pub fn set_active_by_server(&mut self, server: &Hostname, active: bool) {
        if let Some(offset) = self.server_map.get(server) {
            if let Some(conn) = self.conns.get_mut(*offset) {
                if let Some(conn) = conn {
                    return conn.set_active(active);
                }
            }
        }
        warn!(
            "Tried to set active = {} on a connection that wasn't there: {}",
            active, server
        );
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn get(&self, index: usize) -> Option<&ConnHandler> {
        match self.conns.get(index) {
            None => None,
            Some(c) => match c {
                None => None,
                Some(ref c) => Some(c),
            },
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut ConnHandler> {
        match self.conns.get_mut(index) {
            None => None,
            Some(c) => match c {
                None => None,
                Some(ref mut c) => Some(c),
            },
        }
    }

    pub fn get_by_server(&self, server: &Hostname) -> Option<&ConnHandler> {
        self.server_map
            .get(server)
            .and_then(|offset| self.conns.get(*offset))
            .unwrap_or(&None)
            .as_ref()
    }

    // TODO: Make this a client configurable
    const HASH_RING_REPLICAS: usize = 1;

    // This is run with locks held so it is very important that it not panic!
    pub fn get_hashed_conn(&mut self, hashable: &Vec<u8>) -> Option<&mut ConnHandler> {
        match self
            .ring
            .get_with_replicas(hashable, Self::HASH_RING_REPLICAS)
        {
            None => None,
            Some(offsets) => {
                match offsets.iter().find(|offset| {
                    self.conns
                        .get(**offset)
                        .and_then(|c| c.as_ref().and_then(|c2| Some(c2.is_active())))
                        .unwrap_or(false)
                }) {
                    Some(offset) => self
                        .conns
                        .get_mut(*offset)
                        .expect("Logic error in conns find predicate")
                        .as_mut(),
                    None => None,
                }
            }
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<Option<ConnHandler>> {
        self.conns.iter_mut()
    }

    pub fn iter(&self) -> Iter<Option<ConnHandler>> {
        self.conns.iter()
    }
}
