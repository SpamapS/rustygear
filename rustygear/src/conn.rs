use std::{fmt::Debug, slice::{Iter, IterMut}, io};

use bytes::Bytes;
use hashring::HashRing;
use tokio::{sync::mpsc::Sender, runtime};

use crate::{codec::Packet, clientdata::ClientData, constants::*, util::{new_req, no_response, next_field, bytes2bool}, client::{JobStatus, WorkUpdate, WorkerJob}};

/// Each individual connection has one of these for handling packets
#[derive(Clone, Debug)]
pub struct ConnHandler {
    client_id: Option<Bytes>,
    sink_tx: Sender<Packet>,
    client_data: ClientData,
}

impl ConnHandler {
    pub fn new (
        client_id: &Option<Bytes>,
        sink_tx: Sender<Packet>,
        client_data: ClientData,
    ) -> ConnHandler {
        ConnHandler {
            client_id: client_id.clone(),
            sink_tx: sink_tx,
            client_data: client_data,
        }
    }
    
    pub async fn send_packet(&self, packet: Packet) -> Result<(), io::Error> {
        if let Err(e) = self.sink_tx.send(packet).await {
            error!("Receiver dropped");
            return Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)));
        }
        Ok(())
    }


    pub fn call(&mut self, req: Packet) -> Result<Packet, io::Error> {
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
        let tx = self.client_data.echo_tx();
        let data = req.data.clone();
        runtime::Handle::current().spawn(async move { tx.send(data).await });
        Ok(no_response())
    }

    fn handle_job_created(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        info!("Job Created: {:?}", req);
        let tx = self.client_data.job_created_tx();
        let handle = req.data.clone();
        runtime::Handle::current().spawn(async move { tx.send(handle).await });
        Ok(no_response())
    }

    fn handle_error(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut data = req.data.clone();
        let code = next_field(&mut data);
        let text = next_field(&mut data);
        let tx = self.client_data.error_tx();
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
        let tx = self.client_data.status_res_tx();
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
        if let Some(tx) = self.client_data.get_sender_by_handle(&handle) {
            runtime::Handle::current().spawn(async move { tx.send(work_update).await });
        } else {
            error!("Received {:?} for unknown job: {:?}", req, handle);
        };
        Ok(no_response())
    }

    fn handle_job_assign(&mut self, req: &Packet) -> Result<Packet, io::Error> {
        let mut data = req.data.clone();
        let handle = next_field(&mut data);
        let function = next_field(&mut data);
        let payload = next_field(&mut data);
        let job = WorkerJob {
            handle,
            function,
            payload,
            sink_tx: self.sink_tx.clone(),
        };
        let tx = self.client_data.worker_job_tx();
        runtime::Handle::current().spawn(async move { tx.send(job).await });
        Ok(no_response())
    }
}

pub struct Connections {
    conns: Vec<Option<ConnHandler>>,
    ring: HashRing<usize>,
}

impl Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ring = format!("HashRing(len={})", self.ring.len());
        f.debug_struct("Connections").field("conns", &self.conns).field("ring", &ring).finish()
    }
}

/* This pairs a vector of ConnHandler's with a ring to select them by hash. This is used in the high-level client to implement predictable job sumission distribution. */
impl Connections {
    pub fn new() -> Connections {
        Connections {
            conns: Vec::new(),
            ring: HashRing::new(),
        }
    }

    pub fn insert(&mut self, offset: usize, handler: ConnHandler) {
        if offset > self.conns.len() {
            (0..offset).for_each(|_| self.conns.push(None))
        }
        self.conns.insert(offset, Some(handler));
        self.ring.add(offset);
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

    pub fn get_hashed_conn(&mut self, hashable: &Vec<u8>) -> Option<&mut ConnHandler> {
        match self.ring.get(hashable) {
            None => None,
            Some(ring_index) => match self.conns.get_mut(*ring_index) {
                None => None,
                Some(c) => match c {
                    None => None,
                    Some(c) => Some(c),
                },
            },
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<Option<ConnHandler>> {
        self.conns.iter_mut()
    }

    pub fn iter(&self) -> Iter<Option<ConnHandler>> {
        self.conns.iter()
    }
}
