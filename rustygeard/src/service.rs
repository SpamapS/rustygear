use std::collections::HashMap;
use std::io;
use std::ops::Drop;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::RefCell;

use futures::{future, Future, BoxFuture, Sink};
use futures::sync::mpsc::Sender;
use tokio_core::reactor::Remote;
use tokio_service::Service;

use bytes::{BufMut, Bytes, BytesMut};

use rustygear::codec::Packet;
use rustygear::constants::*;
use rustygear::job::Job;
use rustygear::packet::PacketMagic;

use admin;
use queues::{HandleJobStorage, JobQueuePriority, SharedJobStorage};
use worker::{SharedWorkers, Worker, Wake};

pub fn new_res(ptype: u32, data: Bytes) -> Packet {
    Packet {
        magic: PacketMagic::RES,
        ptype: ptype,
        psize: data.len() as u32,
        data: data,
    }
}

fn new_noop() -> Packet {
    new_res(NOOP, Bytes::new())
}

type JobWaiters = Arc<Mutex<HashMap<Bytes, Vec<usize>>>>;
type SendersByConnId = Arc<Mutex<HashMap<usize, Sender<Packet>>>>;

pub struct GearmanService {
    pub conn_id: usize,
    pub queues: SharedJobStorage,
    pub workers: SharedWorkers,
    pub worker: Arc<Mutex<Worker>>,
    pub job_count: Arc<AtomicUsize>,
    senders_by_conn_id: SendersByConnId,
    job_waiters: JobWaiters,
    remote: Remote,
    client_id: RefCell<Bytes>,
}

pub fn next_field(buf: &mut Bytes) -> Result<Bytes, io::Error> {
    match buf[..].iter().position(|b| *b == b'\0') {
        Some(null_pos) => {
            let value = buf.split_to(null_pos);
            buf.split_to(1);
            Ok(value)
        }
        None => {
            let buflen = buf.len();
            Ok(buf.split_to(buflen))
        }
    }
}

impl Drop for GearmanService {
    fn drop(&mut self) {
        trace!("Dropping conn_id = {}", self.conn_id);
        self.workers.shutdown(self.conn_id);
        debug!("Dropped conn_id = {}", self.conn_id);
    }
}


impl GearmanService {
    /// Things that don't require a body should use this
    fn response_from_packet(&self, packet: &Packet) -> Packet {
        match packet.ptype {
            ADMIN_VERSION => {
                let resp_str = "OK some-rustygear-version\n";
                let mut resp_body = BytesMut::with_capacity(resp_str.len());
                resp_body.put(&resp_str[..]);
                Packet {
                    magic: PacketMagic::TEXT,
                    ptype: packet.ptype,
                    psize: resp_str.len() as u32,
                    data: resp_body.freeze(),
                }
            }
            ADMIN_STATUS => admin::admin_command_status(self.queues.clone(), self.workers.clone()),
            _ => {
                panic!(
                    "response_from_packet called with invalid ptype: {}",
                    packet.ptype
                )
            }
        }
    }

    fn send_to_conn_id(&self, conn_id: usize, packet: Packet) {
        let senders_by_conn_id = self.senders_by_conn_id.lock().unwrap();
        match senders_by_conn_id.get(&conn_id) {
            None => {
                panic!("No connection found for conn_id = {}", &conn_id); // XXX You can do better
            }
            Some(tx) => {
                let tx = tx.clone();
                self.remote.spawn(move |handle| {
                    handle.spawn(tx.send(packet).then(|res| {
                        match res {
                            Ok(_) => {}
                            Err(e) => error!("Send Error! {:?}", e),
                        }
                        Ok(())
                    }));
                    Ok(())
                });
            }
        }
    }

    pub fn new(
        conn_id: usize,
        queues: SharedJobStorage,
        workers: SharedWorkers,
        job_count: Arc<AtomicUsize>,
        senders_by_conn_id: SendersByConnId,
        job_waiters: JobWaiters,
        remote: Remote,
    ) -> GearmanService {
        GearmanService {
            conn_id: conn_id,
            queues: queues,
            worker: Arc::new(Mutex::new((Worker::new()))),
            workers: workers,
            job_count: job_count,
            senders_by_conn_id: senders_by_conn_id,
            job_waiters: job_waiters,
            remote: remote,
            client_id: RefCell::new(Bytes::new()),
        }
    }

    fn no_response() -> Packet {
        Packet {
            magic: PacketMagic::TEXT,
            ptype: ADMIN_RESPONSE,
            psize: 0,
            data: Bytes::new(),
        }
    }

    fn handle_can_do(&self, packet: &Packet) -> BoxFuture<Packet, io::Error> {
        let worker = self.worker.clone();
        let workers = self.workers.clone();
        let conn_id = self.conn_id;
        debug!("CAN_DO fname = {:?}", packet.data);
        let mut worker = worker.lock().unwrap();
        worker.can_do(packet.data.clone());
        workers.clone().wakeup(&mut worker, conn_id);
        future::finished(Self::no_response()).boxed()
    }

    fn handle_cant_do(&self, packet: &Packet) -> BoxFuture<Packet, io::Error> {
        let worker = self.worker.clone();
        debug!("CANT_DO fname = {:?}", packet.data);
        let mut worker = worker.lock().unwrap();
        worker.cant_do(&packet.data);
        future::finished(Self::no_response()).boxed()
    }

    fn handle_grab_job_all(&self) -> BoxFuture<Packet, io::Error> {
        let mut queues = self.queues.clone();
        let worker = self.worker.clone();
        let mut worker = worker.lock().unwrap();
        let ref mut worker = worker;
        match queues.get_job(worker) {
            Some(ref j) => {
                let mut data = BytesMut::with_capacity(
                    4 + j.handle.len() + j.fname.len() + j.unique.len() + j.data.len(),
                );
                data.extend(&j.handle);
                data.put_u8(b'\0');
                data.extend(&j.fname);
                data.put_u8(b'\0');
                data.extend(&j.unique);
                data.put_u8(b'\0');
                // reducer not implemented
                data.put_u8(b'\0');
                data.extend(&j.data);
                return future::finished(new_res(JOB_ASSIGN_ALL, data.freeze())).boxed();
            }
            None => {}
        };
        future::finished(new_res(NO_JOB, Bytes::new())).boxed()
    }

    fn handle_grab_job_uniq(&self) -> BoxFuture<Packet, io::Error> {
        let mut queues = self.queues.clone();
        let worker = self.worker.clone();
        let mut worker = worker.lock().unwrap();
        let ref mut worker = worker;
        match queues.get_job(worker) {
            Some(ref j) => {
                let mut data = BytesMut::with_capacity(
                    3 + j.handle.len() + j.fname.len() + j.unique.len() + j.data.len(),
                );
                data.extend(&j.handle);
                data.put_u8(b'\0');
                data.extend(&j.fname);
                data.put_u8(b'\0');
                data.extend(&j.unique);
                data.put_u8(b'\0');
                data.extend(&j.data);
                return future::finished(new_res(JOB_ASSIGN_UNIQ, data.freeze())).boxed();
            }
            None => {}
        };
        future::finished(new_res(NO_JOB, Bytes::new())).boxed()
    }

    fn handle_grab_job(&self) -> BoxFuture<Packet, io::Error> {
        let mut queues = self.queues.clone();
        let worker = self.worker.clone();
        let mut worker = worker.lock().unwrap();
        let ref mut worker = worker;
        match queues.get_job(worker) {
            Some(ref j) => {
                let mut data =
                    BytesMut::with_capacity(2 + j.handle.len() + j.fname.len() + j.data.len());
                data.extend(&j.handle);
                data.put_u8(b'\0');
                data.extend(&j.fname);
                data.put_u8(b'\0');
                data.extend(&j.data);
                return future::finished(new_res(JOB_ASSIGN, data.freeze())).boxed();
            }
            None => {}
        };
        future::finished(new_res(NO_JOB, Bytes::new())).boxed()
    }

    fn handle_pre_sleep(&self) -> BoxFuture<Packet, io::Error> {
        let worker = self.worker.clone();
        let ref mut w = worker.lock().unwrap();
        self.workers.clone().sleep(w, self.conn_id);
        future::finished(Self::no_response()).boxed()
    }

    fn handle_submit_job(
        &self,
        priority: JobQueuePriority,
        wait: bool,
        packet: Packet,
    ) -> BoxFuture<Packet, io::Error> {
        let mut queues = self.queues.clone();
        let conn_id = match wait {
            true => Some(self.conn_id),
            false => None,
        };
        let mut workers = self.workers.clone();
        let job_count = self.job_count.clone();
        let remote = self.remote.clone();
        let senders_by_conn_id = self.senders_by_conn_id.clone();
        let mut fields = packet.data.clone();
        trace!("fields = {:?}", fields);
        let fname = next_field(&mut fields).unwrap();
        let unique = next_field(&mut fields).unwrap();
        trace!("  --> fname = {:?} unique = {:?}", fname, unique);
        let mut add = false;
        let handle = match queues.coalesce_unique(&unique, conn_id) {
            Some(handle) => handle,
            None => {
                {
                    for wake in workers.queue_wake(&fname) {
                        let senders_by_conn_id = senders_by_conn_id.lock().unwrap();
                        match senders_by_conn_id.get(&wake) {
                            None => {
                                debug!("No connection found to wake up for conn_id = {}", wake);
                            }
                            Some(tx) => {
                                let tx = tx.clone();
                                remote.spawn(move |handle| {
                                    handle.spawn(tx.send(new_noop()).then(|res| {
                                        match res {
                                            Ok(_) => {}
                                            Err(e) => error!("Send Error! {:?}", e),
                                        }
                                        Ok(())
                                    }));
                                    Ok(())
                                });
                            }
                        }
                    }
                }
                // H:091234567890
                let mut handle = BytesMut::with_capacity(12);
                let job_num = job_count.fetch_add(1, Ordering::Relaxed);
                debug!("job_num = {}", job_num);
                handle.extend(format!("H:{:010}", job_num).as_bytes());
                add = true;
                handle.freeze()
            }
        };
        if add {
            let job = Arc::new(Job::new(fname, unique, fields, handle.clone()));
            info!("Created job {:?}", job);
            queues.add_job(job.clone(), priority, conn_id);
            trace!(
                "job weak = {} strong = {}",
                Arc::weak_count(&job),
                Arc::strong_count(&job)
            );
        }
        // If we don't store any senders, the sender will be dropped and the rx
        // stream should end thus releasing the waiter immediately.
        let psize = handle.len() as u32;
        if wait {
            // Fetch our sender
            let mut job_waiters = self.job_waiters.lock().unwrap();
            let mut waiters = job_waiters.entry(handle.clone()).or_insert(Vec::new());
            waiters.push(self.conn_id);
        }
        future::finished(Packet {
            magic: PacketMagic::RES,
            ptype: JOB_CREATED,
            psize: psize,
            data: handle,
        }).boxed()
    }

    fn handle_work_complete(&self, packet: &Packet) -> BoxFuture<Packet, io::Error> {
        // Search for handle
        let mut fields = packet.data.clone();
        let handle = next_field(&mut fields).unwrap();
        let worker = self.worker.clone();
        let queues = self.queues.clone();
        info!("Job is complete {:?}", handle);
        let mut worker = worker.lock().unwrap();
        match worker.get_assigned_job(&handle) {
            Some(ref mut j) => {
                let mut queues = queues.lock().unwrap();
                queues.remove_job(&j.unique);
            }
            None => {
                error!("WORK_COMPLETE received but no active jobs");
            }
        }
        worker.unassign_job(&handle);
        let mut job_waiters = self.job_waiters.lock().unwrap();
        // If there are waiters, send the packet to them
        if let Some(waiters) = job_waiters.remove(&handle) {
            for conn_id in waiters.iter() {
                self.send_to_conn_id(*conn_id, packet.clone());
            }
        }
        future::finished(Self::no_response()).boxed()
    }

    fn handle_work_update(&self, packet: &Packet) -> BoxFuture<Packet, io::Error> {
        let mut fields = packet.data.clone();
        let handle = next_field(&mut fields).unwrap();
        let job_waiters = self.job_waiters.lock().unwrap();
        if let Some(waiters) = job_waiters.get(&handle) {
            for conn_id in waiters.iter() {
                self.send_to_conn_id(*conn_id, packet.clone());
            }
        }
        future::finished(Self::no_response()).boxed()
    }

    fn handle_set_client_id(&self, packet: &Packet) -> BoxFuture<Packet, io::Error> {
        let d = packet.data.clone();
        let mut client_id = self.client_id.borrow_mut();
        *client_id = d;
        future::finished(Self::no_response()).boxed()
    }
}

impl Service for GearmanService {
    type Request = Packet;
    type Response = Packet;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("[{:?}] Got a req {:?}", self.client_id.borrow(), req);
        match req.ptype {
            ADMIN_VERSION | ADMIN_STATUS => future::ok(self.response_from_packet(&req)).boxed(),
            SUBMIT_JOB => self.handle_submit_job(PRIORITY_NORMAL, true, req),
            SUBMIT_JOB_HIGH => self.handle_submit_job(PRIORITY_HIGH, true, req),
            SUBMIT_JOB_LOW => self.handle_submit_job(PRIORITY_LOW, true, req),
            SUBMIT_JOB_BG => self.handle_submit_job(PRIORITY_NORMAL, false, req),
            SUBMIT_JOB_HIGH_BG => self.handle_submit_job(PRIORITY_HIGH, false, req),
            SUBMIT_JOB_LOW_BG => self.handle_submit_job(PRIORITY_LOW, false, req),
            PRE_SLEEP => self.handle_pre_sleep(),
            CAN_DO => self.handle_can_do(&req),
            CANT_DO => self.handle_cant_do(&req),
            GRAB_JOB => self.handle_grab_job(),
            GRAB_JOB_UNIQ => self.handle_grab_job_uniq(),
            GRAB_JOB_ALL => self.handle_grab_job_all(),
            WORK_COMPLETE => self.handle_work_complete(&req),
            WORK_STATUS | WORK_DATA | WORK_WARNING => self.handle_work_update(&req),
            SET_CLIENT_ID => self.handle_set_client_id(&req),
            ECHO_REQ => future::finished(new_res(ECHO_RES, req.data)).boxed(),
            _ => {
                error!("Unimplemented: {:?} processing packet", req);
                future::err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Invalid packet type {}", req.ptype),
                )).boxed()
            }
        }
    }
}
