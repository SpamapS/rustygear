use std::io;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{future, Future, BoxFuture, Stream};
use tokio_proto::streaming::{Message, Body};
use tokio_service::Service;

use bytes::{BufMut, Bytes, BytesMut};

use admin;
use codec::PacketHeader;
use job::Job;
use packet::PacketMagic;
use queues::{HandleJobStorage, JobQueuePriority, SharedJobStorage};
use worker::{SharedWorkers, Worker, Wake};
use constants::*;

pub type GearmanBody = Body<BytesMut, io::Error>;
pub type GearmanMessage = Message<PacketHeader, GearmanBody>;

fn new_res(ptype: u32, data: BytesMut) -> GearmanMessage {
    Message::WithBody(PacketHeader {
                          magic: PacketMagic::RES,
                          ptype: ptype,
                          psize: data.len() as u32,
                      },
                      Body::from(data))
}

pub struct GearmanService {
    pub conn_id: usize,
    pub queues: SharedJobStorage,
    pub workers: SharedWorkers,
    pub worker: Arc<Mutex<Worker>>,
    pub job_count: Arc<AtomicUsize>,
}

fn next_field(buf: &mut BytesMut) -> Result<Bytes, io::Error> {
    match buf[..].iter().position(|b| *b == b'\0') {
        Some(null_pos) => {
            let value = buf.split_to(null_pos);
            buf.split_to(1);
            Ok(value.freeze())
        }
        None => Err(io::Error::new(io::ErrorKind::Other, "Can't find null")),
    }
}

impl GearmanService {
    /// Things that don't require a body should use this
    fn response_from_header(&self,
                            header: &PacketHeader)
                            -> Message<PacketHeader, Body<BytesMut, io::Error>> {
        match header.ptype {
            ADMIN_VERSION => {
                let resp_str = "OK some-rustygear-version\n";
                let mut resp_body = BytesMut::with_capacity(resp_str.len());
                resp_body.put(&resp_str[..]);
                let resp_body = Body::from(resp_body);
                Message::WithBody(PacketHeader {
                                      magic: PacketMagic::TEXT,
                                      ptype: header.ptype,
                                      psize: resp_str.len() as u32,
                                  },
                                  resp_body)
            }
            ADMIN_STATUS => admin::admin_command_status(self.queues.clone(), self.workers.clone()),
            _ => {
                panic!("response_from_header called with invalid ptype: {}",
                       header.ptype)
            }
        }
    }

    pub fn new(conn_id: usize,
               queues: SharedJobStorage,
               workers: SharedWorkers,
               job_count: Arc<AtomicUsize>)
               -> GearmanService {
        GearmanService {
            conn_id: conn_id,
            queues: queues.clone(),
            worker: Arc::new(Mutex::new((Worker::new()))),
            workers: workers.clone(),
            job_count: job_count.clone(),
        }
    }

    fn handle_can_do(&self, body: GearmanBody) -> BoxFuture<GearmanMessage, io::Error> {
        let worker = self.worker.clone();
        let workers = self.workers.clone();
        let conn_id = self.conn_id;
        trace!("handle_can_do");
        body.concat2()
            .and_then(move |fname| {
                let fname = fname.freeze();
                debug!("CAN_DO fname = {:?}", fname);
                let mut worker = worker.lock().unwrap();
                worker.can_do(fname);
                workers.clone().wakeup(&mut worker, conn_id);
                future::finished(Message::WithBody(PacketHeader::noop(),
                                                   Body::from(BytesMut::new())))
            })
            .boxed()
    }

    fn handle_cant_do(&self, body: GearmanBody) -> BoxFuture<GearmanMessage, io::Error> {
        let worker = self.worker.clone();
        body.concat2()
            .and_then(move |fname| {
                let fname = fname.freeze();
                debug!("CANT_DO fname = {:?}", fname);
                let mut worker = worker.lock().unwrap();
                worker.cant_do(&fname);
                future::finished(Message::WithBody(PacketHeader::noop(),
                                                   Body::from(BytesMut::new())))
            })
            .boxed()
    }

    fn handle_grab_job_all(&self, body: GearmanBody) -> BoxFuture<GearmanMessage, io::Error> {
        let mut queues = self.queues.clone();
        let worker = self.worker.clone();
        trace!("handle_grab_job_all");
        body.concat2()
            .and_then(move |_| {
                let mut worker = worker.lock().unwrap();
                let ref mut worker = worker;
                if queues.get_job(worker) {
                    match worker.job() {
                        Some(ref j) => {
                            let mut data = BytesMut::with_capacity(4 + j.handle.len() +
                                                                   j.fname.len() +
                                                                   j.unique.len() +
                                                                   j.data.len());
                            data.extend(&j.handle);
                            data.put_slice(b"\0");
                            data.extend(&j.fname);
                            data.put_slice(b"\0");
                            data.extend(&j.unique);
                            data.put_slice(b"\0");
                            // reducer not implemented
                            data.put_slice(b"\0");
                            data.extend(&j.data);
                            return future::finished(new_res(JOB_ASSIGN_ALL, data)).boxed();
                        }
                        None => {}
                    }
                };
                future::finished(new_res(NO_JOB, BytesMut::new())).boxed()
            })
            .boxed()
    }

    fn handle_pre_sleep(&self) -> BoxFuture<GearmanMessage, io::Error> {
        let worker = self.worker.clone();
        let ref mut w = worker.lock().unwrap();
        self.workers.clone().sleep(w, self.conn_id);
        future::finished(Message::WithBody(PacketHeader::noop(), Body::from(BytesMut::new())))
            .boxed()
    }

    fn handle_submit_job(&self,
                         priority: JobQueuePriority,
                         wait: bool,
                         body: GearmanBody)
                         -> BoxFuture<GearmanMessage, io::Error> {
        let mut queues = self.queues.clone();
        let conn_id = match wait {
            true => Some(self.conn_id),
            false => None,
        };
        let mut workers = self.workers.clone();
        let job_count = self.job_count.clone();

        body.concat2()
            .and_then(move |mut fields| {
                let fname = next_field(&mut fields).unwrap();
                let unique = next_field(&mut fields).unwrap();
                let data = fields.freeze();
                let mut add = false;
                let handle = match queues.coalesce_unique(&unique, conn_id) {
                    Some(handle) => handle,
                    None => {
                        workers.queue_wake(&fname);
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
                    let job = Arc::new(Job::new(fname, unique, data, handle.clone()));
                    info!("Created job {:?}", job);
                    queues.add_job(job.clone(), priority, conn_id);
                }
                future::finished(new_res(JOB_CREATED, BytesMut::from(handle)))
            })
            .boxed()
    }
}

impl Service for GearmanService {
    type Request = GearmanMessage;
    type Response = GearmanMessage;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Got a req {:?}", req);
        match req {
            Message::WithoutBody(header) => {
                match header.ptype {
                    ADMIN_VERSION | ADMIN_STATUS => {
                        future::ok(self.response_from_header(&header)).boxed()
                    }

                    _ => {
                        future::err(io::Error::new(io::ErrorKind::Other,
                                                   format!("Bodyless packet type = {}",
                                                           header.ptype)))
                            .boxed()
                    }
                }
            }
            Message::WithBody(header, body) => {
                match header.ptype {
                    SUBMIT_JOB => self.handle_submit_job(PRIORITY_NORMAL, false, body),
                    SUBMIT_JOB_HIGH => self.handle_submit_job(PRIORITY_HIGH, false, body),
                    SUBMIT_JOB_LOW => self.handle_submit_job(PRIORITY_LOW, false, body),
                    SUBMIT_JOB_BG => self.handle_submit_job(PRIORITY_NORMAL, true, body),
                    SUBMIT_JOB_HIGH_BG => self.handle_submit_job(PRIORITY_HIGH, true, body),
                    SUBMIT_JOB_LOW_BG => self.handle_submit_job(PRIORITY_LOW, true, body),
                    PRE_SLEEP => self.handle_pre_sleep(),
                    CAN_DO => self.handle_can_do(body),
                    CANT_DO => self.handle_cant_do(body),/*
                    GRAB_JOB => self.handle_grab_job(),
                    GRAB_JOB_UNIQ => self.handle_grab_job_uniq(),*/
                    GRAB_JOB_ALL => self.handle_grab_job_all(body),/*
                    WORK_COMPLETE => self.handle_work_complete(),
                    WORK_STATUS | WORK_DATA | WORK_WARNING => self.handle_work_update(),
                    ECHO_REQ => self.handle_echo_req(&req),*/
                    _ => {
                        error!("Unimplemented: {:?} processing packet", header);
                        future::err(io::Error::new(io::ErrorKind::Other,
                                                   format!("Invalid packet type {}", header.ptype)))
                            .boxed()
                    }
                }
            }
        }
    }
}
