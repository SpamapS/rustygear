use std::io;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;

use futures::{future, Future, BoxFuture, Stream};
use tokio_proto::streaming::{Message, Body};
use tokio_service::Service;

use bytes::{BufMut, BytesMut};

use admin;
use codec::PacketHeader;
use packet::PacketMagic;
use queues::SharedJobStorage;
use worker::{SharedWorkers, Worker, Wake};
use constants::*;

pub type GearmanBody = Body<BytesMut, io::Error>;
pub type GearmanMessage = Message<PacketHeader, GearmanBody>;

pub struct GearmanService {
    pub conn_id: usize,
    pub queues: SharedJobStorage,
    pub workers: SharedWorkers,
    pub worker: Arc<Mutex<Worker>>,
    pub job_count: Arc<Mutex<AtomicUsize>>,
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
               job_count: Arc<Mutex<AtomicUsize>>)
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
        body.for_each(move |chunk| {
                let fname = chunk.freeze();
                debug!("CAN_DO fname = {:?}", fname);
                let mut worker = worker.lock().unwrap();
                worker.can_do(fname);
                workers.clone().wakeup(&mut worker, conn_id);
                Ok(())
            })
            .and_then(|_| {
                future::finished(Message::WithBody(PacketHeader::noop(),
                                                   Body::from(BytesMut::new())))
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
                    /*
                    SUBMIT_JOB => self.handle_submit_job(PRIORITY_NORMAL, false),
                    SUBMIT_JOB_HIGH => self.handle_submit_job(PRIORITY_HIGH, false),
                    SUBMIT_JOB_LOW => self.handle_submit_job(PRIORITY_LOW, false),
                    SUBMIT_JOB_BG => self.handle_submit_job(PRIORITY_NORMAL, true),
                    SUBMIT_JOB_HIGH_BG => self.handle_submit_job(PRIORITY_HIGH, true),
                    SUBMIT_JOB_LOW_BG => self.handle_submit_job(PRIORITY_LOW, true),
                    PRE_SLEEP => self.handle_pre_sleep(),
                    */
                    CAN_DO => self.handle_can_do(body),/*
                    CANT_DO => self.handle_cant_do(),
                    GRAB_JOB => self.handle_grab_job(),
                    GRAB_JOB_UNIQ => self.handle_grab_job_uniq(),
                    GRAB_JOB_ALL => self.handle_grab_job_all(),
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
