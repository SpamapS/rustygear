use std::io;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;

use futures::{future, Future, BoxFuture};
use tokio_proto::streaming::{Message, Body};
use tokio_service::Service;

use bytes::{BufMut, BytesMut};

use admin;
use codec::PacketHeader;
use packet::PacketMagic;
use queues::SharedJobStorage;
use worker::SharedWorkers;
use constants::*;

pub type GearmanMessage = Message<PacketHeader, Body<BytesMut, io::Error>>;

pub struct GearmanService {
    pub conn_id: usize,
    pub queues: SharedJobStorage,
    pub workers: SharedWorkers,
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
            workers: workers.clone(),
            job_count: job_count.clone(),
        }
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
                                                   format!("format ptype = {}", header.ptype)))
                            .boxed()
                    }
                }
            }
            Message::WithBody(header, body) => {
                future::err(io::Error::new(io::ErrorKind::Other,
                                           format!("format ptype = {}", header.ptype)))
                    .boxed()
            }
        }
    }
}
