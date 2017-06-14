use std::io;

use futures;
use futures::{future, Future, BoxFuture};
use tokio_proto::streaming::{Message, Body};
use tokio_service::Service;

use bytes::{BufMut, BytesMut};

use codec::PacketHeader;
use packet::PacketMagic;
use constants::*;

pub struct GearmanService;

impl Service for GearmanService {
    type Request = Message<PacketHeader, Body<BytesMut, io::Error>>;
    type Response = Message<PacketHeader, Body<BytesMut, io::Error>>;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Got a req {:?}", req);
        //Box::new(future::done(Ok(req)))
        match req {
            Message::WithoutBody(header) => {
                match header.ptype {
                    ADMIN_VERSION => {
                        let resp_str = "some-rustygear-version\n";
                        let mut resp_body = BytesMut::with_capacity(resp_str.len());
                        resp_body.put(&resp_str[..]);
                        let resp_body = Body::from(resp_body);
                        let resp = Message::WithBody(PacketHeader {
                                                         magic: PacketMagic::TEXT,
                                                         ptype: header.ptype,
                                                         psize: resp_str.len() as u32,
                                                     },
                                                     resp_body);
                        debug!("Returning ok({:?})", resp);
                        future::ok(resp).boxed()
                    }
                    _ => {
                        future::err(io::Error::new(io::ErrorKind::Other,
                                                   format!("format ptype = {}", header.ptype)))
                            .boxed()
                    }
                }
            }
            Message::WithBody(header, body) => {
                match header.ptype {
                    ADMIN_VERSION => {
                        let resp_str = "some-rustygear-version\n";
                        let mut resp_body = BytesMut::with_capacity(resp_str.len());
                        resp_body.put(&resp_str[..]);
                        let resp_body = Body::from(resp_body);
                        let resp = Message::WithBody(PacketHeader {
                                                         magic: PacketMagic::TEXT,
                                                         ptype: header.ptype,
                                                         psize: resp_str.len() as u32,
                                                     },
                                                     resp_body);
                        future::ok(resp).boxed()
                    }
                    _ => {
                        future::err(io::Error::new(io::ErrorKind::Other,
                                                   format!("format ptype = {}", header.ptype)))
                            .boxed()
                    }
                }
            }
        }
    }
}
