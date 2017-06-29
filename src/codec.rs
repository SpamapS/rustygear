use std::cmp::min;
use std::fmt;
use std::io;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use bytes::{IntoBuf, Buf, BufMut, BigEndian};
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::streaming::multiplex::{Frame, RequestId};

use constants::*;
use packet::{PacketMagic, PTYPES};

pub struct PacketHeader {
    pub magic: PacketMagic,
    pub ptype: u32,
    pub psize: u32,
}

impl fmt::Debug for PacketHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "PacketHeader {{ magic: {:?}, ptype: {}, size: {} }}",
               match self.magic {
                   PacketMagic::REQ => "REQ",
                   PacketMagic::RES => "RES",
                   PacketMagic::TEXT => "TEXT",
                   _ => "UNKNOWN",
               },
               match self.ptype {
                   p @ 0...42 => PTYPES[p as usize].name,
                   _ => "__UNIMPLEMENTED__",
               },
               self.psize)
    }
}

pub struct PacketCodec {
    data_todo: Option<usize>,
    request_counter: Arc<AtomicUsize>,
    active_request_id: RequestId,
}

impl PacketCodec {
    pub fn new(request_counter: Arc<AtomicUsize>) -> PacketCodec {
        PacketCodec {
            data_todo: None,
            request_counter: request_counter,
            active_request_id: 0,
        }
    }
}

pub type PacketItem = Frame<PacketHeader, BytesMut, io::Error>;

impl PacketHeader {
    pub fn admin_decode(buf: &mut BytesMut) -> Result<Option<PacketItem>, io::Error> {
        let newline = buf[..].iter().position(|b| *b == b'\n');
        if let Some(n) = newline {
            let line = buf.split_to(n);
            buf.split_to(1); // drop the newline itself
            let data_str = match str::from_utf8(&line[..]) {
                Ok(s) => s,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "invalid string")),
            };
            info!("admin command data: {:?}", data_str);
            let command = match data_str.trim() {
                "version" => ADMIN_VERSION,
                "status" => ADMIN_STATUS,
                _ => ADMIN_UNKNOWN,
            };
            return Ok(Some(Frame::Message {
                id: 0,
                message: PacketHeader {
                    magic: PacketMagic::TEXT,
                    ptype: command,
                    psize: 0,
                },
                body: false,
                solo: false,
            }));
        }
        Ok(None) // Wait for more data
    }

    pub fn decode(buf: &mut BytesMut,
                  request_counter: &Arc<AtomicUsize>)
                  -> Result<Option<PacketItem>, io::Error> {
        debug!("Decoding {:?}", buf);
        // Peek at first 4
        // Is this a req/res
        if buf.len() < 4 {
            return Ok(None);
        }
        let mut magic_buf: [u8; 4] = [0; 4];
        magic_buf.clone_from_slice(&buf[0..4]);
        let magic = match magic_buf {
            REQ => PacketMagic::REQ,
            RES => PacketMagic::RES,
            // TEXT/ADMIN protocol
            _ => PacketMagic::TEXT,
        };
        debug!("Magic is {:?}", magic);
        if magic == PacketMagic::TEXT {
            debug!("admin protocol detected");
            return PacketHeader::admin_decode(buf);
        }
        if buf.len() < 12 {
            return Ok(None);
        }
        buf.split_to(4);
        // Now get the type
        let ptype = buf.split_to(4).into_buf().get_u32::<BigEndian>();
        debug!("We got a {}", &PTYPES[ptype as usize].name);
        // Now the length
        let psize = buf.split_to(4).into_buf().get_u32::<BigEndian>();
        debug!("Data section is {} bytes", psize);
        let solo = match ptype {
            SUBMIT_JOB |
            SUBMIT_JOB_LOW |
            SUBMIT_JOB_HIGH |
            SUBMIT_REDUCE_JOB => false,
            GRAB_JOB | GRAB_JOB_UNIQ | GRAB_JOB_ALL => false,
            GET_STATUS |
            GET_STATUS_UNIQUE => false,
            _ => false,  // TODO: when we figure out how to Service::call on these, set to true
        };
        let req_id: RequestId = if solo {
            0
        } else {
            request_counter.fetch_add(1, Ordering::Relaxed) as RequestId
        };
        Ok(Some(Frame::Message {
            id: req_id,
            message: PacketHeader {
                magic: magic,
                ptype: ptype,
                psize: psize,
            },
            body: true, // TODO: false for 0 psize?
            solo: solo,
        }))
    }

    pub fn to_bytes(&self) -> Bytes {
        let magic = match self.magic {
            PacketMagic::UNKNOWN => panic!("Unknown packet magic cannot be sent"),
            PacketMagic::REQ => REQ,
            PacketMagic::RES => RES,
            PacketMagic::TEXT => {
                return Bytes::from_static(b"");
            }
        };
        let mut buf = BytesMut::with_capacity(12);
        buf.extend(magic.iter());
        buf.put_u32::<BigEndian>(self.ptype);
        buf.put_u32::<BigEndian>(self.psize);
        buf.freeze()
    }

    pub fn new_text_res(body: &BytesMut) -> PacketHeader {
        PacketHeader {
            magic: PacketMagic::TEXT,
            ptype: ADMIN_RESPONSE,
            psize: body.len() as u32,
        }
    }
}

impl Decoder for PacketCodec {
    type Item = Frame<PacketHeader, BytesMut, io::Error>;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        match self.data_todo {
            None => {
                match PacketHeader::decode(buf, &self.request_counter)? {
                    Some(Frame::Message { id, message, body, solo }) => {
                        self.active_request_id = id;
                        self.data_todo = Some(message.psize as usize);
                        Ok(Some(Frame::Message {
                            id: id,
                            message: message,
                            body: body,
                            solo: solo,
                        }))
                    }
                    Some(_) => panic!("Expecting Frame::Message, got something else"),
                    None => Ok(None),
                }
            }
            Some(0) => {
                self.data_todo = None;
                Ok(Some(Frame::Body {
                    id: self.active_request_id,
                    chunk: None,
                }))
            }
            Some(data_todo) => {
                let chunk_size = min(buf.len(), data_todo);
                self.data_todo = Some(data_todo - chunk_size);
                Ok(Some(Frame::Body {
                    id: self.active_request_id,
                    chunk: Some(buf.split_to(chunk_size)),
                }))
            }
        }
    }
}

impl Encoder for PacketCodec {
    type Item = Frame<PacketHeader, BytesMut, io::Error>;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        debug!("Encoding {:?}", msg);
        match msg {
            Frame::Message { id, message, body, solo } => {
                trace!("Encoding header for id={} solo={}", id, solo);
                if body {
                    debug!("body follows")
                }
                buf.extend(message.to_bytes())
            }
            Frame::Body { id, chunk } => {
                trace!("Encoding body for id={}", id);
                match chunk {
                    Some(chunk) => buf.extend_from_slice(&chunk[..]),
                    None => {}
                }
            }
            Frame::Error { id, error } => {
                error!("Sending error frame for id={}. {}", id, error);
                buf.extend("ERR UNKNOWN_COMMAND Unknown+server+command".bytes())
            }
        }
        Ok(())
    }
}
