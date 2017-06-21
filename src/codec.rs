use std::cmp::min;
use std::io;
use std::str;

use bytes::{Bytes, BytesMut};
use bytes::{IntoBuf, Buf, BufMut, BigEndian};
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::streaming::pipeline::Frame;

use constants::*;
use packet::{PacketMagic, PTYPES};

#[derive(Debug)]
pub struct PacketHeader {
    pub magic: PacketMagic,
    pub ptype: u32,
    pub psize: u32,
}

pub struct PacketCodec {
    pub data_todo: Option<usize>,
}

type PacketItem = Frame<PacketHeader, BytesMut, io::Error>;

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
                message: PacketHeader {
                    magic: PacketMagic::TEXT,
                    ptype: command,
                    psize: 0,
                },
                body: false,
            }));
        }
        Ok(None) // Wait for more data
    }

    pub fn decode(buf: &mut BytesMut) -> Result<Option<PacketItem>, io::Error> {
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
        Ok(Some(Frame::Message {
            message: PacketHeader {
                magic: magic,
                ptype: ptype,
                psize: psize,
            },
            body: true, // TODO: false for 0 psize?
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
                match PacketHeader::decode(buf)? {
                    Some(Frame::Message { message, body }) => {
                        self.data_todo = Some(message.psize as usize);
                        Ok(Some(Frame::Message {
                            message: message,
                            body: body,
                        }))
                    }
                    Some(_) => panic!("Expecting Frame::Message, got something else"),
                    None => Ok(None),
                }
            }
            Some(0) => {
                self.data_todo = None;
                Ok(Some(Frame::Body { chunk: None }))
            }
            Some(data_todo) => {
                let chunk_size = min(buf.len(), data_todo);
                self.data_todo = Some(data_todo - chunk_size);
                Ok(Some(Frame::Body { chunk: Some(buf.split_to(chunk_size)) }))
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
            Frame::Message { message, body } => {
                if body {
                    debug!("body follows")
                }
                buf.extend(message.to_bytes())
            }
            Frame::Body { chunk } => {
                match chunk {
                    Some(chunk) => buf.extend_from_slice(&chunk[..]),
                    None => {}
                }
            }
            Frame::Error { error } => {
                error!("Sending error frame. {}", error);
                buf.extend("ERR UNKNOWN_COMMAND Unknown+server+command".bytes())
            }
        }
        Ok(())
    }
}
