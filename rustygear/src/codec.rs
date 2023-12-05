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
use std::fmt;
use std::io;
use std::str;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::constants::*;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum PacketMagic {
    UNKNOWN,
    REQ,
    RES,
    TEXT,
}

pub struct Packet {
    pub magic: PacketMagic,
    pub ptype: u32,
    pub psize: u32,
    pub data: Bytes,
}

impl Clone for Packet {
    fn clone(&self) -> Packet {
        Packet {
            magic: self.magic,
            ptype: self.ptype,
            psize: self.psize,
            data: self.data.clone(),
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let unimpl = format!("__UNIMPLEMENTED__({})", self.ptype);
        let ptype_str = match self.ptype {
            p @ 0..=42 => PTYPES[p as usize].name,
            _p @ ADMIN_STATUS => "ADMIN_STATUS",
            _p @ ADMIN_VERSION => "ADMIN_VERSION",
            _p @ ADMIN_UNKNOWN => "ADMIN_UNKNOWN",
            _p @ ADMIN_RESPONSE => "ADMIN_RESPONSE",
            _p @ ADMIN_WORKERS => "ADMIN_WORKERS",
            _p @ ADMIN_SHUTDOWN => "ADMIN_SHUTDOWN",
            _ => &unimpl,
        };
        write!(
            f,
            "Packet {{ magic: {:?}, ptype: {}, size: {} }}",
            match self.magic {
                PacketMagic::REQ => "REQ",
                PacketMagic::RES => "RES",
                PacketMagic::TEXT => "TEXT",
                _ => "UNKNOWN",
            },
            ptype_str,
            self.psize
        )
    }
}

impl Packet {
    pub fn admin_decode(buf: &mut BytesMut) -> Result<Option<Packet>, io::Error> {
        let newline = buf[..].iter().position(|b| *b == b'\n');
        if let Some(n) = newline {
            let line = buf.split_to(n);
            let _ = buf.split_to(1); // drop the newline itself
            let data_str = match str::from_utf8(&line[..]) {
                Ok(s) => s,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "invalid string")),
            };
            let trimmed = data_str.trim();
            info!("admin command data: {:?}", trimmed);
            let command = match data_str.trim() {
                "version" => ADMIN_VERSION,
                "status" => ADMIN_STATUS,
                "workers" => ADMIN_WORKERS,
                "prioritystatus" => ADMIN_PRIORITYSTATUS,
                "shutdown" => ADMIN_SHUTDOWN,
                _ => ADMIN_UNKNOWN,
            };
            return Ok(Some(Packet {
                magic: PacketMagic::TEXT,
                ptype: command,
                psize: 0,
                data: Bytes::new(),
            }));
        }
        Ok(None) // Wait for more data
    }

    fn into_bytes(self) -> (Bytes, Bytes) {
        let magic = match self.magic {
            PacketMagic::UNKNOWN => panic!("Unknown packet magic cannot be sent"),
            PacketMagic::REQ => REQ,
            PacketMagic::RES => RES,
            PacketMagic::TEXT => return (Bytes::from_static(b""), self.data),
        };
        let mut buf = BytesMut::with_capacity(12);
        buf.extend(magic.iter());
        buf.put_u32(self.ptype);
        buf.put_u32(self.psize);
        (buf.freeze(), self.data)
    }

    pub fn new_text_res(body: Bytes) -> Packet {
        Packet {
            magic: PacketMagic::TEXT,
            ptype: ADMIN_RESPONSE,
            psize: body.len() as u32,
            data: body,
        }
    }
}

#[derive(Debug)]
pub struct PacketCodec;

impl Decoder for PacketCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        debug!("Decoding {:?}", src);
        // Peek at first 4
        // Is this a req/res
        if src.len() < 4 {
            return Ok(None);
        }
        let mut magic_buf: [u8; 4] = [0; 4];
        magic_buf.clone_from_slice(&src[0..4]);
        let magic = match magic_buf {
            REQ => PacketMagic::REQ,
            RES => PacketMagic::RES,
            // TEXT/ADMIN protocol
            _ => PacketMagic::TEXT,
        };
        debug!("Magic is {:?}", magic);
        if magic == PacketMagic::TEXT {
            debug!("admin protocol detected");
            return Packet::admin_decode(src);
        }
        if src.len() < 12 {
            return Ok(None);
        }
        trace!("Buf is >= 12 bytes ({}) -- check header", src.len());
        let header = &src.clone()[0..12];
        // Now get the type
        let ptype = (&header[4..8]).get_u32();
        debug!("We got a {}", &PTYPES[ptype as usize].name);
        // Now the length
        let psize = (&header[8..12]).get_u32();
        debug!("Data section is {} bytes", psize);
        let packet_len = 12 + psize as usize;
        if src.len() < packet_len {
            return Ok(None);
        }
        let _ = src.split_to(12);
        Ok(Some(Packet {
            magic: magic,
            ptype: ptype,
            psize: psize,
            data: src.split_to(psize as usize).freeze(),
        }))
    }
}

impl Encoder<Packet> for PacketCodec {
    type Error = io::Error;
    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), io::Error> {
        let allbytes = item.into_bytes();
        dst.extend(allbytes.0);
        dst.extend(allbytes.1);
        Ok(())
    }
}
