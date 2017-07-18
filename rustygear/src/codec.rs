use std::fmt;
use std::io;
use std::str;

use bytes::{Bytes, BytesMut};
use bytes::{IntoBuf, Buf, BufMut, BigEndian};
use tokio_io::codec::{Encoder, Decoder};

use constants::*;

pub struct PacketType {
    pub name: &'static str,
    pub ptype: u32,
    pub nargs: i8,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum PacketMagic {
    UNKNOWN,
    REQ,
    RES,
    TEXT,
}

pub static PTYPES: [PacketType; 43] = [
    PacketType {
        name: "__UNUSED__",
        ptype: 0,
        nargs: -1,
    },
    PacketType {
        name: "CAN_DO",
        ptype: 1,
        nargs: 0,
    },
    PacketType {
        name: "CANT_DO",
        ptype: 2,
        nargs: 0,
    },
    PacketType {
        name: "RESET_ABILITIES",
        ptype: 3,
        nargs: -1,
    },
    PacketType {
        name: "PRE_SLEEP",
        ptype: 4,
        nargs: -1,
    },
    PacketType {
        name: "__UNUSED__",
        ptype: 5,
        nargs: -1,
    },
    PacketType {
        name: "NOOP",
        ptype: 6,
        nargs: -1,
    },
    PacketType {
        name: "SUBMIT_JOB",
        ptype: 7,
        nargs: 2,
    },
    PacketType {
        name: "JOB_CREATED",
        ptype: 8,
        nargs: 0,
    },
    PacketType {
        name: "GRAB_JOB",
        ptype: 9,
        nargs: -1,
    },
    PacketType {
        name: "NO_JOB",
        ptype: 10,
        nargs: -1,
    },
    PacketType {
        name: "JOB_ASSIGN",
        ptype: 11,
        nargs: 2,
    },
    PacketType {
        name: "WORK_STATUS",
        ptype: 12,
        nargs: 2,
    },
    PacketType {
        name: "WORK_COMPLETE",
        ptype: 13,
        nargs: 1,
    },
    PacketType {
        name: "WORK_FAIL",
        ptype: 14,
        nargs: 0,
    },
    PacketType {
        name: "GET_STATUS",
        ptype: 15,
        nargs: 0,
    },
    PacketType {
        name: "ECHO_REQ",
        ptype: 16,
        nargs: 0,
    },
    PacketType {
        name: "ECHO_RES",
        ptype: 17,
        nargs: 1,
    },
    PacketType {
        name: "SUBMIT_JOB_BG",
        ptype: 18,
        nargs: 2,
    },
    PacketType {
        name: "ERROR",
        ptype: 19,
        nargs: 1,
    },
    PacketType {
        name: "STATUS_RES",
        ptype: 20,
        nargs: 4,
    },
    PacketType {
        name: "SUBMIT_JOB_HIGH",
        ptype: 21,
        nargs: 2,
    },
    PacketType {
        name: "SET_CLIENT_ID",
        ptype: 22,
        nargs: 0,
    },
    PacketType {
        name: "CAN_DO_TIMEOUT",
        ptype: 23,
        nargs: 1,
    },
    PacketType {
        name: "ALL_YOURS",
        ptype: 24,
        nargs: -1,
    },
    PacketType {
        name: "WORK_EXCEPTION",
        ptype: 25,
        nargs: 1,
    },
    PacketType {
        name: "OPTION_REQ",
        ptype: 26,
        nargs: 0,
    },
    PacketType {
        name: "OPTION_RES",
        ptype: 27,
        nargs: 0,
    },
    PacketType {
        name: "WORK_DATA",
        ptype: 28,
        nargs: 1,
    },
    PacketType {
        name: "WORK_WARNING",
        ptype: 29,
        nargs: 1,
    },
    PacketType {
        name: "GRAB_JOB_UNIQ",
        ptype: 30,
        nargs: -1,
    },
    PacketType {
        name: "JOB_ASSIGN_UNIQ",
        ptype: 31,
        nargs: 3,
    },
    PacketType {
        name: "SUBMIT_JOB_HIGH_BG",
        ptype: 32,
        nargs: 2,
    },
    PacketType {
        name: "SUBMIT_JOB_LOW",
        ptype: 33,
        nargs: 2,
    },
    PacketType {
        name: "SUBMIT_JOB_LOW_BG",
        ptype: 34,
        nargs: 2,
    },
    PacketType {
        name: "SUBMIT_JOB_SCHED",
        ptype: 35,
        nargs: 7,
    },
    PacketType {
        name: "SUBMIT_JOB_EPOCH",
        ptype: 36,
        nargs: 3,
    },
    PacketType {
        name: "SUBMIT_REDUCE_JOB",
        ptype: 37,
        nargs: 3,
    },
    PacketType {
        name: "SUBMIT_REDUCE_JOB_BACKGROUND",
        ptype: 38,
        nargs: 3,
    },
    PacketType {
        name: "GRAB_JOB_ALL",
        ptype: 39,
        nargs: -1,
    },
    PacketType {
        name: "JOB_ASSIGN_ALL",
        ptype: 40,
        nargs: 4,
    },
    PacketType {
        name: "GET_STATUS_UNIQUE",
        ptype: 41,
        nargs: 0,
    },
    PacketType {
        name: "STATUS_RES_UNIQUE",
        ptype: 42,
        nargs: 5,
    },
];

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
            p @ 0...42 => PTYPES[p as usize].name,
            _p @ ADMIN_STATUS => "ADMIN_STATUS",
            _p @ ADMIN_VERSION => "ADMIN_VERSION",
            _p @ ADMIN_UNKNOWN => "ADMIN_UNKNOWN",
            _p @ ADMIN_RESPONSE => "ADMIN_RESPONSE",
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

#[derive(Debug)]
pub struct PacketCodec;

impl Packet {
    pub fn admin_decode(buf: &mut BytesMut) -> Result<Option<Packet>, io::Error> {
        let newline = buf[..].iter().position(|b| *b == b'\n');
        if let Some(n) = newline {
            let line = buf.split_to(n);
            buf.split_to(1); // drop the newline itself
            let data_str = match str::from_utf8(&line[..]) {
                Ok(s) => s,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "invalid string")),
            };
            let trimmed = data_str.trim();
            info!("admin command data: {:?}", trimmed);
            let command = match data_str.trim() {
                "version" => ADMIN_VERSION,
                "status" => ADMIN_STATUS,
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

    pub fn decode(buf: &mut BytesMut) -> Result<Option<Packet>, io::Error> {
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
            return Packet::admin_decode(buf);
        }
        if buf.len() < 12 {
            return Ok(None);
        }
        trace!("Buf is >= 12 bytes ({})", buf.len());
        //buf.split_to(4);
        // Now get the type
        let ptype = Bytes::from(&buf[4..8]).into_buf().get_u32::<BigEndian>();
        debug!("We got a {}", &PTYPES[ptype as usize].name);
        // Now the length
        let psize = Bytes::from(&buf[8..12]).into_buf().get_u32::<BigEndian>();
        debug!("Data section is {} bytes", psize);
        let packet_len = 12 + psize as usize;
        if buf.len() < packet_len {
            return Ok(None);
        }
        // Discard header bytes now
        buf.split_to(12);
        Ok(Some(Packet {
            magic: magic,
            ptype: ptype,
            psize: psize,
            data: buf.split_to(psize as usize).freeze(),
        }))
    }

    pub fn into_bytes(self) -> (Bytes, Bytes) {
        let magic = match self.magic {
            PacketMagic::UNKNOWN => panic!("Unknown packet magic cannot be sent"),
            PacketMagic::REQ => REQ,
            PacketMagic::RES => RES,
            PacketMagic::TEXT => return (Bytes::from_static(b""), self.data),
        };
        let mut buf = BytesMut::with_capacity(12);
        buf.extend(magic.iter());
        buf.put_u32::<BigEndian>(self.ptype);
        buf.put_u32::<BigEndian>(self.psize);
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

impl Decoder for PacketCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        Packet::decode(buf)
    }
}

impl Encoder for PacketCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> Result<(), io::Error> {
        let allbytes = msg.into_bytes();
        buf.extend(allbytes.0);
        buf.extend(allbytes.1);
        Ok(())
    }
}
