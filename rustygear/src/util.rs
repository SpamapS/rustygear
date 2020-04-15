use crate::codec::{Packet, PacketMagic};
use crate::constants::*;
use bytes::{Buf, Bytes};

pub fn bytes2bool(input: &Bytes) -> bool {
    if input.len() != 1 {
        false
    } else if input[0] == b'1' {
        true
    } else {
        false
    }
}

pub fn new_res(ptype: u32, data: Bytes) -> Packet {
    Packet {
        magic: PacketMagic::RES,
        ptype: ptype,
        psize: data.len() as u32,
        data: data,
    }
}

pub fn new_req(ptype: u32, data: Bytes) -> Packet {
    Packet {
        magic: PacketMagic::REQ,
        ptype: ptype,
        psize: data.len() as u32,
        data: data,
    }
}

pub fn next_field(buf: &mut Bytes) -> Bytes {
    match buf[..].iter().position(|b| *b == b'\0') {
        Some(null_pos) => {
            let value = buf.split_to(null_pos);
            buf.advance(1);
            value
        }
        None => {
            let buflen = buf.len();
            buf.split_to(buflen)
        }
    }
}

pub fn no_response() -> Packet {
    Packet {
        magic: PacketMagic::TEXT,
        ptype: ADMIN_RESPONSE,
        psize: 0,
        data: Bytes::new(),
    }
}
