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
use crate::codec::{Packet, PacketMagic};
use crate::constants::*;
use crate::error::RustygearServerError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

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

pub fn new_err(err: RustygearServerError, message: Bytes) -> Packet {
    let code = format!("{}", err as i32);
    let code = code.bytes();
    let mut data = BytesMut::with_capacity(code.len() + message.len() + 1);
    data.extend(code);
    data.put_u8(b'\0');
    data.extend(message);
    new_res(ERROR, data.freeze())
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
