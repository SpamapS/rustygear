extern crate rustygear;
extern crate bytes;

use std::str;

use bytes::Bytes;

use rustygear::constants::*;
use rustygear::service::{next_field, new_res};

#[test]
fn test_next_binary_data() {
    let mut data = Bytes::from(&b"handle\0data\0\x7f"[..]);
    let f = next_field(&mut data).unwrap();
    assert_eq!(f, &b"handle"[..]);
    let f2 = next_field(&mut data).unwrap();
    assert_eq!(f2, &b"data"[..]);
}

#[test]
fn test_next_empty_end() {
    let mut data = Bytes::from(&"handle2\0"[..]);
    let f = next_field(&mut data).unwrap();
    assert_eq!(f, &b"handle2"[..]);
    let f2 = next_field(&mut data).unwrap();
    assert_eq!(f2, &b""[..]);
}

#[test]
fn test_packet_debug() {
    let p = new_res(NOOP, Bytes::new());
    let f = format!("->{:?}<-", p);
    assert_eq!("->Packet { magic: \"RES\", ptype: NOOP, size: 0 }<-", &f);
}

#[test]
fn test_packet_debug_unimplemented() {
    let p = new_res((PTYPES.len() + 10) as u32, Bytes::new());
    let f = format!("->{:?}<-", p);
    assert_eq!(
        "->Packet { magic: \"RES\", ptype: __UNIMPLEMENTED__(53), size: 0 \
                }<-",
        &f
    );
}
