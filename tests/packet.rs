extern crate rustygear;

use std::str;

use rustygear::constants::*;
use rustygear::packet::*;

#[test]
fn test_next_binary_data() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle\0data\0\x7f");
    let p = Packet::new_res(WORK_COMPLETE, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle");
    let f2 = i.next().unwrap();
    assert_eq!(f2, [b'd', b'a', b't', b'a', b'\0', b'\x7f' ]);
}

#[test]
fn test_next_empty_end() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle2\0");
    let p = Packet::new_res(WORK_COMPLETE, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle2");
    let f2 = i.next().unwrap();
    let f2 = str::from_utf8(f2).unwrap();
    assert_eq!(f2, "");
}

#[test]
fn test_next_0nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname");
    let p = Packet::new_res(CAN_DO, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
}

#[test]
fn test_next_2nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname\0unique\0data");
    let p = Packet::new_res(SUBMIT_JOB, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "unique");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "data");
}

#[test]
fn test_next_3nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle\0funcname\0unique\0data");
    let p = Packet::new_res(JOB_ASSIGN_UNIQ, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "unique");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "data");
}

#[test]
fn test_packet_debug() {
    let p = Packet::new_res(NOOP, Box::new(Vec::new()));
    let f = format!("->{:?}<-", p);
    assert_eq!("->Packet { magic: \"RES\", ptype: NOOP, size: 0, remote: None }<-", &f);
}

#[test]
fn test_packet_debug_unimplemented() {
    let p = Packet::new_res((PTYPES.len() + 10) as u32, Box::new(Vec::new()));
    let f = format!("->{:?}<-", p);
    assert_eq!("->Packet { magic: \"RES\", ptype: __UNIMPLEMENTED__, size: 0, remote: None }<-", &f);
}
