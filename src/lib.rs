extern crate time;
extern crate byteorder;
extern crate uuid;

use std::option::Option;
use std::cmp::Ordering;
use std::io::Write;
use std::io::Read;
use std::net::TcpStream;

use byteorder::{BigEndian, WriteBytesExt};
use byteorder::ReadBytesExt;
use uuid::Uuid;

pub mod constants;
use constants::*;

#[test]
fn constructor() {
    let c = Connection::new("localhost", 4730);

    assert!(c.host == "localhost");
    assert!(c.port == 4730);
    assert!(c.state == ConnectionState::INIT);

    let now = time::now_utc();
    match c.state_time {
        Some(c_time) => {
            let result = now.cmp(&c_time);  // Could fail if no nanoseconds
            assert_eq!(Ordering::Greater, result);
        },
        None => assert!(false)
    }
}

#[test]
fn change_state() {
    let mut c = Connection::new("localhost", 4730);

    c.change_state(ConnectionState::INIT);
    assert!(c.state == ConnectionState::INIT);
}

#[test]
fn packet_constructor() {
    let data: Box<[u8]> = Box::new([0x00u8]);
    let p = Packet::new(PacketCode::REQ, ECHO_REQ, data);
    assert!(p.code == PacketCode::REQ);
    assert!(p.ptype == ECHO_REQ);
    assert!(p.data.len() == 1);
}

#[test]
#[should_panic]
fn send_packet() {
    let mut conn = Connection::new("localost", 4730);
    let data: Box<[u8]> = Box::new([0x00u8]);
    let p = Packet::new(PacketCode::REQ, ECHO_REQ, data);
    conn.sendPacket(p)
}

#[test]
fn echo() {
    let mut test: Vec<u8> = Vec::new();
    test.extend("abc123".to_string().bytes());
    let mut conn = Connection::new("localhost", 4730);
    let result = String::from_utf8(conn.echo(test, 0)).unwrap();
    assert_eq!("abc123", result);
    let mut test2: Vec<u8> = Vec::new();
    let mut output = String::from_utf8(conn.echo(test2, 0)).unwrap();
    println!("UUID = {}", output);
}


#[derive(PartialEq)]
enum ConnectionState {
    INIT,
}

struct Connection {
    host: String,
    port: u16,
    conn: Option<TcpStream>,
    connected: bool,
    connect_time: Option<time::Tm>,
    state: ConnectionState,
    state_time: Option<time::Tm>,
}

struct Packet {
    code: PacketCode,
    ptype: u32,
    data: Box<[u8]>,
}

impl Connection {
    fn new(host: &str, port: u16) -> Connection {
        let mut c = Connection {
            host: host.to_string(),
            port: port,
            conn: None,
            connected: false,
            connect_time: None,
            state: ConnectionState::INIT,
            state_time: None,
        };
        c.change_state(ConnectionState::INIT);
        return c
    }

    fn change_state(&mut self, state: ConnectionState) {
        self.state_time = Some(time::now_utc());
        self.state = state;
    }

    fn connect(&mut self) {
        match self.conn {
            None => {
                self.conn = Some(TcpStream::connect((&*self.host, self.port)).unwrap());
self.connected = true;
                self.connect_time = Some(time::now_utc());
            },
            Some(_) => { /* log debug "already connected" */ },
        }
    }

    fn disconnect(&mut self) {
        match self.conn {
            Some(_) => self.conn = None,
            None => { /* log debug "already disconnected" */ },
        }
    }

    fn reconnect(&mut self) {
        self.disconnect();
        self.connect()
    }

    fn sendPacket(&mut self, packet: Packet) {
        match self.conn {
            Some(ref mut c) => {
                match packet.code {
                    PacketCode::REQ => { c.write(&REQ).unwrap(); }
                    PacketCode::RES => { c.write(&RES).unwrap(); }
                }
                c.write_u32::<BigEndian>(packet.ptype);
                c.write_u32::<BigEndian>(packet.data.len() as u32).unwrap();
                c.write_all(&packet.data).unwrap()
            },
            None => panic!("Attempted to send packet on disconnected socket")
        }
    }

    fn _code_from_vec(code: Vec<u8>) -> Result<PacketCode, &'static str> {
        if code[..] == REQ {
            return Ok(PacketCode::REQ);
        } else if code[..] == RES {
            return Ok(PacketCode::RES);
        }
        return Err("Invalid packet code");
    }



    fn readPacket(&mut self) -> Packet {
        match self.conn {
            Some(ref mut c) => {
                let mut code: Vec<u8> = Vec::new();
                {
                    let mut codestream = c.take(4);
                    codestream.read_to_end(&mut code).unwrap();
                }
                let code: PacketCode = Connection::_code_from_vec(code).unwrap();
                let ptype   = c.read_u32::<BigEndian>().unwrap();
                let datalen = c.read_u32::<BigEndian>().unwrap();
                let mut datastream = c.take(datalen as u64);
                let mut buf: Vec<u8> = Vec::new();
                datastream.read_to_end(&mut buf).unwrap();
                return Packet {
                    code: code,
                    ptype: ptype,
                    data: buf.into_boxed_slice(),
                }
            },
            None => panic!("Attempted to read packet on disconnected socket")
        }
    }

    fn echo(&mut self, mut data: Vec<u8>, timeout: u32) -> Vec<u8> {
        if data.len() == 0 {
            data.extend(Uuid::new_v4().to_simple_string().bytes());
        }

        data
    }

    fn sendEchoReq(&mut self, mut data: Vec<u8>) {
        let p = Packet::new(PacketCode::REQ, ECHO_REQ, data.into_boxed_slice());
        self.sendPacket(p);
    }
}

impl PartialEq for Packet {
    fn eq(&self, other: &Packet) -> bool {
        self.code == other.code &&
        self.ptype == other.ptype &&
        self.data == other.data
    }
}


impl Packet {
    fn new(code: PacketCode, ptype: u32, data: Box<[u8]>) -> Packet {
        Packet {
            code: code,
            ptype: ptype,
            data: data,
        }
    }
}
