extern crate time;
extern crate byteorder;

mod constants;

use std::option::Option;
use std::cmp::Ordering;
use std::io::Write;
use std::net::TcpStream;

use byteorder::{BigEndian, WriteBytesExt};

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
    let data = [0x00u8];
    let p = Packet::new(0x00000000, 0x00000000, &data);
    assert!(p.code == 0x00000000);
    assert!(p.ptype == 0x00000000);
    assert!(p.data.len() == 1);
    match p.conn {
        None => {},
        Some(_) => panic!("Should not have a connection assigned.")
    }
}

#[test]
#[should_panic]
fn send_packet() {
    let mut conn = Connection::new("localost", 4730);
    let data = [0x00u8];
    let p = Packet::new(0x00000000, 0x00000000, &data);
    conn.sendPacket(&p)
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

struct Packet<'a> {
    code: u32,
    ptype: u32,
    data: &'a [u8],
    conn: Option<&'a Connection>,
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

    fn sendPacket(&mut self, packet: &Packet) {
        match self.conn {
            Some(ref mut c) => {
                c.write_u32::<BigEndian>(packet.code).unwrap();
                c.write_u32::<BigEndian>(packet.ptype);
                c.write_u32::<BigEndian>(packet.data.len() as u32).unwrap();
                c.write_all(packet.data).unwrap()
            },
            None => panic!("Attempted to send packet on disconnected socket")
        }
    }
}

impl<'a> PartialEq for Packet<'a> {
    fn eq(&self, other: &Packet) -> bool {
        self.code == other.code &&
        self.ptype == other.ptype &&
        self.data == other.data
    }
}


impl<'a> Packet<'a> {
    fn new(code: u32, ptype: u32, data: &'a [u8]) -> Packet {
        Packet {
            code: code,
            ptype: ptype,
            data: data,
            conn: None,
        }
    }
}
