/*
 * Copyright (c) 2015, Hewlett Packard Development Company L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
extern crate time;
extern crate uuid;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::io::Write;
use std::io::Read;
use std::net::TcpStream;
use std::sync::{Mutex, Condvar, Arc};
use packet::Packet;

use byteorder::{BigEndian, WriteBytesExt};
use byteorder::ReadBytesExt;
use uuid::Uuid;

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

#[derive(PartialEq)]
pub enum ConnectionState {
    INIT,
    CONNECTED,
}

pub struct Connection {
    pub host: String,
    pub port: u16,
    pub conn: Option<TcpStream>,
    pub connect_time: Option<time::Tm>,
    pub state: ConnectionState,
    pub state_time: Option<time::Tm>,
    echo_conditions: HashMap<Vec<u8>, Arc<(Mutex<bool>, Condvar)>>,
}

impl fmt::Display for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let connected = match self.conn {
            None => "disconnected",
            Some(_) => "connected",
        };
        let state = match self.state {
            ConnectionState::INIT => "Initializing",
            ConnectionState::CONNECTED => "Connected",
        };
        fmt.write_fmt(format_args!("{}:{} ({}/{})", self.host, self.port, connected, state))
    }
}



impl Connection {
    pub fn new(host: &str, port: u16) -> Connection {
        let mut c = Connection {
            host: host.to_string(),
            port: port,
            conn: None,
            connect_time: None,
            state: ConnectionState::INIT,
            state_time: None,
            echo_conditions: HashMap::new(),
        };
        c.change_state(ConnectionState::INIT);
        return c
    }

    pub fn new_incoming(host: &str, port: u16, conn: TcpStream) -> Connection {
        Connection {
            host: host.to_string(),
            port: port,
            conn: Some(conn),
            connect_time: Some(time::now_utc()),
            state: ConnectionState::CONNECTED,
            state_time: Some(time::now_utc()),
            echo_conditions: HashMap::new(),
        }
    }


    pub fn change_state(&mut self, state: ConnectionState) {
        self.state_time = Some(time::now_utc());
        self.state = state;
    }

    pub fn connect(&mut self) -> io::Result<()> {
        match self.conn {
            None => {
                debug!("about to try connecting to {}:{}", self.host, self.port);
                self.conn = Some(try!(TcpStream::connect((&self.host as &str, self.port))));
                self.connect_time = Some(time::now_utc());
            },
            Some(_) => { info!("Trying to connect to already connected {}", self); },
        }
        debug!("connected to {}:{}!", self.host, self.port);
        return Ok(());
    }

    pub fn disconnect(&mut self) {
        match self.conn {
            Some(_) => self.conn = None,
            None => {
                warn!("Disconnect called on already disconnected connection {}", &self);
            },
        }
    }

    pub fn reconnect(&mut self) -> io::Result<()> {
        self.disconnect();
        try!(self.connect());
        return Ok(());
    }

    pub fn send_packet(&mut self, packet: Packet) -> io::Result<()> {
        match self.conn {
            Some(ref mut c) => {
                match packet.code {
                    PacketCode::REQ => { try!(c.write(&REQ)); }
                    PacketCode::RES => { try!(c.write(&RES)); }
                }
                try!(c.write_u32::<BigEndian>(packet.ptype));
                try!(c.write_u32::<BigEndian>(packet.data.len() as u32));
                try!(c.write_all(&packet.data));
                Ok(())
            },
            None => panic!("Attempted to send packet on disconnected socket")
        }
    }

    fn code_from_vec(code: Vec<u8>) -> Result<PacketCode, &'static str> {
        if code[..] == REQ {
            return Ok(PacketCode::REQ);
        } else if code[..] == RES {
            return Ok(PacketCode::RES);
        }
        return Err("Invalid packet code");
    }

    pub fn read_packet(&mut self) -> io::Result<Packet> {
        match self.conn {
            Some(ref mut c) => {
                let mut code: Vec<u8> = Vec::new();
                {
                    let mut codestream = c.take(4);
                    try!(codestream.read_to_end(&mut code));
                }
                let code: PacketCode = Connection::code_from_vec(code).unwrap();
                let ptype   = try!(c.read_u32::<BigEndian>());
                let datalen = try!(c.read_u32::<BigEndian>());
                let mut datastream = c.take(datalen as u64);
                let mut buf: Vec<u8> = Vec::new();
                try!(datastream.read_to_end(&mut buf));
                Ok(Packet::new(code, ptype, buf.into_boxed_slice()))
            },
            None => panic!("Attempted to read packet on disconnected socket")
        }
    }

    pub fn echo(&mut self, mut data: Vec<u8>, timeout: u32) -> Vec<u8> {
        if data.len() == 0 {
            data.extend(Uuid::new_v4().to_simple_string().bytes());
        }
        let lock_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let &(ref lock, ref cond) = &*lock_cond;
        let handled_echo = lock.lock().unwrap();
        self.send_echo_req(data.clone());
        self.echo_conditions.insert(data.clone(), lock_cond.clone());
        if !*cond.wait_timeout_ms(handled_echo, timeout * 1000).unwrap().0 {
            panic!("Timed out waiting for response from echo.");
        };
        self.echo_conditions.remove(&data);
        data
    }

    pub fn send_echo_req(&mut self, mut data: Vec<u8>) -> io::Result<()> {
        let p = Packet::new(PacketCode::REQ, ECHO_REQ, data.into_boxed_slice());
        try!(self.send_packet(p));
        Ok(())
    }

    pub fn handle_echo_res(&mut self, mut data: Vec<u8>) {
        if let Some(mutexlock) = self.echo_conditions.get_mut(&data) {
            let lock = &mutexlock.0;
            let cond = &mutexlock.1;
            let mut handled_echo = lock.lock().unwrap();
            *handled_echo = true;

            cond.notify_one();
        }
    }
}

