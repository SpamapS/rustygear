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
#![feature(mpsc_select)]
#![feature(ip_addr)]
extern crate byteorder;
extern crate hash_ring;
extern crate uuid;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub mod packet;
use packet::Packet;
pub mod connection;
use connection::Connection;
pub mod constants;
use constants::*;
pub mod base;
use base::BaseClientServer;
pub mod util;
use util::LoggingRwLock as RwLock;

fn run_test_server() -> (Arc<RwLock<Box<BaseClientServer>>>, Vec<JoinHandle<()>>, u16) {
    match env_logger::init() {
        Ok(_) => { },
        Err(e) => {
            info!("{}", e);
        }
    }
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0 , 1), 0));
    let (bcs, threads) = BaseClientServer::run("runserver".to_string().into_bytes(), Some(vec![addr]));
    println!("Waiting 100ms for server");
    let mut port: u16;
    thread::sleep_ms(100);
    {
        let binds;
        {
            let bcs2 = bcs.clone();
            let bcs2 = bcs2.read(line!()).unwrap();
            binds = bcs2.binds.clone();
        }
        println!("Checking binds for actual port");
        match binds {
            None => unreachable!(),
            Some(binds) => {
                loop {
                    port = binds.lock().unwrap()[0].port();
                    if port != 0 {
                        println!("First bind port is {}", port);
                        break;
                    }
                    // condvar or channel is probably a better idea
                    thread::sleep_ms(100);
                }
            }
        }
    }
    (bcs, threads, port)
}

#[test]
fn send_packet() {
    let (bcs, _, port) = run_test_server();
    let mut conn = Connection::new("127.0.0.1", port);
    conn.connect().unwrap();
    let data: Box<[u8]> = Box::new([0x00u8]);
    let p = Packet::new(PacketCode::REQ, ECHO_REQ, data);
    match conn.send_packet(p) {
        Ok(_) => {},
        Err(_) => panic!("Error in send_packet."),
    }
    let bcs = bcs.read(line!()).unwrap();
    bcs.stop();
}

#[test]
#[should_panic]  // Still no echo implementation in server
fn echo() {
    let (bcs, _, port) = run_test_server();
    let mut test: Vec<u8> = Vec::new();
    test.extend("abc123".to_string().bytes());
    let mut conn = Connection::new("127.0.0.1", port);
    conn.connect().unwrap();
    let result = String::from_utf8(conn.echo(test, 0)).unwrap();
    assert_eq!("abc123", result);
    let test2: Vec<u8> = Vec::new();
    let output = String::from_utf8(conn.echo(test2, 0)).unwrap();
    println!("UUID = {}", output);
    let bcs = bcs.read(line!()).unwrap();
    bcs.stop();
}

#[test]
fn bcs_constructor() {
    let bcs = BaseClientServer::new("clientid".to_string().into_bytes());
}

#[test]
fn run_server_client_can_reach() {
    let (bcs, _, port) = run_test_server();
    // Is it working?
    let (bcs_client, _) = BaseClientServer::run("servers_client".to_string().into_bytes(), None);
    {
        let bcs_client = bcs_client.write(line!()).unwrap();
        bcs_client.add_server("127.0.0.1".to_string(), port);
    }
    {
        //let bcs_client = bcs_client.read(line!()).unwrap();
        BaseClientServer::wait_for_connection(bcs_client.clone(), Some(5000)).unwrap();
    }
    {
        let bcs_client = bcs_client.write(line!()).unwrap();
        bcs_client.stop();
    }
    // Now shut server down
    let bcs = bcs.read(line!()).unwrap();
    println!("Stopping bcs...");
    bcs.stop();
}

#[test]
#[should_panic]
fn bcs_select_connection() {
    let (bcs, threads) = BaseClientServer::run("selconn".to_string().into_bytes(), None);
    println!("Waiting 100ms for connections");
    BaseClientServer::wait_for_connection(bcs.clone(), Some(100)).unwrap();
    let mut bcs = bcs.write(line!()).unwrap();
    bcs.select_connection("some string".to_string());
    bcs.stop();
    for thread in threads {
        thread.join();
    }
}

#[test]
fn bcs_run() {
    println!("Begin");
    let (bcs, threads) = BaseClientServer::run("clientid".to_string().into_bytes(), None);
    println!("Waiting 100ms");
    thread::sleep_ms(100);
    {
        let bcs = &bcs.read(line!()).unwrap();
        bcs.stop();
    }
    for thread in threads {
        thread.join();
    }
}
