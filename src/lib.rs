#![feature(mpsc_select)]
#![feature(scoped)]
extern crate time;
extern crate byteorder;
extern crate uuid;

use std::option::Option;
use std::cmp::Ordering;
use std::io::Write;
use std::io::Read;
use std::io;
use std::net::TcpStream;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::thread;
use std::thread::{JoinHandle, sleep_ms, Thread};
use std::sync::{Mutex, RwLock, Condvar, Arc};
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender, RecvError};

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
    match conn.sendPacket(p) {
        Ok(_) => {},
        Err(_) => panic!("Error in sendPacket."),
    }
}

#[test]
#[should_panic]
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

#[test]
fn bcs_constructor() {
    let bcs = BaseClientServer::new("clientid".to_string().into_bytes());
}

#[test]
fn bcs_run() {
    println!("Begin");
    let mut bcs = BaseClientServer::run("clientid".to_string().into_bytes());
    println!("Started");
    let mut bcs0 = bcs.clone();
    let mut bcs1 = bcs.clone();
    println!("Waiting 100ms");
    thread::sleep_ms(100);
    (*bcs).write().unwrap().running = false;
    bcs.read().unwrap().stop();
}


#[derive(PartialEq)]
enum ConnectionState {
    INIT,
}

struct Connection {
    host: String,
    port: u16,
    conn: Option<TcpStream>,
    connect_time: Option<time::Tm>,
    state: ConnectionState,
    state_time: Option<time::Tm>,
    echo_conditions: HashMap<Vec<u8>, Arc<(Mutex<bool>, Condvar)>>,
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
            connect_time: None,
            state: ConnectionState::INIT,
            state_time: None,
            echo_conditions: HashMap::new(),
        };
        c.change_state(ConnectionState::INIT);
        return c
    }

    fn change_state(&mut self, state: ConnectionState) {
        self.state_time = Some(time::now_utc());
        self.state = state;
    }

    fn connect(&mut self) -> io::Result<()> {
        match self.conn {
            None => {
                self.conn = Some(try!(TcpStream::connect((&self.host as &str, self.port))));
                self.connect_time = Some(time::now_utc());
            },
            Some(_) => { /* log debug "already connected" */ },
        }
        return Ok(());
    }

    fn disconnect(&mut self) {
        match self.conn {
            Some(_) => self.conn = None,
            None => { /* log debug "already disconnected" */ },
        }
    }

    fn reconnect(&mut self) -> io::Result<()> {
        self.disconnect();
        try!(self.connect());
        return Ok(());
    }

    fn sendPacket(&mut self, packet: Packet) -> io::Result<()> {
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

    fn _code_from_vec(code: Vec<u8>) -> Result<PacketCode, &'static str> {
        if code[..] == REQ {
            return Ok(PacketCode::REQ);
        } else if code[..] == RES {
            return Ok(PacketCode::RES);
        }
        return Err("Invalid packet code");
    }

    fn readPacket(&mut self) -> io::Result<Packet> {
        match self.conn {
            Some(ref mut c) => {
                let mut code: Vec<u8> = Vec::new();
                {
                    let mut codestream = c.take(4);
                    try!(codestream.read_to_end(&mut code));
                }
                let code: PacketCode = Connection::_code_from_vec(code).unwrap();
                let ptype   = try!(c.read_u32::<BigEndian>());
                let datalen = try!(c.read_u32::<BigEndian>());
                let mut datastream = c.take(datalen as u64);
                let mut buf: Vec<u8> = Vec::new();
                try!(datastream.read_to_end(&mut buf));
                Ok(
                    Packet {
                        code: code,
                        ptype: ptype,
                        data: buf.into_boxed_slice(),
                    })
            },
            None => panic!("Attempted to read packet on disconnected socket")
        }
    }

    fn echo(&mut self, mut data: Vec<u8>, timeout: u32) -> Vec<u8> {
        if data.len() == 0 {
            data.extend(Uuid::new_v4().to_simple_string().bytes());
        }
        let lock_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let &(ref lock, ref cond) = &*lock_cond;
        let mut handled_echo = lock.lock().unwrap();
        self.sendEchoReq(data.clone());
        self.echo_conditions.insert(data.clone(), lock_cond.clone());
        if !*cond.wait_timeout_ms(handled_echo, timeout * 1000).unwrap().0 {
            panic!("Timed out waiting for response from echo.");
        };
        self.echo_conditions.remove(&data);
        data
    }

    fn sendEchoReq(&mut self, mut data: Vec<u8>) -> io::Result<()> {
        let p = Packet::new(PacketCode::REQ, ECHO_REQ, data.into_boxed_slice());
        try!(self.sendPacket(p));
        Ok(())
    }

    fn handleEchoRes(&mut self, mut data: Vec<u8>) {
        if let Some(mutexlock) = self.echo_conditions.get_mut(&data) {
            let lock = &mutexlock.0;
            let cond = &mutexlock.1;
            let mut handled_echo = lock.lock().unwrap();
            *handled_echo = true;

            cond.notify_one();
        }
    }
}

struct BaseClientServer {
    client_id: Vec<u8>,
    running: bool,
    active_connections: HashMap<(String, u16), Arc<Mutex<Box<Connection>>>>,
    hosts: Vec<String>,
    stop_cm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    stop_pm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    host_tx: Option<Arc<Mutex<Box<Sender<Arc<Mutex<Box<(String, u16)>>>>>>>>,
}

impl BaseClientServer {

    fn new(client_id: Vec<u8>) -> BaseClientServer {
        BaseClientServer {
            client_id: client_id,
            running: true,
            active_connections: HashMap::new(),
            hosts: Vec::new(),
            stop_cm_tx: None,
            stop_pm_tx: None,
            host_tx: None,
        }
    }

    fn run(client_id: Vec<u8>) -> Arc<RwLock<Box<BaseClientServer>>> {
        let mut bcs = Box::new(BaseClientServer::new(client_id));
        let (host_tx, host_rx): (Sender<Arc<Mutex<Box<(String, u16)>>>>, Receiver<Arc<Mutex<Box<(String, u16)>>>>) = channel();
        let (conn_tx, conn_rx) = channel();
        let (stop_cm_tx, stop_cm_rx) = channel();
        let (stop_pm_tx, stop_pm_rx) = channel();
        bcs.stop_cm_tx = Some(Arc::new(Mutex::new(Box::new(stop_cm_tx.clone()))));
        bcs.stop_pm_tx = Some(Arc::new(Mutex::new(Box::new(stop_pm_tx.clone()))));
        bcs.host_tx = Some(Arc::new(Mutex::new(Box::new(host_tx.clone()))));
        let mut bcs: Arc<RwLock<Box<BaseClientServer>>> = Arc::new(RwLock::new(bcs));
        let bcs0 = bcs.clone();
        let bcs1 = bcs.clone();
        let conn_tx = conn_tx.clone();
        let host_tx = host_tx.clone();
        thread::spawn(move || {
            BaseClientServer::connectionManager(bcs0, stop_cm_rx, host_rx, conn_tx);
        });
        thread::spawn(move || {
            BaseClientServer::pollingManager(bcs1, stop_pm_rx, host_tx, conn_rx);
        });
        bcs
    }

    fn stop(&self) {
        {
            let stop_cm_tx: Option<Arc<Mutex<Box<Sender<()>>>>> = self.stop_cm_tx.clone();
            match stop_cm_tx {
                Some(stop_cm_tx) => { stop_cm_tx.lock().unwrap().send(()); },
                None => { panic!("No stop cm channel"); },
            }
        }
        {
            let stop_pm_tx: Option<Arc<Mutex<Box<Sender<()>>>>> = self.stop_cm_tx.clone();
            match stop_pm_tx {
                Some(stop_pm_tx) => { stop_pm_tx.lock().unwrap().send(()); },
                None => { panic!("No stop pm channel"); },
            }
        }
    }

    #[allow(unstable)]
    fn connectionManager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                         stop_rx: Receiver<()>,
                         host_rx: Receiver<Arc<Mutex<Box<(String, u16)>>>>,
                         conn_tx: Sender<Arc<Mutex<Box<Connection>>>>) {
        /* Run as a dedicated thread to manage the list of active/inactive connections */
        let (new_tx, new_rx) = channel();
        let mut reconn_thread;
        let mut conn_thread;
        let mut host: String;
        let mut port: u16;
        while bcs.read().unwrap().running {
            let container: Result<Arc<Mutex<Box<(String, u16)>>>, RecvError>;
            /* either a new conn is requested, or a new conn is connected */
            select!(
                container = host_rx.recv() => {
                    let container = match container {
                        Ok(container) => container,
                        Err(_) => panic!("Could not receive in connection manager"),
                    };
                    let container = container.clone();
                    {
                        let ref mut hostport = **container.lock().unwrap();
                        host = hostport.0.clone();
                        port = hostport.1;
                    }
                    let mut insert = false;
                    let conn_tx = conn_tx.clone();
                    let new_tx = new_tx.clone();
                    {
                        match bcs.read().unwrap().active_connections.get(&(host.clone(), port)) {
                            Some(conn) => {
                                let conn = conn.clone();
                                reconn_thread = thread::scoped(move || {
                                    let mut real_conn = conn.lock().unwrap();
                                    while true {
                                        match real_conn.reconnect() {
                                            Ok(_) => break,
                                            Err(_) => {
                                                /* log warning about reconnecting */
                                                thread::sleep_ms(1000);
                                            },
                                        }
                                    };
                                    conn_tx.send(conn.clone());
                                });
                            },
                            None => {
                                conn_thread = thread::scoped(move || {
                                    let conn: Arc<Mutex<Box<Connection>>> = Arc::new(Mutex::new(Box::new(Connection::new(&host, port)))).clone();
                                    {
                                        let mut real_conn = conn.lock().unwrap();
                                        while true {
                                            match real_conn.connect() {
                                                Ok(_) => break,
                                                Err(_) => {
                                                    /* log warning */
                                                    thread::sleep_ms(1000);
                                                }
                                            }
                                        };
                                    }
                                    new_tx.send(conn.clone());
                                });
                            }
                        };
                    } // unlock bcs
                },
                conn = new_rx.recv() => {
                    let conn = match conn {
                        Ok(conn) => conn,
                        Err(_) => panic!("Could not receive in connection manager"),
                    };
                    {
                        let ref real_conn = **conn.lock().unwrap();
                        let host = real_conn.host.clone();
                        {
                            let ac = &mut bcs.write().unwrap().active_connections;
                            ac.insert((host, real_conn.port), conn.clone());
                        }
                    }
                    conn_tx.send(conn.clone());
                },
                _ = stop_rx.recv() => { break }
            )
        }
        for (_, conn) in bcs.read().unwrap().active_connections.iter() {
            let mut conn = conn.lock().unwrap();
            conn.disconnect();
        }
        // scoped threads should join here
    }

    fn addServer(&self, host: String, port: u16, host_tx: Sender<Arc<Mutex<Box<(String, u16)>>>>) {
        host_tx.send(Arc::new(Mutex::new(Box::new((host, port)))));
    }

    fn pollingManager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                      stop_rx: Receiver<()>,
                      host_tx: Sender<Arc<Mutex<Box<(String, u16)>>>>,
                      conn_rx: Receiver<Arc<Mutex<Box<Connection>>>>) {
        let mut threads: Vec<JoinHandle<()>> = Vec::new();
        while bcs.read().unwrap().running {
            let conn: Result<Arc<Mutex<Box<Connection>>>, RecvError>;
            select!(
                conn = conn_rx.recv() => {
                    let conn = conn.unwrap();
                    let host_tx = host_tx.clone();
                    // poll this conn
                    threads.push(thread::spawn(move || {
                        while true {
                            let mut conn = conn.lock().unwrap();
                            match conn.readPacket() {
                                Ok(p) => { /* handle packet */ },
                                Err(e) => {
                                    /* Log failure */
                                    host_tx.send(Arc::new(Mutex::new(Box::new((conn.host.clone(), conn.port)))));
                                    break;
                                }
                            }
                        }
                    }));
                },
                _ = stop_rx.recv() => { break }
            );
        };
        // scoped threads all joined here
        for mut thread in threads {
            thread.join();
        }
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
