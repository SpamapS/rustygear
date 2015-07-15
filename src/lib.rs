#![feature(mpsc_select)]
#![feature(scoped)]
#![feature(ip_addr)]
extern crate time;
extern crate byteorder;
extern crate uuid;
extern crate hash_ring;

use std::option::Option;
use std::cmp::Ordering;
use std::io::Write;
use std::io::Read;
use std::io;
use std::net::{TcpStream, TcpListener};
use std::collections::HashMap;
use std::thread;
use std::thread::{JoinHandle, sleep_ms};
use std::sync::{Mutex, RwLock, Condvar, Arc};
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender, RecvError};

use byteorder::{BigEndian, WriteBytesExt};
use byteorder::ReadBytesExt;
use uuid::Uuid;

use hash_ring::HashRing;

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
    match conn.send_packet(p) {
        Ok(_) => {},
        Err(_) => panic!("Error in send_packet."),
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
fn bcs_run_server() {
    let (mut bcs, threads) = BaseClientServer::run("runserver".to_string().into_bytes(), Some(vec!["127.0.0.1:0"]));
    println!("Waiting 100ms for server");
    thread::sleep_ms(100);
    {
        let mut bcs = bcs.read().unwrap();
        bcs.stop();
    }
    for thread in threads {
        thread.join();
    }
}


#[test]
#[should_panic]
fn bcs_select_connection() {
    let (mut bcs, threads) = BaseClientServer::run("selconn".to_string().into_bytes(), None);
    let mut bcs = bcs.write().unwrap();
    println!("Waiting 100ms for connections");
    bcs.wait_for_connection(Some(100)).unwrap();
    bcs.select_connection("some string".to_string());
    bcs.stop();
    for thread in threads {
        thread.join();
    }
}

#[test]
fn bcs_run() {
    println!("Begin");
    let (mut bcs, threads) = BaseClientServer::run("clientid".to_string().into_bytes(), None);
    println!("Started");
    let mut bcs0 = bcs.clone();
    let mut bcs1 = bcs.clone();
    println!("Waiting 100ms");
    thread::sleep_ms(100);
    {
        let bcs = &bcs.read().unwrap();
        bcs.stop();
    }
    for thread in threads {
        thread.join();
    }
}


#[derive(PartialEq)]
enum ConnectionState {
    INIT,
    CONNECTED,
}

#[derive(Clone)]
#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Hash)]
struct NodeInfo {
    host: String,
    port: u16,
}

impl ToString for NodeInfo {
    fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
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

    fn new_incoming(host: &str, port: u16, conn: TcpStream) -> Connection {
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

    fn send_packet(&mut self, packet: Packet) -> io::Result<()> {
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

    fn read_packet(&mut self) -> io::Result<Packet> {
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
                Ok(Packet::new(code, ptype, buf.into_boxed_slice()))
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
        let handled_echo = lock.lock().unwrap();
        self.send_echo_req(data.clone());
        self.echo_conditions.insert(data.clone(), lock_cond.clone());
        if !*cond.wait_timeout_ms(handled_echo, timeout * 1000).unwrap().0 {
            panic!("Timed out waiting for response from echo.");
        };
        self.echo_conditions.remove(&data);
        data
    }

    fn send_echo_req(&mut self, mut data: Vec<u8>) -> io::Result<()> {
        let p = Packet::new(PacketCode::REQ, ECHO_REQ, data.into_boxed_slice());
        try!(self.send_packet(p));
        Ok(())
    }

    fn handle_echo_res(&mut self, mut data: Vec<u8>) {
        if let Some(mutexlock) = self.echo_conditions.get_mut(&data) {
            let lock = &mutexlock.0;
            let cond = &mutexlock.1;
            let mut handled_echo = lock.lock().unwrap();
            *handled_echo = true;

            cond.notify_one();
        }
    }
}

trait HandlePacket: Send {
    fn handle_packet(&self, packet: Packet);
}

struct BaseClientServer {
    client_id: Vec<u8>,
    active_connections: HashMap<NodeInfo, Arc<Mutex<Box<Connection>>>>,
    active_connections_cond: Arc<(Mutex<bool>, Condvar)>,
    active_ring: HashRing<NodeInfo>,
    hosts: Vec<String>,
    stop_cm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    stop_pm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    host_tx: Option<Arc<Mutex<Box<Sender<Arc<Mutex<Box<(String, u16)>>>>>>>>,
    handler: Option<Arc<Mutex<Box<HandlePacket + Send>>>>,
    binds: Option<Arc<Mutex<Box<Vec<ToSocketAddrs>>>>>,
}

impl BaseClientServer {

    fn new(client_id: Vec<u8>) -> BaseClientServer {
        BaseClientServer {
            client_id: client_id,
            active_connections: HashMap::new(),
            active_connections_cond: Arc::new((Mutex::new(false), Condvar::new())),
            active_ring: HashRing::new(Vec::new(), 1),
            hosts: Vec::new(),
            stop_cm_tx: None,
            stop_pm_tx: None,
            host_tx: None,
            handler: None,
            binds: None,
        }
    }

    /// Runs the client or server threads associated with this BCS
    fn run(client_id: Vec<u8>, binds: Option<Vec<ToSocketAddrs>>) -> (Arc<RwLock<Box<BaseClientServer>>>, Vec<JoinHandle<()>>) {
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
        let mut threads = Vec::new();
        {
            match binds {
            Some(binds) => {
                *bcs0.read().unwrap().binds.lock().unwrap() = binds;
                threads.push(thread::spawn(move || {
                    BaseClientServer::listen_manager(bcs0, stop_cm_rx, host_rx, conn_tx);
                }));
            } else {
                threads.push(thread::spawn(move || {
                    BaseClientServer::connection_manager(bcs0, stop_cm_rx, host_rx, conn_tx);
                }));
            }
            threads.push(thread::spawn(move || {
                BaseClientServer::polling_manager(bcs1, stop_pm_rx, host_tx, conn_rx);
            }));
        };
        (bcs, threads)
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

    fn connection_manager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                         stop_rx: Receiver<()>,
                         host_rx: Receiver<Arc<Mutex<Box<(String, u16)>>>>,
                         conn_tx: Sender<Arc<Mutex<Box<Connection>>>>) {
        /* Run as a dedicated thread to manage the list of active/inactive connections */
        let (new_tx, new_rx) = channel();
        let mut reconn_thread;
        let mut conn_thread;
        let mut host: String;
        let mut port: u16;
        loop {
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
                    let conn_tx = conn_tx.clone();
                    let new_tx = new_tx.clone();
                    {
                        let nodeinfo = NodeInfo { host: host.clone(), port: port };
                        match bcs.read().unwrap().active_connections.get(&nodeinfo) {
                            Some(conn) => {
                                // stop sending things to it
                                bcs.write().unwrap().active_ring.remove_node(&NodeInfo{host: host.clone(), port: port});
                                let conn = conn.clone();
                                reconn_thread = thread::scoped(move || {
                                    let mut real_conn = conn.lock().unwrap();
                                    loop {
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
                                        loop {
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
                            let bcs = &mut bcs.write().unwrap();
                            let host = host.clone();
                            let nodeinfo = NodeInfo{ host: host, port: real_conn.port };
                            bcs.active_connections.insert(nodeinfo.clone(), conn.clone());
                            bcs.active_ring.add_node(&nodeinfo);
                            let &(ref lock, ref cvar) = &*bcs.active_connections_cond;
                            let mut available = lock.lock().unwrap();
                            *available = true;
                            cvar.notify_all();
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

    /// Listens for new connections. Server complement to connection_manager
    fn listen_manager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                      stop_rx: Receiver<()>,
                      host_rx: Receiver<Arc<Mutex<Box<(String, u16)>>>>,
                      conn_tx: Sender<Arc<Mutex<Box<Connection>>>>) {
        let listener_thread = thread::spawn(move || {
            let listener = TcpListener::bind("0.0.0.0:4730").unwrap();
            for stream in listener.incoming() {
                let conn_tx = conn_tx.clone();
                let bcs = bcs.clone();
                let in_thread = thread::scoped(move || {
                    let stream = stream.unwrap();
                    let peer_addr = stream.peer_addr().unwrap();
                    let host = format!("{}", peer_addr.ip());
                    let bcs = &mut bcs.write().unwrap();
                    let nodeinfo = NodeInfo{ host: host.clone(), port: peer_addr.port() };
                    let conn = Arc::new(Mutex::new(Box::new(Connection::new_incoming(&host[..], peer_addr.port(), stream))));
                    bcs.active_connections.insert(nodeinfo.clone(), conn.clone());
                    // no need for filling active_ring it's not used in servers
                    let &(ref lock, ref cvar) = &*bcs.active_connections_cond;
                    let mut available = lock.lock().unwrap();
                    *available = true;
                    cvar.notify_all();
                    conn_tx.send(conn.clone());
                });
            }
        });
        // No joining, just accept the explosion because TcpListener has no
        // real way to be interrupted.
        let _ = stop_rx.recv().unwrap();
    }

    fn add_server(&self, host: String, port: u16, host_tx: Sender<Arc<Mutex<Box<(String, u16)>>>>) {
        host_tx.send(Arc::new(Mutex::new(Box::new((host, port)))));
    }

    fn polling_manager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                      stop_rx: Receiver<()>,
                      host_tx: Sender<Arc<Mutex<Box<(String, u16)>>>>,
                      conn_rx: Receiver<Arc<Mutex<Box<Connection>>>>) {
        let mut threads: Vec<JoinHandle<()>> = Vec::new();
        let bcs = bcs.clone();
        loop {
            let conn: Result<Arc<Mutex<Box<Connection>>>, RecvError>;
            select!(
                conn = conn_rx.recv() => {
                    let conn = conn.unwrap();
                    let host_tx = host_tx.clone();
                    // poll this conn
                    let bcs = bcs.clone();
                    threads.push(thread::spawn(move || {
                        loop {
                            let mut conn = conn.lock().unwrap();
                            match conn.read_packet() {
                                Ok(p) => {
                                    bcs.read().unwrap().handle_packet(p);
                                }
                                Err(_) => {
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
        for thread in threads {
            thread.join();
        }
    }

    fn handle_packet(&self, packet: Packet) {
        match self.handler {
            None => return,
            Some(ref handler) => {
                let handler = handler.clone();
                handler.lock().unwrap().handle_packet(packet);
            }
        }
    }

    fn wait_for_connection(&self, timeout: Option<u32>) -> Result<bool, &'static str> {
        // will wait until there are perceived active connections
        let &(ref lock, ref cvar) = &*self.active_connections_cond;
        let mut available = lock.lock().unwrap();
        while !*available {
            match timeout {
                Some(timeout) => {
                    let wait_result = cvar.wait_timeout_ms(available, timeout).unwrap();
                    if wait_result.1 {
                        return Err("Timed out waiting for connections")
                    }
                    available = wait_result.0;
                },
                None => {
                    available = cvar.wait(available).unwrap();
                }
            }
        }
        Ok(true)
    }

    // mutable self because active_ring.get_node wants mutable
    fn select_connection(&mut self, key: String) -> Arc<Mutex<Box<Connection>>> {
        let nodeinfo = self.active_ring.get_node(key);
        match self.active_connections.get(nodeinfo) {
            Some(conn) => { conn.clone() },
            None => panic!("Hash ring contains non-existant node!"),
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
