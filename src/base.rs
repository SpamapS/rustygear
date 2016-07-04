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

use std::fmt;
use std::collections::HashMap;
use std::net::{TcpListener, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Mutex, MutexGuard, Condvar, Arc};
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender, RecvError};
use std::thread;
use std::thread::{JoinHandle, sleep_ms};

use hash_ring::HashRing;

use connection::Connection;
use packet::Packet;
use util::LoggingRwLock as RwLock;

#[derive(Clone)]
#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Hash)]
pub struct NodeInfo {
    host: String,
    port: u16,
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_fmt(format_args!("{}:{}", self.host, self.port))
    }
}

pub trait HandlePacket: Send {
    fn handle_packet(&self, packet: Packet);
}

pub struct BaseClientServer {
    pub client_id: Vec<u8>,
    pub active_connections: HashMap<NodeInfo, Arc<Mutex<Box<Connection>>>>,
    pub active_connections_cond: Arc<Box<(Mutex<bool>, Condvar)>>,
    pub active_ring: HashRing<NodeInfo>,
    pub hosts: Vec<String>,
    pub stop_cm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    pub stop_pm_tx: Option<Arc<Mutex<Box<Sender<()>>>>>,
    pub host_tx: Option<Arc<Mutex<Box<Sender<Arc<Mutex<Box<(String, u16)>>>>>>>>,
    pub handler: Option<Arc<Mutex<Box<HandlePacket + Send>>>>,
    pub binds: Option<Arc<Mutex<Box<Vec<SocketAddr>>>>>,
}

impl BaseClientServer {

    pub fn new(client_id: Vec<u8>) -> BaseClientServer {
        BaseClientServer {
            client_id: client_id,
            active_connections: HashMap::new(),
            active_connections_cond: Arc::new(Box::new((Mutex::new(false), Condvar::new()))),
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
    pub fn run(client_id: Vec<u8>, binds: Option<Vec<SocketAddr>>) -> (Arc<RwLock<Box<BaseClientServer>>>, Vec<JoinHandle<()>>) {
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
        let bcs2 = bcs.clone();
        let conn_tx = conn_tx.clone();
        let host_tx = host_tx.clone();
        let mut threads = Vec::new();
        {
            match binds {
                Some(binds) => {
                    let listen_binds = bcs0.read(line!()).unwrap().binds.clone();
                    match listen_binds {
                        Some(_) => unreachable!(),
                        None => {
                            bcs0.write(line!()).unwrap().binds = Some(Arc::new(Mutex::new(Box::new(binds))))
                        },
                    }
                    threads.push(thread::spawn(move || {
                        BaseClientServer::listen_manager(bcs0, stop_cm_rx, conn_tx);
                    }));
                    threads.push(thread::spawn(move || {
                        BaseClientServer::disconnect_manager(bcs2, host_rx);
                    }));
                },
                None => {
                    threads.push(thread::spawn(move || {
                        BaseClientServer::connection_manager(bcs0, stop_cm_rx, host_rx, conn_tx);
                    }));
                }
            };
            threads.push(thread::spawn(move || {
                BaseClientServer::polling_manager(bcs1, stop_pm_rx, host_tx, conn_rx);
            }));
        }
        (bcs, threads)
    }

    pub fn stop(&self) {
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
                    debug!("received {}:{}", host, port);
                    let conn_tx = conn_tx.clone();
                    let new_tx = new_tx.clone();
                    {
                        let nodeinfo = NodeInfo { host: host.clone(), port: port };
                        debug!("searching for {}:{}", host, port);
                        let found = {
                            let bcs = bcs.read(line!()).unwrap();
                            match bcs.active_connections.get(&nodeinfo) {
                                Some(conn) => Some(conn.clone()),
                                None => None,
                            }
                        };
                        match found {
                            Some(conn) => {
                                debug!("found {}:{}", host, port);
                                // stop sending things to it
                                {
                                    bcs.write(line!()).unwrap().active_ring.remove_node(&NodeInfo{host: host.clone(), port: port});
                                }
                                let conn = conn.clone();
                                let child = thread::spawn(move || {
                                    let mut real_conn = conn.lock().unwrap();
                                    loop {
                                        match real_conn.reconnect() {
                                            Ok(_) => break,
                                            Err(err) => {
                                                info!("Connection to {} failed with ({}). Retry in 1s", *real_conn, err);
                                                thread::sleep_ms(1000);
                                            },
                                        }
                                    };
                                    conn_tx.send(conn.clone());
                                });
                                child.join();
                            },
                            None => {
                                debug!("NEW {}:{}", host, port);
                                let child = thread::spawn(move || {
                                    let conn: Arc<Mutex<Box<Connection>>> = Arc::new(Mutex::new(Box::new(Connection::new(&host, port)))).clone();
                                    {
                                        let mut real_conn = conn.lock().unwrap();
                                        loop {
                                            debug!("trying to connect {}", *real_conn);
                                            match real_conn.connect() {
                                                Ok(_) => break,
                                                Err(err) => {
                                                    info!("Connection to {} failed with ({}). Retry in 1s", *real_conn, err);
                                                    thread::sleep_ms(1000);
                                                }
                                            }
                                        };
                                    }
                                    debug!("sending {}", *conn.lock().unwrap());
                                    new_tx.send(conn.clone());
                                });
                                child.join();
                            }
                        };
                    } // unlock bcs
                },
                conn = new_rx.recv() => {
                    let conn = match conn {
                        Ok(conn) => conn,
                        Err(_) => panic!("Could not receive in connection manager"),
                    };
                    debug!("received {}", *conn.lock().unwrap());
                    {
                        let ref real_conn = **conn.lock().unwrap();
                        let host = real_conn.host.clone();
                        {
                            debug!("locking bcs");
                            let bcs = &mut bcs.write(line!()).unwrap();
                            debug!("locked bcs");
                            let host = host.clone();
                            let nodeinfo = NodeInfo{ host: host, port: real_conn.port };
                            bcs.active_connections.insert(nodeinfo.clone(), conn.clone());
                            bcs.active_ring.add_node(&nodeinfo);
                            let &(ref lock, ref cvar) = &**bcs.active_connections_cond;
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
        for (_, conn) in bcs.read(line!()).unwrap().active_connections.iter() {
            let mut conn = conn.lock().unwrap();
            conn.disconnect();
        }
    }

    /// Hosts that have been disconnected go through here for cleanup
    fn disconnect_manager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                          host_rx: Receiver<Arc<Mutex<Box<(String, u16)>>>>)
    {
        loop {
            let host = host_rx.recv().unwrap();
            let host = host.lock().unwrap();
            let nodeinfo = NodeInfo { host: host.0.clone(), port: host.1 };
            info!("{} disconnected", nodeinfo);
            let mut delete = false;
            match bcs.read(line!()).unwrap().active_connections.get(&nodeinfo) {
                Some(conn) => {
                    conn.lock().unwrap().disconnect();
                    /* TODO Handler for reassigning jobs will need to go here */
                    delete = true;
                },
                None => {
                    warn!("Disconnected host {} not found in active connections", nodeinfo);
                },
            }
            if delete {
                bcs.write(line!()).unwrap().active_connections.remove(&nodeinfo);
            }
        }
    }

    /// Listens for new connections. Server complement to connection_manager
    fn listen_manager(bcs: Arc<RwLock<Box<BaseClientServer>>>,
                      stop_rx: Receiver<()>,
                      conn_tx: Sender<Arc<Mutex<Box<Connection>>>>) {
        // This block here is intended to just copy the vector. Seems like maybe
        // this should be turned into a parameter for simplicity sake.
        let binds = bcs.read(line!()).unwrap().binds.clone();
        let binds = match binds {
            None => panic!("No binds for server!"),
            Some(binds) => binds,
        };
        let real_binds = binds.clone();
        let binds: Vec<SocketAddr>;
        {
            binds = *real_binds.lock().unwrap().clone();
        }
        let mut offset = 0;
        for bind in binds.iter() {
            info!("Binding to {}", bind);
            let bind = bind.clone();
            let conn_tx = conn_tx.clone();
            let bcs = bcs.clone();
            thread::spawn(move || {
                let listener = TcpListener::bind(bind).unwrap();
                let assignedport = listener.local_addr().unwrap().port();
                if bind.port() == 0 {
                    let bcs = bcs.clone();
                    let bcs = bcs.read(line!()).unwrap();
                    debug!("port == 0, reading assigned from socket");
                    let binds = bcs.binds.clone();
                    let binds = match binds {
                        None => panic!("Somehow got no binds despite being in binds for loop."),
                        Some(binds) => binds,
                    };
                    binds.lock().unwrap()[offset] = match bind {
                        SocketAddr::V4(bind) => {
                            SocketAddr::V4(SocketAddrV4::new(bind.ip().clone(), assignedport))
                        },
                        SocketAddr::V6(bind) => {
                            SocketAddr::V6(SocketAddrV6::new(bind.ip().clone(), assignedport, bind.flowinfo(), bind.scope_id()))
                        },
                    };
                } else {
                    debug!("port != 0 but == {}", bind.port());
                }
                info!("Bound to {}", bind);
                for stream in listener.incoming() {
                    debug!("incoming connection!");
                    let conn_tx = conn_tx.clone();
                    let bcs = bcs.clone();
                    let child = thread::spawn(move || {
                        let stream = stream.unwrap();
                        let peer_addr = stream.peer_addr().unwrap();
                        let host = format!("{}", peer_addr.ip());
                        let nodeinfo = NodeInfo{ host: host.clone(), port: peer_addr.port() };
                        let conn = Arc::new(Mutex::new(Box::new(Connection::new_incoming(&host[..], peer_addr.port(), stream))));
                        {
                            let bcs = &mut bcs.write(line!()).unwrap();
                            bcs.active_connections.insert(nodeinfo.clone(), conn.clone());
                        }
                        let bcs = bcs.read(line!()).unwrap();
                        // no need for filling active_ring it's not used in servers
                        let &(ref lock, ref cvar) = &**bcs.active_connections_cond;
                        let mut available = lock.lock().unwrap();
                        *available = true;
                        cvar.notify_all();
                        conn_tx.send(conn.clone());
                    });
                }
            });
            offset += 1;
        }
        // No joining, just accept the explosion because TcpListener has no
        // real way to be interrupted.
        info!("Waiting for stop");
        let _ = stop_rx.recv().unwrap();
        info!("Got stop");
    }

    pub fn add_server(&self, host: String, port: u16) {
        let host_tx = self.host_tx.clone();
        match host_tx {
            None => panic!("adding server to broken BaseClientServer object."),
            Some(host_tx) => {
                debug!("Sending {}:{}", host, port);
                host_tx.lock().unwrap().send(Arc::new(Mutex::new(Box::new((host, port)))));
            },
        }
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
                                    bcs.read(line!()).unwrap().handle_packet(p);
                                }
                                Err(_) => {
                                    warn!("{} failed to read packet", *conn);
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

    /// Wait for connection condition from connection_manager.
    ///
    /// We need the rwlock, and not just an unlocked structure, so that we can
    /// just peek at the structure to see the condition variable, and then let
    /// other threads write lock.
    pub fn wait_for_connection(bcs: Arc<RwLock<Box<BaseClientServer>>>, timeout: Option<u32>) -> Result<bool, &'static str> {
        loop {
            let mut active_connections_cond: Arc<Box<(Mutex<bool>, Condvar)>>;
            {
                let bcs = bcs.read(line!()).unwrap();
                active_connections_cond = bcs.active_connections_cond.clone();
            }
            let lock = &active_connections_cond.clone().0;
            let cvar = &active_connections_cond.clone().1;
            let mut available = lock.lock().unwrap();
            if *available {
                break;
            }
            // will wait until there are perceived active connections
            match timeout {
                Some(timeout) => {
                    let wait_result = cvar.wait_timeout_ms(available, timeout).unwrap();
                    if !wait_result.1 {
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
    pub fn select_connection(&mut self, key: String) -> Arc<Mutex<Box<Connection>>> {
        let nodeinfo = self.active_ring.get_node(key);
        match self.active_connections.get(nodeinfo) {
            Some(conn) => { conn.clone() },
            None => panic!("Hash ring contains non-existant node!"),
        }
    }

}
