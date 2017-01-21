use std::collections::HashMap;
use std::fmt;
use std::net::{SocketAddr};
use std::ops::Deref;
use std::time::Duration;
use std::io::Write;
use std::io::ErrorKind::WouldBlock;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::TryRecvError;
use std::thread;

use mio::*;
use mio::channel::*;
use mio::tcp::*;
use bytes::buf::{Buf, ByteBuf};

use ::constants::*;
use ::packet::{Packet, EofError};
use ::queues::*;
use ::worker::*;

struct GearmanRemote {
    socket: Arc<Mutex<TcpStream>>,
    addr: SocketAddr,
    packet: Packet,
    queues: SharedJobStorage,
    worker: Arc<Mutex<Worker>>,
    workers: SharedWorkers,
    sendqueue: Vec<ByteBuf>,
    interest: Ready,
    token: usize,
}

pub struct GearmanServer {
    pub socket: TcpListener,
    remotes: HashMap<usize, GearmanRemote>,
    token_counter: usize,
    queues: SharedJobStorage,
    workers: SharedWorkers,
    job_count: Arc<AtomicUsize>,
}

const SERVER_TOKEN: Token = Token(0);
const PACKETS_TOKEN: Token = Token(1);
const POLL_TOKEN: Token = Token(2);
const SHUTDOWN_TOKEN: Token = Token(3);
const FIRST_FREE_TOKEN: usize = 4;

impl GearmanRemote {
    pub fn new(socket: TcpStream, addr: SocketAddr, queues: SharedJobStorage, token: usize, workers: SharedWorkers) -> GearmanRemote {
        GearmanRemote {
            socket: Arc::new(Mutex::new(socket)),
            addr: addr,
            packet: Packet::new(token),
            queues: queues,
            worker: Arc::new(Mutex::new(Worker::new())),
            workers: workers,
            sendqueue: Vec::with_capacity(2), // Don't need many bufs
            interest: Ready::readable(),
            token: token,
        }
    }

    pub fn queue_packet(&mut self, packet: &Packet) {
        info!("{} Queueing {:?}", self, &packet);
        self.sendqueue.push(ByteBuf::from_slice(&packet.to_byteslice()));
        self.interest.remove(Ready::readable());
        self.interest.insert(Ready::writable());
    }

    pub fn dispatch_read(&mut self, job_count: Arc<AtomicUsize>,
                         packet_sender: &Sender<Arc<Vec<Packet>>>,
                         poll_sender: &Sender<usize>,
                         shutdown_sender: &Sender<usize>) {
        debug!("{} readable", self);
        GearmanRemote::_dispatch_read(self.socket.clone(),
                                     self.worker.clone(),
                                     self.workers.clone(),
                                     self.queues.clone(),
                                     self.token,
                                     job_count.clone(),
                                     packet_sender.clone(),
                                     poll_sender.clone(),
                                     shutdown_sender.clone(),
                                     Packet::new(self.token));
    }

    fn _dispatch_read(socket: Arc<Mutex<TcpStream>>,
                      worker: Arc<Mutex<Worker>>,
                      workers: SharedWorkers,
                      queues: SharedJobStorage,
                      token: usize,
                      job_count: Arc<AtomicUsize>,
                      packet_sender: Sender<Arc<Vec<Packet>>>,
                      poll_sender: Sender<usize>,
                      shutdown_sender: Sender<usize>,
                      mut packet: Packet) {
        thread::spawn(move || {
            match packet.from_socket(socket,
                                     worker,
                                     workers,
                                     queues,
                                     token,
                                     job_count) {
                Ok(packets) => {
                    match packets {
                        None => debug!("{} Done reading", token),
                        Some(packets) => {
                            packet_sender.send(Arc::new(packets)).unwrap();
                        }
                    }
                    poll_sender.send(token).unwrap();
                },
                Err(e) => {
                    shutdown_sender.send(token).unwrap();
                }
            }
        });
    }

    pub fn write(&mut self) -> Result<(), io::Error> {
        debug!("{} writable", self);
        let mut socket = self.socket.lock().unwrap();
        while !self.sendqueue.is_empty() {
            if !self.sendqueue.first().unwrap().has_remaining() {
                self.sendqueue.pop();
                continue;
            }
            let buf = self.sendqueue.first_mut().unwrap();
            match socket.write(buf.bytes()) {
                Ok(value) => {
                    buf.advance(value);
                    continue
                },
                Err(err) => {
                    if let WouldBlock = err.kind() {
                        break
                    } else {
                        return Err(err)
                    }
                }
            };
        }
        if self.sendqueue.is_empty() {
            self.interest.remove(Ready::writable());
            self.interest.insert(Ready::readable());
        };
        Ok(())
    }

    pub fn shutdown(mut self) {
        {
            let worker = self.worker.lock().unwrap();
            match worker.job() {
                Some(ref mut j) => {
                    self.queues.clone().add_job(j.clone(), PRIORITY_NORMAL, None);
                },
                None => {},
            };
        }
        self.workers.shutdown(&self.token);
        let socket = self.socket.lock().unwrap();
        match socket.shutdown(Shutdown::Both) {
            Err(e) => warn!("{:?} fail on shutdown ({:?})", self.addr, e),
            Ok(_) => {},
        }
    }
}

impl fmt::Display for GearmanRemote {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Remote {{ addr: {:?} token: {:?} }}", self.addr, self.token)
    }
}

impl GearmanServer {
    pub fn new(server_socket: TcpListener,
               queues: SharedJobStorage,
               workers: SharedWorkers,
               job_count: Arc<AtomicUsize>) -> GearmanServer {
        GearmanServer {
            token_counter: FIRST_FREE_TOKEN,
            remotes: HashMap::new(),
            socket: server_socket,
            queues: queues,
            workers: workers,
            job_count: job_count,
        }
    }
}

const N_WORKERS: usize = 8;

impl GearmanServer {
    pub fn poll(&mut self) {
        let mut poll = Poll::new().unwrap();
        let (packet_sender, packet_receiver) = channel();
        let (poll_sender, poll_receiver) = channel();
        let (shutdown_sender, shutdown_receiver) = channel();
        poll.register(&packet_receiver,
                      PACKETS_TOKEN,
                      Ready::readable(),
                      PollOpt::edge()).unwrap();
        poll.register(&poll_receiver,
                      POLL_TOKEN,
                      Ready::readable(),
                      PollOpt::edge()).unwrap();
        poll.register(&shutdown_receiver,
                      SHUTDOWN_TOKEN,
                      Ready::readable(),
                      PollOpt::edge()).unwrap();
        poll.register(&self.socket,
                      Token(0),
                      Ready::readable(),
                      PollOpt::edge()).unwrap();
        let mut pevents = Events::with_capacity(1024);
        loop {
            poll.poll(&mut pevents, Some(Duration::from_millis(1000))).unwrap();
            for event in pevents.iter() {
                self.poll_inner(&mut poll,
                                &event,
                                &packet_sender,
                                &packet_receiver,
                                &poll_sender,
                                &poll_receiver,
                                &shutdown_sender,
                                &shutdown_receiver)
            }
        }
    }

    fn poll_inner(&mut self,
                  poll: &mut Poll,
                  event: &Event,
                  packet_sender: &Sender<Arc<Vec<Packet>>>,
                  packet_receiver: &Receiver<Arc<Vec<Packet>>>,
                  poll_sender: &Sender<usize>,
                  poll_receiver: &Receiver<usize>,
                  shutdown_sender: &Sender<usize>,
                  shutdown_receiver: &Receiver<usize>) {
        if event.kind().is_readable() {
            match event.token() {
                SERVER_TOKEN => {
                    let (remote_socket, remote_addr) = match self.socket.accept() {
                        Err(e) => {
                            error!("Accept error: {}", e);
                            return;
                        },
                        Ok((sock, addr)) => (sock, addr),
                    };

                    self.token_counter += 1;
                    let new_token = self.token_counter;

                    self.remotes.insert(new_token, GearmanRemote::new(remote_socket, remote_addr, self.queues.clone(), new_token, self.workers.clone()));

                    let socket = self.remotes[&new_token].socket.lock().unwrap();
                    {
                        let ref socket = *socket;
                        poll.register(socket,
                                      Token(new_token), Ready::readable(),
                                      PollOpt::edge() | PollOpt::oneshot()).unwrap();
                    }
                },
                PACKETS_TOKEN => {
                    debug!("Packets arriving on channel");
                    let mut send_packets = Vec::new();
                    loop {
                        match packet_receiver.try_recv() {
                            Err(TryRecvError::Empty) => break,
                            Err(e) => {
                                panic!("Packets channel is fully disconnected.");
                            }
                            Ok(packets) => {
                                // Wake ups get queued in processing
                                send_packets.extend(self.workers.do_wakes());
                                // Now tack these packets onto the end of send_packets
                                for p in packets.iter() {
                                    send_packets.push(p.clone());
                                }
                            }
                        }
                    }
                    for p in send_packets {
                        match p.remote {
                            None => warn!("Sent packet has no token! Dropping. {:?}", &p),
                            Some(token) => {
                                match self.remotes.get_mut(&token) {
                                    None => info!("No remote for sent packet, dropping. {:?}", &p),
                                    Some(remote) => {
                                        remote.queue_packet(&p);
                                        let socket = remote.socket.lock().unwrap();
                                        {
                                            let socket = socket.deref();
                                            poll.reregister(socket, Token(token), remote.interest,
                                                            PollOpt::edge() | PollOpt::oneshot()).unwrap();
                                        }
                                    },
                                }
                            },
                        }
                    }
                }
                POLL_TOKEN => {
                    debug!("Poll token arriving on channel");
                    loop {
                        match poll_receiver.try_recv() {
                            Err(TryRecvError::Empty) => break,
                            Err(e) => {
                                error!("Error receiving from channel. {}", &e);
                                return
                            },
                            Ok(token) => {
                                let mut remote = self.remotes.get_mut(&token).unwrap();
                                let socket = remote.socket.lock().unwrap();
                                {
                                    let socket = socket.deref();
                                    poll.reregister(socket, Token(token), remote.interest,
                                                    PollOpt::edge() | PollOpt::oneshot()).unwrap();
                                    debug!("{} registered", token);
                                }
                            },
                        }
                    }
                },
                SHUTDOWN_TOKEN => {
                    debug!("Shutdown token arriving on channel");
                    loop {
                        match shutdown_receiver.try_recv() {
                            Err(TryRecvError::Empty) => break,
                            Err(e) => {
                                error!("Error receiving from channel. {}", &e);
                                return
                            },
                            Ok(token) => {
                                match self.remotes.remove(&token) {
                                    None => warn!("No remote for token == {}", token),
                                    Some(remote) => {
                                        info!("{} hung up", &remote);
                                        remote.shutdown();
                                    },
                                }
                            },
                        }
                    }
                },
                token => {
                    let token = token.0;
                    let mut remote = self.remotes.get_mut(&token).unwrap();
                    remote.dispatch_read(self.job_count.clone(), packet_sender, poll_sender, shutdown_sender);
                },
            }
        }

        if event.kind().is_writable() {
            match event.token() {
                SERVER_TOKEN => panic!("Received writable event for server socket."),
                token => {
                    let mut shutdown = false;
                    {
                        let mut remote = self.remotes.get_mut(&token.0).unwrap();
                        match remote.write() {
                            Ok(_) => {
                                let socket = remote.socket.lock().unwrap();
                                {
                                    let socket = socket.deref();
                                    poll.reregister(socket, token, remote.interest,
                                                         PollOpt::edge() | PollOpt::oneshot()).unwrap();
                                }
                            },
                            Err(e) => {
                                info!("remote({}) hung up: {}", &remote, e);
                                shutdown = true;
                            }
                        }
                    }
                    if shutdown {
                        let remote = self.remotes.remove(&token.0).unwrap();
                        remote.shutdown();
                    }
                },
            }
        }
    }
}
