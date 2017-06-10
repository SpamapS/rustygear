use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use std::io::Write;
use std::io::ErrorKind::WouldBlock;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use mio::*;
use mio::tcp::*;
use bytes::BytesMut;

use ::constants::*;
use packet::{Packet, EofError};
use ::queues::*;
use ::worker::*;

struct GearmanRemote {
    socket: TcpStream,
    addr: SocketAddr,
    packet: Packet,
    queues: SharedJobStorage,
    worker: Worker,
    workers: SharedWorkers,
    sendqueue: Vec<BytesMut>,
    interest: Ready,
    token: Token,
}

pub struct GearmanServer {
    pub socket: TcpListener,
    remotes: HashMap<Token, GearmanRemote>,
    token_counter: usize,
    queues: SharedJobStorage,
    workers: SharedWorkers,
    job_count: AtomicUsize,
}


const SERVER_TOKEN: Token = Token(0);

impl GearmanRemote {
    pub fn new(socket: TcpStream,
               addr: SocketAddr,
               queues: SharedJobStorage,
               token: Token,
               workers: SharedWorkers)
               -> GearmanRemote {
        GearmanRemote {
            socket: socket,
            addr: addr,
            packet: Packet::new(token),
            queues: queues,
            worker: Worker::new(),
            workers: workers,
            sendqueue: Vec::with_capacity(2), // Don't need many bufs
            interest: Ready::readable(),
            token: token,
        }
    }

    pub fn queue_packet(&mut self, packet: &Packet) {
        info!("{} Queueing {:?}", self, &packet);
        //self.sendqueue.push(BytesMut::from_slice(&packet.to_byteslice()));
        self.interest.remove(Ready::readable());
        self.interest.insert(Ready::writable());
    }

    pub fn read(&mut self, job_count: Arc<&AtomicUsize>) -> Result<Option<Vec<Packet>>, EofError> {
        let mut ret = Ok(None);
        debug!("{} readable", self);
        match self.packet
            .from_socket(&mut self.socket,
                         &mut self.worker,
                         self.workers.clone(),
                         self.queues.clone(),
                         self.token,
                         job_count)? {
            None => debug!("{} Done reading", self),
            Some(p) => {
                ret = Ok(Some(p));
            }
        }
        info!("{} Processed {:?}", &self, &self.packet);
        // Non-text packets are consumed, reset
        if self.packet.consumed {
            self.packet = Packet::new(self.token);
        }
        ret
    }

    pub fn write(&mut self) -> Result<(), io::Error> {
        debug!("{} writable", self);
        while !self.sendqueue.is_empty() {
            /*if !self.sendqueue.first().unwrap().has_remaining() {
                self.sendqueue.pop();
                continue;
            }
            */
            let buf = self.sendqueue.first_mut().unwrap();
            match self.socket.write(&buf) {
                Ok(value) => {
                    buf.split_to(value);
                    continue;
                }
                Err(err) => {
                    if let WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err);
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
        match self.worker.job() {
            Some(ref mut j) => {
                self.queues.clone().add_job(j.clone(), PRIORITY_NORMAL, None);
            }
            None => {}
        };
        self.workers.shutdown(&self.token);
        match self.socket.shutdown(Shutdown::Both) {
            Err(e) => warn!("{:?} fail on shutdown ({:?})", self.addr, e),
            Ok(_) => {}
        }
    }
}

impl fmt::Display for GearmanRemote {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Remote {{ addr: {:?} token: {:?} }}",
               self.addr,
               self.token)
    }
}

impl GearmanServer {
    pub fn new(server_socket: TcpListener,
               queues: SharedJobStorage,
               workers: SharedWorkers,
               job_count: AtomicUsize)
               -> GearmanServer {
        GearmanServer {
            token_counter: 1,
            remotes: HashMap::new(),
            socket: server_socket,
            queues: queues,
            workers: workers,
            job_count: job_count,
        }
    }
}

impl GearmanServer {
    pub fn poll(&mut self) {
        let mut poll = Poll::new().unwrap();
        poll.register(&self.socket, Token(0), Ready::readable(), PollOpt::edge())
            .unwrap();
        let mut pevents = Events::with_capacity(1024);
        loop {
            poll.poll(&mut pevents, Some(Duration::from_millis(1000))).unwrap();
            for event in pevents.iter() {
                self.poll_inner(&mut poll, &event)
            }
        }
    }

    fn poll_inner(&mut self, poll: &mut Poll, event: &Event) {
        if event.kind().is_readable() {
            match event.token() {
                SERVER_TOKEN => {
                    let (remote_socket, remote_addr) = match self.socket.accept() {
                        Err(e) => {
                            error!("Accept error: {}", e);
                            return;
                        }
                        Ok((sock, addr)) => (sock, addr),
                    };

                    self.token_counter += 1;
                    let new_token = Token(self.token_counter);

                    self.remotes.insert(new_token,
                                        GearmanRemote::new(remote_socket,
                                                           remote_addr,
                                                           self.queues.clone(),
                                                           new_token,
                                                           self.workers.clone()));

                    poll.register(&self.remotes[&new_token].socket,
                                  new_token,
                                  Ready::readable(),
                                  PollOpt::edge() | PollOpt::oneshot())
                        .unwrap();
                }
                token => {
                    let mut shutdown = false;
                    {
                        let mut other_packets = Vec::new();
                        {
                            let mut remote = self.remotes.get_mut(&token).unwrap();
                            let job_count = Arc::new(&self.job_count);
                            match remote.read(job_count) {
                                Ok(sp) => {
                                    match sp {
                                        Some(packets) => {
                                            for p in packets.into_iter() {
                                                match p.remote {
                                                    None => remote.queue_packet(&p), // for us
                                                    Some(t) => {
                                                        if t == token {
                                                            // Packet is also meant for us
                                                            remote.queue_packet(&p);
                                                        } else {
                                                            // Packet is for a different remote
                                                            other_packets.push(p);
                                                        }
                                                    }
                                                };
                                            }
                                        }
                                        None => {}
                                    }
                                    other_packets.extend(self.workers.do_wakes());
                                    poll.reregister(&remote.socket,
                                                    token,
                                                    remote.interest,
                                                    PollOpt::edge() | PollOpt::oneshot())
                                        .unwrap();
                                }
                                Err(e) => {
                                    info!("{} hung up: {:?}", &remote, &e);
                                    shutdown = true;
                                }
                            }
                        }
                        for p in other_packets {
                            let t = p.remote.unwrap();
                            match self.remotes.get_mut(&t) {
                                None => warn!("No remote for packet, dropping: {:?}", &p),
                                Some(mut remote) => {
                                    remote.queue_packet(&p);
                                    poll.reregister(&remote.socket,
                                                    t,
                                                    remote.interest,
                                                    PollOpt::edge() | PollOpt::oneshot())
                                        .unwrap();
                                }
                            }
                        }
                    }
                    if shutdown {
                        let remote = self.remotes.remove(&token).unwrap();
                        remote.shutdown();
                    }
                }
            }
        }

        if event.kind().is_writable() {
            match event.token() {
                SERVER_TOKEN => panic!("Received writable event for server socket."),
                token => {
                    let mut shutdown = false;
                    {
                        let mut remote = self.remotes.get_mut(&token).unwrap();
                        match remote.write() {
                            Ok(_) => {
                                poll.reregister(&remote.socket,
                                                token,
                                                remote.interest,
                                                PollOpt::edge() | PollOpt::oneshot())
                                    .unwrap()
                            }
                            Err(e) => {
                                info!("remote({}) hung up: {}", &remote, e);
                                shutdown = true;
                            }
                        }
                    }
                    if shutdown {
                        let remote = self.remotes.remove(&token).unwrap();
                        remote.shutdown();
                    }
                }
            }
        }
    }
}
