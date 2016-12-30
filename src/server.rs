use std::collections::HashMap;
use std::fmt;
use std::net::{SocketAddr};
use std::io;

use mio::deprecated::{EventLoop, Handler, TryWrite};
use mio::*;
use mio::tcp::*;
use bytes::buf::{Buf, ByteBuf};

use packet::{Packet, EofError};
use queues::QueueHolder;
use worker::Worker;

struct GearmanRemote {
    socket: TcpStream,
    addr: SocketAddr,
    packet: Packet,
    queues: QueueHolder,
    worker: Worker,
    sendqueue: Vec<ByteBuf>,
    interest: Ready,
    token: Token,
}

pub struct GearmanServer {
    pub socket: TcpListener,
    remotes: HashMap<Token, GearmanRemote>,
    token_counter: usize,
    queues: QueueHolder,
}


const SERVER_TOKEN: Token = Token(0);

impl GearmanRemote {
    pub fn new(socket: TcpStream, addr: SocketAddr, queues: QueueHolder, token: Token) -> GearmanRemote {
        GearmanRemote {
            socket: socket,
            addr: addr,
            packet: Packet::new(token),
            queues: queues,
            worker: Worker::new(),
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

    pub fn read(&mut self) -> Result<Option<Packet>, EofError> {
        let mut ret = Ok(None);
        match self.packet.from_socket(&mut self.socket, &mut self.worker, self.queues.clone(), self.token)? {
            None => debug!("{} Done reading", self),
            Some(p) => {
                ret = Ok(Some(p));
            }
        }
        // Packet is consumed, reset
        self.packet = Packet::new(self.token);
        ret
    }

    pub fn write(&mut self) -> Result<(), io::Error> {
        while !self.sendqueue.is_empty() {
            if !self.sendqueue.first().unwrap().has_remaining() {
                self.sendqueue.pop();
                continue;
            }
            let buf = self.sendqueue.first_mut().unwrap();
            match self.socket.try_write(buf.bytes())? {
                None => break,
                Some(n) => {
                    buf.advance(n);
                    continue
                }
            };
        }
        if self.sendqueue.is_empty() {
            self.interest.remove(Ready::writable());
            self.interest.insert(Ready::readable());
        };
        Ok(())
    }

    pub fn shutdown(self) {
        match self.worker.job {
            Some(j) => {
                self.queues.clone().add_job(j)
            },
            None => {},
        }
        self.socket.shutdown(Shutdown::Both).unwrap();
    }
}

impl fmt::Display for GearmanRemote {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Remote {{ addr: {:?} }}", self.addr)
    }
}

impl GearmanServer {
    pub fn new(server_socket: TcpListener, queues: QueueHolder) -> GearmanServer {
        GearmanServer {
            token_counter: 1,
            remotes: HashMap::new(),
            socket: server_socket,
            queues: queues,
        }
    }

}

impl Handler for GearmanServer {
    type Timeout = usize;
    type Message = ();

    fn ready(&mut self, event_loop: &mut EventLoop<GearmanServer>,
             token: Token, events: Ready)
    {
        if events.is_readable() {
            match token {
                SERVER_TOKEN => {
                    let (remote_socket, remote_addr) = match self.socket.accept() {
                        Err(e) => {
                            error!("Accept error: {}", e);
                            return;
                        },
                        Ok((sock, addr)) => (sock, addr),
                    };

                    self.token_counter += 1;
                    let new_token = Token(self.token_counter);

                    self.remotes.insert(new_token, GearmanRemote::new(remote_socket, remote_addr, self.queues.clone(), new_token));

                    event_loop.register(&self.remotes[&new_token].socket,
                                        new_token, Ready::readable(),
                                        PollOpt::edge() | PollOpt::oneshot()).unwrap();
                },
                token => {
                    let mut shutdown = false;
                    {
                        let mut other_packet = None;
                        {
                            let mut remote = self.remotes.get_mut(&token).unwrap();
                            match remote.read() {
                                Ok(sp) => {
                                    match sp {
                                        Some(p) => {
                                            match p.remote {
                                                None => remote.queue_packet(&p), // Packet is for us
                                                Some(t) => {
                                                    if t == token {
                                                        // Packet is also meant for us
                                                        remote.queue_packet(&p);
                                                    } else {
                                                        // Packet is for a different remote
                                                        other_packet = Some(p);
                                                    }
                                                },
                                            };
                                        },
                                        None => {},
                                    }
                                    event_loop.reregister(&remote.socket, token, remote.interest,
                                                      PollOpt::edge() | PollOpt::oneshot()).unwrap();
                                },
                                Err(e) => {
                                    info!("remote({}) hung up: {:?}", &remote, e);
                                    shutdown = true;
                                }
                            }
                        }
                        match other_packet {
                            None => {},
                            Some(p) => {
                                let t = p.remote.unwrap();
                                match self.remotes.get_mut(&t) {
                                    None => warn!("No remote for packet, dropping: {:?}", &p),
                                    Some(mut remote) => {
                                        remote.queue_packet(&p);
                                        event_loop.reregister(&remote.socket, t, remote.interest,
                                                              PollOpt::edge() | PollOpt::oneshot()).unwrap();
                                    },
                                }
                            },
                        }
                    }
                    if shutdown {
                        let remote = self.remotes.remove(&token).unwrap();
                        remote.shutdown();
                    }
                },
            }
        }

        if events.is_writable() {
            match token {
                SERVER_TOKEN => panic!("Received writable event for server socket."),
                token => {
                    let mut shutdown = false;
                    {
                        let mut remote = self.remotes.get_mut(&token).unwrap();
                        match remote.write() {
                            Ok(_) => event_loop.reregister(&remote.socket, token, remote.interest,
                                                          PollOpt::edge() | PollOpt::oneshot()).unwrap(),
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
                },
            }
        }
    }
}
