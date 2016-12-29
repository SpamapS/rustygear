use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr};
use std::io;

use byteorder::{ByteOrder, BigEndian};
use mio::deprecated::{EventLoop, Handler, TryWrite};
use mio::*;
use mio::tcp::*;
use bytes::buf::{Buf, ByteBuf};

use constants::*;
use packet::{Packet, PacketMagic, PTYPES, EofError};
use queues::QueueHolder;
use worker::Worker;

struct GearmanRemote {
    socket: TcpStream,
    packet: Packet,
    queues: QueueHolder,
    worker: Worker,
    sendqueue: Vec<ByteBuf>,
    interest: Ready,
}

pub struct GearmanServer {
    pub socket: TcpListener,
    remotes: HashMap<Token, GearmanRemote>,
    token_counter: usize,
    queues: QueueHolder,
}


const SERVER_TOKEN: Token = Token(0);

impl GearmanRemote {
    pub fn new(socket: TcpStream, queues: QueueHolder) -> GearmanRemote {
        GearmanRemote {
            socket: socket,
            packet: Packet::new(),
            queues: queues,
            worker: Worker::new(),
            sendqueue: Vec::with_capacity(2), // Don't need many bufs
            interest: Ready::readable(),
        }
    }

    pub fn read(&mut self) -> Result<(), EofError> {
        match self.packet.from_socket(&mut self.socket, &mut self.worker, self.queues.clone())? {
            None => println!("That's all folks {:?}", self.interest),
            Some(p) => {
                println!("Queueing {:?}", &p);
                self.sendqueue.push(ByteBuf::from_slice(&p.to_byteslice()));
                self.interest.remove(Ready::readable());
                self.interest.insert(Ready::writable());
            }
        }
        Ok(())
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
                    let remote_socket = match self.socket.accept() {
                        Err(e) => {
                            println!("Accept error: {}", e);
                            return;
                        },
                        Ok((sock, addr)) => sock,
                    };

                    self.token_counter += 1;
                    let new_token = Token(self.token_counter);

                    self.remotes.insert(new_token, GearmanRemote::new(remote_socket, self.queues.clone()));

                    event_loop.register(&self.remotes[&new_token].socket,
                                        new_token, Ready::readable(),
                                        PollOpt::edge() | PollOpt::oneshot()).unwrap();
                },
                token => {
                    println!("more from clients");
                    let mut remote = self.remotes.get_mut(&token).unwrap();
                    match remote.read() {
                        Ok(_) => event_loop.reregister(&remote.socket, token, remote.interest,
                                                      PollOpt::edge() | PollOpt::oneshot()).unwrap(),
                        Err(e) => remote.socket.shutdown(Shutdown::Both).unwrap(),
                    }
                },
            }
        }

        if events.is_writable() {
            match token {
                SERVER_TOKEN => panic!("Received writable event for server socket."),
                token => {
                    let mut remote = self.remotes.get_mut(&token).unwrap();
                    match remote.write() {
                        Ok(_) => event_loop.reregister(&remote.socket, token, remote.interest,
                                                      PollOpt::edge() | PollOpt::oneshot()).unwrap(),
                        Err(e) => remote.socket.shutdown(Shutdown::Both).unwrap(),
                    }
                },
            }
        }
    }
}
