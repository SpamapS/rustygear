use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr};

use byteorder::{ByteOrder, BigEndian};
use mio::*;
use mio::tcp::*;

use constants::*;
use packet::{Packet, PacketMagic, PTYPES};
use queues::QueueHolder;
use worker::Worker;

struct GearmanRemote {
    socket: TcpStream,
    packet: Packet,
    queues: QueueHolder,
    worker: Worker,
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
        }
    }

    pub fn read(&mut self) -> bool {
        let mut magic_buf = [0; 4];
        let mut typ_buf = [0; 4];
        let mut size_buf = [0; 4];
        let mut psize: u32 = 0;
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut magic_buf) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return false
                },
                Ok(None) => {
                    println!("No more to read");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (magic)");
                    return false
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // Is this a req/res
                        match magic_buf {
                            REQ => {
                                self.packet.magic = PacketMagic::REQ;
                            },
                            RES => {
                                self.packet.magic = PacketMagic::RES;
                            },
                            // TEXT/ADMIN protocol
                            _ => {
                                println!("Possible admin protocol usage");
                                self.packet.magic = PacketMagic::TEXT;
                                return true
                            },
                        }
                    };
                    break
                },
            }
        }
        // Now get the type
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut typ_buf) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return false
                },
                Ok(None) => {
                    println!("No more to read (type)");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (typ)");
                    return false
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // validate typ
                        self.packet.ptype = BigEndian::read_u32(&typ_buf); 
                        println!("We got a {}", PTYPES[self.packet.ptype as usize].name);
                    };
                    break
                }
            }
        }
        // Now the length
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut size_buf) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return false
                },
                Ok(None) => {
                    println!("Need size!");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (size)");
                    return false
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        self.packet.psize = BigEndian::read_u32(&size_buf);
                        println!("Data section is {} bytes", self.packet.psize);
                    };
                    break
                }
            }
        }
        self.packet.data.resize(self.packet.psize as usize, 0);
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut self.packet.data) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return false
                },
                Ok(None) => {
                    println!("done reading data, tot_read = {}", tot_read);
                    break
                },
                Ok(Some(len)) => {
                    tot_read += len;
                    println!("got {} out of {} bytes of data", len, tot_read);
                    if tot_read >= psize as usize {
                        println!("got all data");
                        {
                            let worker = &mut self.worker;
                            match self.packet.process(self.queues.clone(), worker) {
                                Err(e) => println!("An error ocurred"),
                                Ok(pr) => {
                                    match pr {
                                        None => println!("That's all folks"),
                                        Some(p) => {
                                            // we need to send this
                                            println!("Sending {:?}", &p);
                                            self.socket.try_write(&p.to_byteslice());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        true
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
             token: Token, events: EventSet)
    {
        match token {
            SERVER_TOKEN => {
                let remote_socket = match self.socket.accept() {
                    Err(e) => {
                        println!("Accept error: {}", e);
                        return;
                    },
                    Ok(None) => unreachable!("Accept has returned 'None'"),
                    Ok(Some((sock, addr))) => sock
                };

                self.token_counter += 1;
                let new_token = Token(self.token_counter);

                self.remotes.insert(new_token, GearmanRemote::new(remote_socket, self.queues.clone()));

                event_loop.register(&self.remotes[&new_token].socket,
                                    new_token, EventSet::readable(),
                                    PollOpt::edge() | PollOpt::oneshot()).unwrap();
            },
            token => {
                println!("more from clients");
                let mut remote = self.remotes.get_mut(&token).unwrap();
                if remote.read() {
                    event_loop.reregister(&remote.socket, token, EventSet::readable(),
                                          PollOpt::edge() | PollOpt::oneshot()).unwrap();
                } else {
                    remote.socket.shutdown(Shutdown::Both).unwrap();
                }
            },
        }
    }
}
