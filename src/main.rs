extern crate mio;
extern crate byteorder;

use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr};

pub mod constants;
use constants::*;
pub mod job;
use job::*;
pub mod packet;
use packet::*;

use self::byteorder::{ByteOrder, BigEndian};
use self::mio::*;
use self::mio::tcp::*;

struct GearmanRemote {
    socket: TcpStream,
    packet: Packet,
}

struct GearmanServer {
    socket: TcpListener,
    remotes: HashMap<Token, GearmanRemote>,
    token_counter: usize,
}

const SERVER_TOKEN: Token = Token(0);

impl GearmanRemote {
    fn new(socket: TcpStream) -> GearmanRemote {
        GearmanRemote {
            socket: socket,
            packet: Packet::new(),
        }
    }

    fn read(&mut self) -> bool {
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
                            // Could be admin
                            _ => {
                                println!("Possible admin protocol usage");
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
                        match self.packet.process() {
                            Err(e) => println!("An error ocurred"),
                            Ok(pr) => {
                                match pr {
                                    None => println!("That's all folks"),
                                    Some(p) => {
                                        // we need to send this
                                        self.socket.try_write(&p.to_byteslice());
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

                self.remotes.insert(new_token, GearmanRemote::new(remote_socket));

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

fn main() {
    let address = "0.0.0.0:4730".parse::<SocketAddr>().unwrap();
    let server_socket = TcpListener::bind(&address).unwrap();
    let mut event_loop = EventLoop::new().unwrap();

    let mut server = GearmanServer {
        token_counter: 1,
        remotes: HashMap::new(),
        socket: server_socket,
    };

    event_loop.register(&server.socket,
                        Token(0),
                        EventSet::readable(),
                        PollOpt::edge()).unwrap();

    event_loop.run(&mut server).unwrap();
}
