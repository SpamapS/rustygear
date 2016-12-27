extern crate mio;
extern crate byteorder;

use std::collections::HashMap;
use std::net::SocketAddr;

pub mod constants;
use constants::*;

use self::byteorder::{ByteOrder, BigEndian};
use self::mio::*;
use self::mio::tcp::*;

struct GearmanRemote {
    socket: TcpStream,
}

struct GearmanServer {
    socket: TcpListener,
    remotes: HashMap<Token, GearmanRemote>,
    token_counter: usize,
}

const SERVER_TOKEN: Token = Token(0);

//const GEARMAN_REQ: [u8; 4] = [b'\0', b'R', b'E', b'Q'];
//const GEARMAN_RES: [u8; 4] = [b'\0', b'R', b'E', b'S'];

impl GearmanRemote {
    fn read(&mut self) {
        let mut magic = [0; 4];
        let mut typ_buf = [0; 4];
        let ptype: u32;
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut magic) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return
                },
                Ok(None) =>
                    // No more to read
                    break,
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // Is this a req/res
                        match magic {
                            REQ => {
                                break
                            },
                            RES => {
                                break
                            },
                            // Could be admin
                            _ => {
                                println!("Possible admin protocol usage");
                                return
                            },
                        }
                    }
                },
            }
        }
        // Now get the type
        loop {
            let mut tot_read = 0;
            match self.socket.try_read(&mut typ_buf) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return
                },
                Ok(None) =>
                    // No more to read
                    break,
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // validate typ
                        ptype = BigEndian::read_u32(&typ_buf); 
                        println!("We got a type I think {}", ptype);
                        return;
                    }
                }
            }
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

                self.remotes.insert(new_token, GearmanRemote { socket: remote_socket });

                event_loop.register(&self.remotes[&new_token].socket,
                                    new_token, EventSet::readable(),
                                    PollOpt::edge() | PollOpt::oneshot()).unwrap();
            },
            token => {
                let mut remote = self.remotes.get_mut(&token).unwrap();
                remote.read();
                event_loop.reregister(&remote.socket, token, EventSet::readable(),
                                      PollOpt::edge() | PollOpt::oneshot()).unwrap();
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
