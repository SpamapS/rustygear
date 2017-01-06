use std::slice;
use std::convert::From;
use std::fmt;
use std::result;
use std::str;
use std::str::{FromStr, Utf8Error};

use mio::Token;
use mio::tcp::*;
use mio::deprecated::TryRead;
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use constants::*;
use job::*;
use worker::*;
use queues::*;

#[test]
fn test_next_binary_data() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle\0data\0\x7f");
    let p = Packet::new_res(WORK_COMPLETE, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle");
    let f2 = i.next().unwrap();
    assert_eq!(f2, [b'd', b'a', b't', b'a', b'\0', b'\x7f' ]);
}

#[test]
fn test_next_empty_end() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle2\0");
    let p = Packet::new_res(WORK_COMPLETE, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle2");
    let f2 = i.next().unwrap();
    let f2 = str::from_utf8(f2).unwrap();
    assert_eq!(f2, "");
}

#[test]
fn test_next_0nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname");
    let p = Packet::new_res(CAN_DO, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
}

#[test]
fn test_next_2nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname\0unique\0data");
    let p = Packet::new_res(SUBMIT_JOB, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "unique");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "data");
}

#[test]
fn test_next_3nargs() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"handle\0funcname\0unique\0data");
    let p = Packet::new_res(JOB_ASSIGN_UNIQ, data);
    let mut i = p.iter();
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "handle");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "funcname");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "unique");
    let f = i.next().unwrap();
    let f = str::from_utf8(f).unwrap();
    assert_eq!(f, "data");
}


#[test]
fn admin_command_status() {
    let j = Job::new(vec![b'f'],
                     vec![b'u'],
                     Vec::new());
    let mut w = Worker::new();
    w.can_do(vec![b'f']);
    let mut queues = JobQueues::new_queues();
    let mut workers = SharedWorkers::new_workers();
    queues.add_job(j);
    workers.sleep(&mut w, Token(1));
    let t = Token(0);
    let p = Packet::new(t);
    let response = p.admin_command_status(queues, workers);
    let response = str::from_utf8(&response.data).unwrap();
    assert_eq!("f\t1\t0\t1\n.\n", response);
}

#[test]
fn admin_command_status_empty() {
    let queues = JobQueues::new_queues();
    let workers = SharedWorkers::new_workers();
    let t = Token(0);
    let p = Packet::new(t);
    let response = p.admin_command_status(queues, workers);
    let response = str::from_utf8(&response.data).unwrap();
    assert_eq!(".\n", response);
}


pub struct PacketType {
    pub name: &'static str,
    pub ptype: u32,
    pub nargs: i8,
}

#[derive(PartialEq)]
pub enum PacketMagic {
    UNKNOWN,
    REQ,
    RES,
    TEXT,
}

pub struct Packet {
    pub magic: PacketMagic,
    pub ptype: u32,
    pub psize: u32,
    pub data: Box<Vec<u8>>,
    pub remote: Option<Token>,
    pub consumed: bool,
}

const READ_BUFFER_INIT_CAPACITY: usize = 2048;

pub struct IterPacket<'a> {
    packet: &'a Packet,
    data_iter: slice::Iter<'a, u8>,
    lastpos: usize,
    null_adjust: usize,
    field_count: i8,
}

impl<'a> Iterator for IterPacket<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        let nargs = PTYPES[self.packet.ptype as usize].nargs;
        if self.field_count > nargs {
            debug!("DEBUG: returning early field #{} nargs={}", self.field_count, nargs);
            return None
        }
        self.field_count += 1;
        debug!("DEBUG: returning field #{}", self.field_count);
        if self.field_count == nargs + 1 {
            return Some(&self.packet.data[(self.lastpos)..self.packet.data.len()])
        }

        let start = self.lastpos;
        let mut return_end = self.lastpos;
        {
            loop {
                self.lastpos += 1;
                match self.data_iter.next() {
                    None => break,
                    Some(byte) => {
                        return_end += 1;
                        if *byte == 0 {
                            self.null_adjust = 1;
                            break
                        }
                    },
                }
            }
        }
        Some(&self.packet.data[start..return_end - self.null_adjust])
    }
}

impl<'a> IterPacket<'a> {
    /// Convenience method that makes Vecs insetad of slices
    fn next_field(&mut self) -> Result<Vec<u8>> {
        let rslice = match self.next() {
            None => {
                return Err(ParseError{})
            },
            Some(rslice) => rslice,
        };
        let mut r = Vec::with_capacity(rslice.len());
        let new_size = r.capacity();
        r.resize(new_size, 0);
        r.clone_from_slice(rslice);
        Ok(r)
    }
}

#[derive(Debug)]
pub struct ParseError {}

#[derive(Debug)]
pub struct EofError {}

impl From<Utf8Error> for EofError {
    fn from(_: Utf8Error) -> Self {
        EofError {}
    }
}

pub type Result<T> = result::Result<T, ParseError>;

impl Packet {
    pub fn new(remote: Token) -> Packet {
        Packet { 
            magic: PacketMagic::UNKNOWN,
            ptype: 0,
            psize: 0,
            data: Box::new(Vec::with_capacity(READ_BUFFER_INIT_CAPACITY)),
            remote: Some(remote),
            consumed: false,
        }
    }

    pub fn new_res(ptype: u32, data: Box<Vec<u8>>) -> Packet {
        Packet::new_res_remote(ptype, data, None)
    }
    pub fn new_res_remote(ptype: u32, data: Box<Vec<u8>>, remote: Option<Token>) -> Packet {
        Packet {
            magic: PacketMagic::RES,
            ptype: ptype,
            psize: data.len() as u32,
            data: data,
            remote: remote,
            consumed: false,
        }
    }

    pub fn new_str_res(msg: &str) -> Packet {
        let mut data = Box::new(Vec::with_capacity(msg.len() + 1)); // For newline
        let msg = String::from_str(msg).unwrap(); // We make all the strings
        data.extend(msg.into_bytes());
        data.push(b'\n');
        Packet::new_text_res(data)
    }

    pub fn new_text_res(data: Box<Vec<u8>>) -> Packet {
        Packet {
            magic: PacketMagic::TEXT,
            ptype: 0,
            psize: data.len() as u32,
            data: data,
            remote: None,
            consumed: false,
        }
    }

    pub fn iter<'a>(&'a self) -> IterPacket<'a> {
        IterPacket {
            packet: self,
            data_iter: self.data.iter(),
            lastpos: 0,
            null_adjust: 0,
            field_count: 0,
        }
    }

    fn admin_command_status(&self, queues: JobQueues, workers: SharedWorkers) -> Packet {
        let mut response = Box::new(Vec::with_capacity(1024*1024)); // XXX Wild guess.
        let queues = queues.lock().unwrap();
        for (func, fqueues) in queues.iter() {
            let mut qtot = 0;
            for q in fqueues {
                qtot += q.len();
            }
            let (active_workers, inactive_workers) = workers.clone().count_workers(func);
            response.extend(func);
            response.extend(format!("\t{}\t{}\t{}\n",
                                    qtot,
                                    active_workers,
                                    inactive_workers+active_workers).into_bytes());
        }
        response.extend(b".\n");
        Packet::new_text_res(response)
    }

    pub fn admin_from_socket(&mut self,
                             socket: &mut TcpStream,
                             queues: JobQueues,
                             workers: SharedWorkers) -> result::Result<Option<Packet>, EofError> {
        let mut admin_buf = [0; 64];
        loop {
            match socket.try_read(&mut admin_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) | Ok(Some(0)) => {
                    debug!("Admin no more to read");
                    break
                },
                Ok(Some(l)) => {
                    debug!("Read {} for admin command", l);
                    self.data.extend(admin_buf[0..l].iter());
                    continue
                },
            }
        }
        let data_copy = self.data.clone().into_boxed_slice();
        let data_str = str::from_utf8(&data_copy)?;
        info!("admin command data: {:?}", data_str);
        if !self.data.contains(&b'\n') {
            debug!("Admin partial read");
            return Ok(None)
        }
        self.consumed = true;
        match data_str.trim() {
            "version" => return Ok(Some(Packet::new_str_res("OK rustygear-version-here"))),
            "status" => return Ok(Some(self.admin_command_status(queues, workers))),
            _ => return Ok(Some(Packet::new_str_res("ERR UNKNOWN_COMMAND Unknown+server+command"))),
        }
    }

    pub fn from_socket(&mut self,
                       socket: &mut TcpStream,
                       worker: &mut Worker,
                       workers: SharedWorkers,
                       queues: JobQueues,
                       remote: Token) -> result::Result<Option<Packet>, EofError> {
        let mut magic_buf = [0; 4];
        let mut typ_buf = [0; 4];
        let mut size_buf = [0; 4];
        let psize: u32 = 0;
        let mut tot_read = 0;
        loop {
            if self.magic == PacketMagic::TEXT {
                return Ok(self.admin_from_socket(socket, queues, workers)?)
            }
            match socket.try_read(&mut magic_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    debug!("No more to read");
                    break
                },
                Ok(Some(0)) => {
                    debug!("Eof (magic)");
                    return Err(EofError {})
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // Is this a req/res
                        match magic_buf {
                            REQ => {
                                self.magic = PacketMagic::REQ;
                            },
                            RES => {
                                self.magic = PacketMagic::RES;
                            },
                            // TEXT/ADMIN protocol
                            _ => {
                                debug!("admin protocol detected");
                                self.magic = PacketMagic::TEXT;
                                self.data.extend(magic_buf.iter());
                                continue;
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
            match socket.try_read(&mut typ_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    debug!("No more to read (type)");
                    break
                },
                Ok(Some(0)) => {
                    debug!("Eof (typ)");
                    return Err(EofError {})
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // validate typ
                        self.ptype = BigEndian::read_u32(&typ_buf); 
                        debug!("We got a {} from {:?}", &PTYPES[self.ptype as usize].name, &typ_buf);
                    };
                    break
                }
            }
        }
        // Now the length
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut size_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    debug!("Need size!");
                    break
                },
                Ok(Some(0)) => {
                    debug!("Eof (size)");
                    return Err(EofError {})
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        self.psize = BigEndian::read_u32(&size_buf);
                        debug!("Data section is {} bytes", self.psize);
                    };
                    break
                }
            }
        }
        self.data.resize(self.psize as usize, 0);
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut self.data) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    debug!("done reading data, tot_read = {}", tot_read);
                    break
                },
                Ok(Some(len)) => {
                    tot_read += len;
                    debug!("got {} out of {} bytes of data", len, tot_read);
                    if tot_read >= psize as usize {
                        debug!("got all data -> {:?}", String::from_utf8_lossy(&self.data));
                        self.consumed = true;
                        {
                            match self.process(queues.clone(), worker, remote, workers) {
                                Err(_) => {
                                    error!("Packet parsing error");
                                    return Err(EofError {})
                                },
                                Ok(pr) => {
                                    return Ok(pr)
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        Ok(None)
    }

    pub fn process(&mut self, queues: JobQueues, worker: &mut Worker, remote: Token, workers: SharedWorkers) -> Result<Option<Packet>> {
        let p = match self.ptype {
            SUBMIT_JOB => self.handle_submit_job(queues, Some(remote), workers)?,
            SUBMIT_JOB_BG => self.handle_submit_job(queues, None, workers)?,
            PRE_SLEEP => self.handle_pre_sleep(worker, workers, remote)?,
            CAN_DO => self.handle_can_do(worker, workers, remote)?,
            CANT_DO => self.handle_cant_do(worker)?,
            GRAB_JOB => self.handle_grab_job(queues, worker)?,
            GRAB_JOB_UNIQ => self.handle_grab_job_uniq(queues, worker)?,
            GRAB_JOB_ALL => self.handle_grab_job_all(queues, worker)?,
            WORK_COMPLETE => self.handle_work_complete(worker)?,
            _ => {
                debug!("Unimplemented: {:?} processing packet", self);
                None
            },
        };
        Ok(p)
    }

    fn handle_can_do(&mut self, worker: &mut Worker, workers: SharedWorkers, remote: Token) -> Result<Option<Packet>> {
        let mut iter = self.iter();
        let fname = iter.next_field()?;
        debug!("CAN_DO fname = {:?}", fname);
        worker.can_do(fname);
        workers.clone().wakeup(worker, remote);
        Ok(None)
    }

    fn handle_cant_do(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let mut iter = self.iter();
        let fname = iter.next_field()?;
        worker.cant_do(&fname);
        Ok(None)
    }

    fn handle_pre_sleep(&mut self, worker: &mut Worker, workers: SharedWorkers, remote: Token) -> Result<Option<Packet>> {
        workers.clone().sleep(worker, remote);
        Ok(None)
    }

    fn handle_grab_job(&mut self, mut queues: JobQueues, worker: &mut Worker) -> Result<Option<Packet>> {
        if queues.get_job(worker) {
            match worker.job {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() + j.fname.len() + j.unique.len() + j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN, data)))
                },
                None => {},
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_grab_job_uniq(&mut self, mut queues: JobQueues, worker: &mut Worker) -> Result<Option<Packet>> {
        if queues.get_job(worker) {
            match worker.job {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() + j.fname.len() + j.unique.len() + j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.unique);
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN_UNIQ, data)))
                },
                None => {},
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_grab_job_all(&mut self, mut queues: JobQueues, worker: &mut Worker) -> Result<Option<Packet>> {
        if queues.get_job(worker) {
            match worker.job {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() + j.fname.len() + j.unique.len() + j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.unique);
                    data.push(b'\0');
                    // reducer not implemented
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN_ALL, data)))
                },
                None => {},
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_submit_job(&mut self, mut queues: JobQueues, remote: Option<Token>, workers: SharedWorkers) -> Result<Option<Packet>> {
        let mut iter = self.iter();
        let fname = iter.next_field()?;
        let unique = iter.next_field()?;
        let data = iter.next_field()?;
        let mut j = Job::new(fname.clone(), unique, data);
        match remote {
            None => {},
            Some(remote) => j.add_remote(remote),
        }
        info!("Created job {:?}", j);
        let p = Packet::new_res(JOB_CREATED, Box::new(j.handle.clone()));
        queues.add_job(j);
        workers.clone().queue_wake(&fname);
        Ok(Some(p))
    }

    fn handle_work_complete(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let mut iter = self.iter();
        let handle = iter.next_field()?;
        let data = iter.next_field()?;
        info!("Job is complete {:?}", String::from_utf8(handle.clone()));
        let mut ret = Ok(None);
        match worker.job {
            Some(ref mut j) => {
                if j.handle != handle {
                    let msg = "WORK_COMPLETE received for inactive job handle";
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(msg.len() + 1));
                    data.push(b'\0');
                    data.extend_from_slice(msg.as_bytes());
                    return Ok(Some(Packet::new_res(ERROR, data)))
                }
                for remote in j.iter_remotes() {
                    let mut client_data: Box<Vec<u8>> = Box::new(Vec::with_capacity(handle.len() + 1 + data.len()));
                    client_data.extend(&j.handle);
                    client_data.push(b'\0');
                    client_data.extend(&data);
                    ret = Ok(Some(Packet::new_res_remote(WORK_COMPLETE, client_data, Some(*remote))));
                    break; // TODO make packet handler return a list of packets to enqueue
                }
            },
            None => {
                let msg = "WORK_COMPLETE received but no active jobs";
                let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(msg.len() + 1));
                data.push(b'\0');
                data.extend_from_slice(msg.as_bytes());
                return Ok(Some(Packet::new_res(ERROR, data)))
            }
        }
        worker.job = None;
        ret
    }

    pub fn to_byteslice(&self) -> Box<[u8]> {
        let len = 12 + self.psize;
        let mut buf = Vec::with_capacity(len as usize) as Vec<u8>;
        let magic = match self.magic {
            PacketMagic::UNKNOWN => panic!("Unknown packet magic cannot be sent"),
            PacketMagic::REQ => REQ,
            PacketMagic::RES => RES,
            PacketMagic::TEXT => {
                return self.data.clone().into_boxed_slice();
            },
        };
        buf.extend(magic.iter());
        buf.write_u32::<BigEndian>(self.ptype).unwrap();
        buf.write_u32::<BigEndian>(self.psize).unwrap();
        buf.extend(self.data.iter());
        buf.into_boxed_slice()
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Packet {{ magic: {:?}, ptype: {}, size: {}, remote: {:?} }}",
               match self.magic {
                   PacketMagic::REQ => "REQ",
                   PacketMagic::RES => "RES",
                   PacketMagic::TEXT => "TEXT",
                   _ => "UNKNOWN",
               },
               PTYPES[self.ptype as usize].name,
               self.psize,
               self.remote)
    }
}

pub static PTYPES: [PacketType; 43] = [
    PacketType { name: "__UNUSED__", ptype: 0, nargs: -1 },
    PacketType { name: "CAN_DO", ptype: 1, nargs: 0 }, 
    PacketType { name: "CANT_DO", ptype: 2, nargs: 0 },
    PacketType { name: "RESET_ABILITIES", ptype: 3, nargs: -1 },
    PacketType { name: "PRE_SLEEP", ptype: 4, nargs: -1 },
    PacketType { name: "__UNUSED__", ptype: 5, nargs: -1 },
    PacketType { name: "NOOP", ptype: 6, nargs: -1 },
    PacketType { name: "SUBMIT_JOB", ptype: 7, nargs: 2 },
    PacketType { name: "JOB_CREATED", ptype: 8, nargs: 0 },
    PacketType { name: "GRAB_JOB", ptype: 9, nargs: -1 },
    PacketType { name: "NO_JOB", ptype: 10, nargs: -1 },
    PacketType { name: "JOB_ASSIGN", ptype: 11, nargs: 2 },
    PacketType { name: "WORK_STATUS", ptype: 12, nargs: 2 },
    PacketType { name: "WORK_COMPLETE", ptype: 13, nargs: 1 },
    PacketType { name: "WORK_FAIL", ptype: 14, nargs: 0 },
    PacketType { name: "GET_STATUS", ptype: 15, nargs: 0 },
    PacketType { name: "ECHO_REQ", ptype: 16, nargs: 0 },
    PacketType { name: "ECHO_RES", ptype: 17, nargs: 1 },
    PacketType { name: "SUBMIT_JOB_BG", ptype: 18, nargs: 2 },
    PacketType { name: "ERROR", ptype: 19, nargs: 1 },
    PacketType { name: "STATUS_RES", ptype: 20, nargs: 4 },
    PacketType { name: "SUBMIT_JOB_HIGH", ptype: 21, nargs: 2 },
    PacketType { name: "SET_CLIENT_ID", ptype: 22, nargs: 0 },
    PacketType { name: "CAN_DO_TIMEOUT", ptype: 23, nargs: 1 },
    PacketType { name: "ALL_YOURS", ptype: 24, nargs: -1 },
    PacketType { name: "WORK_EXCEPTION", ptype: 25, nargs: 1 },
    PacketType { name: "OPTION_REQ", ptype: 26, nargs: 0 },
    PacketType { name: "OPTION_RES", ptype: 27, nargs: 0 },
    PacketType { name: "WORK_DATA", ptype: 28, nargs: 1 },
    PacketType { name: "WORK_WARNING", ptype: 29, nargs: 1 },
    PacketType { name: "GRAB_JOB_UNIQ", ptype: 30, nargs: -1 },
    PacketType { name: "JOB_ASSIGN_UNIQ", ptype: 31, nargs: 3 },
    PacketType { name: "SUBMIT_JOB_HIGH_BG", ptype: 32, nargs: 2 },
    PacketType { name: "SUBMIT_JOB_LOW", ptype: 33, nargs: 2 },
    PacketType { name: "SUBMIT_JOB_LOW_BG", ptype: 34, nargs: 2 },
    PacketType { name: "SUBMIT_JOB_SCHED", ptype: 35, nargs: 7 },
    PacketType { name: "SUBMIT_JOB_EPOCH", ptype: 36, nargs: 3 },
    PacketType { name: "SUBMIT_REDUCE_JOB", ptype: 37, nargs: 3 },
    PacketType { name: "SUBMIT_REDUCE_JOB_BACKGROUND", ptype: 38, nargs: 3 },
    PacketType { name: "GRAB_JOB_ALL", ptype: 39, nargs: -1 },
    PacketType { name: "JOB_ASSIGN_ALL", ptype: 40, nargs: 4 },
    PacketType { name: "GET_STATUS_UNIQUE", ptype: 41, nargs: 0 },
    PacketType { name: "STATUS_RES_UNIQUE", ptype: 42, nargs: 5 },
];
