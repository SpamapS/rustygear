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
use worker::Worker;
use queues::{JobQueues, QueueHolder};

#[test]
fn next() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname\0data");
    let mut p = Packet::new_res(WORK_COMPLETE, data);
    let f = p.next().unwrap();
    assert_eq!(f, (0, 7));
    let f2 = p.next().unwrap();
    assert_eq!(f2, (9, 12));
}

#[test]
fn next_field() {
    let mut data = Box::new(Vec::new());
    data.extend_from_slice(b"funcname\0data\0\x7f");
    let mut p = Packet::new_res(WORK_COMPLETE, data);
    let f = p.next_field().unwrap().into_boxed_slice();
    let f = str::from_utf8(&f).unwrap();
    assert_eq!(f, "funcname");
    let f2 = p.next_field().unwrap().into_boxed_slice();
    assert_eq!(*f2, [b'd', b'a', b't', b'a', b'\0', b'\x7f' ]);
}

#[test]
fn admin_command_status() {
    let queues = QueueHolder::new();
    let queues = queues.queues;
    let t = Token(0);
    let p = Packet::new(t);
    let response = p.admin_command_status(queues);
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
    _field_byte_count: usize,
    _field_count: i8,
}

const READ_BUFFER_INIT_CAPACITY: usize = 2048;

impl Iterator for Packet {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<(usize, usize)> {
        let nargs = PTYPES[self.ptype as usize].nargs;
        if self._field_count > nargs {
            debug!("DEBUG: returning early field #{} nargs={}", self._field_count, nargs);
            return None
        }
        self._field_count += 1;
        debug!("DEBUG: returning field #{}", self._field_count);
        if self._field_count > nargs {
            return Some((self._field_byte_count, self.data.len() - 1))
        };
        let start = self._field_byte_count;
        for byte in &self.data[start..] {
            if *byte == '\0' as u8 {
                self._field_byte_count += 1; // Skip the null
                break
            }
            self._field_byte_count += 1;
        };
        Some((start, self._field_byte_count - 2)) // And don't return it
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
            _field_byte_count: 0,
            _field_count: 0,
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
            _field_byte_count: 0,
            _field_count: 0,
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
            _field_byte_count: 0,
            _field_count: 0,
        }
    }

    fn admin_command_status(&self, queues: JobQueues) -> Packet {
        let mut response = Box::new(Vec::with_capacity(1024*1024)); // XXX Wild guess.
        let queues = queues.lock().unwrap();
        for (func, fqueues) in queues.iter() {
            let mut qtot = 0;
            for q in fqueues {
                qtot += q.len();
            }
            response.extend(func);
            response.extend(format!("\t{}\t{}\t{}\n", qtot, 0, 0).into_bytes()); // TODO running/workers
        }
        response.extend(b".\n");
        Packet::new_text_res(response)
    }

    pub fn admin_from_socket(&mut self,
                             socket: &mut TcpStream) -> result::Result<Option<Packet>, EofError> {
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
            _ => return Ok(Some(Packet::new_str_res("ERR UNKNOWN_COMMAND Unknown+server+command"))),
        }
    }

    pub fn from_socket(&mut self,
                       socket: &mut TcpStream,
                       worker: &mut Worker,
                       queues: QueueHolder,
                       remote: Token) -> result::Result<Option<Packet>, EofError> {
        debug!("we are this {:?}", self);
        let mut magic_buf = [0; 4];
        let mut typ_buf = [0; 4];
        let mut size_buf = [0; 4];
        let psize: u32 = 0;
        let mut tot_read = 0;
        loop {
            if self.magic == PacketMagic::TEXT {
                return Ok(self.admin_from_socket(socket)?)
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
                        debug!("We got a {}", PTYPES[self.ptype as usize].name);
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
                        debug!("got all data");
                        self.consumed = true;
                        {
                            match self.process(queues.clone(), worker, remote) {
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

    pub fn process(&mut self, queues: QueueHolder, worker: &mut Worker, remote: Token) -> Result<Option<Packet>> {
        let p = match self.ptype {
            SUBMIT_JOB => self.handle_submit_job(queues, remote)?,
            CAN_DO => self.handle_can_do(worker)?,
            CANT_DO => self.handle_cant_do(worker)?,
            GRAB_JOB_ALL => self.handle_grab_job_all(queues, worker)?,
            WORK_COMPLETE => self.handle_work_complete(worker)?,
            _ => {
                debug!("Unimplemented: {:?} processing packet", self);
                None
            },
        };
        Ok(p)
    }

    fn next_field(&mut self) -> Result<Vec<u8>> {
        let (start, finish) = match self.next() {
            None => return Err(ParseError{}),
            Some((start, finish)) => (start, finish),
        };
        let mut r = Vec::with_capacity(finish - start + 1);
        let new_size = r.capacity();
        r.resize(new_size, 0);
        r.clone_from_slice(&self.data[start..finish + 1]);
        Ok(r)
    }

    fn handle_can_do(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let fname = self.next_field()?;
        debug!("CAN_DO fname = {:?}", fname);
        worker.functions.insert(fname);
        Ok(None)
    }

    fn handle_cant_do(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let fname = self.next_field()?;
        worker.functions.remove(&fname);
        Ok(None)
    }

    fn handle_grab_job_all(&mut self, mut queues: QueueHolder, worker: &mut Worker) -> Result<Option<Packet>> {
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

    fn handle_submit_job(&mut self, mut queues: QueueHolder, remote: Token) -> Result<Option<Packet>> {
        let fname = self.next_field()?;
        debug!("fname = {:?}", &fname);
        let unique = self.next_field()?;
        let data = self.next_field()?;
        let mut j = Job::new(fname, unique, data);
        j.remotes.push(remote);
        info!("Created job {:?}", j);
        let p = Packet::new_res(JOB_CREATED, Box::new(j.handle.clone()));
        queues.add_job(j);
        Ok(Some(p))
    }

    fn handle_work_complete(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        debug!("self._field_count = {}", self._field_count);
        let handle = self.next_field()?;
        let data = self.next_field()?;
        info!("Job is complete {:?}", String::from_utf8(handle.clone()));
        let mut ret = Ok(None);
        match worker.job {
            Some(ref j) => {
                if j.handle != handle {
                    let msg = "WORK_COMPLETE received for inactive job handle";
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(msg.len() + 1));
                    data.push(b'\0');
                    data.extend_from_slice(msg.as_bytes());
                    return Ok(Some(Packet::new_res(ERROR, data)))
                }
                if !j.remotes.is_empty() {
                    let mut client_data: Box<Vec<u8>> = Box::new(Vec::with_capacity(handle.len() + 1 + data.len()));
                    client_data.extend(&j.handle);
                    client_data.push(b'\0');
                    client_data.extend(&data);
                    ret = Ok(Some(Packet::new_res_remote(WORK_COMPLETE, client_data, Some(j.remotes[0]))));
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
        write!(f, "Packet {{ magic: {:?}, ptype: {}, size: {}, remote: {:?}, fc: {:?}, fb: {:?} }}",
               match self.magic {
                   PacketMagic::REQ => "REQ",
                   PacketMagic::RES => "RES",
                   PacketMagic::TEXT => "TEXT",
                   _ => "UNKNOWN",
               },
               PTYPES[self.ptype as usize].name,
               self.psize,
               self.remote, self._field_count, self._field_byte_count)
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
