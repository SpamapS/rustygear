use std::fmt;
use std::io;
use std::result;

use mio::tcp::*;
use mio::deprecated::TryRead;
use mio::*;
use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use constants::*;
use job::*;
use worker::Worker;
use queues::QueueHolder;

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
    _field_byte_count: usize,
    _field_count: i8,
}

const READ_BUFFER_INIT_CAPACITY: usize = 2048;

impl Iterator for Packet {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<(usize, usize)> {
        let nargs = PTYPES[self.ptype as usize].nargs;
        if self._field_count > nargs {
            return None
        }
        self._field_count += 1;
        println!("DEBUG: returning field #{}", self._field_count);
        if self._field_count == nargs {
            return Some((self._field_byte_count, self.data.len()))
        };
        let start = self._field_byte_count;
        for byte in &self.data[start..] {
            self._field_byte_count += 1;
            if *byte == '\0' as u8 {
                break
            }
        };
        Some((start, self._field_byte_count))
    }
}

pub struct ParseError {}
pub struct EofError {}

pub type Result<T> = result::Result<T, ParseError>;

impl Packet {
    pub fn new() -> Packet {
        Packet { 
            magic: PacketMagic::UNKNOWN,
            ptype: 0,
            psize: 0,
            data: Box::new(Vec::with_capacity(READ_BUFFER_INIT_CAPACITY)),
            _field_byte_count: 0,
            _field_count: 0,
        }
    }

    pub fn new_res(ptype: u32, data: Box<Vec<u8>>) -> Packet {
        Packet {
            magic: PacketMagic::RES,
            ptype: ptype,
            psize: data.len() as u32,
            data: data,
            _field_byte_count: 0,
            _field_count: 0,
        }
    }

    pub fn from_socket(&mut self,
                       socket: &mut TcpStream,
                       worker: &mut Worker,
                       queues: QueueHolder) -> result::Result<Option<Packet>, EofError> {
        let mut magic_buf = [0; 4];
        let mut typ_buf = [0; 4];
        let mut size_buf = [0; 4];
        let mut psize: u32 = 0;
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut magic_buf) {
                Err(e) => {
                    println!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    println!("No more to read");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (magic)");
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
                                println!("Possible admin protocol usage");
                                self.magic = PacketMagic::TEXT;
                                return Ok(None)
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
                    println!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    println!("No more to read (type)");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (typ)");
                    return Err(EofError {})
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // validate typ
                        self.ptype = BigEndian::read_u32(&typ_buf); 
                        println!("We got a {}", PTYPES[self.ptype as usize].name);
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
                    println!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
                },
                Ok(None) => {
                    println!("Need size!");
                    break
                },
                Ok(Some(0)) => {
                    println!("Eof (size)");
                    return Err(EofError {})
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        self.psize = BigEndian::read_u32(&size_buf);
                        println!("Data section is {} bytes", self.psize);
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
                    println!("Error while reading socket: {:?}", e);
                    return Err(EofError {})
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
                            match self.process(queues.clone(), worker) {
                                Err(e) => {
                                    println!("An error ocurred");
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

    pub fn process(&mut self, mut queues: QueueHolder, worker: &mut Worker) -> Result<Option<Packet>> {
        let p = match self.ptype {
            SUBMIT_JOB => self.handleSubmitJob(queues)?,
            CAN_DO => self.handleCanDo(worker)?,
            CANT_DO => self.handleCantDo(worker)?,
            GRAB_JOB_ALL => self.handleGrabJobAll(queues, worker)?,
            _ => {
                println!("Unimplemented: {:?} processing packet", self);
                None
            },
        };
        Ok(p)
    }

    fn nextField(&mut self) -> Result<Vec<u8>> {
        let (start, finish) = match self.next() {
            None => return Err(ParseError{}),
            Some((start, finish)) => (start, finish),
        };
        let mut r = Vec::with_capacity(finish - start);
        let new_size = r.capacity();
        r.resize(new_size, 0);
        r.clone_from_slice(&self.data[start..finish]);
        Ok(r)
    }

    fn handleCanDo(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let fname = self.nextField()?;
        worker.functions.insert(fname);
        Ok(None)
    }

    fn handleCantDo(&mut self, worker: &mut Worker) -> Result<Option<Packet>> {
        let fname = self.nextField()?;
        worker.functions.remove(&fname);
        Ok(None)
    }

    fn handleGrabJobAll(&mut self, mut queues: QueueHolder, worker: &mut Worker) -> Result<Option<Packet>> {
        let j = match queues.get_job(&worker.functions) {
            None => {
                return Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))));
            },
            Some(j) => j,
        };
        Ok(None)
    }

    fn handleSubmitJob(&mut self, mut queues: QueueHolder) -> Result<Option<Packet>> {
        let fname = self.nextField()?;
        let unique = self.nextField()?;
        let data = self.nextField()?;
        let j = Job::new(fname, unique, data);
        println!("Created job {:?}", j);
        let p = Packet::new_res(JOB_CREATED, Box::new(j.handle.clone()));
        queues.add_job(j);
        Ok(Some(p))
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
        buf.write_u32::<BigEndian>(self.ptype);
        buf.write_u32::<BigEndian>(self.psize);
        buf.extend(self.data.iter());
        buf.into_boxed_slice()
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Packet {{ magic: {:?}, ptype: {}, size: {} }}",
               match self.magic {
                   PacketMagic::REQ => "REQ",
                   PacketMagic::RES => "RES",
                   PacketMagic::TEXT => "TEXT",
                   _ => "UNKNOWN",
               },
               PTYPES[self.ptype as usize].name,
               self.psize)
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
