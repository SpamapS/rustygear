use std::convert::From;
use std::fmt;
use std::result;
use std::str;
use std::str::{FromStr, Utf8Error};

use memchr::Memchr;
//use mio::net::*;
//use mio::deprecated::TryRead;
//use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use byteorder::{BigEndian, WriteBytesExt};

use ::constants::*;

pub struct PacketType {
    pub name: &'static str,
    pub ptype: u32,
    pub nargs: i8,
}

#[derive(Clone, Copy, PartialEq, Debug)]
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
    pub remote: Option<usize>,
    pub consumed: bool,
}

const READ_BUFFER_INIT_CAPACITY: usize = 2048;

pub struct IterPacket<'a> {
    packet: &'a Packet,
    data_iter: Memchr<'a>,
    lastpos: usize,
    null_adjust: usize,
    field_count: i8,
}

impl<'a> Iterator for IterPacket<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        let nargs = PTYPES[self.packet.ptype as usize].nargs;
        if self.field_count > nargs {
            debug!("DEBUG: returning early field #{} nargs={}",
                   self.field_count,
                   nargs);
            return None;
        }
        self.field_count += 1;
        debug!("DEBUG: returning field #{}", self.field_count);
        if self.field_count == nargs + 1 {
            return Some(&self.packet.data[(self.lastpos)..self.packet.data.len()]);
        }

        let start = self.lastpos;
        match self.data_iter.next() {
            None => {
                warn!("No null found where one was expected");
                self.lastpos = self.packet.data.len();
                return None;
            }
            Some(pos) => {
                self.lastpos = pos;
                let return_end = pos - 1;
                return Some(&self.packet.data[start..return_end - self.null_adjust]);
            }
        }
    }
}

/*
impl<'a> IterPacket<'a> {
    /// Convenience method that makes Vecs insetad of slices
    fn next_field(&mut self) -> Result<Vec<u8>> {
        match self.next() {
            None => Err(ParseError {}),
            Some(rslice) => Ok(rslice.to_vec()),
        }
    }
}
*/

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
    pub fn new(remote: usize) -> Packet {
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
    pub fn new_res_remote(ptype: u32, data: Box<Vec<u8>>, remote: Option<usize>) -> Packet {
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
            data_iter: Memchr::new(b'\0', &self.data),
            lastpos: 0,
            null_adjust: 0,
            field_count: 0,
        }
    }

    /*
    pub fn admin_from_socket(&mut self,
                             socket: &mut TcpStream,
                             storage: SharedJobStorage,
                             workers: SharedWorkers)
                             -> result::Result<Option<Packet>, EofError> {
        let mut admin_buf = [0; 64];
        loop {
            match socket.try_read(&mut admin_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {});
                }
                Ok(None) | Ok(Some(0)) => {
                    debug!("Admin no more to read");
                    break;
                }
                Ok(Some(l)) => {
                    debug!("Read {} for admin command", l);
                    self.data.extend(admin_buf[0..l].iter());
                    continue;
                }
            }
        }
        let data_copy = self.data.clone().into_boxed_slice();
        let data_str = str::from_utf8(&data_copy)?;
        info!("admin command data: {:?}", data_str);
        if !self.data.contains(&b'\n') {
            debug!("Admin partial read");
            return Ok(None);
        }
        self.consumed = true;
        match data_str.trim() {
            "version" => return Ok(Some(Packet::new_str_res("OK rustygear-version-here"))),
            "status" => return Ok(Some(self.admin_command_status(storage, workers))),
            _ => return Ok(Some(Packet::new_str_res("ERR UNKNOWN_COMMAND Unknown+server+command"))),
        }
    }

    pub fn from_socket(&mut self,
                       socket: &mut TcpStream,
                       worker: &mut Worker,
                       workers: SharedWorkers,
                       storage: SharedJobStorage,
                       remote: usize,
                       job_count: Arc<&AtomicUsize>)
                       -> result::Result<Option<Vec<Packet>>, EofError> {
        let mut magic_buf = [0; 4];
        let mut typ_buf = [0; 4];
        let mut size_buf = [0; 4];
        let psize: u32 = 0;
        let mut tot_read = 0;
        loop {
            if self.magic == PacketMagic::TEXT {
                match self.admin_from_socket(socket, storage, workers)? {
                    None => return Ok(None),
                    Some(p) => {
                        let mut packets = Vec::with_capacity(1);
                        packets.push(p);
                        return Ok(Some(packets));
                    }
                }
            }
            match socket.try_read(&mut magic_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {});
                }
                Ok(None) => {
                    debug!("No more to read");
                    break;
                }
                Ok(Some(0)) => {
                    debug!("Eof (magic)");
                    return Err(EofError {});
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // Is this a req/res
                        match magic_buf {
                            REQ => {
                                self.magic = PacketMagic::REQ;
                            }
                            RES => {
                                self.magic = PacketMagic::RES;
                            }
                            // TEXT/ADMIN protocol
                            _ => {
                                debug!("admin protocol detected");
                                self.magic = PacketMagic::TEXT;
                                self.data.extend(magic_buf.iter());
                                continue;
                            }
                        }
                    };
                    break;
                }
            }
        }
        // Now get the type
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut typ_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {});
                }
                Ok(None) => {
                    debug!("No more to read (type)");
                    break;
                }
                Ok(Some(0)) => {
                    debug!("Eof (typ)");
                    return Err(EofError {});
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        // validate typ
                        self.ptype = BigEndian::read_u32(&typ_buf);
                        debug!("We got a {} from {:?}",
                               &PTYPES[self.ptype as usize].name,
                               &typ_buf);
                    };
                    break;
                }
            }
        }
        // Now the length
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut size_buf) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {});
                }
                Ok(None) => {
                    debug!("Need size!");
                    break;
                }
                Ok(Some(0)) => {
                    debug!("Eof (size)");
                    return Err(EofError {});
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    if tot_read == 4 {
                        self.psize = BigEndian::read_u32(&size_buf);
                        debug!("Data section is {} bytes", self.psize);
                    };
                    break;
                }
            }
        }
        self.data.resize(self.psize as usize, 0);
        loop {
            let mut tot_read = 0;
            match socket.try_read(&mut self.data) {
                Err(e) => {
                    error!("Error while reading socket: {:?}", e);
                    return Err(EofError {});
                }
                Ok(None) => {
                    debug!("done reading data, tot_read = {}", tot_read);
                    break;
                }
                Ok(Some(len)) => {
                    tot_read += len;
                    debug!("got {} out of {} bytes of data", len, tot_read);
                    if tot_read >= psize as usize {
                        debug!("got all data -> {:?}", String::from_utf8_lossy(&self.data));
                        self.consumed = true;
                        {
                            match self.process(
                                storage.clone(), worker, remote, workers, job_count) {
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
    pub fn process(&mut self,
                   storage: SharedJobStorage,
                   worker: &mut Worker,
                   remote: usize,
                   workers: SharedWorkers,
                   job_count: Arc<&AtomicUsize>)
                   -> Result<Option<Vec<Packet>>> {
        let p = match self.ptype {
            SUBMIT_JOB => {
                self.handle_submit_job(storage, Some(remote), workers, PRIORITY_NORMAL, job_count)?
            }
            SUBMIT_JOB_HIGH => {
                self.handle_submit_job(storage, Some(remote), workers, PRIORITY_HIGH, job_count)?
            }
            SUBMIT_JOB_LOW => {
                self.handle_submit_job(storage, Some(remote), workers, PRIORITY_LOW, job_count)?
            }
            SUBMIT_JOB_BG => {
                self.handle_submit_job(storage, None, workers, PRIORITY_NORMAL, job_count)?
            }
            SUBMIT_JOB_HIGH_BG => {
                self.handle_submit_job(storage, None, workers, PRIORITY_HIGH, job_count)?
            }
            SUBMIT_JOB_LOW_BG => {
                self.handle_submit_job(storage, None, workers, PRIORITY_LOW, job_count)?
            }
            PRE_SLEEP => self.handle_pre_sleep(worker, workers, remote)?,
            CAN_DO => self.handle_can_do(worker, workers, remote)?,
            CANT_DO => self.handle_cant_do(worker)?,
            GRAB_JOB => self.handle_grab_job(storage, worker)?,
            GRAB_JOB_UNIQ => self.handle_grab_job_uniq(storage, worker)?,
            GRAB_JOB_ALL => self.handle_grab_job_all(storage, worker)?,
            WORK_COMPLETE => {
                let packets = self.handle_work_complete(worker, storage)?;
                return Ok(packets);
            }
            WORK_STATUS | WORK_DATA | WORK_WARNING => {
                let packets = self.handle_work_update(storage)?;
                return Ok(packets);
            }
            ECHO_REQ => Some(Packet::new_res(ECHO_RES, self.data.clone())),
            _ => {
                error!("Unimplemented: {:?} processing packet", self);
                None
            }
        };
        match p {
            None => Ok(None),
            Some(p) => {
                let mut packets = Vec::with_capacity(1);
                packets.push(p);
                Ok(Some(packets))
            }
        }
    }
*/

    /*fn handle_can_do(&mut self,
                     worker: &mut Worker,
                     workers: SharedWorkers,
                     remote: usize)
                     -> Result<Option<Packet>> {
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

    fn handle_pre_sleep(&mut self,
                        worker: &mut Worker,
                        workers: SharedWorkers,
                        remote: usize)
                        -> Result<Option<Packet>> {
        workers.clone().sleep(worker, remote);
        Ok(None)
    }

    fn handle_grab_job(&mut self,
                       mut storage: SharedJobStorage,
                       worker: &mut Worker)
                       -> Result<Option<Packet>> {
        if storage.get_job(worker) {
            match worker.job() {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() +
                                                                             j.fname.len() +
                                                                             j.unique.len() +
                                                                             j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN, data)));
                }
                None => {}
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_grab_job_uniq(&mut self,
                            mut storage: SharedJobStorage,
                            worker: &mut Worker)
                            -> Result<Option<Packet>> {
        if storage.get_job(worker) {
            match worker.job() {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() +
                                                                             j.fname.len() +
                                                                             j.unique.len() +
                                                                             j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.unique);
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN_UNIQ, data)));
                }
                None => {}
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_grab_job_all(&mut self,
                           mut storage: SharedJobStorage,
                           worker: &mut Worker)
                           -> Result<Option<Packet>> {
        if storage.get_job(worker) {
            match worker.job() {
                Some(ref j) => {
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(4 + j.handle.len() +
                                                                             j.fname.len() +
                                                                             j.unique.len() +
                                                                             j.data.len()));
                    data.extend(&j.handle);
                    data.push(b'\0');
                    data.extend(&j.fname);
                    data.push(b'\0');
                    data.extend(&j.unique);
                    data.push(b'\0');
                    // reducer not implemented
                    data.push(b'\0');
                    data.extend(&j.data);
                    return Ok(Some(Packet::new_res(JOB_ASSIGN_ALL, data)));
                }
                None => {}
            }
        };
        Ok(Some(Packet::new_res(NO_JOB, Box::new(Vec::new()))))
    }

    fn handle_submit_job(&mut self,
                         mut storage: SharedJobStorage,
                         remote: Option<usize>,
                         workers: SharedWorkers,
                         priority: JobQueuePriority,
                         job_count: Arc<&AtomicUsize>)
                         -> Result<Option<Packet>> {
        let mut iter = self.iter();
        let fname = iter.next_field()?;
        let unique = iter.next_field()?;
        let data = iter.next_field()?;
        let mut add = false;
        let handle = match storage.coalesce_unique(&unique, remote) {
            Some(handle) => handle,
            None => {
                workers.clone().queue_wake(&fname);
                // H:091234567890
                let mut handle = Vec::with_capacity(12);
                let job_num = job_count.fetch_add(1, Ordering::Relaxed);
                debug!("job_num = {}", job_num);
                handle.extend(format!("H:{:010}", job_num).as_bytes());
                add = true;
                handle
            }
        };
        if add {
            let job = Arc::new(Job::new(fname, unique, data, handle.clone()));
            info!("Created job {:?}", job);
            storage.add_job(job.clone(), priority, remote);
        }
        let p = Packet::new_res(JOB_CREATED, Box::new(handle));
        Ok(Some(p))
    }

    fn handle_work_complete(&mut self,
                            worker: &mut Worker,
                            storage: SharedJobStorage)
                            -> Result<Option<Vec<Packet>>> {
        let mut iter = self.iter();
        let handle = iter.next_field()?;
        let data = iter.next_field()?;
        info!("Job is complete {:?}", String::from_utf8(handle.clone()));
        let mut ret = Ok(None);
        match worker.job() {
            Some(ref mut j) => {
                if j.handle != handle {
                    let msg = "WORK_COMPLETE received for inactive job handle";
                    let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(msg.len() + 1));
                    data.push(b'\0');
                    data.extend_from_slice(msg.as_bytes());
                    let mut packets = Vec::with_capacity(1);
                    packets.push(Packet::new_res(ERROR, data));
                    return Ok(Some(packets));
                }
                {
                    // We need to keep it locked while we iterate so no new threads can join this
                    // already complete job
                    let mut storage = storage.lock().unwrap();
                    match storage.remotes_by_unique(&j.unique) {
                        None => {}
                        Some(remotes) => {
                            let mut packets = Vec::with_capacity(remotes.len());
                            let client_data_len = handle.len() + 1 + data.len();
                            for remote in remotes.iter() {
                                let mut client_data: Box<Vec<u8>> =
                                    Box::new(Vec::with_capacity(client_data_len));
                                client_data.extend(&j.handle);
                                client_data.push(b'\0');
                                client_data.extend(&data);
                                packets.push(Packet::new_res_remote(WORK_COMPLETE,
                                                                    client_data,
                                                                    Some(*remote)));
                            }
                            ret = Ok(Some(packets));
                        }
                    }
                    storage.remove_job(&j.unique);
                }
            }
            None => {
                let msg = "WORK_COMPLETE received but no active jobs";
                let mut data: Box<Vec<u8>> = Box::new(Vec::with_capacity(msg.len() + 1));
                data.push(b'\0');
                data.extend_from_slice(msg.as_bytes());
                let mut packets = Vec::with_capacity(1);
                packets.push(Packet::new_res(ERROR, data));
                return Ok(Some(packets));
            }
        }
        worker.unassign_job();
        ret
    }

    /// Several packets, notable WORK_STATUS, WORK_ERROR, and WORK_DATA, all use the same pattern
    pub fn handle_work_update(&self, storage: SharedJobStorage) -> Result<Option<Vec<Packet>>> {
        let mut iter = self.iter();
        let handle = iter.next_field()?;
        let nargs = PTYPES[self.ptype as usize].nargs;
        // These are unused but we're validating the packet with next_field
        for _ in 0..nargs {
            iter.next_field()?;
        }
        let storage = storage.lock().unwrap();
        match storage.remotes_by_handle(&handle) {
            None => Ok(None),
            Some(remotes) => {
                let mut packets = Vec::with_capacity(remotes.len());
                for remote in remotes.iter() {
                    let packet =
                        Packet::new_res_remote(self.ptype, self.data.clone(), Some(remote.clone()));
                    packets.push(packet);
                }
                Ok(Some(packets))
            }
        }
    }
    */

    pub fn to_byteslice(&self) -> Box<[u8]> {
        let len = 12 + self.psize;
        let mut buf = Vec::with_capacity(len as usize) as Vec<u8>;
        let magic = match self.magic {
            PacketMagic::UNKNOWN => panic!("Unknown packet magic cannot be sent"),
            PacketMagic::REQ => REQ,
            PacketMagic::RES => RES,
            PacketMagic::TEXT => {
                return self.data.clone().into_boxed_slice();
            }
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
        write!(f,
               "Packet {{ magic: {:?}, ptype: {}, size: {}, remote: {:?} }}",
               match self.magic {
                   PacketMagic::REQ => "REQ",
                   PacketMagic::RES => "RES",
                   PacketMagic::TEXT => "TEXT",
                   _ => "UNKNOWN",
               },
               match self.ptype {
                   p @ 0...42 => PTYPES[p as usize].name,
                   _ => "__UNIMPLEMENTED__",
               },
               self.psize,
               self.remote)
    }
}

pub static PTYPES: [PacketType; 43] = [PacketType {
                                           name: "__UNUSED__",
                                           ptype: 0,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "CAN_DO",
                                           ptype: 1,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "CANT_DO",
                                           ptype: 2,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "RESET_ABILITIES",
                                           ptype: 3,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "PRE_SLEEP",
                                           ptype: 4,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "__UNUSED__",
                                           ptype: 5,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "NOOP",
                                           ptype: 6,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB",
                                           ptype: 7,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "JOB_CREATED",
                                           ptype: 8,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "GRAB_JOB",
                                           ptype: 9,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "NO_JOB",
                                           ptype: 10,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "JOB_ASSIGN",
                                           ptype: 11,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "WORK_STATUS",
                                           ptype: 12,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "WORK_COMPLETE",
                                           ptype: 13,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "WORK_FAIL",
                                           ptype: 14,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "GET_STATUS",
                                           ptype: 15,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "ECHO_REQ",
                                           ptype: 16,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "ECHO_RES",
                                           ptype: 17,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_BG",
                                           ptype: 18,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "ERROR",
                                           ptype: 19,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "STATUS_RES",
                                           ptype: 20,
                                           nargs: 4,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_HIGH",
                                           ptype: 21,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "SET_CLIENT_ID",
                                           ptype: 22,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "CAN_DO_TIMEOUT",
                                           ptype: 23,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "ALL_YOURS",
                                           ptype: 24,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "WORK_EXCEPTION",
                                           ptype: 25,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "OPTION_REQ",
                                           ptype: 26,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "OPTION_RES",
                                           ptype: 27,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "WORK_DATA",
                                           ptype: 28,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "WORK_WARNING",
                                           ptype: 29,
                                           nargs: 1,
                                       },
                                       PacketType {
                                           name: "GRAB_JOB_UNIQ",
                                           ptype: 30,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "JOB_ASSIGN_UNIQ",
                                           ptype: 31,
                                           nargs: 3,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_HIGH_BG",
                                           ptype: 32,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_LOW",
                                           ptype: 33,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_LOW_BG",
                                           ptype: 34,
                                           nargs: 2,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_SCHED",
                                           ptype: 35,
                                           nargs: 7,
                                       },
                                       PacketType {
                                           name: "SUBMIT_JOB_EPOCH",
                                           ptype: 36,
                                           nargs: 3,
                                       },
                                       PacketType {
                                           name: "SUBMIT_REDUCE_JOB",
                                           ptype: 37,
                                           nargs: 3,
                                       },
                                       PacketType {
                                           name: "SUBMIT_REDUCE_JOB_BACKGROUND",
                                           ptype: 38,
                                           nargs: 3,
                                       },
                                       PacketType {
                                           name: "GRAB_JOB_ALL",
                                           ptype: 39,
                                           nargs: -1,
                                       },
                                       PacketType {
                                           name: "JOB_ASSIGN_ALL",
                                           ptype: 40,
                                           nargs: 4,
                                       },
                                       PacketType {
                                           name: "GET_STATUS_UNIQUE",
                                           ptype: 41,
                                           nargs: 0,
                                       },
                                       PacketType {
                                           name: "STATUS_RES_UNIQUE",
                                           ptype: 42,
                                           nargs: 5,
                                       }];
