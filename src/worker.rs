extern crate wrappinghashset;

use self::wrappinghashset::{WrappingHashSet, Iter};

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use mio::Token;

use ::constants::*;
use ::packet::*;
use job::Job;

#[derive(Debug)]
pub struct WorkerSet {
    inactive: HashSet<Token>,
    active: HashSet<Token>,
}

impl WorkerSet {
    pub fn new() -> WorkerSet {
        WorkerSet {
            inactive: HashSet::new(),
            active: HashSet::new(),
        }
    }
}

pub struct Workers {
    allworkers: HashMap<Vec<u8>, WorkerSet>,
    wakeworkers: HashSet<Token>,
}

pub type SharedWorkers = Arc<Mutex<Workers>>;

pub trait Wake {
    fn new_workers() -> Self;
    fn queue_wake(&mut self, &Vec<u8>);
    fn do_wakes(&mut self) -> Vec<Packet>;
    fn sleep(&mut self, &mut Worker, Token);
    fn wakeup(&mut self, &mut Worker, Token);
    fn count_workers(&mut self, &Vec<u8>) -> (usize, usize);
    fn shutdown(&mut self, remote: &Token);
}

impl Wake for SharedWorkers {
    fn new_workers() -> SharedWorkers {
        Arc::new(Mutex::new(Workers::new()))
    }

    fn queue_wake(&mut self, fname: &Vec<u8>) {
        let mut workers = self.lock().unwrap();
        let mut inserts = Vec::new();
        debug!("allworkers({:?}) = {:?}", fname, workers.allworkers);
        match workers.allworkers.get_mut(fname) {
            None => {}
            Some(workerset) => {
                // Copy the contents into active
                workerset.active.extend(workerset.inactive.iter());
                // Empty the contents into inserts
                for worker in workerset.inactive.drain() {
                    inserts.push(worker.clone());
                }
            }
        }
        debug!("Queueing inserts {:?}", inserts);
        workers.wakeworkers.extend(inserts);
    }

    fn do_wakes(&mut self) -> Vec<Packet> {
        let mut workers = self.lock().unwrap();
        let mut packets = Vec::with_capacity(workers.wakeworkers.len());
        for worker in workers.wakeworkers.drain() {
            packets.push(Packet::new_res_remote(NOOP, Box::new(Vec::new()), Some(worker)));
        }
        packets
    }

    fn sleep(&mut self, worker: &mut Worker, remote: Token) {
        debug!("Sleeping with fnames = {:?}", worker.functions);
        let mut workers = self.lock().unwrap();
        for fname in worker.iter() {
            let mut add = false;
            debug!("Looking for {:?} in workers.allworkers, {:?}",
                   &fname,
                   &workers.allworkers);
            match workers.allworkers.get_mut(&fname) {
                None => {
                    debug!("Did not find {:?}", &fname);
                    add = true;
                }
                Some(workerset) => {
                    debug!("Deactivating remote {:?} from fname {:?}", remote, &fname);
                    workerset.active.remove(&remote);
                    workerset.inactive.insert(remote);
                }
            };
            if add {
                debug!("Adding new workerset {:?} from fname {:?}", remote, &fname);
                let mut workerset = WorkerSet::new();
                workerset.inactive.insert(remote);
                workers.allworkers.insert(fname.clone(), workerset);
            }
        }
    }

    fn wakeup(&mut self, worker: &mut Worker, remote: Token) {
        let mut workers = self.lock().unwrap();
        for fname in worker.iter() {
            let mut add = false;
            match workers.allworkers.get_mut(&fname) {
                None => {
                    add = true;
                }
                Some(workerset) => {
                    workerset.inactive.remove(&remote);
                    workerset.active.insert(remote);
                }
            };
            if add {
                let mut workerset = WorkerSet::new();
                workerset.active.insert(remote);
                workers.allworkers.insert(fname.clone(), workerset);
            }
        }
    }

    fn count_workers(&mut self, fname: &Vec<u8>) -> (usize, usize) {
        let workers = self.lock().unwrap();
        match workers.allworkers.get(fname) {
            None => (0, 0),
            Some(workerset) => (workerset.active.len(), workerset.inactive.len()),
        }
    }

    fn shutdown(&mut self, remote: &Token) {
        let mut workers = self.lock().unwrap();
        for (_, workerset) in workers.allworkers.iter_mut() {
            workerset.inactive.remove(&remote);
            workerset.active.remove(&remote);
        }
    }
}

impl Workers {
    fn new() -> Workers {
        Workers {
            allworkers: HashMap::new(),
            wakeworkers: HashSet::new(),
        }
    }
}

#[derive(Debug)]
pub struct Worker {
    pub functions: WrappingHashSet<Vec<u8>>,
    job: Option<Rc<Job>>,
}

impl Worker {
    pub fn new() -> Worker {
        Worker {
            functions: WrappingHashSet::new(),
            job: None,
        }
    }

    pub fn can_do(&mut self, fname: Vec<u8>) {
        self.functions.insert(fname);
    }

    pub fn cant_do<'b>(&mut self, fname: &'b Vec<u8>) {
        self.functions.remove(fname);
    }

    pub fn iter<'i>(&'i mut self) -> Iter<'i, Vec<u8>> {
        self.functions.iter()
    }

    pub fn assign_job(&mut self, job: &Rc<Job>) {
        self.job = Some(job.clone());
    }

    pub fn unassign_job(&mut self) {
        match self.job {
            None => {}
            Some(ref j) => {
                match Rc::weak_count(j) {
                    0 => {}
                    a @ _ => {
                        warn!("Unassigning queued {:?} ({}+{} refs)",
                              j,
                              Rc::strong_count(j),
                              a);
                    }
                }
            }
        }
        self.job = None;
    }


    pub fn job(&mut self) -> Option<Rc<Job>> {
        self.job.clone()
    }
}
