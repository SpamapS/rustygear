use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

use mio::Token;

use constants::*;
use packet::*;
use job::Job;

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
    fn sleep(&mut self, &Worker, Token);
    fn wakeup(&mut self, &Worker, Token);
}

impl Wake for SharedWorkers {
    fn new_workers() -> SharedWorkers {
        Arc::new(Mutex::new(Workers::new()))
    }

    fn queue_wake(&mut self, fname: &Vec<u8>) {
        let mut workers = self.lock().unwrap();
        let mut inserts = Vec::new();
        match workers.allworkers.get_mut(fname) {
            None => {},
            Some(workerset) => {
                for worker in workerset.inactive.iter() {
                    inserts.push(worker.clone());
                }
            }
        }
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

    fn sleep(&mut self, worker: &Worker, remote: Token) {
        let mut workers = self.lock().unwrap();
        for fname in worker.functions.iter() {
            let mut add = false;
            match workers.allworkers.get_mut(fname) {
                None => {
                    add = true;
                },
                Some(workerset) => {
                    workerset.active.remove(&remote);
                    workerset.inactive.insert(remote);
                },
            };
            if add {
                let mut workerset = WorkerSet::new();
                workerset.active.insert(remote);
                workers.allworkers.insert(fname.clone(), workerset);
            }
        }
    }

    fn wakeup(&mut self, worker: &Worker, remote: Token) {
        let mut workers = self.lock().unwrap();
        for fname in worker.functions.iter() {
            let mut add = false;
            match workers.allworkers.get_mut(fname) {
                None => {
                    add = true;
                },
                Some(workerset) => {
                    workerset.inactive.remove(&remote);
                    workerset.active.insert(remote);
                },
            };
            if add {
                let mut workerset = WorkerSet::new();
                workerset.inactive.insert(remote);
                workers.allworkers.insert(fname.clone(), workerset);
            }
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

pub struct Worker {
    pub functions: HashSet<Vec<u8>>,
    pub job: Option<Job>,
}

impl Worker {
    pub fn new() -> Worker {
        Worker {
            functions: HashSet::new(),
            job: None,
        }
    }
}
