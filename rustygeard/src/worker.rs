extern crate wrappinghashset;

use self::wrappinghashset::{WrappingHashSet, Iter};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use rustygear::job::Job;

#[derive(Debug)]
pub struct WorkerSet {
    inactive: HashSet<usize>,
    active: HashSet<usize>,
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
    allworkers: HashMap<Bytes, WorkerSet>,
    wakeworkers: HashSet<usize>,
}

pub type SharedWorkers = Arc<Mutex<Workers>>;

pub trait Wake {
    fn new_workers() -> Self;
    fn queue_wake(&mut self, &Bytes) -> Vec<usize>;
    fn wakeworkers_drain(&mut self) -> Vec<usize>;
    fn sleep(&mut self, &mut Worker, usize);
    fn wakeup(&mut self, &mut Worker, usize);
    fn count_workers(&mut self, &Bytes) -> (usize, usize);
    fn shutdown(&mut self, conn_id: usize);
}

impl Wake for SharedWorkers {
    fn new_workers() -> SharedWorkers {
        Arc::new(Mutex::new(Workers::new()))
    }

    fn queue_wake(&mut self, fname: &Bytes) -> Vec<usize> {
        let mut workers = self.lock().unwrap();
        debug!("allworkers({:?}) = {:?}", fname, workers.allworkers);
        match workers.allworkers.get_mut(fname) {
            None => Vec::new(),
            Some(workerset) => {
                // Copy the contents into active
                workerset.active.extend(workerset.inactive.iter());
                info!("Waking up inactive workers: {:?}", &workerset.inactive);
                // Empty the contents into inserts
                workerset.inactive.drain().collect()
            }
        }
    }

    fn wakeworkers_drain(&mut self) -> Vec<usize> {
        let mut workers = self.lock().unwrap();
        let to_drain: Vec<usize> = workers.wakeworkers.drain().collect();
        to_drain
    }

    fn sleep(&mut self, worker: &mut Worker, remote: usize) {
        debug!("Sleeping with fnames = {:?}", worker.functions);
        let mut workers = self.lock().unwrap();
        for fname in worker.iter() {
            let mut add = false;
            debug!(
                "Looking for {:?} in workers.allworkers, {:?}",
                &fname,
                &workers.allworkers
            );
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

    fn wakeup(&mut self, worker: &mut Worker, remote: usize) {
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

    fn count_workers(&mut self, fname: &Bytes) -> (usize, usize) {
        let workers = self.lock().unwrap();
        match workers.allworkers.get(fname) {
            None => (0, 0),
            Some(workerset) => (workerset.active.len(), workerset.inactive.len()),
        }
    }

    fn shutdown(&mut self, conn_id: usize) {
        let mut workers = self.lock().unwrap();
        for (_, workerset) in workers.allworkers.iter_mut() {
            workerset.inactive.remove(&conn_id);
            workerset.active.remove(&conn_id);
        }
        trace!(
            "Shutdown complete for {}. Left: {:?}",
            conn_id,
            workers.allworkers
        );
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
    pub functions: WrappingHashSet<Bytes>,
    jobs: HashMap<Bytes, Arc<Job>>,
}

impl Worker {
    pub fn new() -> Worker {
        Worker {
            functions: WrappingHashSet::new(),
            jobs: HashMap::new(),
        }
    }

    pub fn can_do(&mut self, fname: Bytes) {
        self.functions.insert(fname);
    }

    pub fn cant_do<'b>(&mut self, fname: &'b Bytes) {
        self.functions.remove(fname);
    }

    pub fn iter<'i>(&'i mut self) -> Iter<'i, Bytes> {
        self.functions.iter()
    }

    pub fn assign_job(&mut self, job: &Arc<Job>) {
        self.jobs.insert(job.handle.clone(), job.clone());
    }

    pub fn unassign_job(&mut self, handle: &Bytes) {
        match self.jobs.remove(handle) {
            None => warn!("Worker was not assigned {:?}", handle),
            Some(ref j) => {
                match Arc::weak_count(j) {
                    0 => {}
                    a @ _ => {
                        warn!(
                            "Unassigning queued {:?} ({}+{} refs)",
                            j,
                            Arc::strong_count(j),
                            a
                        );
                    }
                }
            }
        }
    }

    pub fn get_assigned_job(&self, handle: &Bytes) -> Option<&Arc<Job>> {
        self.jobs.get(handle)
    }
}
