use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};

use ::job::Job;
use ::worker::Worker;

pub type JobQueue = VecDeque<Weak<Job>>;
pub type JobQueues = HashMap<Vec<u8>, [JobQueue; 3]>;

pub struct JobStorage {
    jobs: HashMap<Vec<u8>, Rc<Job>>, // Owns the job objects forever
    queues: JobQueues,
}

pub type SharedJobStorage = Arc<Mutex<JobStorage>>;

pub trait HandleJobStorage {
    fn new_job_storage() -> SharedJobStorage;
    fn add_job(&mut self, job: Rc<Job>, priority: JobQueuePriority);
    fn get_job(&mut self, worker: &mut Worker) -> bool;
    fn remove_job(&mut self, unique: &Vec<u8>);
}

pub type JobQueuePriority = usize;

const INIT_JOB_STORAGE_CAPACITY: usize = 10000000; // XXX This should be configurable
const INIT_JOB_FUNCTIONS_CAPACITY: usize = 4096; // XXX This should be configurable

impl JobStorage {
    fn new() -> JobStorage {
        JobStorage {
            jobs: HashMap::with_capacity(INIT_JOB_STORAGE_CAPACITY),
            queues: HashMap::with_capacity(INIT_JOB_FUNCTIONS_CAPACITY),
        }
    }

    pub fn queues(&self) -> &JobQueues {
        &self.queues
    }
}

impl HandleJobStorage for SharedJobStorage {
    fn new_job_storage() -> SharedJobStorage {
        Arc::new(Mutex::new(JobStorage::new()))
    }

    fn add_job(&mut self, job: Rc<Job>, priority: JobQueuePriority) {
        let mut storage = self.lock().unwrap();
        if !storage.queues.contains_key(&job.fname) {
            let high_queue = VecDeque::new();
            let norm_queue = VecDeque::new();
            let low_queue = VecDeque::new();
            storage.queues.insert(job.fname.clone(), [high_queue, norm_queue, low_queue]);
        }
        storage.jobs.insert(job.fname.clone(), job.clone());
        storage.queues.get_mut(&job.fname).unwrap()[priority].push_back(Rc::downgrade(&job));
    }

    fn get_job(&mut self, worker: &mut Worker) -> bool {
        let mut storage = self.lock().unwrap();
        let mut job: Option<Rc<Job>> = None;
        debug!("{:?}", &worker);
        for func in worker.iter() {
            debug!("func = {:?}", &func);
            match storage.queues.get_mut(&func) {
                None => {},
                Some(prios) => {
                    debug!("found func with {} priority queues", prios.len());
                    let mut i = 0;
                    for q in prios {
                        debug!("searching priority {}", i);
                        i = i + 1;
                        if !q.is_empty() {
                            debug!("Queue has items! {:?}", q.len());
                            loop {
                                match q.pop_front().unwrap().upgrade() { // Unwrap depends on queue!empty
                                    Some(j) => {
                                        job = Some(j);
                                        break
                                    },
                                    None => {
                                        debug!("Perhaps cancelled job.");
                                    }, // Cancelled job
                                }
                            }
                            match job {
                                None => {},
                                Some(_) => break,
                            }
                        }
                    }
                },
            }
        }
        match job {
            Some(job) => {
                worker.assign_job(&job);
                true
            },
            None => false,
        }
    }

    fn remove_job(&mut self, unique: &Vec<u8>) {
        let mut storage = self.lock().unwrap();
        match storage.jobs.get(unique) {
            None => return,
            Some(j) => {
                match Rc::weak_count(j) {
                    0 => {},
                    a @ _ => {
                        warn!("Removing job with weak references {:?} ({}+{})",
                              j, Rc::strong_count(j), a);
                    },
                }
            }
        }
        storage.jobs.remove(unique);
    }
}
