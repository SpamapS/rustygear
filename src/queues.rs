use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, VecDeque};

use mio::Token;

use ::job::Job;
use ::worker::Worker;

pub type JobQueue = VecDeque<Rc<Job>>;
pub type JobQueues = HashMap<Vec<u8>, [JobQueue; 3]>;

pub struct JobStorage {
    jobs: HashMap<Vec<u8>, Rc<Job>>, // Owns the job objects forever
    queues: JobQueues,
    remotes: HashMap<Vec<u8>, HashSet<Token>>,
}

pub type SharedJobStorage = Arc<Mutex<JobStorage>>;

pub trait HandleJobStorage {
    fn new_job_storage() -> SharedJobStorage;
    fn coalesce_unique(&mut self, unique: &Vec<u8>, remote: Option<Token>) -> Option<Vec<u8>>;
    fn add_job(&mut self, job: Rc<Job>, priority: JobQueuePriority, remote: Option<Token>);
    fn get_job(&mut self, worker: &mut Worker) -> bool;
}

pub type JobQueuePriority = usize;

const INIT_JOB_STORAGE_CAPACITY: usize = 10000000; // XXX This should be configurable
const INIT_JOB_FUNCTIONS_CAPACITY: usize = 4096; // XXX This should be configurable
const INIT_JOB_REMOTES_CAPACITY: usize = 8;

impl JobStorage {
    fn new() -> JobStorage {
        JobStorage {
            jobs: HashMap::with_capacity(INIT_JOB_STORAGE_CAPACITY),
            queues: HashMap::with_capacity(INIT_JOB_FUNCTIONS_CAPACITY),
            remotes: HashMap::with_capacity(INIT_JOB_STORAGE_CAPACITY),
        }
    }

    pub fn queues(&self) -> &JobQueues {
        &self.queues
    }

    pub fn remove_job(&mut self, unique: &Vec<u8>) {
        self.jobs.remove(unique);
        self.remotes.remove(unique);
    }

    pub fn remotes_by_unique(&self, unique: &Vec<u8>) -> Option<&HashSet<Token>> {
        self.remotes.get(unique)
    }
}

impl HandleJobStorage for SharedJobStorage {
    fn new_job_storage() -> SharedJobStorage {
        Arc::new(Mutex::new(JobStorage::new()))
    }

    fn coalesce_unique(&mut self, unique: &Vec<u8>, remote: Option<Token>) -> Option<Vec<u8>> {
        let mut storage = self.lock().unwrap();
        let handle = match storage.jobs.get(unique) {
            None => return None,
            Some(job) => job.handle.clone(),
        };
        match storage.remotes.get_mut(unique) {
            None => warn!("Job with no remote storage found: {:?}", &handle),
            Some(remotes) => {
                match remote {
                    None => {},
                    Some(remote) => {
                        remotes.insert(remote);
                    },
                };
            },
        }
        Some(handle)
    }


    fn add_job(&mut self, job: Rc<Job>, priority: JobQueuePriority, remote: Option<Token>) {
        let mut storage = self.lock().unwrap();
        if !storage.queues.contains_key(&job.fname) {
            let high_queue = VecDeque::new();
            let norm_queue = VecDeque::new();
            let low_queue = VecDeque::new();
            storage.queues.insert(job.fname.clone(), [high_queue, norm_queue, low_queue]);
        }
        storage.jobs.insert(job.unique.clone(), job.clone());
        match remote {
            None => {},
            Some(remote) => {
                let mut remotes = HashSet::with_capacity(INIT_JOB_REMOTES_CAPACITY);
                remotes.insert(remote);
                storage.remotes.insert(job.unique.clone(), remotes);
            },
        }
        storage.queues.get_mut(&job.fname).unwrap()[priority].push_back(job.clone());
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
                            job = q.pop_front();
                            break
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

}
