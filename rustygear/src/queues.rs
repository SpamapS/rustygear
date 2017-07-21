use std::sync::{Arc, Mutex, Weak};
use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;

use job::Job;

use worker::Worker;

pub type JobQueue = VecDeque<Weak<Job>>;
pub type JobQueues = HashMap<Bytes, [JobQueue; 3]>;

pub struct JobStorage {
    jobs: HashMap<Bytes, Arc<Job>>, // Owns the job objects forever
    queues: JobQueues,
    remotes_by_unique: HashMap<Bytes, HashSet<usize>>,
    remotes_by_handle: HashMap<Bytes, Vec<usize>>,
}

pub type SharedJobStorage = Arc<Mutex<JobStorage>>;

pub trait HandleJobStorage {
    fn new_job_storage() -> SharedJobStorage;
    fn coalesce_unique(&mut self, unique: &Bytes, remote: Option<usize>) -> Option<Bytes>;
    fn add_job(&mut self, job: Arc<Job>, priority: JobQueuePriority, remote: Option<usize>);
    fn get_job(&mut self, worker: &mut Worker) -> Option<Arc<Job>>;
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
            remotes_by_unique: HashMap::with_capacity(INIT_JOB_STORAGE_CAPACITY),
            remotes_by_handle: HashMap::with_capacity(INIT_JOB_STORAGE_CAPACITY),
        }
    }

    pub fn queues(&self) -> &JobQueues {
        &self.queues
    }

    pub fn remove_job(&mut self, unique: &Bytes) {
        match self.jobs.get(unique) {
            None => {}
            Some(job) => {
                self.remotes_by_handle.remove(&job.handle);
            }
        }
        self.jobs.remove(unique);
        self.remotes_by_unique.remove(unique);
    }

    pub fn remotes_by_unique(&self, unique: &Bytes) -> Option<&HashSet<usize>> {
        self.remotes_by_unique.get(unique)
    }

    pub fn remotes_by_handle(&self, handle: &Bytes) -> Option<&Vec<usize>> {
        self.remotes_by_handle.get(handle)
    }
}

impl HandleJobStorage for SharedJobStorage {
    fn new_job_storage() -> SharedJobStorage {
        Arc::new(Mutex::new(JobStorage::new()))
    }

    fn coalesce_unique(&mut self, unique: &Bytes, remote: Option<usize>) -> Option<Bytes> {
        let mut storage = self.lock().unwrap();
        let handle = match storage.jobs.get(unique) {
            None => return None,
            Some(job) => job.handle.clone(),
        };
        let mut add_remote = false;
        match storage.remotes_by_unique.get_mut(unique) {
            None => warn!("Job with no remote storage found: {:?}", &handle),
            Some(remotes) => {
                match remote {
                    None => {}
                    Some(remote) => {
                        add_remote = remotes.insert(remote);
                    }
                };
            }
        }
        // We don't need two hashsets for the same set, so just push if we added to the unique set
        if add_remote {
            match storage.remotes_by_handle.get_mut(&handle) {
                None => warn!("Job with no remote storage found: {:?}", &handle),
                Some(remotes) => {
                    match remote {
                        None => {}
                        Some(remote) => {
                            remotes.push(remote);
                        }
                    };
                }
            }
        }
        Some(handle)
    }


    fn add_job(&mut self, job: Arc<Job>, priority: JobQueuePriority, remote: Option<usize>) {
        let job = job.clone();
        trace!(
            "job {:?} weak = {} strong = {}",
            &job,
            Arc::weak_count(&job),
            Arc::strong_count(&job)
        );
        let mut storage = self.lock().unwrap();
        {
            let func_queues = storage.queues.entry(job.fname.clone()).or_insert_with(|| {
                let high_queue = VecDeque::new();
                let norm_queue = VecDeque::new();
                let low_queue = VecDeque::new();
                [high_queue, norm_queue, low_queue]
            });
            func_queues[priority].push_back(Arc::downgrade(&job.clone()));
        }
        storage.jobs.insert(job.unique.clone(), job.clone());
        trace!(
            "job {:?} weak = {} strong = {}",
            &job,
            Arc::weak_count(&job),
            Arc::strong_count(&job)
        );
        let mut remotes_by_unique = HashSet::with_capacity(INIT_JOB_REMOTES_CAPACITY);
        let mut remotes_by_handle = Vec::with_capacity(INIT_JOB_REMOTES_CAPACITY);
        match remote {
            None => {}
            Some(remote) => {
                remotes_by_unique.insert(remote);
                remotes_by_handle.push(remote);
            }
        }
        storage.remotes_by_unique.insert(
            job.unique.clone(),
            remotes_by_unique,
        );
        storage.remotes_by_handle.insert(
            job.handle.clone(),
            remotes_by_handle,
        );
        trace!(
            "job {:?} weak = {} strong = {}",
            &job,
            Arc::weak_count(&job),
            Arc::strong_count(&job)
        );
    }

    fn get_job(&mut self, worker: &mut Worker) -> Option<Arc<Job>> {
        let mut storage = self.lock().unwrap();
        let mut job: Option<Arc<Job>> = None;
        debug!("{:?}", &worker);
        for func in worker.iter() {
            debug!("func = {:?}", &func);
            match storage.queues.get_mut(&func) {
                None => {}
                Some(prios) => {
                    debug!("found func with {} priority queues", prios.len());
                    let mut i = 0;
                    for q in prios {
                        debug!("searching priority {}", i);
                        i = i + 1;
                        loop {
                            debug!("Queue has items! {:?}", q.len());
                            match q.pop_front() {
                                None => break,
                                Some(a_job) => {
                                    match a_job.upgrade() {
                                        None => trace!("Deleted job encountered."),
                                        Some(a_job) => {
                                            job = Some(a_job);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if job.is_some() {
                            break;
                        }
                    }
                }
            }
        }
        match job {
            Some(job) => {
                worker.assign_job(&job);
                Some(job)
            }
            None => None,
        }
    }
}
