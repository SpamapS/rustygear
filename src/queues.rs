use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};

use job::Job;
use worker::Worker;

pub type JobQueue = VecDeque<Job>;
pub type JobQueues = Arc<Mutex<HashMap<Vec<u8>, [JobQueue; 3]>>>;


pub trait HandleJobs {
    fn new_queues() -> Self;
    fn add_job(&mut self, job: Job);
    fn get_job(&mut self, worker: &mut Worker) -> bool;
}

impl HandleJobs for JobQueues {
    fn new_queues() -> JobQueues {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn add_job(&mut self, job: Job) {
        let mut queues = self.lock().unwrap();
        if !queues.contains_key(&job.fname) {
            let high_queue = VecDeque::new();
            let norm_queue = VecDeque::new();
            let low_queue = VecDeque::new();
            queues.insert(job.fname.clone(), [high_queue, norm_queue, low_queue]);
        }
        queues.get_mut(&job.fname).unwrap()[1].push_back(job)
    }

    fn get_job(&mut self, worker: &mut Worker) -> bool {
        let mut queues = self.lock().unwrap();
        let mut job: Option<Job> = None;
        for func in worker.iter() {
            debug!("looking for func={:?} in {:?}",
                   func,
                   *queues);
            match queues.get_mut(&func) {
                None => {},
                Some(prios) => {
                    debug!("found func with {} priority queues", prios.len());
                    let mut i = 0;
                    for q in prios {
                        debug!("searching priority {}", i);
                        i = i + 1;
                        if !q.is_empty() {
                            job = q.pop_front();
                            break
                        }
                    }
                },
            }
        }
        match job {
            Some(job) => {
                worker.job = Some(job);
                true
            },
            None => false,
        }
    }
}
