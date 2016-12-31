use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};

use job::Job;
use worker::Worker;

pub type JobQueue = VecDeque<Job>;
pub type JobQueues = Arc<Mutex<HashMap<Vec<u8>, [JobQueue; 3]>>>;

#[derive(Clone)]
pub struct QueueHolder {
    pub queues: JobQueues,
}

impl QueueHolder {
    pub fn new() -> QueueHolder {
        QueueHolder {
            queues: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_job(&mut self, job: Job) {
        let ql = self.queues.clone();
        let mut queues = ql.lock().unwrap();
        if !queues.contains_key(&job.fname) {
            let high_queue = VecDeque::new();
            let norm_queue = VecDeque::new();
            let low_queue = VecDeque::new();
            queues.insert(job.fname.clone(), [high_queue, norm_queue, low_queue]);
        }
        queues.get_mut(&job.fname).unwrap()[1].push_back(job)
    }

    pub fn get_job(&mut self, worker: &mut Worker) -> bool {
        let ql = self.queues.clone();
        let mut queues = ql.lock().unwrap();
        for func in worker.functions.iter() {
            debug!("looking for func={:?} in {:?}",
                   func,
                   *queues);
            match queues.get_mut(func) {
                None => return false,
                Some(prios) => {
                    debug!("found func with {} priority queues", prios.len());
                    let mut i = 0;
                    for q in prios {
                        debug!("searching priority {}", i);
                        i = i + 1;
                        if !q.is_empty() {
                            worker.job = q.pop_front();
                            return true;
                        }
                    }
                },
            }
        }
        false
    }
}
