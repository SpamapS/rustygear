use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, VecDeque};

use job::Job;

type JobQueue = VecDeque<Job>;

#[derive(Clone)]
pub struct QueueHolder {
    pub queues: Arc<Mutex<HashMap<Vec<u8>, [JobQueue; 3]>>>,
}

impl QueueHolder {
    pub fn new() -> QueueHolder {
        QueueHolder {
            queues: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_job(&mut self, job: Job) {
        let mut ql = self.queues.clone();
        let mut queues = ql.lock().unwrap();
        if !queues.contains_key(&job.fname) {
            let high_queue = VecDeque::new();
            let norm_queue = VecDeque::new();
            let low_queue = VecDeque::new();
            queues.insert(job.fname.clone(), [high_queue, norm_queue, low_queue]);
        }
        queues.get_mut(&job.fname).unwrap()[1].push_back(job)
    }

    pub fn get_job(&mut self, funcs: &HashSet<Vec<u8>>) -> Option<Job> {
        let mut ql = self.queues.clone();
        let mut queues = ql.lock().unwrap();
        for func in funcs.iter() {
            debug!("looking for func={:?} in {:?}",
                   func,
                   *queues);
            match queues.get_mut(func) {
                None => return None,
                Some(prios) => {
                    debug!("found func with {} priority queues", prios.len());
                    let mut i = 0;
                    for q in prios {
                        debug!("searching priority {}", i);
                        i = i + 1;
                        if !q.is_empty() {
                            return q.pop_front();
                        }
                    }
                },
            }
        }
        None
    }
}
