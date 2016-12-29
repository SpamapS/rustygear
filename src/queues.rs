use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};

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
}
