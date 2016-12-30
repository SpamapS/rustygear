use std::collections::HashSet;

use job::Job;

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
