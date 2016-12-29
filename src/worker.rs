use std::collections::HashSet;

pub struct Worker {
    pub functions: HashSet<Vec<u8>>,
}

impl Worker {
    pub fn new() -> Worker {
        Worker {
            functions: HashSet::new(),
        }
    }
}
