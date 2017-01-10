use std::fmt;
use std::collections::HashSet;
use std::collections::hash_set;

use mio::Token;

pub struct Job {
   pub handle: Vec<u8>,
   pub fname: Vec<u8>,
   pub unique: Vec<u8>,
   pub data: Vec<u8>,
   remotes: HashSet<Token>,
}

impl Job {
    pub fn new(fname: Vec<u8>, unique: Vec<u8>, data: Vec<u8>, handle: Vec<u8>) -> Job {
        Job {
            handle: handle,
            fname: fname,
            unique: unique,
            data: data,
            remotes: HashSet::with_capacity(1),
        }
    }
    pub fn add_remote(&mut self, remote: Token) {
        self.remotes.insert(remote);
    }
    pub fn remove_remote(&mut self, remote: &Token) {
        self.remotes.remove(remote);
    }
    pub fn iter_remotes(&self) -> hash_set::Iter<Token> {
        self.remotes.iter()
    }
    pub fn len_remotes(&self) -> usize {
        self.remotes.len()
    }
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Job {{ handle: {}, fname: {}, unique: {}, remote: {:?}, +{} data }}",
               String::from_utf8_lossy(&self.handle),
               String::from_utf8_lossy(&self.fname),
               String::from_utf8_lossy(&self.unique),
               &self.remotes,
               self.data.len())
    }
}
