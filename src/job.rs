use std::fmt;

use mio::Token;
use uuid::{Uuid, UuidVersion};

pub struct Job {
   pub handle: Vec<u8>,
   pub fname: Vec<u8>,
   pub unique: Vec<u8>,
   pub data: Vec<u8>,
   pub remotes: Vec<Token>,
}

impl Job {
    pub fn new(fname: Vec<u8>, unique: Vec<u8>, data: Vec<u8>) -> Job {
        Job {
            handle: Uuid::new(UuidVersion::Random).
                unwrap().hyphenated().to_string().into_bytes(),
            fname: fname,
            unique: unique,
            data: data,
            remotes: Vec::with_capacity(1),
        }
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
