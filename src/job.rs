use std::fmt;

use uuid::{Uuid, UuidVersion};

pub struct Job {
   pub handle: Vec<u8>,
   pub fname: Vec<u8>,
   pub unique: Vec<u8>,
   pub data: Vec<u8>,
}

impl Job {
    pub fn new(fname: Vec<u8>, unique: Vec<u8>, data: Vec<u8>) -> Job {
        Job {
            handle: Uuid::new(UuidVersion::Random).
                unwrap().hyphenated().to_string().into_bytes(),
            fname: fname,
            unique: unique,
            data: data,
        }
    }
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Job {{ handle: {}, fname: {}, unique: {}, +{} data }}",
               String::from_utf8_lossy(&self.handle),
               String::from_utf8_lossy(&self.fname),
               String::from_utf8_lossy(&self.unique),
               self.data.len())
    }
}
