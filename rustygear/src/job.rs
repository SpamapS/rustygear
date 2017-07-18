use std::fmt;

use bytes::Bytes;

pub struct Job {
    pub handle: Bytes,
    pub fname: Bytes,
    pub unique: Bytes,
    pub data: Bytes,
}

impl Job {
    pub fn new(fname: Bytes, unique: Bytes, data: Bytes, handle: Bytes) -> Job {
        Job {
            handle: handle,
            fname: fname,
            unique: unique,
            data: data,
        }
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        trace!("Dropping {:?}", self);
    }
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Job {{ handle: {}, fname: {}, unique: {}, +{} data }}",
            String::from_utf8_lossy(&self.handle),
            String::from_utf8_lossy(&self.fname),
            String::from_utf8_lossy(&self.unique),
            self.data.len()
        )
    }
}
