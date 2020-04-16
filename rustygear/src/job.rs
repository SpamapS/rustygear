/*
 * Copyright 2017 Clint Byrum
 * Copyright (c) 2015, Hewlett Packard Development Company L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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
