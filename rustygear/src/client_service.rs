use std::io;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

use tokio_service::Service;
use bytes::Bytes;
use futures::BoxFuture;
use futures::{Future, future};

use constants::*;
use codec::Packet;
use client::ClientJob;

pub struct ClientService {
    sync_jobs: RefCell<HashMap<Arc<Bytes>, ClientJob>>,
    submitted_jobs: RefCell<VecDeque<ClientJob>>,
}

impl ClientService {
    fn new() -> ClientService {
        ClientService {
            sync_jobs: RefCell::new(HashMap::new()),
            submitted_jobs: RefCell::new(VecDeque::new()),
        }
    }

    fn push_sync_job(&mut self, job: ClientJob) {
        let mut submitted_jobs = self.submitted_jobs.borrow_mut();
        submitted_jobs.push_back(job);
    }

    fn handle_job_created(&self, req: Packet) -> BoxFuture<Option<Packet>, io::Error> {
        let submitted_jobs = self.submitted_jobs.borrow();
        if submitted_jobs.len() < 1 {
            return future::err(io::Error::new(
                io::ErrorKind::Other,
                "No submitted jobs but got JOB_CREATED",
            )).boxed();
        }
        let mut submitted_jobs = self.submitted_jobs.borrow_mut();
        let mut job = submitted_jobs.pop_front().unwrap();
        let handle = req.data;
        job.set_handle(Arc::new(handle));
        let mut sync_jobs = self.sync_jobs.borrow_mut();
        sync_jobs.insert(job.handle().unwrap().clone(), job);
        future::finished(None).boxed()
    }
}

impl Service for ClientService {
    type Request = Packet;
    type Response = Option<Packet>;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        match req.ptype {
            JOB_CREATED => self.handle_job_created(req),
            _ => {
                future::err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unknown packet type received",
                )).boxed()
            }
        }
    }
}
