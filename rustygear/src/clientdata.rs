use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::{Sender, Receiver, channel};

use crate::client::{WorkUpdate, WorkerJob, JobStatus};

pub struct ClientData {
    senders_by_handle: HashMap<Bytes, Sender<WorkUpdate>>,
    jobs_tx_by_func: HashMap<Vec<u8>, Sender<WorkerJob>>,
    echo_tx: Sender<Bytes>,
    echo_rx: Receiver<Bytes>,
    job_created_tx: Sender<Bytes>,
    job_created_rx: Receiver<Bytes>,
    status_res_tx: Sender<JobStatus>,
    status_res_rx: Receiver<JobStatus>,
    error_tx: Sender<(Bytes, Bytes)>,
    error_rx: Receiver<(Bytes, Bytes)>,
    worker_job_tx: Sender<WorkerJob>,
    worker_job_rx: Receiver<WorkerJob>,
}

// Everything we might need to store or access about the client is here.
impl ClientData {
    pub fn new() -> ClientData {
        const CLIENT_CHANNEL_BOUND_SIZE: usize = 100;
        let (echo_tx, echo_rx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        let (job_created_tx, job_created_rx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        let (status_res_tx, status_res_rx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        let (error_tx, error_rx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        let (worker_job_tx, worker_job_rx) = channel(CLIENT_CHANNEL_BOUND_SIZE);
        ClientData {
            senders_by_handle: HashMap::new(),
            jobs_tx_by_func: HashMap::new(),
            echo_tx: echo_tx,
            echo_rx: echo_rx,
            job_created_tx: job_created_tx,
            job_created_rx: job_created_rx,
            status_res_tx: status_res_tx,
            status_res_rx: status_res_rx,
            error_tx: error_tx,
            error_rx: error_rx,
            worker_job_tx: worker_job_tx,
            worker_job_rx: worker_job_rx,
        }
    }

    pub fn echo_tx(&self) -> Sender<Bytes> {
        self.echo_tx.clone()
    }

    pub fn echo_rx(&mut self) -> &mut Receiver<Bytes> {
        &mut self.echo_rx
    }

    pub fn job_created_tx(&self) -> Sender<Bytes> {
        self.job_created_tx.clone()
    }

    pub fn job_created_rx(&mut self) -> &mut Receiver<Bytes> {
        &mut self.job_created_rx
    }

    pub fn error_tx(&self) -> Sender<(Bytes, Bytes)> {
        self.error_tx.clone()
    }

    pub fn error_rx(&mut self) -> &mut Receiver<(Bytes, Bytes)> {
        &mut self.error_rx
    }

    pub fn status_res_tx(&self) -> Sender<JobStatus> {
        self.status_res_tx.clone()
    }

    pub fn status_res_rx(&mut self) -> &mut Receiver<JobStatus> {
        &mut self.status_res_rx
    }

    pub fn worker_job_tx(&self) -> Sender<WorkerJob> {
        self.worker_job_tx.clone()
    }

    pub fn worker_job_rx(&mut self) -> &mut Receiver<WorkerJob> {
        &mut self.worker_job_rx
    }

    pub fn get_sender_by_handle(&self, handle: &Bytes) -> Option<Sender<WorkUpdate>> {
        match self.senders_by_handle.get(handle) {
            None => None,
            Some(sender) => Some(sender.clone()),
        }
    }

    pub fn set_sender_by_handle(&mut self, handle: Bytes, tx: Sender<WorkUpdate>) {
        self.senders_by_handle.insert(handle, tx);
    }

    pub fn get_jobs_tx_by_func(&self, func: &Vec<u8>) -> Option<Sender<WorkerJob>> {
        match self.jobs_tx_by_func.get(func) {
            None => None,
            Some(tx) => Some(tx.clone()),
        }
    }

    pub fn set_jobs_tx_by_func(&mut self, func: Vec<u8>, tx: Sender<WorkerJob>) {
        self.jobs_tx_by_func.insert(func, tx);
    }
}