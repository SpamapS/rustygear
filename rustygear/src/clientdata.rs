use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::{
    client::{JobStatus, WorkUpdate, WorkerJob},
    conn::ServerHandle,
};

pub type JobCreated = ServerHandle;

#[derive(Debug)]
pub struct ClientReceivers {
    pub echo_rx: Receiver<Bytes>,
    pub job_created_rx: Receiver<JobCreated>,
    pub status_res_rx: Receiver<JobStatus>,
    pub error_rx: Receiver<(Bytes, Bytes)>,
    pub worker_job_rx: Receiver<WorkerJob>,
}

#[derive(Debug)]
struct ClientSenders {
    pub senders_by_handle: HashMap<ServerHandle, Sender<WorkUpdate>>,
    pub jobs_tx_by_func: HashMap<Vec<u8>, Sender<WorkerJob>>,
    pub echo_tx: Sender<Bytes>,
    pub job_created_tx: Sender<JobCreated>,
    pub status_res_tx: Sender<JobStatus>,
    pub error_tx: Sender<(Bytes, Bytes)>,
    pub worker_job_tx: Sender<WorkerJob>,
}

impl ClientSenders {
    pub fn new(
        echo_tx: Sender<Bytes>,
        job_created_tx: Sender<JobCreated>,
        status_res_tx: Sender<JobStatus>,
        error_tx: Sender<(Bytes, Bytes)>,
        worker_job_tx: Sender<WorkerJob>,
    ) -> ClientSenders {
        ClientSenders {
            senders_by_handle: HashMap::new(),
            jobs_tx_by_func: HashMap::new(),
            echo_tx: echo_tx,
            job_created_tx: job_created_tx,
            status_res_tx: status_res_tx,
            error_tx: error_tx,
            worker_job_tx: worker_job_tx,
        }
    }
}

#[derive(Debug)]
pub struct ClientData {
    receivers: Arc<Mutex<ClientReceivers>>,
    senders: Arc<RwLock<ClientSenders>>,
}

impl Clone for ClientData {
    fn clone(&self) -> Self {
        Self {
            receivers: self.receivers.clone(),
            senders: self.senders.clone(),
        }
    }
}

impl ClientReceivers {
    pub fn new(
        echo_rx: Receiver<Bytes>,
        job_created_rx: Receiver<JobCreated>,
        status_res_rx: Receiver<JobStatus>,
        error_rx: Receiver<(Bytes, Bytes)>,
        worker_job_rx: Receiver<WorkerJob>,
    ) -> ClientReceivers {
        ClientReceivers {
            echo_rx: echo_rx,
            job_created_rx: job_created_rx,
            status_res_rx: status_res_rx,
            error_rx: error_rx,
            worker_job_rx: worker_job_rx,
        }
    }
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
            receivers: Arc::new(Mutex::new(ClientReceivers::new(
                echo_rx,
                job_created_rx,
                status_res_rx,
                error_rx,
                worker_job_rx,
            ))),
            senders: Arc::new(RwLock::new(ClientSenders::new(
                echo_tx,
                job_created_tx,
                status_res_tx,
                error_tx,
                worker_job_tx,
            ))),
        }
    }

    pub fn receivers(&self) -> MutexGuard<ClientReceivers> {
        trace!("Locking receivers");
        self.receivers.lock().unwrap()
    }

    pub fn echo_tx(&self) -> Sender<Bytes> {
        self.senders.read().unwrap().echo_tx.clone()
    }

    pub fn job_created_tx(&self) -> Sender<JobCreated> {
        self.senders.read().unwrap().job_created_tx.clone()
    }

    pub fn error_tx(&self) -> Sender<(Bytes, Bytes)> {
        self.senders.read().unwrap().error_tx.clone()
    }

    pub fn status_res_tx(&self) -> Sender<JobStatus> {
        self.senders.read().unwrap().status_res_tx.clone()
    }

    pub fn worker_job_tx(&self) -> Sender<WorkerJob> {
        self.senders.read().unwrap().worker_job_tx.clone()
    }

    pub fn get_sender_by_handle(&self, handle: &ServerHandle) -> Option<Sender<WorkUpdate>> {
        match self.senders.read().unwrap().senders_by_handle.get(handle) {
            None => None,
            Some(sender) => Some(sender.clone()),
        }
    }

    pub fn set_sender_by_handle(&mut self, handle: ServerHandle, tx: Sender<WorkUpdate>) {
        self.senders
            .write()
            .unwrap()
            .senders_by_handle
            .insert(handle, tx);
    }

    pub fn get_jobs_tx_by_func(&self, func: &Vec<u8>) -> Option<Sender<WorkerJob>> {
        match self.senders.read().unwrap().jobs_tx_by_func.get(func) {
            None => None,
            Some(tx) => Some(tx.clone()),
        }
    }

    pub fn set_jobs_tx_by_func(&mut self, func: Vec<u8>, tx: Sender<WorkerJob>) {
        self.senders
            .write()
            .unwrap()
            .jobs_tx_by_func
            .insert(func, tx);
    }
}
