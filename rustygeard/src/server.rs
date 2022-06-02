use std::collections::{HashMap, BTreeMap};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::os::unix::io::AsRawFd;

use futures::stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio::sync::mpsc::channel;
use tokio_util::codec::

Decoder;
use tower_service::Service;

use rustygear::codec::{Packet, PacketCodec};

use crate::queues::{HandleJobStorage, SharedJobStorage};
use crate::service::GearmanService;
use crate::worker::{SharedWorkers, Wake};

pub struct GearmanServer;

const MAX_UNHANDLED_OUT_FRAMES: usize = 1024;

impl GearmanServer {
    pub fn run(addr: SocketAddr) {
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let senders_by_conn_id = Arc::new(Mutex::new(HashMap::new()));
        let workers_by_conn_id = Arc::new(Mutex::new(BTreeMap::new()));
        let job_waiters = Arc::new(Mutex::new(HashMap::new()));
        let rt = runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();
            loop {
                let socket_res = listener.accept().await;
                match socket_res {
                    Ok((sock, peer_addr)) => {
                        let conn_id: usize = sock.as_raw_fd().try_into().unwrap();
                        let pc = PacketCodec {};
                        let (mut sink, mut stream) = pc.framed(sock).split();
                        let (tx, mut rx) = channel::<Packet>(MAX_UNHANDLED_OUT_FRAMES);
                        {
                            let senders_by_conn_id = senders_by_conn_id.clone();
                            let mut senders_by_conn_id = senders_by_conn_id.lock().unwrap();
                            senders_by_conn_id.insert(conn_id, tx.clone());
                        }
                        // Read stuff, write if needed
                        let senders_by_conn_id = senders_by_conn_id.clone();
                        let workers_by_conn_id = workers_by_conn_id.clone();
                        let senders_by_conn_id_w = senders_by_conn_id.clone();
                        let workers_by_conn_id_w = workers_by_conn_id.clone();
                        let queues = queues.clone();
                        let workers = workers.clone();
                        let job_count = job_count.clone();
                        let job_waiters = job_waiters.clone();
                        let reader = async move {
                            let mut service = GearmanService::new(
                                conn_id,
                                queues,
                                workers,
                                job_count,
                                senders_by_conn_id,
                                workers_by_conn_id.clone(),
                                job_waiters,
                                peer_addr,
                            );
                            {
                                let mut workers_by_conn_id = workers_by_conn_id.lock().unwrap();
                                workers_by_conn_id.insert(conn_id, service.worker.clone());
                            }
                            let tx = tx.clone();
                            while let Some(frame) = stream.next().await {
                                let response = service.call(frame.unwrap()).await;
                                if let Ok(response) = response {
                                    if let Err(_) = tx.send(response).await {
                                        error!("receiver dropped!")
                                    }
                                }
                            }
                        };

                        let writer = async move {
                            while let Some(packet) = rx.recv().await {
                                trace!("Sending {:?}", &packet);
                                if let Err(_) = sink.send(packet).await {
                                    {
                                        let mut workers_by_conn_id = workers_by_conn_id_w.lock().unwrap();
                                        workers_by_conn_id.remove(&conn_id);
                                    }
                                    {
                                        let mut senders_by_conn_id = senders_by_conn_id_w.lock().unwrap();
                                        senders_by_conn_id.remove(&conn_id);
                                    }
                                    error!("Connection ({}) dropped", conn_id);
                                }
                            }
                        };
                        runtime::Handle::current().spawn(reader);
                        runtime::Handle::current().spawn(writer);
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            }
        })
    }
}
