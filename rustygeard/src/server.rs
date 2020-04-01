use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::SinkExt;
use futures::channel::mpsc::channel;
use futures::stream::StreamExt;
use tokio::runtime;
use tokio::net::TcpListener;
use tokio_util::codec::Decoder;
use tower_service::Service;

use rustygear::codec::{PacketCodec, Packet};

use crate::queues::{HandleJobStorage, SharedJobStorage};
use crate::worker::{SharedWorkers, Wake};
use crate::service::GearmanService;


pub struct GearmanServer;

const MAX_UNHANDLED_OUT_FRAMES: usize = 1024;


impl GearmanServer {
    async fn run(addr: SocketAddr) {
        let curr_conn_id = Arc::new(AtomicUsize::new(0));
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let senders_by_conn_id = Arc::new(Mutex::new(HashMap::new()));
        let job_waiters = Arc::new(Mutex::new(HashMap::new()));
        let mut listener = TcpListener::bind(&addr).await.unwrap();
        let mut incoming = listener.incoming();
        let mut rt = runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            while let Some(socket_res) = incoming.next().await {
                match socket_res {
                    Ok(sock) => {
                        let conn_id = curr_conn_id.clone().fetch_add(1, Ordering::Relaxed);
                        let pc = PacketCodec{};
                        let (mut sink, mut stream) = pc.framed(sock).split();
                        let (tx, mut rx) = channel::<Packet>(MAX_UNHANDLED_OUT_FRAMES);
                        {
                            let mut senders_by_conn_id = senders_by_conn_id.lock().unwrap();
                            senders_by_conn_id.insert(conn_id, tx.clone());
                        }
                        // Read stuff, write if needed
                        let tx = tx.clone();
                        let reader = async {
                            let mut service = GearmanService::new(
                                conn_id,
                                queues.clone(),
                                workers.clone(),
                                job_count.clone(),
                                senders_by_conn_id.clone(),
                                job_waiters.clone(),
                            );
                            while let Some(frame) = stream.next().await {
                                let response = service.call(frame.unwrap()).await;
                                let tx = tx.clone();
                                runtime::Handle::current().spawn(async {
                                    let mut tx = tx.clone();
                                    if let Ok(response) = response {
                                        tx.send(response).await;
                                    }
                                });
                            }
                        };
                        
                        let writer = async move {
                            while let Some(packet) = rx.next().await {
                                trace!("Sending {:?}", &packet);
                                sink.send(packet).await;
                            }
                        };
                        runtime::Handle::current().spawn(reader);
                        runtime::Handle::current().spawn(writer);
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            };
        })
    }
}
