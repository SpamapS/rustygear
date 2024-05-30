use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::StreamExt;
use futures::SinkExt;
use rustygear::constants::NOOP;
use rustygear::util::new_req;
use rustygear::wrappedstream::WrappedStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;
use tokio::sync::mpsc::channel;

#[cfg(feature = "tls")]
use tokio_rustls::rustls::ServerConfig;
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Decoder;
use tower_service::Service;

use rustygear::codec::{Packet, PacketCodec};

use crate::queues::{HandleJobStorage, SharedJobStorage};
use crate::service::GearmanService;
use crate::worker::{SharedWorkers, Wake};

pub struct GearmanServer;

const MAX_UNHANDLED_OUT_FRAMES: usize = 1024;
const SHUTDOWN_BUFFER_SIZE: usize = 4;

#[cfg(feature = "tls")]
pub type TlsConfig = Option<ServerConfig>;
#[cfg(not(feature = "tls"))]
pub type TlsConfig = Option<()>;

#[cfg(feature = "tls")]
type Acceptor = Option<TlsAcceptor>;
#[cfg(not(feature = "tls"))]
type Acceptor = Option<()>;

impl GearmanServer {
    pub fn run(addr: SocketAddr, tls: TlsConfig) -> io::Result<()> {
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let senders_by_conn_id = Arc::new(Mutex::new(HashMap::new()));
        let workers_by_conn_id = Arc::new(Mutex::new(BTreeMap::new()));
        let job_waiters = Arc::new(Mutex::new(HashMap::new()));
        let rt = runtime::Runtime::new().unwrap();
        let acceptor: Acceptor = match tls {
            None => None,
            #[cfg(feature = "tls")]
            Some(config) => Some(TlsAcceptor::from(Arc::new(config))),
            #[cfg(not(feature = "tls"))]
            Some(_) => unreachable!("Should not have a TLS config without tls feature"),
        };
        rt.block_on(async move {
            let listener = match TcpListener::bind(&addr).await {
                Err(e) => return Err(e),
                Ok(listener) => listener,
            };
            let shutdown: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
            loop {
                let shutdown = shutdown.clone();
                if shutdown.load(Ordering::Relaxed) {
                    break Ok(());
                }
                let (plain_sock, peer_addr) = match listener.accept().await {
                    Ok((plain_sock, peer_addr)) => (plain_sock, peer_addr),
                    Err(e) => {
                        warn!("Error while accepting: {}", e);
                        continue;
                    }
                };
                let conn_id: usize = plain_sock.as_raw_fd().try_into().unwrap();
                let sock = match acceptor {
                    None => WrappedStream::from(plain_sock),
                    #[cfg(feature = "tls")]
                    Some(ref acceptor) => {
                        let tls_sock = match acceptor.accept(plain_sock).await {
                            Ok(tls_sock) => tls_sock,
                            Err(e) => {
                                warn!(
                                    "Error while negotiating TLS for connection ({}): {}",
                                    conn_id, e
                                );
                                continue;
                            }
                        };
                        WrappedStream::from(tls_sock)
                    }
                    #[cfg(not(feature = "tls"))]
                    Some(_) => unreachable!("Should not have a TLS acceptor without tls feature"),
                };
                // TODO: new connection ID needed
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
                let senders_by_conn_id_r = senders_by_conn_id.clone();
                let workers_by_conn_id_r = workers_by_conn_id.clone();
                let senders_by_conn_id_w = senders_by_conn_id.clone();
                let workers_by_conn_id_w = workers_by_conn_id.clone();
                let queues = queues.clone();
                let workers = workers.clone();
                let job_count = job_count.clone();
                let job_waiters = job_waiters.clone();
                let (shut_tx, mut shut_rx) = channel(SHUTDOWN_BUFFER_SIZE);
                let reader_tx = tx.clone();
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
                        shut_tx,
                    );
                    {
                        let mut workers_by_conn_id = workers_by_conn_id.lock().unwrap();
                        workers_by_conn_id.insert(conn_id, service.worker.clone());
                    }
                    while let Some(frame) = stream.next().await {
                        let response = match frame {
                            Err(e) => {
                                warn!("Connection ({}) dropped while reading: {}", conn_id, e);
                                break;
                            }
                            Ok(frame) => service.call(frame).await,
                        };
                        if let Ok(response) = response {
                            if let Err(_) = reader_tx.send(response).await {
                                error!("receiver dropped!");
                                break;
                            }
                        }
                    }
                    info!("Reader for ({}) shutting down.", conn_id);
                    {
                        let mut senders_by_conn_id = senders_by_conn_id_r.lock().unwrap();
                        senders_by_conn_id.remove(&conn_id);
                    }
                    {
                        let mut workers_by_conn_id = workers_by_conn_id_r.lock().unwrap();
                        workers_by_conn_id.remove(&conn_id);
                    }
                    // This NOOP is necessary sometimes to wake up the connection, but if the
                    // Client shuts down it is likely to fail.
                    if let Err(e) = reader_tx.send(new_req(NOOP, Bytes::new())).await {
                        debug!("Client shut down connection before NOOP: {:?}", e);
                    }
                    drop(reader_tx);
                    drop(service);
                };
                let writer_shutdown = shutdown.clone();
                let writer = async move {
                    while let Some(packet) = rx.recv().await {
                        trace!("Sending {:?}", &packet);
                        if let Err(_) = sink.send(packet).await {
                            info!("Connection ({}) dropped", conn_id);
                            break;
                        }
                        let shutdown = writer_shutdown.clone();
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    debug!("Writer for ({}) shutting down", conn_id);
                    {
                        let mut workers_by_conn_id = workers_by_conn_id_w.lock().unwrap();
                        workers_by_conn_id.remove(&conn_id);
                    }
                    {
                        let mut senders_by_conn_id = senders_by_conn_id_w.lock().unwrap();
                        senders_by_conn_id.remove(&conn_id);
                    }
                };

                let shutterdown = async move {
                    while shut_rx.recv().await.is_some() {
                        let shutdown = shutdown.clone();
                        shutdown.store(true, Ordering::Relaxed);
                        // In case there's no incoming conns, force the listener to fire
                        let _ = TcpStream::connect(&addr).await;
                    }
                };
                runtime::Handle::current().spawn(reader);
                runtime::Handle::current().spawn(writer);
                runtime::Handle::current().spawn(shutterdown);
                drop(tx);
            }
        })
    }
}
