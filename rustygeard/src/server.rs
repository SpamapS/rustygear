use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::Sink;
use futures::future;
use futures::channel::mpsc::channel;
use tokio_io::AsyncRead;
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tower_service::Service;

use rustygear::codec::{PacketCodec, Packet};

use crate::queues::{HandleJobStorage, SharedJobStorage};
use crate::worker::{SharedWorkers, Wake};
use crate::service::GearmanService;


pub struct GearmanServer;

const MAX_UNHANDLED_OUT_FRAMES: usize = 1024;


impl GearmanServer {
    pub fn run(addr: SocketAddr) {
        let curr_conn_id = Arc::new(AtomicUsize::new(0));
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let senders_by_conn_id = Arc::new(Mutex::new(HashMap::new()));
        let job_waiters = Arc::new(Mutex::new(HashMap::new()));
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let remote = core.remote();
        let listener = TcpListener::bind(&addr, &handle).unwrap();
        let server = listener.incoming().for_each(move |(sock, _)| {
            let conn_id = curr_conn_id.clone().fetch_add(1, Ordering::Relaxed);
            let (sink, stream) = sock.framed(PacketCodec).split();
            let (tx, rx) = channel::<Packet>(MAX_UNHANDLED_OUT_FRAMES);
            {
                let mut senders_by_conn_id = senders_by_conn_id.lock().unwrap();
                senders_by_conn_id.insert(conn_id, tx.clone());
            }
            let service = GearmanService::new(
                conn_id,
                queues.clone(),
                workers.clone(),
                job_count.clone(),
                senders_by_conn_id.clone(),
                job_waiters.clone(),
                remote.clone(),
            );
            // Read stuff, write if needed
            let reader = Box::new(
                stream
                .for_each(move |frame| {
                    let tx = tx.clone();
                    service.call(frame).and_then(move |response| {
                        tx.send(response).then(|_| future::ok(()))
                    })
                })
                .map_err(|_| {}));
            let sink_cell = Rc::new(RefCell::new(sink));
            let writer = rx.for_each(move |to_send| {
                trace!("Sending {:?}", &to_send);
                sink.start_send(to_send)
            });
            handle.spawn(reader);
            handle.spawn(writer);
            Ok(())
        });
        core.run(server).unwrap();
    }
}
