use std::io::Error;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Async, Future, Sink, Stream, Poll};
use futures::{future, AsyncSink, StartSend};
use futures::sync::mpsc::channel;
use tokio_io::AsyncRead;
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_service::Service;

use queues::{HandleJobStorage, SharedJobStorage};
use worker::{SharedWorkers, Wake};
use codec::{PacketCodec, Packet};
use service::GearmanService;


pub struct GearmanServer;

const MAX_UNHANDLED_OUT_FRAMES: usize = 1024;

struct MySinkSend {
    sink: Rc<RefCell<Sink<SinkItem = Packet, SinkError = Error>>>,
    item: StartSend<Packet, Error>,
}

impl Future for MySinkSend {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<(), Error> {
        let mut sink = self.sink.borrow_mut();
        let to_send = match self.item {
            Ok(AsyncSink::NotReady(ref to_send)) => to_send.clone(),
            Ok(AsyncSink::Ready) => return sink.poll_complete(),
            Err(ref e) => panic!("Sink is broken: {:?}", e),
        };
        self.item = sink.start_send(to_send);
        match self.item {
            Ok(AsyncSink::Ready) => sink.poll_complete(),
            Ok(AsyncSink::NotReady(_)) => Ok(Async::NotReady),
            Err(ref e) => panic!("Sink is broken: {:?}", e),
        }
    }
}


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
            let service = GearmanService::new(conn_id,
                                              queues.clone(),
                                              workers.clone(),
                                              job_count.clone(),
                                              senders_by_conn_id.clone(),
                                              job_waiters.clone(),
                                              remote.clone());
            // Read stuff, write if needed
            let reader = stream.for_each(move |frame| {
                    let tx = tx.clone();
                    service.call(frame)
                        .and_then(move |response| tx.send(response).then(|_| future::ok(())))
                })
                .map_err(|_| {})
                .boxed();
            let sink_cell = Rc::new(RefCell::new(sink));
            let writer = rx.for_each(move |to_send| {
                let sender = MySinkSend {
                    sink: sink_cell.clone(),
                    item: sink_cell.borrow_mut().start_send(to_send),
                };
                sender.map_err(|_| ())
            });
            handle.spawn(reader);
            handle.spawn(writer);
            Ok(())
        });
        core.run(server).unwrap();
    }
}
