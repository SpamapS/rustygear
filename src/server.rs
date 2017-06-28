use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;

use futures::{Async, Future, Stream, Poll};
use futures::sync::mpsc::{Sender, channel};
use futures::future;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_service::Service;
use tokio_proto::streaming::multiplex::{advanced, RequestId, ServerProto};
use tokio_proto::streaming::Body;

use queues::{HandleJobStorage, SharedJobStorage};
use worker::{SharedWorkers, Wake};
use codec::PacketCodec;
use transport::GearmanFramed;
use service::{GearmanBody, GearmanService};
use proto::GearmanProto;

pub struct GearmanServer;

// Arbitrarily limit the number of waiting backchannel messages to 1024 to prevent a dead client
// from using up all memory.
const BACKCHANNEL_BUFFER_SIZE: usize = 1024;
const MAX_IN_FLIGHT_REQUESTS: usize = 32;

struct Dispatch<T>
    where T: AsyncRead + AsyncWrite + 'static,
{
    service: GearmanService,
    transport: <GearmanProto as ServerProto<T>>::Transport,
    in_flight: Arc<Mutex<Vec<(RequestId, InFlight<<GearmanService as Service>::Future>)>>>,
}

enum InFlight<F: Future> {
    Active(F),
    Done(Result<F::Item, F::Error>),
}

impl<T> Dispatch<T>
    where T: AsyncRead + AsyncWrite + 'static
{
    pub fn new(service: GearmanService,
               transport: <GearmanProto as ServerProto<T>>::Transport,
               in_flight: Arc<Mutex<Vec<(RequestId, InFlight<<GearmanService as Service>::Future>)>>>) -> Dispatch<T> {
        Dispatch {
            service: service,
            transport: transport,
            in_flight: in_flight,
        }
    }
}

impl<T> advanced::Dispatch for Dispatch<T>
    where T: AsyncRead + AsyncWrite + 'static
{
    type Io = T;
    type In = <GearmanProto as ServerProto<T>>::Response;
    type BodyIn = <GearmanProto as ServerProto<T>>::ResponseBody;
    type Out = <GearmanProto as ServerProto<T>>::Request;
    type BodyOut = <GearmanProto as ServerProto<T>>::RequestBody;
    type Error = <GearmanProto as ServerProto<T>>::Error;
    type Stream = GearmanBody;
    type Transport = <GearmanProto as ServerProto<T>>::Transport;

    fn transport(&mut self) -> &mut <GearmanProto as ServerProto<T>>::Transport {
        &mut self.transport
    }

    fn dispatch(&mut self,
                message: advanced::MultiplexMessage<Self::Out,
                                                    Body<Self::BodyOut, Self::Error>,
                                                    Self::Error>)
                -> io::Result<()> {
        assert!(self.poll_ready().is_ready());

        let advanced::MultiplexMessage { id, message, solo } = message;

        //assert!(!solo);

        if !solo {
            if let Ok(request) = message {
                let response = self.service.call(request);
                let mut in_flight = self.in_flight.lock().unwrap();
                in_flight.push((id, InFlight::Active(response)));
            }
        }

        // TODO: Should the error be handled differently?
        Ok(())
    }

    fn poll
        (&mut self)
         -> Poll<Option<advanced::MultiplexMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
        let mut in_flight = self.in_flight.lock().unwrap();
        trace!("Dispatch::poll");

        let mut idx = None;

        for (i, &mut (request_id, ref mut slot)) in in_flight.iter_mut().enumerate() {
            trace!("   --> poll; request_id={:?}", request_id);
            if slot.poll() && idx.is_none() {
                idx = Some(i);
            }
        }

        if let Some(idx) = idx {
            let (request_id, message) = in_flight.remove(idx);
            let message = advanced::MultiplexMessage {
                id: request_id,
                message: message.unwrap_done(),
                solo: false,
            };

            Ok(Async::Ready(Some(message)))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn poll_ready(&self) -> Async<()> {
        let in_flight = self.in_flight.lock().unwrap();
        if in_flight.len() < MAX_IN_FLIGHT_REQUESTS {
            Async::Ready(())
        } else {
            Async::NotReady
        }
    }

    fn cancel(&mut self, _request_id: RequestId) -> io::Result<()> {
        // todo: implement
        Ok(())
    }
}

impl<F: Future> InFlight<F> {
    fn poll(&mut self) -> bool {
        let res = match *self {
            InFlight::Active(ref mut f) => {
                trace!("   --> polling future");
                match f.poll() {
                    Ok(Async::Ready(e)) => Ok(e),
                    Err(e) => Err(e),
                    Ok(Async::NotReady) => return false,
                }
            }
            _ => return true,
        };

        *self = InFlight::Done(res);
        true
    }

    fn unwrap_done(self) -> Result<F::Item, F::Error> {
        match self {
            InFlight::Done(res) => res,
            _ => panic!("future is not ready"),
        }
    }
}

pub type BackchannelSender =
    Sender<<GearmanService as Service>::Response>;

impl GearmanServer {
    pub fn run(addr: SocketAddr) {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let listener = TcpListener::bind(&addr, &handle).unwrap();
        let curr_conn_id = AtomicUsize::new(0);
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let sleeps = Arc::new(Mutex::new(HashMap::new()));
        let remote = core.remote();
        let service_remote = remote.clone();
        let request_counter = Arc::new(AtomicUsize::new(1)); // 0 reserved for solo-ish
        let server =
            listener.incoming().for_each(|(socket, _)| {
                let conn_id = curr_conn_id.fetch_add(1, Ordering::Relaxed);
                let transport =
                    GearmanFramed(socket.framed(PacketCodec::new(request_counter.clone(),
                                                                 conn_id,
                                                                 sleeps.clone())));
                let service = GearmanService::new(conn_id,
                                                  queues.clone(),
                                                  workers.clone(),
                                                  job_count.clone(),
                                                  connections.clone(),
                                                  service_remote.clone());
                // Create backchannel for sending packets to other connections
                let (tx, rx) =
                    channel::<<GearmanService as Service>::Response>(BACKCHANNEL_BUFFER_SIZE);

                {
                    let mut connections = connections.lock().unwrap();
                    connections.insert(conn_id, tx.clone());
                }
                let in_flight = Arc::new(Mutex::new(Vec::with_capacity(MAX_IN_FLIGHT_REQUESTS)));
                let dispatch = Dispatch::new(service, transport, in_flight.clone());
                let sleeps = sleeps.clone();
                let backchannel = rx.for_each(move |message| {
                    debug!("Got backchannel for {}: {:?}", conn_id, message);
                    let mut in_flight = in_flight.lock().unwrap();
                    let mut sleeps = sleeps.lock().unwrap();
                    match sleeps.remove(&conn_id) {
                        None => warn!("Trying to wake up a connection that isn't asleep!"),
                        Some(request_id) => {
                            in_flight.push((request_id,
                                            InFlight::Active(future::finished(message).boxed())));
                        }
                    }
                    Ok(())
                });
                handle.spawn(backchannel);
                let pipeline = advanced::Multiplex::new(dispatch);
                handle.spawn(pipeline.map_err(|_| ()));
                Ok(())
            });
        core.run(server).unwrap();
    }
}
