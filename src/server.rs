use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;

use bytes::BytesMut;
use futures::{Async, Future, Stream, Poll};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_service::Service;
use tokio_proto::streaming::multiplex::{advanced, RequestId};
use tokio_proto::streaming::multiplex::advanced::MultiplexMessage;
use tokio_proto::streaming::Body;

use queues::{HandleJobStorage, SharedJobStorage};
use worker::{SharedWorkers, Wake};
use codec::{PacketCodec, PacketHeader};
use transport::GearmanFramed;
use service::{GearmanBody, GearmanService};

type Request = PacketHeader;
type RequestBody = BytesMut;
type Response = PacketHeader;
type ResponseBody = BytesMut;

pub struct GearmanServer;

const MAX_IN_FLIGHT_REQUESTS: usize = 32;

struct Dispatch<T>
    where T: AsyncRead + AsyncWrite + 'static,
{
    service: GearmanService,
    transport: GearmanFramed<T>,
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
               transport: GearmanFramed<T>,
               in_flight: Arc<Mutex<Vec<(
                   RequestId, InFlight<<GearmanService as Service>::Future>)>>>) -> Dispatch<T> {
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
    type In = Response;
    type BodyIn = ResponseBody;
    type Out = Request;
    type BodyOut = RequestBody;
    type Error = io::Error;
    type Stream = GearmanBody;
    type Transport = GearmanFramed<T>;

    fn transport(&mut self) -> &mut GearmanFramed<T> {
        &mut self.transport
    }

    fn dispatch(&mut self,
                message: MultiplexMessage<Self::Out,
                                          Body<Self::BodyOut, Self::Error>,
                                          Self::Error>)
                -> io::Result<()> {
        assert!(self.poll_ready().is_ready());

        let MultiplexMessage { id, message, solo } = message;

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

    fn poll(&mut self)
            -> Poll<Option<MultiplexMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
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
            let message = MultiplexMessage {
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
        let remote = core.remote();
        let service_remote = remote.clone();
        let request_counter = Arc::new(AtomicUsize::new(1)); // 0 reserved for solo-ish
        let server = listener.incoming().for_each(|(socket, _)| {
            let conn_id = curr_conn_id.fetch_add(1, Ordering::Relaxed);
            let transport = GearmanFramed(socket.framed(PacketCodec::new(request_counter.clone())));
            let service = GearmanService::new(conn_id,
                                              queues.clone(),
                                              workers.clone(),
                                              job_count.clone(),
                                              connections.clone(),
                                              service_remote.clone());
            let in_flight = Arc::new(Mutex::new(Vec::with_capacity(MAX_IN_FLIGHT_REQUESTS)));
            let dispatch = Dispatch::new(service, transport, in_flight.clone());
            let pipeline = advanced::Multiplex::new(dispatch);
            handle.spawn(pipeline.map_err(|_| ()));
            Ok(())
        });
        core.run(server).unwrap();
    }
}
