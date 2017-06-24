use std::collections::{HashMap, VecDeque};
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
use tokio_proto::streaming::pipeline::{advanced, ServerProto};
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

struct Dispatch<T>
    where T: AsyncRead + AsyncWrite + 'static,
{
    service: GearmanService,
    transport: <GearmanProto as ServerProto<T>>::Transport,
    in_flight: Arc<Mutex<VecDeque<InFlight<<GearmanService as Service>::Future>>>>,
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
               in_flight: Arc<Mutex<VecDeque<InFlight<<GearmanService as Service>::Future>>>>) -> Dispatch<T> {
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
                request: advanced::PipelineMessage<Self::Out,
                                                   Body<Self::BodyOut, Self::Error>,
                                                   Self::Error>)
                -> io::Result<()> {
        if let Ok(request) = request {
            let response = self.service.call(request);
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.push_back(InFlight::Active(response));
        }
        Ok(())
    }

    fn poll
        (&mut self)
         -> Poll<Option<advanced::PipelineMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
        let mut in_flight = self.in_flight.lock().unwrap();
        for slot in in_flight.iter_mut() {
            slot.poll();
        }

        match in_flight.front() {
            Some(&InFlight::Done(_)) => {}
            _ => return Ok(Async::NotReady),
        }

        match in_flight.pop_front() {
            Some(InFlight::Done(res)) => Ok(Async::Ready(Some(res))),
            _ => panic!(),
        }
    }

    fn has_in_flight(&self) -> bool {
        let in_flight = self.in_flight.lock().unwrap();
        !in_flight.is_empty()
    }
}

impl<F: Future> InFlight<F> {
    fn poll(&mut self) {
        let res = match *self {
            InFlight::Active(ref mut f) => {
                match f.poll() {
                    Ok(Async::Ready(e)) => Ok(e),
                    Err(e) => Err(e),
                    Ok(Async::NotReady) => return,
                }
            }
            _ => return,
        };
        *self = InFlight::Done(res);
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
        let remote = core.remote();
        let service_remote = remote.clone();
        let server = listener.incoming().for_each(|(socket, _)| {
            let transport = GearmanFramed(socket.framed(PacketCodec::new()));
            let conn_id = curr_conn_id.fetch_add(1, Ordering::Relaxed);
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
            let in_flight = Arc::new(Mutex::new(VecDeque::with_capacity(32)));
            let dispatch = Dispatch::new(service, transport, in_flight.clone());
            let backchannel = rx.for_each(move |message| {
                let mut in_flight = in_flight.lock().unwrap();
                in_flight.push_back(InFlight::Active(future::finished(message).boxed()));
                Ok(())
            });
            handle.spawn(backchannel);
            let pipeline = advanced::Pipeline::new(dispatch);
            handle.spawn(pipeline.map_err(|_| ()));
            Ok(())
        });
        core.run(server).unwrap();
    }
}
