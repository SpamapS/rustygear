use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io;

use futures::{Async, Future, Stream, Poll};
use futures::sync::mpsc::channel;
use futures::future;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_service::Service;
use tokio_proto::streaming::pipeline::{advanced, ServerProto};
use tokio_proto::streaming::{Body, Message};

use queues::{HandleJobStorage, SharedJobStorage};
use worker::{SharedWorkers, Wake};
use codec::{PacketCodec, PacketItem};
use transport::GearmanFramed;
use service::GearmanService;
use proto::GearmanProto;

pub struct GearmanServer;

// Arbitrarily limit the number of waiting backchannel messages to 1024 to prevent a dead client
// from using up all memory.
const BACKCHANNEL_BUFFER_SIZE: usize = 1024;

struct Dispatch<S, T, P>
    where T: 'static,
          P: ServerProto<T>,
          S: Service
{
    service: S,
    transport: P::Transport,
    in_flight: VecDeque<InFlight<S::Future>>,
}

enum InFlight<F: Future> {
    Active(F),
    Done(Result<F::Item, F::Error>),
}

impl<S, T, P> Dispatch<S, T, P>
    where T: 'static,
          P: ServerProto<T>,
          S: Service
{
    pub fn new(service: S, transport: P::Transport) -> Dispatch<S, T, P> {
        Dispatch {
            service: service,
            transport: transport,
            in_flight: VecDeque::with_capacity(32),
        }
    }

    pub fn send_response(&mut self, response: S::Future) {
        self.in_flight.push_back(InFlight::Active(response));
    }
}

impl<P, T, B, S> advanced::Dispatch for Dispatch<S, T, P>
    where P: ServerProto<T>,
          T: 'static,
          B: Stream<Item = P::ResponseBody, Error = P::Error>,
          S: Service<Request = Message<P::Request, Body<P::RequestBody, P::Error>>,
                     Response = Message<P::Response, B>,
                     Error = P::Error>
{
    type Io = T;
    type In = P::Response;
    type BodyIn = P::ResponseBody;
    type Out = P::Request;
    type BodyOut = P::RequestBody;
    type Error = P::Error;
    type Stream = B;
    type Transport = P::Transport;

    fn transport(&mut self) -> &mut P::Transport {
        &mut self.transport
    }

    fn dispatch(&mut self,
                request: advanced::PipelineMessage<Self::Out,
                                                   Body<Self::BodyOut, Self::Error>,
                                                   Self::Error>)
                -> io::Result<()> {
        if let Ok(request) = request {
            let response = self.service.call(request);
            self.in_flight.push_back(InFlight::Active(response));
        }
        Ok(())
    }

    fn poll
        (&mut self)
         -> Poll<Option<advanced::PipelineMessage<Self::In, Self::Stream, Self::Error>>, io::Error> {
        for slot in self.in_flight.iter_mut() {
            slot.poll();
        }

        match self.in_flight.front() {
            Some(&InFlight::Done(_)) => {}
            _ => return Ok(Async::NotReady),
        }

        match self.in_flight.pop_front() {
            Some(InFlight::Done(res)) => Ok(Async::Ready(Some(res))),
            _ => panic!(),
        }
    }

    fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
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


impl GearmanServer {
    pub fn run<T: AsyncRead + AsyncWrite + 'static>(listener: TcpListener)
    {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let curr_conn_id = AtomicUsize::new(0);
        let queues = SharedJobStorage::new_job_storage();
        let workers = SharedWorkers::new_workers();
        let job_count = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let server = listener.incoming().for_each(|(socket, _)| {
            let transport = GearmanFramed(socket.framed(PacketCodec::new()));
            let conn_id = curr_conn_id.fetch_add(1, Ordering::Relaxed);
            let service =
                GearmanService::new(conn_id, queues.clone(), workers.clone(), job_count.clone());
            // Create backchannel for sending packets to other connections
            let (tx, rx) =
                channel::<<GearmanService as Service>::Response>(BACKCHANNEL_BUFFER_SIZE);

            {
                let mut connections = connections.lock().unwrap();
                connections.insert(conn_id, tx.clone());
            }
            let dispatch: Dispatch<GearmanService, GearmanFramed<T>, GearmanProto> =
                Dispatch::new(service, transport);
            let backchannel = rx.for_each(|message| {
                dispatch.send_response(future::finished(message).boxed());
                Ok(())
            });
            let pipeline = advanced::Pipeline::new(dispatch);
            /*
            let responder = pipeline.select(backchannel).then(|res| {
                match res {
                    Ok(_) => debug!("OK!"),
                    Err(e) => error!("Error! {:?}", e),
                }
                Ok(())
            });
            */
            handle.spawn(pipeline.map_err(|_| ()));
            Ok(())
        });
        core.run(server).unwrap();
    }
}
