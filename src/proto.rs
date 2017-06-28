use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::streaming::multiplex::ServerProto;
use bytes::BytesMut;

use codec::{PacketHeader, PacketCodec};
use transport::GearmanFramed;

pub struct GearmanProto {
    request_counter: Arc<AtomicUsize>,
}

impl GearmanProto {
    pub fn new(request_counter: Arc<AtomicUsize>) -> GearmanProto {
        GearmanProto {
            request_counter: request_counter,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for GearmanProto {
    type Request = PacketHeader;
    type RequestBody = BytesMut;
    type Response = PacketHeader;
    type ResponseBody = BytesMut;
    type Error = io::Error;

    type Transport = GearmanFramed<T>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        let codec = PacketCodec::new(self.request_counter.clone());

        Ok(GearmanFramed::<T>(io.framed(codec)))
    }
}
