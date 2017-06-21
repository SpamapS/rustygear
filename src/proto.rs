use std::io;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::streaming::pipeline::ServerProto;
use bytes::BytesMut;

use codec::{PacketHeader, PacketCodec};
use transport::GearmanFramed;

pub struct GearmanProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for GearmanProto {
    type Request = PacketHeader;
    type RequestBody = BytesMut;
    type Response = PacketHeader;
    type ResponseBody = BytesMut;
    type Error = io::Error;

    type Transport = GearmanFramed<T>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        let codec = PacketCodec {
            data_todo: None,
        };

        Ok(GearmanFramed::<T>(io.framed(codec)))
    }
}
