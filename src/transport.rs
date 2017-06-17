use std::io;

use mio::net::TcpListener;
use tokio_core::reactor::PollEvented;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_proto::streaming::pipeline::Transport;

use codec::PacketCodec;

type GearmanIO = PollEvented<TcpListener>;
type GearmanFramed = Framed<GearmanIO, PacketCodec>;

impl Transport for GearmanFramed
{
    fn tick(&mut self) {
        trace!("tick!");
    }

    fn cancel(&mut self) -> io::Result<()> {
        trace!("cancel!");
    }
}
