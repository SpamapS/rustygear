use std::io;

use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_proto::streaming::pipeline::Transport;
use futures::{Poll, Sink, StartSend, Stream};

use codec::PacketCodec;

pub struct GearmanFramed<T>(pub Framed<T, PacketCodec>);

impl<T> Transport for GearmanFramed<T>
    where T: AsyncRead + AsyncWrite + 'static
{
    fn tick(&mut self) {
        trace!("tick!");
    }

    fn cancel(&mut self) -> io::Result<()> {
        trace!("cancel!");
        Ok(())
    }
}

impl<T> Stream for GearmanFramed<T>
    where T: AsyncRead
{
    type Item = <PacketCodec as Decoder>::Item;
    type Error = <PacketCodec as Decoder>::Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

impl<T> Sink for GearmanFramed<T>
    where T: AsyncWrite
{
    type SinkItem = <PacketCodec as Encoder>::Item;
    type SinkError = <PacketCodec as Encoder>::Error;
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.0.start_send(item)
    }
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.poll_complete()
    }
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.0.close()
    }
}
