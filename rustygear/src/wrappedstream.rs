use std::pin::Pin;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::client::TlsStream;

#[derive(Debug)]
pub enum WrappedStream {
    Tls(Box<TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl Unpin for WrappedStream {}

impl From<TlsStream<TcpStream>> for WrappedStream {
    fn from(value: TlsStream<TcpStream>) -> Self {
        WrappedStream::Tls(Box::new(value))
    }
}

impl From<TcpStream> for WrappedStream {
    fn from(value: TcpStream) -> Self {
        WrappedStream::Plain(value)
    }
}

impl AsyncRead for WrappedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            WrappedStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            WrappedStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for WrappedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            WrappedStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            WrappedStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            WrappedStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            WrappedStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            WrappedStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            WrappedStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
