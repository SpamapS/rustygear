use std::pin::Pin;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{client, server};

#[derive(Debug)]
pub enum WrappedStream {
    ClientTls(Box<client::TlsStream<TcpStream>>),
    ServerTls(Box<server::TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl Unpin for WrappedStream {}

impl From<client::TlsStream<TcpStream>> for WrappedStream {
    fn from(value: client::TlsStream<TcpStream>) -> Self {
        WrappedStream::ClientTls(Box::new(value))
    }
}

impl From<server::TlsStream<TcpStream>> for WrappedStream {
    fn from(value: server::TlsStream<TcpStream>) -> Self {
        WrappedStream::ServerTls(Box::new(value))
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
            WrappedStream::ClientTls(stream) => Pin::new(stream).poll_read(cx, buf),
            WrappedStream::ServerTls(stream) => Pin::new(stream).poll_read(cx, buf),
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
            WrappedStream::ClientTls(stream) => Pin::new(stream).poll_write(cx, buf),
            WrappedStream::ServerTls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            WrappedStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            WrappedStream::ClientTls(stream) => Pin::new(stream).poll_flush(cx),
            WrappedStream::ServerTls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            WrappedStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            WrappedStream::ClientTls(stream) => Pin::new(stream).poll_shutdown(cx),
            WrappedStream::ServerTls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
