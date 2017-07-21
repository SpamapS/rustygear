extern crate bytes;
extern crate futures;
extern crate rustygear;
extern crate tokio_core;

use std::error::Error;
use std::net::SocketAddr;

use bytes::Bytes;
use futures::{Future, Async};
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;

use rustygear::client::{Client, AddServer};

#[test]
fn test_client_basic() {
    let mut c = Client::new(Some(Bytes::from("client_id_numba_one")));
    let mut reactor = Core::new().unwrap();
    let handle = reactor.handle();
    let addr = "[::]:0".parse().unwrap();
    let listener = TcpListener::bind(&addr, &handle).unwrap();
    c.add_server(format!("[::]:{}", listener.local_addr().unwrap().port()));
    let mut conns = c.connect(handle);
    let c = reactor.run(conns).unwrap();
    let sockets = c.sockets();
    let sockets = sockets.lock().unwrap();
    assert_eq!(1, sockets.len());
    assert_eq!(0, c.errors().len());
    let mut c2 = Client::new(Some(Bytes::from("client_id_numba_two")));
    c2.add_server(format!("[::]:{}", listener.local_addr().unwrap().port()));
    // Possible race in 3..2.. Something may be assigned the listener port
    drop(listener);
    let handle = reactor.handle();
    let conns = c2.connect(handle);
    let c2 = reactor.run(conns).unwrap();
    let sockets = c2.sockets();
    let sockets = sockets.lock().unwrap();
    assert_eq!(0, sockets.len());
    assert_eq!(1, c2.errors().len());
}
