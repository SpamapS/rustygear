extern crate bytes;
extern crate futures;
extern crate rustygear;
extern crate tokio_core;

use std::sync::mpsc;
use std::thread;

use bytes::Bytes;
use futures::Future;
use futures::sync::oneshot;
use tokio_core::reactor::Core;

use rustygear::client::{Client, AddServer};
use rustygear::server::GearmanServer;

#[test]
fn test_client_basic() {
    let mut c = Client::new(Some(Bytes::from("client_id_numba_one")));
    let mut reactor = Core::new().unwrap();
    let handle = reactor.handle();
    let (stop_tx, stop_rx) = oneshot::channel();
    let (port_tx, port_rx) = mpsc::channel();
    let server_thread = thread::spawn(move || {
        let mut core = Core::new().unwrap();
        let addr = "[::]:0".parse().unwrap();
        let server = GearmanServer::new(addr, &core.handle());
        port_tx.send(server.listener().local_addr().unwrap().port()).unwrap();
        server.run_with_stop(stop_rx, &mut core);
    });
    let server_port = port_rx.recv().unwrap();
    c.add_server(format!("[::]:{}", server_port));
    let conns = c.connect(handle);
    let c = reactor.run(conns).unwrap();
    let sockets = c.sockets();
    let sockets = sockets.lock().unwrap();
    assert_eq!(1, sockets.len());
    assert_eq!(0, c.errors().len());
    let mut c2 = Client::new(Some(Bytes::from("client_id_numba_two")));
    c2.add_server(format!("[::]:{}", server_port));
    // Possible race in 3..2.. Something may be assigned the listener port
    stop_tx.send(()).unwrap();
    server_thread.join().unwrap();
    let handle = reactor.handle();
    let conns = c2.connect(handle);
    let c2 = reactor.run(conns).unwrap();
    let sockets = c2.sockets();
    let sockets = sockets.lock().unwrap();
    assert_eq!(0, sockets.len());
    assert_eq!(1, c2.errors().len());
}
