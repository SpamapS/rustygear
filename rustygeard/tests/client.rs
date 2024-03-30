use std::net::Shutdown;

use rustygear::client::Client;
use rustygeard::testutil::start_test_server;
use tokio::{net::TcpStream, io::AsyncWriteExt};

#[tokio::test]
async fn test_client_connects() {
    std::env::set_var("RUST_LOG", "debug");
    let socket = start_test_server().unwrap();
    println!("Server created: {:?}", socket);
    let client = Client::new().add_server(format!("{}", socket.local_addr().unwrap()).as_str());
    println!("Connecting to server");
    client.connect().await.expect("Failed to connect");
    println!("Connected");
    socket.shutdown(Shutdown::Both).expect("Couldn't shutdown");
    let mut csocket = TcpStream::connect(socket.local_addr().unwrap()).await.expect("Could not connect");
    csocket.write(b"shutdown\n").await.expect("could not write shutdown");
    println!("Wrote shutdown");
}