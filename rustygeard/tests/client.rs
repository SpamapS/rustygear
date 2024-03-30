use std::net::Shutdown;

use rustygear::client::Client;
use rustygeard::testutil::start_test_server;
use tokio::{net::TcpStream, io::AsyncWriteExt};

#[tokio::test]
async fn test_client_connects() {
    std::env::set_var("RUST_LOG", "debug");
    let server_addr = start_test_server().unwrap();
    println!("Server created: {:?}", server_addr);
    let client = Client::new().add_server(format!("{}", server_addr).as_str());
    println!("Connecting to server");
    client.connect().await.expect("Failed to connect");
    println!("Connected");
    let mut csocket = TcpStream::connect(server_addr).await.expect("Could not connect");
    csocket.write(b"shutdown\n").await.expect("could not write shutdown");
    println!("Wrote shutdown");
}