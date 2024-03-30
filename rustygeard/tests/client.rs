use std::net::SocketAddr;

use rustygear::client::Client;
use rustygeard::testutil::start_test_server;

async fn connect(addr: &SocketAddr) -> Client {
    let client = Client::new().add_server(&addr.to_string());
    client.connect().await.expect("Failed to connect to serve")
}

#[tokio::test]
async fn test_client_connects() {
    std::env::set_var("RUST_LOG", "debug");
    let server = start_test_server().unwrap();
    connect(server.addr()).await;
    println!("Connected");
}

#[tokio::test]
async fn test_client_echo() {
    let server = start_test_server().unwrap();
    let mut client = connect(server.addr()).await;
    client.echo(b"Hello World").await.expect("Echo Failed");
}