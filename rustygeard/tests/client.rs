use std::net::SocketAddr;

use rustygear::client::Client;
use rustygeard::testutil::start_test_server;

async fn connect(addr: &SocketAddr) -> Client {
    let client = Client::new().add_server(&addr.to_string());
    client.connect().await.expect("Failed to connect to serve")
}

#[tokio::test]
async fn test_client_connects() {
    let server = start_test_server().unwrap();
    connect(server.addr()).await;
}

#[tokio::test]
async fn test_client_echo() {
    // This is here as an example of how to use env_logger to watch parts of the test runex
    //std::env::set_var("RUST_LOG", "rustygear::client=debug");
    let server = start_test_server().unwrap();
    let mut client = connect(server.addr()).await;
    client.echo(b"Hello World").await.expect("Echo Failed");
}

#[tokio::test]
async fn test_client_submit() {
    let server = start_test_server().unwrap();
    let mut client = connect(server.addr()).await;
    let job = client
        .submit("testfunc", b"aaaaa")
        .await
        .expect("Submit job should return a client job");
    assert!(job.handle().handle().len() > 0);
    let ujob = client
        .submit_unique("testunique", b"12345", b"bbbbb")
        .await
        .expect("Submit unique should return a client job");
    assert!(ujob.handle().handle().len() > 0);
}
