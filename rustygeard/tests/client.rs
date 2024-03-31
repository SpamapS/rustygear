use std::net::SocketAddr;

use rustygear::client::{Client, WorkUpdate};
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

    let worker = connect(server.addr()).await;
    worker
        .can_do("testfunc", |workerjob| {
            Ok(format!(
                "worker saw {}",
                String::from_utf8_lossy(workerjob.payload())
            )
            .into_bytes())
        })
        .await
        .expect("CAN_DO should work")
        .do_one_job()
        .await
        .expect("Doing the job should work");
    let mut job = job;
    let response = job
        .response()
        .await
        .expect("expecting response from worker");
    assert!(matches!(
        response,
        WorkUpdate::Complete {
            handle: _,
            payload: _
        }
    ));
    if let WorkUpdate::Complete {
        handle: response_handle,
        payload: response_payload,
    } = response
    {
        assert!(response_handle == job.handle().handle());
        assert!(String::from_utf8_lossy(&response_payload) == "worker saw aaaaa");
    } else {
        panic!("matches macro does not work as expected");
    }
    let ujob = client
        .submit_unique("testunique", b"12345", b"bbbbb")
        .await
        .expect("Submit unique should return a client job");
    assert!(ujob.handle().handle().len() > 0);
}
