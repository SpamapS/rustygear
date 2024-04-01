use std::net::SocketAddr;

use rustygear::client::{Client, WorkUpdate};
use rustygeard::testutil::start_test_server;

async fn connect(addr: &SocketAddr) -> Client {
    let client = Client::new().add_server(&addr.to_string());
    client
        .set_client_id("tests")
        .connect()
        .await
        .expect("Failed to connect to server")
}

async fn worker(addr: &SocketAddr) -> Client {
    connect(addr)
        .await
        .can_do("testfunc", |workerjob| {
            Ok(format!(
                "worker saw {} with unique [{}]",
                String::from_utf8_lossy(workerjob.payload()),
                String::from_utf8_lossy(workerjob.unique())
            )
            .into_bytes())
        })
        .await
        .expect("CAN_DO should work")
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

    let mut worker = worker(server.addr()).await;
    worker
        .do_one_job()
        .await
        .expect("Doing the job should work");
    let mut job = job;
    let response = job
        .response()
        .await
        .expect("expecting response from worker");
    if let WorkUpdate::Complete {
        handle: response_handle,
        payload: response_payload,
    } = response
    {
        assert!(response_handle == job.handle().handle());
        let response_payload = String::from_utf8_lossy(&response_payload);
        if let Some((left, right)) = response_payload.split_once('[') {
            assert_eq!(left, "worker saw aaaaa with unique ");
            assert!(right.ends_with(']'));
        } else {
            panic!("Payload is wrong: {}", response_payload);
        }
    } else {
        panic!("Got unexpected WorkUpdate for job: {:?}", response);
    }
}

#[tokio::test]
async fn test_client_submit_unique() {
    let server = start_test_server().unwrap();
    let mut client = connect(server.addr()).await;
    let mut ujob = client
        .submit_unique("testfunc", b"id:12345", b"bbbbb")
        .await
        .expect("Submit unique should return a client job");
    assert!(ujob.handle().handle().len() > 0);
    let mut worker = worker(server.addr()).await;
    worker
        .do_one_job()
        .await
        .expect("Doing the job should work");
    let response = ujob.response().await.expect("need response from worker");
    if let WorkUpdate::Complete { handle, payload } = response {
        assert_eq!(&handle, ujob.handle().handle());
        let response_payload = String::from_utf8_lossy(&payload);
        assert_eq!(&response_payload, "worker saw bbbbb with unique [id:12345]");
    } else {
        panic!("Worker did not send WORK_COMPLETE")
    }
}
