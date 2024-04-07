use std::{
    sync::{Arc, Mutex},
    thread,
};

use rustygear::client::{Client, WorkUpdate};
use rustygeard::testutil::start_test_server;
use uuid::Uuid;

#[test]
fn test_server_starts() {
    start_test_server().expect("No connection and no panics probably means no available ports.");
}

#[tokio::test]
async fn test_server_coalesces_uniqs() {
    std::env::set_var("RUST_LOG", "debug");
    let server = start_test_server().expect("Starting test server");
    let mut client1 = Client::new()
        .add_server(&server.addr().to_string())
        .set_client_id("client1")
        .connect()
        .await
        .expect("Connecting client1");
    let mut client2 = Client::new()
        .add_server(&server.addr().to_string())
        .set_client_id("client2")
        .connect()
        .await
        .expect("Connecting client2");
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let rx = Arc::new(Mutex::new(rx));
    let server_addr = server.addr().to_string().clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rx = rx.clone();
        rt.block_on(async move {
            Client::new()
                .add_server(&server_addr)
                .set_client_id("worker")
                .connect()
                .await
                .expect("Connecting worker")
                .can_do("uniqfunc", move |_job| {
                    rx.lock()
                        .unwrap()
                        .blocking_recv()
                        .expect("Waiting to continue worker");
                    let payload = Uuid::new_v4();
                    Ok(Vec::from(payload.into_bytes()))
                })
                .await
                .expect("Sending CAN_DO and setting up worker function")
                .do_one_job()
                .await
                .expect("Doing one job");
        });
    });
    // Now send two submits, and see that they both get the same handle + payload
    let mut job1 = client1
        .submit_unique("uniqfunc", b"uniqid1", b"")
        .await
        .expect("Submitting uniqid1 on client1");
    let mut job2 = client2
        .submit_unique("uniqfunc", b"uniqid1", b"")
        .await
        .expect("Submitting uniqid1 on client2");
    tx.send(()).await.expect("Sending to let the worker finish");
    let response1 = job1.response().await.expect("Getting response to job1");
    let response2 = job2.response().await.expect("Getting response to job2");
    if let WorkUpdate::Complete {
        handle: handle1,
        payload: payload1,
    } = response1
    {
        if let WorkUpdate::Complete {
            handle: handle2,
            payload: payload2,
        } = response2
        {
            assert_eq!(handle1, handle2);
            assert_eq!(payload1, payload2);
        } else {
            panic!("Response 2 was not WORK_COMPLETE: {:?}", response2);
        }
    } else {
        panic!("Response 1 was not WORK_COMPLETE: {:?}", response1);
    }
}
