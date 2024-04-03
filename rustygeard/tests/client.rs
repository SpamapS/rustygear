use std::{io::ErrorKind, net::SocketAddr, time::Duration};

use rustygear::client::{Client, WorkUpdate};
use rustygeard::testutil::start_test_server;
use tokio::time::{sleep, timeout};

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

#[tokio::test]
async fn test_client_submit_background() {
    let server = start_test_server().unwrap();
    let mut client = connect(server.addr()).await;
    let mut bgjob = client
        .submit_background("testfunc", b"asyncstuff")
        .await
        .expect("Background submit should work");
    let status = client
        .get_status(bgjob.handle())
        .await
        .expect("GET_STATUS should work");
    assert_eq!(status.handle, bgjob.handle().handle());
    assert!(status.known);
    assert!(!status.running);
    let mut worker = worker(server.addr()).await;
    worker
        .do_one_job()
        .await
        .expect("Doing the job should work");
    if let Ok(_) = bgjob.response().await {
        panic!("Background jobs should never have a response")
    }
    // The job should be gone but we might be ahead of WORK_COMPLETE so try a few times
    let mut retries = 5;
    loop {
        let status = client
            .get_status(bgjob.handle())
            .await
            .expect("GET_STATUS should work");
        if !status.known {
            break;
        }
        // Polling is the pits but it's all we have
        sleep(Duration::from_millis(100)).await;
        retries -= 1;
        if retries <= 0 {
            panic!("Job did not disappear after 15 retries");
        }
    }
}

#[tokio::test]
async fn test_client_multi_server() {
    let server1 = start_test_server().unwrap();
    let server2 = start_test_server().unwrap();
    let server1_str = server1.addr().to_string();
    let server2_str = server2.addr().to_string();
    let mut client = Client::new()
        .add_server(&server1_str)
        .add_server(&server2_str)
        .set_client_id("test_client_multi_server")
        .connect()
        .await
        .expect("Client should connect to both");
    let cjob = client
        .submit_unique("multifunc", b"cjob", b"cjobdata")
        .await
        .expect("submit should work on multiple servers");
    let status = client
        .get_status(cjob.handle())
        .await
        .expect("Should be able to get status on job submitted");
    assert!(status.known);
    let cjob_server_str = cjob.handle().server();
    let job_server = if cjob_server_str == &server1_str {
        server1
    } else if cjob.handle().server() == &server2_str {
        server2
    } else {
        panic!("Server handle for non-server?");
    };
    // Now shut down the one that it was connected to
    drop(job_server);
    let mut retries = 5;
    let status_error = loop {
        if client.active_servers().contains(&cjob_server_str) {
            retries -= 1;
            if retries <= 0 {
                panic!("Failed to detect disconnected server");
            }
            sleep(Duration::from_millis(100)).await;
        }
        // It's expected to timeout, but we want to try again until the server disappears and we get
        // the error.
        match timeout(Duration::from_millis(100), client.get_status(cjob.handle())).await {
            Err(_) => continue,
            Ok(status_result) => match status_result {
                Ok(_status) => continue,
                Err(e) => break e,
            },
        };
    };
    assert_eq!(status_error.kind(), ErrorKind::Other);
}
