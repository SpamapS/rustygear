use std::{io::ErrorKind, time::Duration};

use rustygear::client::{Client, WorkUpdate, WorkerJob};
use rustygeard::testutil::{connect, connect_with_client_id, start_test_server, worker};
use tokio::time::{sleep, timeout};

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
    assert!(status_error.is::<std::io::Error>());
    assert_eq!(
        status_error
            .downcast::<std::io::Error>()
            .expect("downcast after is")
            .kind(),
        ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn test_worker_multi_server() {
    let server1 = start_test_server().unwrap();
    let server2 = start_test_server().unwrap();
    let server1_str = server1.addr().to_string();
    let server2_str = server2.addr().to_string();
    let mut worker = Client::new()
        .add_server(&server1_str)
        .add_server(&server2_str)
        .set_client_id("test_worker_multi_server")
        .connect()
        .await
        .expect("Client should connect to both")
        .can_do("multifunc", |wj| {
            Ok(format!("worker saw handle={}", String::from_utf8_lossy(wj.handle())).into_bytes())
        })
        .await
        .expect("CAN_DO should succeed");
    let mut client = connect(server1.addr()).await;
    let mut cj = client
        .submit_unique("multifunc", b"id:multi1", b"multipayload")
        .await
        .expect("Submit job should be fine");
    worker
        .do_one_job()
        .await
        .expect("Should not error while doing one job");
    assert!(matches!(
        cj.response().await.expect("Should receive a complete!"),
        WorkUpdate::Complete {
            handle: _handle,
            payload: _payload
        }
    ));
    // And now on server 2
    let mut cj2 = connect(server2.addr())
        .await
        .submit_unique("multifunc", b"id2:999", b"payload2")
        .await
        .expect("Submitting to server 2 should work");
    worker
        .do_one_job()
        .await
        .expect("Second job should be done");
    assert!(matches!(
        cj2.response().await.expect("Should receive a complete!"),
        WorkUpdate::Complete {
            handle: _handle,
            payload: _payload
        }
    ));
}

#[tokio::test]
async fn test_work_fail() {
    let server = start_test_server().unwrap();
    let worker = connect_with_client_id(server.addr(), "failing-worker").await;
    let mut worker = worker
        .can_do("alwaysfail", |_work| {
            Err(std::io::Error::new(ErrorKind::Other, "Epic Fail!"))
        })
        .await
        .expect("CAN_DO should succeed");
    let mut client: Client = connect(server.addr()).await;
    let mut job = client
        .submit("alwaysfail", b"preciouspayload")
        .await
        .expect("Submit should succeed");
    worker
        .do_one_job()
        .await
        .expect("One job should be completed");
    let response = job.response().await.expect("Should receive a WorkUpdate");
    if let WorkUpdate::Fail(handle) = response {
        assert_eq!(handle, job.handle().handle())
    } else {
        panic!("Job did not fail! {:?}", response);
    }
}

#[tokio::test]
async fn test_work_status() {
    let server = start_test_server().unwrap();
    let worker = connect_with_client_id(server.addr(), "status-worker").await;
    fn sends_status(work: &mut WorkerJob) -> Result<Vec<u8>, std::io::Error> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(work.work_status(50, 100))?;
        Ok("Done".into())
    }
    let mut worker = worker
        .can_do("statusfunc", sends_status)
        .await
        .expect("CAN_DO should succeed");
    let mut client: Client = connect(server.addr()).await;
    let mut job = client
        .submit("statusfunc", b"statuspayload")
        .await
        .expect("Submit should succeed");
    worker
        .do_one_job()
        .await
        .expect("One job should be completed");
    let response = job.response().await.expect("Should receive a WorkUpdate");
    if let WorkUpdate::Status {
        handle,
        numerator,
        denominator,
    } = response
    {
        assert_eq!(handle, job.handle().handle());
        assert_eq!(numerator, 50);
        assert_eq!(denominator, 100);
    } else {
        panic!("No status returned! {:?}", response);
    }
    let response2 = job
        .response()
        .await
        .expect("Second response should be complete");
    if let WorkUpdate::Complete { handle, payload } = response2 {
        assert_eq!(handle, job.handle().handle());
        assert_eq!(payload, "Done");
    } else {
        panic!("Did not return a WORK_COMPLETE! {:?}", response2);
    }
}

#[tokio::test]
async fn test_unique_routing() {
    // Jobs with explicit unique IDs should always be sent to the same gearmand
    // We need lots of servers on the ring to be more confident about this
    // effect otherwise the test could be flaky if the hasher's RNG is fooling us.
    let mut servers: Vec<rustygeard::testutil::ServerGuard> = (0..10)
        .map(|_i| start_test_server().expect("Starting test server"))
        .collect();
    let client1 = Client::new();
    let client2 = Client::new();
    let client1 = servers.iter().fold(client1, |client, server| {
        client.add_server(&server.addr().to_string())
    });
    let client2 = servers.iter().fold(client2, |client, server| {
        client.add_server(&server.addr().to_string())
    });
    let mut client1 = client1.connect().await.expect("Connecting to all servers");
    let mut client2 = client2.connect().await.expect("Connecting to all servers");
    let job1 = client1
        .submit_unique("testfunc", b"uniqid1", b"dostuff")
        .await
        .expect("Submitting a job");
    let job2 = client2
        .submit_unique("testfunc", b"uniqid1", b"dootherstuff")
        .await
        .expect("Submitting a job");
    assert_eq!(job1.handle().server(), job2.handle().server());
    let routed_server = job1.handle().server();
    // Now let's kill that server and see if the next two jobs go to the same fallback place
    let (offset, _serverguard) = servers
        .iter()
        .enumerate()
        .find(|(_offset, server)| server.addr().to_string() == *routed_server)
        .expect("Find server we just used in server list");
    servers.swap_remove(offset);
    // Wait until it's not in active servers
    loop {
        if client1.active_servers().contains(routed_server) {
            sleep(Duration::from_millis(100)).await;
        } else {
            break;
        }
    }
    // Now another should route to a different server
    let after_dcon = client1
        .submit_unique("testfunc", b"uniqid1", b"afterdcon")
        .await
        .expect("Submitting a job after losing one server");
    assert_ne!(after_dcon.handle().server(), routed_server);
}
