use std::time::Duration;

use bytes::BytesMut;
use rustygear::{
    client::{Client, WorkUpdate, WorkerJob},
    constants::WORK_STATUS,
    util::new_req,
};
use rustygeard::testutil::{connect, connect_with_client_id, start_test_server};
use tokio::time::timeout;

#[tokio::test]
async fn test_worker_sends_bad_work_status() {
    let server = start_test_server().unwrap();
    let worker = connect_with_client_id(server.addr(), "status-worker").await;
    fn sends_status(work: &mut WorkerJob) -> Result<Vec<u8>, std::io::Error> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut data = BytesMut::new();
        data.extend(work.handle());
        data.extend(b"\0notnumbers\0notnumdenom");
        let packet = new_req(WORK_STATUS, data.freeze());
        rt.block_on(work.send_packet(packet))?;
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
    // We'll ignore the broken status packet and still get the WorkComplete
    // The timeout is here to protect the test suite because response can
    // Easily get disconnected from things if errors aren't handled right.
    let response = timeout(Duration::from_millis(500), job.response())
        .await
        .expect("Response happens within 500ms")
        .expect("Response to non-background job should not error");
    assert!(matches!(
        response,
        WorkUpdate::Complete {
            handle: _,
            payload: _
        }
    ));
}
