use std::thread;
use std::time::Duration;

use tokio::prelude::*;

use rustygear::client::{Client, WorkerJob};

/// When we use the status method, we need to be async!
async fn status_user(mut job: WorkerJob) -> Result<Vec<u8>, io::Error> {
    job.work_status(50, 100).await?;
    thread::sleep(Duration::from_secs(1));
    let mut rs = Vec::new();
    rs.extend_from_slice("all done".as_bytes());
    Ok(rs)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let worker = Client::new();
    worker
        .add_server("127.0.0.1:4730")
        .connect()
        .await
        .expect("CONNECT failed")
        .can_do("reverse", |job| {
            let payload = String::from_utf8(job.payload().to_vec()).unwrap();
            println!("reversing {}", payload);
            let reversed: String = payload.chars().rev().collect();
            let reversed: Vec<u8> = reversed.into_bytes();
            Ok(reversed)
        })
        .await
        .expect("CAN_DO reverse failed")
        .can_do("alwaysfail", |_job| {
            Err(io::Error::new(io::ErrorKind::Other, "Always fails"))
        })
        .await
        .expect("CAN_DO alwaysfail failed")
        //.can_do_async("status", status_user)
        .work()
        .await
        .expect("WORK FAILED");
    Ok(())
}
