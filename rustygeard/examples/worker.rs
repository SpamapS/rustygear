use std::time::Duration;
use std::thread;

use tokio::prelude::*;

use rustygeard::client::{Client, ClientJob};

/// When we use the status method, we need to be async!
async fn status_user(mut job: ClientJob) -> Result<Vec<u8>, io::Error> {
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
        .await.expect("CONNECT failed")
        .can_do("reverse", |job| {
            println!("reversing {:?}", job.payload());
            Ok(())
        })
        .await.expect("CAN_DO reverse failed")
        .can_do("alwaysfail", |_job| {
            Err(io::Error::new(io::ErrorKind::Other, "Always fails"))
        })
        .await.expect("CAN_DO alwaysfail failed")
        //.can_do_async("status", status_user)
        .work()
        .await.expect("WORK FAILED");
    Ok(())
}
