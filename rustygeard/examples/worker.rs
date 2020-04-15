use std::time::Duration;
use std::thread;

use tokio::prelude::*;

use rustygeard::client::{Client, ClientJob};

/// When we use the status method, we need to be async!
async fn status_user(job: ClientJob) -> Result<Vec<u8>, io::Error> {
    job.work_status(50, 100).await;
    thread::sleep(Duration::from_secs(1));
    let rs = Vec::new();
    rs.extend_from_slice("all done".as_bytes());
    Ok(rs)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = Client::new();
    worker
        .connect()
        .await?
        .can_do("reverse", |job| Ok(job.payload().reverse()))
        .await?
        .can_do("alwaysfail", |job| {
            Err(io::Error::new(io::ErrorKind::Other, "Always fails"))
        })
        .await?
        //.can_do_async("status", status_user)
        .work()
        .await?
}
