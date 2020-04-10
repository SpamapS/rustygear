use std::time::Duration;
use std::thread;

use tokio::prelude::*;

use rustygeard::client::{Worker, WorkerJob};

/// When we use the status method, we need to be async!
async fn status_user(job: WorkerJob) -> Result<(), io::Error> {
    job.status(50, 100).await;
    thread::sleep(Duration::from_secs(1));
    Ok("all done")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = Worker::new();
    worker
        .connect()
        .await?
        .can_do("reverse", |job| Ok(job.data().reverse()))
        .can_do("alwaysfail", |job| {
            Err(io::Error::new(io::ErrorKind::Other, "Always fails"))
        })
        .can_do_async("status", status_user)
        .work()
        .await?
}
