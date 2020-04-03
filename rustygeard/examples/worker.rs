use tokio::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker = Worker::new().add_host('::1');
    if let Ok(worker) = worker.connect().await {
        worker.work(|job| {
            job.response(job.data().reverse())
        })
    }
}
