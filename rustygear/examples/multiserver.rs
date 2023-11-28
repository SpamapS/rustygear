//use tokio::prelude::*;

use bytes::Bytes;
use rustygear::client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut client = Client::new()
        .add_server("127.0.0.1:4730")
        .add_server("127.0.0.1:4731").connect().await?;
    println!("Connected!");
    println!("Echo: {:?}", client.echo(b"blah").await);
    let mut jobs = Vec::new();
    for x in 0..10 {
        let payload = format!("payload{}", x);
        jobs.push(client.submit("reverse", payload.as_bytes()).await?);
    }
    println!("Submitted {:?}", jobs.iter().map(|job| job.handle()).collect::<Vec<&Bytes>>());
    for job in jobs.iter_mut() {
        println!("Response for [{}] is [{:?}]",
            String::from_utf8(job.handle().to_vec()).unwrap(),
            job.response().await?)
    };
    Ok(())
}
