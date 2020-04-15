//use tokio::prelude::*;

use rustygear::client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut client = Client::new().add_server("127.0.0.1:4730").connect().await?;
    println!("Connected!");
    println!("Echo: {:?}", client.echo(b"blah").await);
    let job = client.submit_background("reverse", b"abcdefg").await?;
    println!("Submitted {:?}", job.handle());
    let status = client.get_status(job.handle()).await?;
    println!("Status {:?}", status);
    let mut job = client.submit("reverse", b"bloop").await?;
    println!("Submitted {:?}", job.handle());
    let response = job.response().await;
    println!("Got Back {:?}", response);
    Ok(())
}
