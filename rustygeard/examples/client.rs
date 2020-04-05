//use tokio::prelude::*;

use rustygeard::client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let mut client = Client::new().add_server("127.0.0.1:4730").connect().await?;
    println!("Connected!");
    println!("Echo: {:?}", client.echo(b"blah").await);
    /* TODO
    let job = client.submit("reverse", "abcdefg").await;
    println!("Submitted {}", job.handle());
    let response = job.response().await;
    println!("Got Back {}", response.data());
    */
    Ok(())
}
