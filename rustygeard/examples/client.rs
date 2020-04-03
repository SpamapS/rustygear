//use tokio::prelude::*;

use rustygeard::client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new().add_server("127.0.0.1:4730").connect().await?;
    /* TODO
    let job = client.submit("reverse", "abcdefg").await;
    println!("Submitted {}", job.handle());
    let response = job.response().await;
    println!("Got Back {}", response.data());
    */
    Ok(())
}
