use rustygear::client::Client;
use std::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let worker = Client::new();
    worker
        .add_server("127.0.0.1:4730")
        //.add_server("127.0.0.1:4731")  Add all of your servers here
        .set_client_id("example")
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
