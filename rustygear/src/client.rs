/// You'll want to use this client as such:
///
/// let mut client = Client::new();
/// client.add_server("203.0.113.5");
/// client.add_server("198.51.100.9");
/// client.connect().wait();
/// let job = ClientJob::new(Bytes::from("sort"), Bytes::from("a\nc\nb\n"), None);
/// let answer = client.submit_job(job).wait();
///
///
use uuid::Uuid;

use bytes::Bytes;

pub struct ClientJob {
    fname: Bytes,
    unique: Bytes,
    data: Bytes,
    handle: Option<Bytes>,
    servers: Vec<Bytes>,
}


trait AddServer<T> {
    fn add_server(&mut self, server: T);
}

impl ClientJob {
    pub fn new(fname: Bytes, data: Bytes, unique: Option<Bytes>) -> ClientJob {
        let unique = match unique {
            None => Bytes::from(Uuid::new_v4().hyphenated().to_string()),
            Some(unique) => unique,
        };
        ClientJob {
            fname: fname,
            unique: unique,
            data: data,
            handle: None,
            servers: Vec::new(),
        }
    }
}

impl<T> AddServer<T> for ClientJob
where
    T: Into<Bytes>,
{
    fn add_server(&mut self, server: T) {
        self.servers.push(server.into())
    }
}
