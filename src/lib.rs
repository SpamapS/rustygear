extern crate time;

use std::option::Option;
use std::cmp::Ordering;

#[test]
fn constructor() {
    let c = Connection::new("localhost", 4730);

    assert!(c.host == "localhost");
    assert!(c.port == 4730);
    assert!(c.state == ConnectionState::INIT);

    let now = time::now_utc();
    match c.state_time {
        Some(c_time) => {
            let result = now.cmp(&c_time);  // Could fail if no nanoseconds
            assert_eq!(Ordering::Greater, result);
        },
        None => assert!(false)
    }
}

#[test]
fn change_state() {
    let mut c = Connection::new("localhost", 4730);

    c.change_state(ConnectionState::INIT);
    assert!(c.state == ConnectionState::INIT);
}

#[derive(PartialEq)]
enum ConnectionState {
    INIT,
}

struct Connection {
    host: String,
    port: u32,
    connected: bool,
    connect_time: Option<time::Tm>,
    state: ConnectionState,
    state_time: Option<time::Tm>,
}

impl Connection {
    fn new(host: &str, port: u32) -> Connection {
        let mut c = Connection {
            host: host.to_string(),
            port: port,
            connected: false,
            connect_time: None,
            state: ConnectionState::INIT,
            state_time: None,
        };
        c.change_state(ConnectionState::INIT);
        return c
    }

    fn change_state(&mut self, state: ConnectionState) {
        self.state_time = Some(time::now_utc());
        self.state = state;
    }
}
