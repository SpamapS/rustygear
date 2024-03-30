use rustygeard::testutil::start_test_server;

#[test]
fn test_server_starts() {
    start_test_server().expect(
        "No connection and no panics probably means no available ports.");
}