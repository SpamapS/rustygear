use bytes::BytesMut;

use rustygear::codec::Packet;

use crate::queues::SharedJobStorage;
use crate::worker::{SharedWorkers, Wake};

pub fn admin_command_status(storage: SharedJobStorage, workers: SharedWorkers) -> Packet {
    let mut response = BytesMut::with_capacity(1024 * 1024); // XXX Wild guess.
    let storage = storage.lock().unwrap();
    let queues = storage.queues();
    for (func, fqueues) in queues.iter() {
        let mut qtot = 0;
        for q in fqueues {
            qtot += q.len();
        }
        let (active_workers, inactive_workers) = workers.clone().count_workers(func);
        response.extend(func);
        response.extend(
            format!(
                "\t{}\t{}\t{}\n",
                qtot,
                active_workers,
                inactive_workers + active_workers
            ).into_bytes(),
        );
    }
    response.extend(b".\n");
    let response = response.freeze();
    Packet::new_text_res(response)
}
