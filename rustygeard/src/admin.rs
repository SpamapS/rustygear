use bytes::{BufMut, Bytes, BytesMut};

use rustygear::codec::Packet;
use rustygear::constants::{PRIORITY_HIGH, PRIORITY_NORMAL, PRIORITY_LOW};

use crate::queues::SharedJobStorage;
use crate::worker::{SharedWorkers, Wake};
use crate::service::WorkersByConnId;

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
            )
            .into_bytes(),
        );
    }
    response.extend(b".\n");
    let response = response.freeze();
    Packet::new_text_res(response)
}

pub fn admin_command_priority_status(storage: SharedJobStorage, workers: SharedWorkers) -> Packet {
    let mut response = BytesMut::with_capacity(1024 * 1024); // XXX Wild guess.
    let storage = storage.lock().unwrap();
    let queues = storage.queues();
    for (func, fqueues) in queues.iter() {
        let (active_workers, inactive_workers) = workers.clone().count_workers(func);
        response.extend(func);
        response.extend(
            format!(
                "\t{}\t{}\t{}\t{}\n",
                fqueues[PRIORITY_HIGH].len(),
                fqueues[PRIORITY_NORMAL].len(),
                fqueues[PRIORITY_LOW].len(),
                inactive_workers + active_workers
            )
            .into_bytes(),
        );
    }
    response.extend(b".\n");
    let response = response.freeze();
    Packet::new_text_res(response)
}

pub fn admin_command_workers(workers: WorkersByConnId) -> Packet {
    let mut response = BytesMut::with_capacity(1024 * 1024); // XXX Wild guess.
    let workers = workers.lock().unwrap();
    for (conn_id, worker) in workers.iter() {
        // This is mutable because it will wrap around the wrapping hashset
        // Since we'll fully wrap it, while locked, that should be fine and
        // actually makes the workers command more useful as it lets us see
        // where in the roundrobin each worker is
        let mut worker = worker.lock().unwrap();
        let client_id = String::from_utf8(worker.client_id.to_vec()).unwrap();
        response.extend(format!("{} {} {} :", conn_id, worker.peer_addr, client_id).bytes());
        for func in worker.functions.iter() {
            response.put_u8(b' ');
            response.extend(func);
        }
        response.put_u8(b'\n');
    }
    response.extend(b".\n");
    let response = response.freeze();
    Packet::new_text_res(response)
}

pub fn admin_command_unknown() -> Packet {
    Packet::new_text_res(Bytes::from_static(b"ERR UNKNOWN_COMMAND Unknown+server+command\n"))
}
