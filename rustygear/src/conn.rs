use std::slice::{IterMut, Iter};

use hashring::HashRing;

use crate::client::ClientHandler;

pub struct Connections {
    conns: Vec<ClientHandler>,
    ring: HashRing<usize>,
}

impl Connections {
    pub fn new() -> Connections {
        Connections {
            conns: Vec::new(),
            ring: HashRing::new(),
        }
    }

    pub fn insert(&mut self, offset: usize, handler: ClientHandler) {
        self.conns.insert(offset, handler);
        self.ring.add(offset);
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn get(&self, index: usize) -> Option<&ClientHandler> {
        self.conns.get(index)
    }

    pub fn get_hashed_conn(&mut self, hashable: &Vec<u8>) -> Option<&mut ClientHandler> {
        if let Some(ring_index) = self.ring.get(hashable) {
            return self.conns.get_mut(*ring_index)
        }
        None
    }

    pub fn iter_mut(&mut self) -> IterMut<ClientHandler> {
        self.conns.iter_mut()
    }

    pub fn iter(&self) -> Iter<ClientHandler> {
        self.conns.iter()
    }
}