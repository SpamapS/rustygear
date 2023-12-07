use std::slice::{IterMut, Iter};

use hashring::HashRing;

use crate::client::ClientHandler;

pub struct Connections {
    conns: Vec<Option<ClientHandler>>,
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
        if offset > self.conns.len() {
            (0..offset).for_each(|_| self.conns.push(None))
        }
        self.conns.insert(offset, Some(handler));
        self.ring.add(offset);
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn get(&self, index: usize) -> Option<&ClientHandler> {
        match self.conns.get(index) {
            None => None,
            Some(c) => match c {
                None => None,
                Some(ref c) => Some(c)
            }
        }
    }

    pub fn get_hashed_conn(&mut self, hashable: &Vec<u8>) -> Option<&mut ClientHandler> {
        match self.ring.get(hashable) {
            None => None,
            Some(ring_index) => match self.conns.get_mut(*ring_index) {
                None => None,
                Some(c) => match c {
                    None => None,
                    Some(c) => Some(c)
                },
            }
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<Option<ClientHandler>> {
        self.conns.iter_mut()
    }

    pub fn iter(&self) -> Iter<Option<ClientHandler>> {
        self.conns.iter()
    }
}