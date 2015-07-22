use std::sync::{RwLock, LockResult, RwLockWriteGuard, RwLockReadGuard, PoisonError};
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

pub struct LoggingRwLock<T: ?Sized> {
    uuid: Uuid,
    lock: RwLock<T>,
}

impl <T> LoggingRwLock<T> {
    pub fn new(t: T) -> LoggingRwLock<T> {
        LoggingRwLock {
            lock: RwLock::new(t),
            uuid: Uuid::new_v4(),
        }
    }
}

impl<T: ?Sized> LoggingRwLock<T> {
    pub fn write(&self, site: u32) -> LockResult<LoggingRwLockWriteGuard<T>> {
        let write_uuid = Uuid::new_v4();
        debug!("{} write() called at line {} {}", self.uuid, site, write_uuid);
        match self.lock.write() {
            Ok(locked) => {
                Ok(LoggingRwLockWriteGuard{guard: locked, uuid: write_uuid.clone()})
            },
            Err(locked) => {
                Err(PoisonError::new(LoggingRwLockWriteGuard{guard: locked.into_inner(), uuid: write_uuid.clone()}))
            },
        }
    }

    pub fn read(&self, site: u32) -> LockResult<LoggingRwLockReadGuard<T>> {
        let read_uuid = Uuid::new_v4();
        debug!("{} read() called at line {} {}", self.uuid, site, read_uuid);
        match self.lock.read() {
            Ok(locked) => {
                Ok(LoggingRwLockReadGuard{guard: locked, uuid: read_uuid.clone()})
            },
            Err(locked) => {
                Err(PoisonError::new(LoggingRwLockReadGuard{guard: locked.into_inner(), uuid: read_uuid.clone()}))
            },
        }
    }
}

pub struct LoggingRwLockReadGuard<'a, T: ?Sized + 'a> {
    guard: RwLockReadGuard<'a, T>,
    uuid: Uuid,
}

impl <'rwlock, T: ?Sized> Deref for LoggingRwLockReadGuard<'rwlock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.deref()
    }
}
impl <'a, T: ?Sized> Drop for LoggingRwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        debug!("{} dropping lock", self.uuid);
    }
}

pub struct LoggingRwLockWriteGuard<'a, T: ?Sized + 'a> {
    guard: RwLockWriteGuard<'a, T>,
    uuid: Uuid,
}

impl <'rwlock, T: ?Sized> Deref for LoggingRwLockWriteGuard<'rwlock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.deref()
    }
}
impl <'rwlock, T: ?Sized> DerefMut for LoggingRwLockWriteGuard<'rwlock, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.deref_mut()
    }
}
impl <'a, T: ?Sized> Drop for LoggingRwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        debug!("{} dropping write lock", self.uuid);
    }
}
