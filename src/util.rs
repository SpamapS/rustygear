use std::sync::{RwLock, LockResult, RwLockWriteGuard, RwLockReadGuard, PoisonError};
use std::ops::{Deref, DerefMut};

pub struct LoggingRwLock<T: ?Sized> {
    lock: RwLock<T>,
}

impl <T> LoggingRwLock<T> {
    pub fn new(t: T) -> LoggingRwLock<T> {
        LoggingRwLock {
            lock: RwLock::new(t),
        }
    }
}

impl<T: ?Sized> LoggingRwLock<T> {
    pub fn write(&self, site: u32) -> LockResult<LoggingRwLockWriteGuard<T>> {
       debug!("write() called at line {}", site);
       match self.lock.write() {
           Ok(locked) => {
               Ok(LoggingRwLockWriteGuard{guard: locked})
           },
           Err(locked) => {
               Err(PoisonError::new(LoggingRwLockWriteGuard{guard: locked.into_inner()}))
           },
       }
    }

    pub fn read(&self) -> LockResult<LoggingRwLockReadGuard<T>> {
        debug!("read() called");
        match self.lock.read() {
            Ok(locked) => {
                Ok(LoggingRwLockReadGuard{guard: locked})
            },
            Err(locked) => {
                Err(PoisonError::new(LoggingRwLockReadGuard{guard: locked.into_inner()}))
            },
        }
    }
}

pub struct LoggingRwLockReadGuard<'a, T: ?Sized + 'a> {
    guard: RwLockReadGuard<'a, T>,
}

impl <'rwlock, T: ?Sized> Deref for LoggingRwLockReadGuard<'rwlock, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.deref()
    }
}
impl <'a, T: ?Sized> Drop for LoggingRwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        debug!("dropping lock");
    }
}

pub struct LoggingRwLockWriteGuard<'a, T: ?Sized + 'a> {
    guard: RwLockWriteGuard<'a, T>,
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
        debug!("dropping write lock");
    }
}
