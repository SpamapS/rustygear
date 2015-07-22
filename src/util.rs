/*
 * Copyright (c) 2015, Hewlett Packard Development Company L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::{RwLock, LockResult, RwLockWriteGuard, RwLockReadGuard, PoisonError};
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

/// A wrapper for RwLock to aid in debugging
///
/// Use this like RwLock to get a logs of where locks are taken and released
/// You will see UUID's of the lock itself, and each instance of each lock
/// guard so you can trace these through the debug log and find deadlocks.
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

    /// Wrapper for RwLock::write
    ///
    /// Takes the additional line argument for the line number of the call
    pub fn write(&self, line: u32) -> LockResult<LoggingRwLockWriteGuard<T>> {
        let write_uuid = Uuid::new_v4();
        debug!("{} write() called at line {} {}", self.uuid, line, write_uuid);
        match self.lock.write() {
            Ok(locked) => {
                Ok(LoggingRwLockWriteGuard{guard: locked, uuid: write_uuid.clone()})
            },
            Err(locked) => {
                Err(PoisonError::new(LoggingRwLockWriteGuard{guard: locked.into_inner(), uuid: write_uuid.clone()}))
            },
        }
    }

    /// Wrapper for RwLock::read
    ///
    /// Takes the additional line argument for the line number of the call
    pub fn read(&self, line: u32) -> LockResult<LoggingRwLockReadGuard<T>> {
        let read_uuid = Uuid::new_v4();
        debug!("{} read() called at line {} {}", self.uuid, line, read_uuid);
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

/// Wrapper for RwLockReadGuard
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

/// Wrapper for RwLockWriteuard
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
