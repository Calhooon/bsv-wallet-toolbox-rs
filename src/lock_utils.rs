//! Lock utility functions for safely acquiring RwLock guards.
//!
//! These helpers convert poisoned lock errors into `Error::Internal` instead of
//! panicking, making the codebase more resilient to panics in other threads.

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::Error;

/// Safely acquire a read lock, returning an error instead of panicking on poison.
pub fn lock_read<T>(lock: &RwLock<T>) -> crate::Result<RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|e| Error::Internal(format!("RwLock read poisoned: {}", e)))
}

/// Safely acquire a write lock, returning an error instead of panicking on poison.
pub fn lock_write<T>(lock: &RwLock<T>) -> crate::Result<RwLockWriteGuard<'_, T>> {
    lock.write()
        .map_err(|e| Error::Internal(format!("RwLock write poisoned: {}", e)))
}
