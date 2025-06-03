use std::{fmt, io};
use shared_memory; // Keep this

// raw_sync related errors will be handled as Box<dyn std::error::Error + Send + Sync + 'static>

#[derive(Debug)]
pub enum ShmemLibError {
    SharedMemory(shared_memory::ShmemError),
    Lock(Box<dyn std::error::Error + 'static>), // Simplified: what raw_sync seems to provide
    Io(io::Error),
    SignalHook(io::Error), // signal_hook errors are often io::Error
    PoisonedLock,
    Logic(String),
}

impl fmt::Display for ShmemLibError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShmemLibError::SharedMemory(e) => write!(f, "Shared memory error: {}", e),
            ShmemLibError::Lock(e) => write!(f, "Lock error: {}", e),
            ShmemLibError::Io(e) => write!(f, "IO error: {}", e),
            ShmemLibError::SignalHook(e) => write!(f, "Signal handling error: {}", e),
            ShmemLibError::PoisonedLock => write!(f, "Mutex was poisoned"),
            ShmemLibError::Logic(s) => write!(f, "Logic error: {}", s),
        }
    }
}

impl std::error::Error for ShmemLibError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShmemLibError::SharedMemory(e) => Some(e),
            ShmemLibError::Lock(e) => Some(e.as_ref()), // e is a Box, so use as_ref()
            ShmemLibError::Io(e) => Some(e),
            ShmemLibError::SignalHook(e) => Some(e),
            ShmemLibError::PoisonedLock => None,
            ShmemLibError::Logic(_) => None,
        }
    }
}

impl From<shared_memory::ShmemError> for ShmemLibError {
    fn from(err: shared_memory::ShmemError) -> Self {
        ShmemLibError::SharedMemory(err)
    }
}

// This From impl is tricky. If raw_sync returns Box<dyn Error>, we need to ensure it's Send + Sync.
// Let's assume for now that the Box<dyn Error> we get from raw_sync is Send + Sync.
// We will construct this variant manually in core/mod.rs using map_err for now.
// impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for ShmemLibError {
//    fn from(err: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
//        ShmemLibError::Lock(err)
//    }
// }


impl From<io::Error> for ShmemLibError {
    fn from(err: io::Error) -> Self {
        ShmemLibError::Io(err)
    }
}

// Specific From for signal_hook if its error type is distinct and needs specific handling
// For now, assuming it's io::Error based on common practice with the crate.
// If signal_hook::Error is its own type, a specific From impl would be:
// impl From<signal_hook::Error> for ShmemLibError {
//     fn from(err: signal_hook::Error) -> Self {
//         ShmemLibError::SignalHook(err) // or wrap it if it's not io::Error
//     }
// }

// Helper for poisoned locks, typically from std::sync::Mutex
impl<T> From<std::sync::PoisonError<T>> for ShmemLibError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        ShmemLibError::PoisonedLock
    }
}

// We need a way to convert the Box<dyn Error> from raw_sync into ShmemLibError::Lock.
// This can be a helper function or direct mapping in core module.
// For now, direct mapping in core module is fine.
