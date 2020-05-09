#[cfg(any(feature = "replica_local", feature = "replica_tcp"))]
mod core;
#[cfg(any(feature = "replica_local", feature = "replica_tcp"))]
pub use self::core::*;

#[cfg(not(any(feature = "replica_local", feature = "replica_tcp")))]
mod stub;
#[cfg(not(any(feature = "replica_local", feature = "replica_tcp")))]
pub use self::stub::*;
