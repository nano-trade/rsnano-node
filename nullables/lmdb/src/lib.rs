mod configured_database;
mod database;
mod environment;
mod ro_cursor;
mod ro_transaction;
mod rw_cursor;
mod rw_transaction;

pub use configured_database::*;
pub use database::*;
pub use environment::*;
pub use lmdb::{DatabaseFlags, EnvironmentFlags, Error, Stat, WriteFlags};

pub mod sys {
    pub use lmdb_sys::*;
}

pub use ro_cursor::*;
pub use ro_transaction::*;
pub use rw_cursor::*;
pub use rw_transaction::*;
use std::time::Duration;

pub type Result<T> = std::result::Result<T, Error>;

pub trait Transaction {
    fn refresh(&mut self);
    fn refresh_if_needed(&mut self) -> bool;
    fn is_refresh_needed(&self) -> bool;
    fn is_refresh_needed_with(&self, max_duration: Duration) -> bool;
    fn get(&self, database: LmdbDatabase, key: &[u8]) -> Result<&[u8]>;
    fn exists(&self, db: LmdbDatabase, key: &[u8]) -> bool {
        match self.get(db, key) {
            Ok(_) => true,
            Err(lmdb::Error::NotFound) => false,
            Err(e) => panic!("exists failed: {:?}", e),
        }
    }
    fn open_ro_cursor(&self, database: LmdbDatabase) -> Result<RoCursor>;
    fn count(&self, database: LmdbDatabase) -> u64;
}
