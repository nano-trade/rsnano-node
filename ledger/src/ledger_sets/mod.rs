mod any;
mod confirmed;
mod unconfirmed;

pub use any::*;
pub use confirmed::*;
pub(crate) use unconfirmed::*;

use rsnano_core::BlockHash;

pub trait LedgerSet {
    fn block_exists(&self, hash: &BlockHash) -> bool;
}
