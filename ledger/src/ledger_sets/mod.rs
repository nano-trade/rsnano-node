mod any;
mod confirmed;
mod unconfirmed;

pub use any::*;
pub use confirmed::*;
pub(crate) use unconfirmed::*;

use rsnano_core::{Account, AccountInfo, Amount, BlockHash};

pub trait LedgerSet {
    fn block_exists(&self, hash: &BlockHash) -> bool;
    fn account_receivable(&self, account: &Account) -> Amount;
    fn account_balance(&self, account: &Account) -> Amount;
    fn get_account(&self, account: &Account) -> Option<AccountInfo>;
}
