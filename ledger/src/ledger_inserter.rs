use crate::{AnySet, Ledger, LedgerSet};
use rsnano_core::{
    Account, Amount, Block, BlockHash, PendingKey, PrivateKey, SavedBlock, StateBlockArgs,
    WorkNonce, DEV_GENESIS_KEY,
};

/// Provides a simplified interface for inserting blocks into the ledger (for tests)
pub struct LedgerInserter<'a> {
    ledger: &'a Ledger,
}

impl<'a> LedgerInserter<'a> {
    pub fn new(ledger: &'a Ledger) -> Self {
        Self { ledger }
    }

    pub fn genesis(&self) -> LedgerBlockInserter {
        LedgerBlockInserter {
            ledger: self.ledger,
            key: &DEV_GENESIS_KEY,
        }
    }

    pub fn account(&self, key: &'a PrivateKey) -> LedgerBlockInserter<'a> {
        LedgerBlockInserter {
            ledger: self.ledger,
            key,
        }
    }
}

pub struct LedgerBlockInserter<'a> {
    ledger: &'a Ledger,
    key: &'a PrivateKey,
}

impl<'a> LedgerBlockInserter<'a> {
    pub fn send(
        &mut self,
        destination: impl Into<Account>,
        amount: impl Into<Amount>,
    ) -> SavedBlock {
        let info = self.get_account_info();

        if info.block_count == 0 {
            panic!(
                "Cannot send from unopened account: {}",
                self.key.account().encode_account()
            );
        }

        let block: Block = StateBlockArgs {
            key: self.key,
            previous: info.head,
            representative: info.representative,
            balance: info.balance - amount.into(),
            link: destination.into().into(),
            work: WorkNonce::new(u64::MAX),
        }
        .into();

        self.ledger.process_one(&block).unwrap()
    }

    pub fn receive(&mut self, corresponding_send: BlockHash) -> SavedBlock {
        let info = self.get_account_info();
        let pending = self
            .ledger
            .any()
            .get_pending(&PendingKey::new(self.key.account(), corresponding_send))
            .expect("no pending receive found");

        let representative = if info.block_count == 0 {
            self.key.public_key()
        } else {
            info.representative
        };

        let block: Block = StateBlockArgs {
            key: self.key,
            previous: info.head,
            representative,
            balance: info.balance + pending.amount,
            link: corresponding_send.into(),
            work: WorkNonce::new(u64::MAX),
        }
        .into();

        self.ledger.process_one(&block).unwrap()
    }

    fn get_account_info(&mut self) -> rsnano_core::AccountInfo {
        self.ledger
            .any()
            .get_account(&self.key.account())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Ledger, DEV_GENESIS_ACCOUNT};

    #[test]
    fn insert_one_block() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let destination = PrivateKey::from(1);

        let inserted = inserter.genesis().send(&destination, 100);

        let acc_info = ledger.any().get_account(&DEV_GENESIS_ACCOUNT).unwrap();
        assert_eq!(acc_info.block_count, 2);
        assert_eq!(acc_info.head, inserted.hash());
        assert_eq!(inserted.balance(), Amount::MAX - Amount::raw(100));
        assert_eq!(inserted.destination_or_link(), destination.account());
    }

    #[test]
    fn open_account() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let destination = PrivateKey::from(1);

        let send = inserter.genesis().send(&destination, 100);
        let open = inserter.account(&destination).receive(send.hash());

        let acc_info = ledger.any().get_account(&destination.account()).unwrap();
        assert_eq!(acc_info.block_count, 1);
        assert_eq!(acc_info.head, open.hash());
        assert_eq!(open.balance(), Amount::raw(100));
    }
}
