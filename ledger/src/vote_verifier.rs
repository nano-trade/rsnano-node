use std::collections::VecDeque;

use rsnano_core::{BlockHash, Root};
use rsnano_store_lmdb::{LmdbStore, LmdbWriteTransaction, Transaction, Writer};

use crate::{AnySet, BorrowingAnySet, LedgerConstants, OwningAnySet};

/// Verifies whether a vote (or a final vote) can be generated for a given block
pub(crate) struct VoteVerifier<'a> {
    pub constants: &'a LedgerConstants,
    pub store: &'a LmdbStore,
}

impl<'a> VoteVerifier<'a> {
    pub fn verify_votes(
        &self,
        candidates: VecDeque<(Root, BlockHash)>,
        is_final: bool,
    ) -> VecDeque<(Root, BlockHash)> {
        let mut verified = VecDeque::new();

        if is_final {
            let mut tx = self.store.tx_begin_write(Writer::VotingFinal);
            for (root, hash) in &candidates {
                tx.refresh_if_needed();
                if self.should_vote_final(&mut tx, root, hash) {
                    verified.push_back((*root, *hash));
                }
            }
        } else {
            let mut any = OwningAnySet::new(self.store, self.constants);
            for (root, hash) in &candidates {
                any.refresh_if_needed();
                if self.should_vote_non_final(&any, root, hash) {
                    verified.push_back((*root, *hash));
                }
            }
        };

        verified
    }

    fn should_vote_non_final(&self, any: &impl AnySet, root: &Root, hash: &BlockHash) -> bool {
        let Some(block) = any.get_block(hash) else {
            return false;
        };
        debug_assert!(block.root() == *root);
        any.dependents_confirmed(&block)
    }

    fn should_vote_final(
        &self,
        tx: &mut LmdbWriteTransaction,
        root: &Root,
        hash: &BlockHash,
    ) -> bool {
        let any = BorrowingAnySet {
            constants: &self.constants,
            store: self.store,
            tx,
        };
        let Some(block) = any.get_block(hash) else {
            return false;
        };
        debug_assert!(block.root() == *root);
        any.dependents_confirmed(&block)
            && self.store.final_vote.put(tx, &block.qualified_root(), hash)
    }
}
