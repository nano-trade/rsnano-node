use crate::{AnySet2, LedgerConstants};
use rsnano_core::{Block, BlockBase, BlockHash, DependentBlocks, SavedBlock, StateBlock};

/// Finds all dependent blocks for a given block.
/// There can be at most two dependencies per block, namely "previous" and "link/source".
pub struct DependentBlocksFinder<'a, T>
where
    T: AnySet2,
{
    any: &'a T,
    constants: &'a LedgerConstants,
}

impl<'a, T> DependentBlocksFinder<'a, T>
where
    T: AnySet2,
{
    pub fn new(any: &'a T, constants: &'a LedgerConstants) -> Self {
        Self { any, constants }
    }

    pub fn find_dependent_blocks(&self, block: &SavedBlock) -> DependentBlocks {
        block.dependent_blocks(&self.constants.epochs, &self.constants.genesis_account)
    }

    pub fn find_dependent_blocks_for_unsaved_block(&self, block: &Block) -> DependentBlocks {
        match block {
            Block::LegacySend(b) => b.dependent_blocks(),
            Block::LegacyChange(b) => b.dependent_blocks(),
            Block::LegacyReceive(b) => b.dependent_blocks(),
            Block::LegacyOpen(b) => b.dependent_blocks(&self.constants.genesis_account),
            // a ledger lookup is needed if it is a state block!
            Block::State(state) => {
                let linked_block = if self.is_receive_or_change(state) {
                    state.link().into()
                } else {
                    BlockHash::zero()
                };
                DependentBlocks::new(block.previous(), linked_block)
            }
        }
    }

    fn is_receive_or_change(&self, state: &StateBlock) -> bool {
        !self.constants.epochs.is_epoch_link(&state.link()) && !self.is_send(state)
    }

    // This function is used in place of block.is_send() as it is tolerant to the block not having the sideband information loaded
    // This is needed for instance in vote generation on forks which have not yet had sideband information attached
    fn is_send(&self, block: &StateBlock) -> bool {
        if block.previous().is_zero() {
            return false;
        }

        let previous_balance = self
            .any
            .block_balance(&block.previous())
            .unwrap_or_default();
        block.balance() < previous_balance
    }
}
