use rsnano_core::{Account, BlockHash, DetailedBlock};
use rsnano_ledger::{AnySet, Ledger};

pub(crate) struct Explorer {
    state: ExplorerState,
}

impl Explorer {
    pub(crate) fn new() -> Self {
        Self {
            state: ExplorerState::Empty,
        }
    }

    pub(crate) fn search(&mut self, ledger: &Ledger, input: &str) -> bool {
        if let Ok(hash) = BlockHash::decode_hex(input.trim()) {
            let any = ledger.any();
            self.state = match any.detailed_block(&hash) {
                Some(block) => ExplorerState::Block(block),
                None => ExplorerState::NotFound,
            };
            return true;
        };

        if let Ok(account) = Account::decode_account(input) {
            let any = ledger.any();
            self.state = if let Some(head) = any.account_head(&account) {
                match any.detailed_block(&head) {
                    Some(block) => ExplorerState::Block(block),
                    None => ExplorerState::NotFound,
                }
            } else {
                ExplorerState::NotFound
            };
            return true;
        }

        false
    }

    pub(crate) fn state(&self) -> &ExplorerState {
        &self.state
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ExplorerState {
    Empty,
    NotFound,
    Block(DetailedBlock),
}
