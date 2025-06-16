use crate::account_map::AccountMap;
use rsnano_core::{Amount, Block, BlockHash, PrivateKey, StateBlockArgs};

pub(crate) struct BlockFactory {
    genesis_key: PrivateKey,
    genesis_hash: BlockHash,
    max_blocks: usize,
    created: usize,
    account_map: AccountMap,
}

impl BlockFactory {
    // Send from genesis
    const INITIAL_AMOUNT_SENT: Amount = Amount::nano(100_000_000);

    pub(crate) fn new(
        genesis_key: PrivateKey,
        genesis_hash: BlockHash,
        account_map: AccountMap,
        max_blocks: usize,
    ) -> Self {
        Self {
            genesis_key,
            genesis_hash,
            max_blocks,
            created: 0,
            account_map,
        }
    }

    pub fn create_next(&mut self) -> Option<Block> {
        if self.created >= self.max_blocks {
            return None;
        }

        let block =
            if let Some((receiver, send_hash, amount_sent)) = self.account_map.next_receivable() {
                let state = self.account_map.state(&receiver).unwrap();
                let receive: Block = StateBlockArgs {
                    key: &state.key,
                    previous: state.frontier,
                    representative: state.key.public_key(),
                    balance: state.balance + amount_sent,
                    link: send_hash.into(),
                    work: 0.into(),
                }
                .into();

                receive
            } else {
                let destination = self.account_map.random_account().unwrap();

                // Initial send from genesis account
                let genesis_send: Block = StateBlockArgs {
                    key: &self.genesis_key,
                    previous: self.genesis_hash,
                    representative: self.genesis_key.public_key(),
                    balance: Amount::MAX - Self::INITIAL_AMOUNT_SENT,
                    link: destination.into(),
                    work: 0.into(),
                }
                .into();

                self.account_map.process_send(
                    destination,
                    genesis_send.hash(),
                    Self::INITIAL_AMOUNT_SENT,
                );

                genesis_send
            };

        self.created += 1;
        Some(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static TEST_GENESIS_KEY: LazyLock<PrivateKey> = LazyLock::new(|| PrivateKey::from(42));
    const TEST_GENESIS_HASH: BlockHash = BlockHash::from_bytes([10; 32]);
    const MAX_BLOCKS: usize = 4;

    #[test]
    fn initial_send_from_genesis_to_random_account() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        let block = block_factory.create_next().unwrap();
        let account = block.account_field().unwrap();
        let destination = block.destination_or_link();

        assert_eq!(account, TEST_GENESIS_KEY.account());
        assert_eq!(
            block.balance_field().unwrap(),
            Amount::MAX - BlockFactory::INITIAL_AMOUNT_SENT
        );
        assert!(block_factory.account_map.contains(&destination));
        assert!(block_factory
            .account_map
            .get_receivable(&destination)
            .is_some());
    }

    #[test]
    fn receive_from_genesis() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        // genesis send
        let send_genesis = block_factory.create_next().unwrap();
        let account = send_genesis.destination_or_link();

        let receive = block_factory.create_next().unwrap();
        assert_eq!(receive.account_field().unwrap(), account);
        assert_eq!(receive.link_field().unwrap(), send_genesis.hash().into());
    }

    fn test_account_map() -> AccountMap {
        let mut map = AccountMap::default();
        map.add_unopened(1.into());
        map.add_unopened(2.into());
        map.add_unopened(3.into());
        map.add_unopened(4.into());
        map.add_unopened(5.into());
        map
    }
}
