use crate::account_map::AccountMap;
use rand::Rng;
use rsnano_core::{Amount, Block, BlockHash, Link, PublicKey, StateBlockArgs, WorkNonce};

pub(crate) struct BlockFactory {
    max_blocks: usize,
    created: usize,
    account_map: AccountMap,
    strategy: SpamStrategy,
}

pub(crate) enum BlockResult {
    Block(Block),
    Waiting,
}

impl BlockResult {
    #[allow(dead_code)]
    pub fn unwrap(self) -> Block {
        match self {
            BlockResult::Block(block) => block,
            BlockResult::Waiting => panic!("Expected block, but was in waiting state"),
        }
    }
}

pub(crate) enum SpamStrategy {
    SendReceive,
    Change,
}

impl BlockFactory {
    pub(crate) fn new(account_map: AccountMap, max_blocks: usize, strategy: SpamStrategy) -> Self {
        Self {
            max_blocks,
            created: 0,
            account_map,
            strategy,
        }
    }

    pub fn create_next(&mut self) -> Option<BlockResult> {
        if self.max_blocks > 0 && self.created >= self.max_blocks {
            return None;
        }

        if matches!(self.strategy, SpamStrategy::Change) {
            let Some(state) = self.account_map.random_account_that_can_send() else {
                return Some(BlockResult::Waiting);
            };
            let block: Block = StateBlockArgs {
                key: &state.key,
                previous: state.confirmed_frontier,
                representative: PublicKey::from_bytes(rand::rng().random()),
                balance: state.balance,
                link: Link::zero(),
                work: WorkNonce::new(0),
            }
            .into();
            self.account_map
                .process_change(state.key.account(), block.hash());
            self.created += 1;
            return Some(BlockResult::Block(block));
        }

        if let Some((receiver, send_hash, amount_sent)) = self.account_map.next_receivable() {
            let state = self.account_map.state(&receiver).unwrap();
            assert!(state.confirmed());
            let receive: Block = StateBlockArgs {
                key: &state.key,
                previous: state.confirmed_frontier,
                representative: state.key.public_key(),
                balance: state.balance + amount_sent,
                link: send_hash.into(),
                work: 0.into(),
            }
            .into();

            self.account_map
                .process_receive(receiver, send_hash, receive.hash());

            self.created += 1;
            Some(BlockResult::Block(receive))
        } else {
            if let Some(state) = self.account_map.random_account_that_can_send() {
                assert!(state.confirmed());
                let destination = self.account_map.random_account().unwrap();
                let new_balance: Amount = rand::rng().random_range(..state.balance.number()).into();
                let amount_sent = state.balance - new_balance;

                let send: Block = StateBlockArgs {
                    key: &state.key,
                    previous: state.confirmed_frontier,
                    representative: state.key.public_key(),
                    balance: new_balance,
                    link: destination.into(),
                    work: 0.into(),
                }
                .into();

                self.account_map.process_send(
                    state.key.account(),
                    destination,
                    send.hash(),
                    amount_sent,
                );
                self.created += 1;

                Some(BlockResult::Block(send))
            } else {
                Some(BlockResult::Waiting)
            }
        }
    }

    pub fn confirm(&mut self, hash: BlockHash) {
        self.account_map.confirm(hash);
    }

    pub fn created(&self) -> usize {
        self.created
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::PrivateKey;
    use std::time::Instant;

    const MAX_BLOCKS: usize = 4;

    #[test]
    fn initial_send_to_random_account() {
        let mut block_factory =
            BlockFactory::new(test_account_map(), MAX_BLOCKS, SpamStrategy::SendReceive);
        let block = block_factory.create_next().unwrap().unwrap();
        let account = block.account_field().unwrap();
        let destination = block.destination_or_link();

        assert_eq!(account, initial_test_key().account());
        assert!(block_factory.account_map.contains(&destination));
        assert!(block_factory
            .account_map
            .get_receivable(&destination)
            .is_some());
    }

    #[test]
    fn initial_receive() {
        let mut block_factory =
            BlockFactory::new(test_account_map(), MAX_BLOCKS, SpamStrategy::SendReceive);
        // genesis send
        let send = block_factory.create_next().unwrap().unwrap();
        block_factory.confirm(send.hash());
        let account = send.destination_or_link();

        let receive = block_factory.create_next().unwrap().unwrap();
        assert_eq!(receive.account_field().unwrap(), account);
        assert_eq!(receive.link_field().unwrap(), send.hash().into());
    }

    #[test]
    #[ignore = "run manually only"]
    fn benchmark() {
        let mut account_map = AccountMap::default();
        let initial_key = PrivateKey::new();
        account_map.add_unopened(initial_key.clone());
        account_map.set_account_state(initial_key.account(), Amount::nano(100_000_000), 123.into());
        for _ in 1..30_000 {
            account_map.add_unopened(PrivateKey::new());
        }

        let block_count = 10_000_000;

        let mut block_factory =
            BlockFactory::new(account_map, block_count, SpamStrategy::SendReceive);

        let mut start = Instant::now();
        let mut created_batch = 0;
        while let Some(BlockResult::Block(b)) = block_factory.create_next() {
            block_factory.confirm(b.hash());
            created_batch += 1;
            if created_batch == 50_000 {
                println!(
                    "Created {} blocks. {} bps",
                    created_batch,
                    (created_batch as f64 / start.elapsed().as_secs_f64()) as i32
                );
                start = Instant::now();
                created_batch = 0;
            }
        }
        println!(
            "Created {} blocks. {} bps",
            created_batch,
            (created_batch as f64 / start.elapsed().as_secs_f64()) as i32
        );
    }

    fn test_account_map() -> AccountMap {
        let mut map = AccountMap::default();
        let initial_key = initial_test_key();
        map.add_unopened(initial_key.clone());
        map.set_account_state(
            initial_key.account(),
            Amount::nano(100_000_000),
            BlockHash::from(123),
        );
        map.add_unopened(1.into());
        map.add_unopened(2.into());
        map.add_unopened(3.into());
        map.add_unopened(4.into());
        map.add_unopened(5.into());
        map
    }

    fn initial_test_key() -> PrivateKey {
        PrivateKey::from(42)
    }
}
