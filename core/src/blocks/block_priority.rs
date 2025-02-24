use crate::{utils::UnixTimestamp, Amount, SavedBlock};
use std::cmp::max;

pub fn block_priority(
    block: &SavedBlock,
    previous_block: Option<&SavedBlock>,
) -> (Amount, UnixTimestamp) {
    let previous_balance = previous_block
        .as_ref()
        .map(|b| b.balance())
        .unwrap_or_default();

    // Handle full send case nicely where the balance would otherwise be 0
    let priority_balance = max(
        block.balance(),
        if block.is_send() {
            previous_balance
        } else {
            Amount::zero()
        },
    );

    // Use previous block timestamp as priority timestamp for least recently used
    // prioritization within the same bucket
    // Account info timestamp is not used here because it will get out of sync when
    // rollbacks happen
    let priority_timestamp = previous_block
        .map(|b| b.timestamp())
        .unwrap_or(block.timestamp());

    (priority_balance, priority_timestamp)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{BlockSideband, StateBlockArgs};

    #[test]
    fn open_block() {
        let open = SavedBlock::new_test_open_block();

        let (prio_balance, prio_time) = block_priority(&open, None);

        assert_eq!(prio_balance, open.balance());
        assert_eq!(prio_time, open.timestamp());
    }

    #[test]
    fn receive_block() {
        let prev_timestamp = UnixTimestamp::new_test_instance();
        let receive_balance = Amount::nano(2000);

        let (prio_balance, prio_time) = test_block_priority(
            receive_balance,
            prev_timestamp + Duration::from_secs(1),
            Some((Amount::nano(1000), prev_timestamp)),
        );

        assert_eq!(prio_balance, receive_balance);
        assert_eq!(prio_time, prev_timestamp);
    }

    #[test]
    fn send_block() {
        let prev_timestamp = UnixTimestamp::new_test_instance();
        let prev_balance = Amount::nano(100);

        let (prio_balance, prio_time) = test_block_priority(
            Amount::nano(50),
            prev_timestamp + Duration::from_secs(1),
            Some((prev_balance, prev_timestamp)),
        );

        assert_eq!(prio_balance, prev_balance);
        assert_eq!(prio_time, prev_timestamp);
    }

    #[test]
    fn full_send() {
        let prev_timestamp = UnixTimestamp::new_test_instance();
        let prev_balance = Amount::nano(100);

        let (prio_balance, prio_time) = test_block_priority(
            Amount::zero(),
            prev_timestamp + Duration::from_secs(1),
            Some((prev_balance, prev_timestamp)),
        );

        assert_eq!(prio_balance, prev_balance);
        assert_eq!(prio_time, prev_timestamp);
    }

    #[test]
    fn change_block() {
        let prev_timestamp = UnixTimestamp::new_test_instance();
        let prev_balance = Amount::nano(100);

        let (prio_balance, prio_time) = test_block_priority(
            prev_balance,
            prev_timestamp + Duration::from_secs(1),
            Some((prev_balance, prev_timestamp)),
        );

        assert_eq!(prio_balance, prev_balance);
        assert_eq!(prio_time, prev_timestamp);
    }

    fn test_block_priority(
        balance: Amount,
        timestamp: UnixTimestamp,
        previous: Option<(Amount, UnixTimestamp)>,
    ) -> (Amount, UnixTimestamp) {
        let previous = previous
            .map(|(prev_balance, prev_timestamp)| create_block(prev_balance, prev_timestamp));

        let block = create_block(balance, timestamp);
        block_priority(&block, previous.as_ref())
    }

    fn create_block(balance: Amount, timestamp: UnixTimestamp) -> SavedBlock {
        SavedBlock::new(
            StateBlockArgs {
                balance,
                ..StateBlockArgs::new_test_instance()
            }
            .into(),
            BlockSideband {
                timestamp,
                ..BlockSideband::new_test_instance()
            },
        )
    }
}
