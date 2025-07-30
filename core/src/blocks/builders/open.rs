use crate::{
    blocks::open_block::OpenBlockArgs, utils::UnixTimestamp, Block, BlockDetails, BlockHash,
    BlockSideband, Epoch, PrivateKey, PublicKey, SavedBlock, WorkNonce,
};

pub struct TestLegacyOpenBlockBuilder {
    representative: Option<PublicKey>,
    source: Option<BlockHash>,
    prv_key: Option<PrivateKey>,
    work: Option<WorkNonce>,
}

impl TestLegacyOpenBlockBuilder {
    pub(super) fn new() -> Self {
        Self {
            representative: None,
            source: None,
            prv_key: None,
            work: None,
        }
    }

    pub fn source(mut self, source: BlockHash) -> Self {
        self.source = Some(source);
        self
    }

    pub fn representative(mut self, representative: PublicKey) -> Self {
        self.representative = Some(representative);
        self
    }

    pub fn sign(mut self, prv_key: &PrivateKey) -> Self {
        self.prv_key = Some(prv_key.clone());
        self
    }

    pub fn work(mut self, work: impl Into<WorkNonce>) -> Self {
        self.work = Some(work.into());
        self
    }
    pub fn build(self) -> Block {
        let source = self.source.unwrap_or(BlockHash::from(1));
        let prv_key = self.prv_key.unwrap_or_default();
        let representative = self.representative.unwrap_or(PublicKey::from(2));
        let work = self.work.unwrap_or(42.into());

        OpenBlockArgs {
            key: &prv_key,
            source,
            representative,
            work,
        }
        .into()
    }

    pub fn build_saved(self) -> SavedBlock {
        let block = self.build();

        let details = BlockDetails {
            epoch: Epoch::Epoch0,
            is_send: false,
            is_receive: true,
            is_epoch: false,
        };

        let sideband = BlockSideband {
            height: 1,
            timestamp: UnixTimestamp::new(2),
            successor: BlockHash::zero(),
            account: block.account_field().unwrap(),
            balance: 5.into(),
            details,
            source_epoch: Epoch::Epoch0,
        };

        SavedBlock::new(block, sideband)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Account, Amount, BlockBase, Signature, TestBlockBuilder};

    #[test]
    fn create_open_block() {
        let block = TestBlockBuilder::legacy_open().build_saved();
        let Block::LegacyOpen(open) = &*block else {
            panic!("not an open block")
        };
        assert_eq!(open.source(), BlockHash::from(1));
        assert_eq!(open.representative(), PublicKey::from(2));
        assert_ne!(open.account(), Account::zero());
        assert_eq!(open.work(), WorkNonce::new(42));
        assert_ne!(*open.signature(), Signature::new());

        assert_eq!(block.balance(), Amount::raw(5));
        assert_eq!(block.height(), 1);
        assert_eq!(block.timestamp(), UnixTimestamp::new(2));
        assert_eq!(block.source_epoch(), Epoch::Epoch0);
    }
}
