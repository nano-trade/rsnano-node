use super::BlockValidator;
use crate::BlockError;
use rsnano_core::{Block, Epoch};

impl<'a> BlockValidator<'a> {
    pub fn ensure_pending_receive_is_correct(&self) -> Result<(), BlockError> {
        self.ensure_source_block_exists()?;
        self.ensure_receive_block_receives_pending_amount()?;
        self.ensure_legacy_source_is_epoch_0()
    }

    fn ensure_source_block_exists(&self) -> Result<(), BlockError> {
        if self.is_receive() && !self.source_block_exists {
            Err(BlockError::GapSource)
        } else {
            Ok(())
        }
    }

    fn ensure_receive_block_receives_pending_amount(&self) -> Result<(), BlockError> {
        if self.is_receive() {
            match &self.pending_receive_info {
                Some(pending) => {
                    if self.amount_received() != pending.amount {
                        return Err(BlockError::BalanceMismatch);
                    }
                }
                None => {
                    return Err(BlockError::Unreceivable);
                }
            };
        }

        Ok(())
    }

    fn ensure_legacy_source_is_epoch_0(&self) -> Result<(), BlockError> {
        let is_legacy_receive =
            matches!(self.block, Block::LegacyReceive(_) | Block::LegacyOpen(_));

        if is_legacy_receive
            && self
                .pending_receive_info
                .as_ref()
                .map(|x| x.epoch)
                .unwrap_or_default()
                != Epoch::Epoch0
        {
            Err(BlockError::Unreceivable)
        } else {
            Ok(())
        }
    }
}
