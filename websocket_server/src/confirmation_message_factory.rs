use rsnano_core::{Amount, BlockType, SavedBlock};
use rsnano_ledger::{AnySet, Ledger};
use rsnano_node::consensus::{EndedElection, VoteSummary};
use rsnano_websocket_messages::{OutgoingMessageEnvelope, Topic};

use crate::{BlockConfirmed, ConfirmationOptions, ElectionInfo, JsonSideband};

pub(super) struct ConfirmationMessageFactory<'a> {
    pub ledger: &'a Ledger,
    pub options: &'a ConfirmationOptions,
    pub block: &'a SavedBlock,
    pub amount: &'a Amount,
    pub election_status: &'a EndedElection,
    pub election_votes: &'a Vec<VoteSummary>,
}

impl<'a> ConfirmationMessageFactory<'a> {
    pub fn create_message(&self) -> OutgoingMessageEnvelope {
        OutgoingMessageEnvelope::new(
            Topic::Confirmation,
            BlockConfirmed {
                account: self.block.account().encode_account(),
                amount: self.amount.to_string_dec(),
                hash: self.block.hash().to_string(),
                confirmation_type: self.confirmation_type(),
                election_info: self.election_info(),
                block: self.json_block(),
                sideband: self.sideband(),
                linked_account: self.linked_account(),
            },
        )
    }

    fn confirmation_type(&self) -> String {
        self.election_status.result.as_str().to_string()
    }

    fn subtype(&self) -> String {
        if self.block.block_type() == BlockType::State {
            self.block.subtype().as_str().to_string()
        } else {
            String::new()
        }
    }

    fn election_info(&self) -> Option<ElectionInfo> {
        if self.options.include_election_info || self.options.include_election_info_with_votes {
            let mut info = ElectionInfo::from(self.election_status);
            if self.options.include_election_info_with_votes {
                info.votes = Some(self.election_votes.iter().map(|v| v.into()).collect());
            }
            Some(info)
        } else {
            None
        }
    }

    fn json_block(&self) -> Option<serde_json::Value> {
        if self.options.include_block {
            let mut block_value: serde_json::Value = (**self.block).clone().into();
            let subtype = self.subtype();
            if !subtype.is_empty() {
                if let serde_json::Value::Object(o) = &mut block_value {
                    o.insert("subtype".to_string(), serde_json::Value::String(subtype));
                }
            }
            Some(block_value)
        } else {
            None
        }
    }

    fn linked_account(&self) -> Option<String> {
        if !self.options.include_block || !self.options.include_linked_account {
            return None;
        }

        let any = self.ledger.any();
        match any.linked_account(self.block) {
            Some(linked) => Some(linked.encode_account()),
            None => Some("0".to_owned()),
        }
    }

    fn sideband(&self) -> Option<JsonSideband> {
        if self.options.include_sideband_info {
            Some(self.block.sideband().into())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use rsnano_node::consensus::ElectionResult;

    use crate::ConfirmationJsonOptions;

    use super::*;

    #[test]
    fn default_options() {
        let ledger = Ledger::new_null();
        let options = ConfirmationOptions::new(ConfirmationJsonOptions::default());
        let block = SavedBlock::new_test_instance();
        let amount = Amount::nano(123);
        let mut election = EndedElection::new(block.clone());
        election.result = ElectionResult::InactiveConfirmationHeight;
        let factory = ConfirmationMessageFactory {
            ledger: &ledger,
            options: &options,
            block: &block,
            amount: &amount,
            election_status: &election,
            election_votes: &Vec::new(),
        };

        let message = factory.create_message();

        assert_eq!(message.topic, Some(Topic::Confirmation));
        let payload: BlockConfirmed = serde_json::from_value(message.message.unwrap()).unwrap();
        assert_eq!(payload.amount, amount.to_string_dec());
        assert_eq!(payload.hash, block.hash().to_string());
        assert_eq!(payload.confirmation_type, "inactive");
        assert!(payload.election_info.is_none());
        assert!(payload.block.is_some());
        assert!(payload.sideband.is_none());
        assert!(payload.linked_account.is_none());
    }

    #[test]
    fn linked_account() {
        let ledger = Ledger::new_null();
        let options = ConfirmationOptions::new(ConfirmationJsonOptions {
            include_block: Some(true),
            include_linked_account: Some(true),
            ..Default::default()
        });
        let block = SavedBlock::new_test_send_block();
        let amount = Amount::nano(123);
        let factory = ConfirmationMessageFactory {
            ledger: &ledger,
            options: &options,
            block: &block,
            amount: &amount,
            election_status: &EndedElection::new(block.clone()),
            election_votes: &Vec::new(),
        };

        let message = factory.create_message();

        let payload: BlockConfirmed = serde_json::from_value(message.message.unwrap()).unwrap();

        assert_eq!(
            payload.linked_account,
            Some(block.destination_or_link().encode_account())
        );
    }
}
