use crate::command_handler::RpcCommandHandler;
use anyhow::{anyhow, bail};
use indexmap::IndexMap;
use rsnano_core::{Account, Amount};
use rsnano_rpc_messages::{ConfirmationBlockInfoDto, ConfirmationInfoArgs, ConfirmationInfoDto};

impl RpcCommandHandler {
    pub(crate) fn confirmation_info(
        &self,
        args: ConfirmationInfoArgs,
    ) -> anyhow::Result<ConfirmationInfoDto> {
        let include_representatives = args.representatives.unwrap_or(false.into()).inner();
        let contents = args.contents.unwrap_or(true.into()).inner();
        let election_mutex = self
            .node
            .active
            .election(&args.root)
            .ok_or_else(|| anyhow!("Active confirmation not found"))?;

        if election_mutex.lock().unwrap().is_confirmed() {
            bail!("Active confirmation not found");
        }

        let election = election_mutex.lock().unwrap();
        let announcements = election.status.confirmation_request_count;
        let voters = election.last_votes.len();
        let last_winner = election.winner_hash().unwrap_or_default();

        let final_tally = election.status.final_tally;
        let mut total_tally = Amount::zero();
        let mut blocks = IndexMap::new();

        for block in election.last_blocks.values() {
            let tally = election
                .last_tally
                .get(&block.hash())
                .cloned()
                .unwrap_or_default();

            total_tally += tally;

            let contents = if contents {
                Some(block.json_representation())
            } else {
                None
            };

            let representatives = if include_representatives {
                let mut reps = IndexMap::new();
                for (representative, vote) in &election.last_votes {
                    if block.hash() == vote.hash {
                        let amount = self.node.ledger.rep_weights.weight(representative);
                        reps.insert(Account::from(representative), amount);
                    }
                }
                reps.sort_by(|k1, _, k2, _| k2.cmp(k1));
                Some(reps)
            } else {
                None
            };

            let entry = ConfirmationBlockInfoDto {
                tally,
                contents,
                representatives,
            };

            blocks.insert(block.hash(), entry);
        }

        Ok(ConfirmationInfoDto {
            announcements: announcements.into(),
            voters: voters.into(),
            last_winner,
            total_tally,
            final_tally,
            blocks,
        })
    }
}
