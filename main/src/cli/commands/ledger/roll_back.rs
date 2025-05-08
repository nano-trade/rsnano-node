use super::HashArgs;
use crate::cli::{build_node, GlobalArgs};
use rsnano_core::BlockHash;

pub(crate) fn roll_back(global_args: GlobalArgs, args: HashArgs) -> anyhow::Result<()> {
    let node = build_node(&global_args)?;
    let block_hash = BlockHash::decode_hex(&args.hash)?;
    println!("Rolling back {block_hash:?}");
    let rolled_back = node.ledger.roll_back(&block_hash)?;
    println!("Block rollback complete");
    println!("Rolled back {rolled_back} dependent blocks");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        cli::{
            commands::ledger::{HashArgs, LedgerCommand, LedgerSubcommands},
            Commands,
        },
        CommandLineArgs,
    };
    use clap::Parser;

    #[test]
    fn parse_roll_back_command() {
        let cmd =
            CommandLineArgs::try_parse_from(["nulled_node_bin", "ledger", "roll-back", "--hash=1"])
                .unwrap();
        assert_eq!(
            cmd,
            CommandLineArgs {
                command: Some(Commands::Ledger(LedgerCommand {
                    subcommand: Some(LedgerSubcommands::RollBack(HashArgs {
                        hash: "1".to_string()
                    })),
                })),
                ..Default::default()
            }
        )
    }
}
