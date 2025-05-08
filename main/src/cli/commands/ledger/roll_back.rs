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
