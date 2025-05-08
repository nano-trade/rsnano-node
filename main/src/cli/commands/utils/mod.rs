use crate::cli::CliInfrastructure;
use clap::{CommandFactory, Parser, Subcommand};
use rsnano_core::{Account, PrivateKey, PublicKey};

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct UtilsCommand {
    #[command(subcommand)]
    pub subcommand: Option<UtilsSubcommands>,
}

#[derive(Subcommand, PartialEq, Debug)]
pub(crate) enum UtilsSubcommands {
    /// Converts a <public_key> into the account
    PublicKeyToAccount(PublicKeyArgs),
    /// Converts an <account> into the public key
    AccountToPublicKey(AccountToPublicKeyArgs),
    /// Expands a <private_key> into the public key and the account
    ExpandPrivateKey(ExpandPrivateKeyArgs),
    /// Generates a adhoc random keypair and prints it to stdout
    CreateKeyPair,
}

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct PublicKeyArgs {
    /// Converts the public_key into the account
    #[arg(long)]
    public_key: String,
}

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct AccountToPublicKeyArgs {
    /// Converts the account into the public key
    #[arg(long)]
    account: String,
}

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct ExpandPrivateKeyArgs {
    /// Derives the public key and the account from the private_key
    #[arg(long)]
    private_key: String,
}

pub(crate) fn run_utils_command(
    infra: &mut CliInfrastructure,
    cmd: UtilsCommand,
) -> anyhow::Result<()> {
    match cmd.subcommand {
        Some(UtilsSubcommands::PublicKeyToAccount(args)) => public_key_to_account(args)?,
        Some(UtilsSubcommands::AccountToPublicKey(args)) => account_to_public_key(args)?,
        Some(UtilsSubcommands::ExpandPrivateKey(args)) => expand_private_key(args)?,
        Some(UtilsSubcommands::CreateKeyPair) => create_key_pair(infra),
        None => UtilsCommand::command().print_long_help()?,
    }
    Ok(())
}

fn public_key_to_account(args: PublicKeyArgs) -> anyhow::Result<()> {
    let account = Account::decode_hex(&args.public_key)?;
    println!("Account: {:?}", account.encode_account());
    Ok(())
}

fn account_to_public_key(args: AccountToPublicKeyArgs) -> anyhow::Result<()> {
    let public_key = Account::decode_account(&args.account)?;
    println!("Public key: {:?}", public_key);
    Ok(())
}

fn expand_private_key(args: ExpandPrivateKeyArgs) -> anyhow::Result<()> {
    let private_key = PrivateKey::from_hex_str(&args.private_key)?;
    let public_key = PublicKey::from(&private_key);
    let account = Account::from(public_key).encode_account();

    println!("Private: {:?}", private_key.raw_key());
    println!("Public: {:?}", public_key);
    println!("Account: {:?}", account);

    Ok(())
}

fn create_key_pair(infra: &mut CliInfrastructure) {
    let key = infra.key_factory.create_key();

    infra.console.println(format!("Private: {}", key.raw_key()));
    infra
        .console
        .println(format!("Public: {}", key.public_key()));
    infra
        .console
        .println(format!("Account: {}", key.account().encode_account()));
}

#[cfg(test)]
mod tests {
    use crate::{cli::CliInfrastructure, Cli, CommandLineArgs};
    use clap::Parser;

    #[test]
    fn create_key_pair() {
        let args =
            CommandLineArgs::try_parse_from(["nulled_node", "utils", "create-key-pair"]).unwrap();
        let mut infra = CliInfrastructure::new_null();
        let print_tracker = infra.console.track();

        Cli {}.run(&mut infra, args).unwrap();

        let output = print_tracker.output();
        assert_eq!(
            output,
            [
                "Private: 000000000000002A000000000000002A000000000000002A000000000000002A",
                "Public: 49074D77DBE728CEB5EA2628A75DC7CE21493FDDCFCA991AAA1629F11D99FFD9",
                "Account: nano_1ka9bouxqssasttynbjanxgwhmj3b6zxumycm6fcn7jby6gsmzysauneamau",
            ]
        );
    }
}
