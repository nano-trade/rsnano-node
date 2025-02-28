use super::LedgerContext;
use crate::{
    ledger_constants::{DEV_GENESIS_PUB_KEY, LEDGER_CONSTANTS_STUB},
    AnySet, ConfirmedSet, LedgerSet, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH,
};
use rsnano_core::{utils::UnixTimestamp, Account, Amount, BlockType};

#[test]
fn account_balance_is_none_for_unknown_account() {
    let ctx = LedgerContext::empty();
    let balance = ctx.ledger.any().account_balance(&Account::zero());
    assert_eq!(balance, Amount::zero());
}

#[test]
fn get_genesis_block() {
    let ctx = LedgerContext::empty();

    let block = ctx
        .ledger
        .any()
        .get_block(&DEV_GENESIS_HASH)
        .expect("genesis block not found");

    assert_eq!(block.block_type(), BlockType::LegacyOpen);
}

#[test]
fn genesis_account_balance() {
    let ctx = LedgerContext::empty();
    let balance = ctx.ledger.any().account_balance(&DEV_GENESIS_ACCOUNT);
    assert_eq!(balance, LEDGER_CONSTANTS_STUB.genesis_amount);
}

#[test]
fn genesis_account_info() {
    let ctx = LedgerContext::empty();

    let account_info = ctx
        .ledger
        .any()
        .get_account(&DEV_GENESIS_ACCOUNT)
        .expect("genesis account not found");

    // Frontier time should have been updated when genesis balance was added
    assert_eq!(account_info.modified, UnixTimestamp::ZERO);
    assert_eq!(account_info.block_count, 1);
    assert_eq!(account_info.balance, LEDGER_CONSTANTS_STUB.genesis_amount);
}

#[test]
fn genesis_confirmation_height_info() {
    let ctx = LedgerContext::empty();

    // Genesis block should be confirmed by default
    let conf_info = ctx
        .ledger
        .confirmed()
        .get_conf_info(&DEV_GENESIS_ACCOUNT)
        .expect("conf height not found");

    assert_eq!(conf_info.height, 1);
    assert_eq!(conf_info.frontier, *DEV_GENESIS_HASH);
}

#[test]
fn cache() {
    let ctx = LedgerContext::empty();
    assert_eq!(ctx.ledger.account_count(), 1);
    assert_eq!(ctx.ledger.cemented_count(), 1);
}

#[test]
fn genesis_representative() {
    let ctx = LedgerContext::empty();
    assert_eq!(
        ctx.ledger
            .any()
            .representative_block_hash(&DEV_GENESIS_HASH),
        *DEV_GENESIS_HASH
    );
}

#[test]
fn genesis_vote_weight() {
    let ctx = LedgerContext::empty();
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
}

#[test]
fn latest_empty() {
    let ctx = LedgerContext::empty();
    assert_eq!(ctx.ledger.any().account_head(&Account::from(1)), None);
}
