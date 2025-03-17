use rsnano_core::{Account, Amount, JsonBlock};
use rsnano_ledger::{test_helpers::UnsavedBlockLatticeBuilder, DEV_GENESIS_HASH};
use rsnano_rpc_messages::ConfirmationInfoArgs;
use test_helpers::{assert_timely2, setup_rpc_client_and_server, System};

#[test]
fn confirmation_info() {
    let mut system = System::new();
    let node = system.build_node().finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(Account::zero(), 100);
    node.process_active(send.clone());

    assert_timely2(|| node.active.is_active_root(&send.qualified_root()));

    let server = setup_rpc_client_and_server(node.clone(), false);

    let root = send.qualified_root();

    let args = ConfirmationInfoArgs::build(root)
        .include_representatives()
        .finish();

    let result = node
        .runtime
        .block_on(async { server.client.confirmation_info(args).await })
        .unwrap();

    assert_eq!(result.announcements, 0.into());
    assert_eq!(result.voters, 0.into());
    assert_eq!(result.last_winner, send.hash());

    let blocks = result.blocks;
    assert_eq!(blocks.len(), 1);

    let block = blocks.get(&send.hash()).unwrap();
    let representatives = block.representatives.clone().unwrap();
    assert_eq!(representatives.len(), 0);

    assert_eq!(result.total_tally, Amount::zero());

    let contents: &JsonBlock = block.contents.as_ref().unwrap();

    match contents {
        JsonBlock::Send(contents) => {
            assert_eq!(contents.previous, *DEV_GENESIS_HASH);
            assert_eq!(contents.destination, Account::zero());
            assert_eq!(
                Amount::from(contents.balance),
                Amount::MAX - Amount::raw(100)
            );
        }
        _ => (),
    }
}
