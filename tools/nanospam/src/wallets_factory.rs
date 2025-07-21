use std::time::Duration;

use tokio::time::sleep;
use tracing::{info, warn};

use rsnano_core::{
    Amount, Block, BlockHash, JsonBlock, PrivateKey, StateBlockArgs, WalletId, WorkNonce,
};
use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::{ReceiveArgs, SendArgs, WalletAddArgs, WalletRepresentativeSetArgs};

use crate::{app::Args, domain::AccountMap, setup::pr_key};

const INITIAL_AMOUNT: Amount = Amount::nano(100_000_000);

pub(crate) async fn create_wallets(
    args: &Args,
    genesis_key: PrivateKey,
    rpc_clients: &[NanoRpcClient],
    genesis_rpc: &NanoRpcClient,
    account_map: &mut AccountMap,
) {
    let mut genesis_wallet = WalletId::zero();
    for i in 0..args.prs {
        let rpc_client = &rpc_clients[i];
        info!("Creating wallet...");
        let resp = rpc_client.wallet_create(None).await.unwrap();
        if i == 0 {
            genesis_wallet = resp.wallet;
        }
        let pr_key = pr_key(i);
        rpc_client
            .wallet_add(WalletAddArgs {
                wallet: resp.wallet,
                key: pr_key.raw_key(),
                work: None,
            })
            .await
            .unwrap();

        if i > 0 {
            info!("Setting default representative...");
            rpc_client
                .wallet_representative_set(WalletRepresentativeSetArgs {
                    wallet: resp.wallet,
                    representative: pr_key.account(),
                    update_existing_accounts: Some(false.into()),
                })
                .await
                .unwrap();

            let pr_balance = (Amount::MAX - INITIAL_AMOUNT) / args.prs as u128;
            info!(
                "Sending Ӿ{} to PR{i} wallet {} ...",
                pr_balance.format_balance(0),
                pr_key.account().encode_account()
            );
            let send_hash = genesis_rpc
                .send(SendArgs {
                    wallet: genesis_wallet,
                    source: genesis_key.account(),
                    destination: pr_key.account(),
                    amount: pr_balance,
                    work: Some(WorkNonce::new(0)),
                    id: None,
                })
                .await
                .unwrap()
                .block;
            wait_until_confirmed(&rpc_client, send_hash).await;

            info!("Receiving...");
            // trigger wallet receive to speed things up
            let _ = rpc_client
                .receive(ReceiveArgs {
                    wallet: resp.wallet,
                    account: pr_key.account(),
                    block: send_hash,
                    work: Some(WorkNonce::new(0)),
                })
                .await;
            let recv_hash = rpc_client
                .account_info(pr_key.account())
                .await
                .unwrap()
                .frontier;
            wait_until_confirmed(&rpc_client, recv_hash).await;
            info!("DONE");
            info!(
                "********************************************************************************"
            );
        }
    }

    info!("Sending initial spam amount...");
    let initial_key = account_map.initial_key().clone();
    // Send total spam amount
    let genesis_send = genesis_rpc
        .send(SendArgs {
            wallet: genesis_wallet,
            source: genesis_key.account(),
            destination: initial_key.account(),
            amount: INITIAL_AMOUNT,
            work: Some(0.into()),
            id: None,
        })
        .await
        .unwrap()
        .block;
    wait_until_confirmed(&genesis_rpc, genesis_send).await;
    info!("Receiving initial spam amount...");
    let genesis_receive: Block = StateBlockArgs {
        key: &initial_key,
        previous: BlockHash::zero(),
        representative: initial_key.public_key(),
        balance: INITIAL_AMOUNT,
        link: genesis_send.into(),
        work: 0.into(),
    }
    .into();

    let recv = genesis_rpc
        .process(JsonBlock::from(genesis_receive.clone()))
        .await
        .unwrap();

    wait_until_confirmed(&genesis_rpc, recv.hash).await;

    account_map.set_account_state(
        initial_key.account(),
        INITIAL_AMOUNT,
        genesis_receive.hash(),
    );
}

async fn wait_until_confirmed(rpc_client: &NanoRpcClient, hash: BlockHash) {
    info!("Waiting for confirmation for {hash}");
    loop {
        match rpc_client.block_info(hash).await {
            Ok(info) => {
                if info.confirmed.inner() {
                    break;
                }
            }
            Err(e) => {
                warn!("Got error: {e:?}")
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}
