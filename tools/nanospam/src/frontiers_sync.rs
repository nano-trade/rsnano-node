use tracing::info;

use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::{AccountsReceivableArgs, AccountsReceivableResponse};

use crate::domain::AccountMap;

/// Fetch the latest frontiers from the first node and add them to the account map
pub(crate) async fn sync_frontiers(rpc_clients: &[NanoRpcClient], account_map: &mut AccountMap) {
    info!("Syncing account frontiers...");
    let rpc_client = &rpc_clients[0];
    let accounts = account_map.accounts().clone();
    let mut count = 0;
    for chunk in accounts.chunks(100) {
        let frontiers = rpc_client
            .accounts_frontiers(chunk.into())
            .await
            .unwrap()
            .frontiers
            .unwrap();

        let balances = rpc_client
            .accounts_balances(chunk.to_vec())
            .await
            .unwrap()
            .balances;

        let AccountsReceivableResponse::Source(receivable) = rpc_client
            .accounts_receivable(
                AccountsReceivableArgs::build(chunk.to_vec())
                    .include_source()
                    .finish(),
            )
            .await
            .unwrap()
        else {
            panic!("not a simple response")
        };

        for account in chunk {
            if let Some(frontier) = frontiers.get(account) {
                let balance = balances.get(account).unwrap().balance;
                account_map.set_account_state(*account, balance, *frontier);
            }

            if let Some(blocks) = receivable.blocks.get(account) {
                for (send_hash, info) in blocks {
                    account_map.add_confirmed_receivable(*account, *send_hash, info.amount);
                }
            }
        }

        count += 1;

        if count % 200 == 0 {
            info!(
                "Done: {}%",
                (count as f64 * 100.0 / account_map.len() as f64 * 100.0) as usize
            )
        }
    }
}
