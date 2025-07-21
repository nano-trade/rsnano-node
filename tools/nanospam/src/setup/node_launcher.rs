use crate::{
    app::Args,
    setup::{peering_port, GENESIS_BLOCK, GENESIS_PRV},
};
use rsnano_rpc_client::NanoRpcClient;
use std::{
    process::{Command, Stdio},
    time::Duration,
};
use tokio::time::sleep;
use tracing::info;

pub(crate) async fn start_nodes(
    args: &Args,
    data_dir: std::path::PathBuf,
    rpc_clients: &[NanoRpcClient],
) -> Vec<std::process::Child> {
    let mut children = Vec::new();
    for i in 0..args.prs {
        let mut node_dir = data_dir.clone();
        node_dir.push(format!("pr{i}"));

        let mut cmd = if args.cpp {
            let mut cmd = Command::new("nano_node");
            cmd.env("NANO_TEST_GENESIS_BLOCK", GENESIS_BLOCK)
                .env("NANO_TEST_GENESIS_PRV ", GENESIS_PRV)
                .env("NANO_TEST_EPOCH_1", "0")
                .env("NANO_TEST_EPOCH_2", "0")
                .env("NANO_TEST_EPOCH_2_RECV", "0")
                .arg("--network")
                .arg("test")
                .arg("--data_path")
                .arg(&node_dir)
                .arg("--daemon")
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd
        } else {
            let mut cmd = Command::new("rsnano_node");
            cmd.env("NANO_TEST_GENESIS_BLOCK", GENESIS_BLOCK)
                .env("NANO_TEST_GENESIS_PRV ", GENESIS_PRV)
                .arg("--network")
                .arg("test")
                .arg("--data-path")
                .arg(&node_dir)
                .arg("node")
                .arg("run")
                .stdout(Stdio::null());
            cmd
        };

        info!("Starting node: {cmd:?}");
        children.push(cmd.spawn().unwrap());

        let rpc_client = &rpc_clients[i];
        info!("Waiting for RPC...");
        while rpc_client.version().await.is_err() {
            sleep(Duration::from_millis(100)).await;
        }
    }

    if args.cpp {
        // Send keepalives so that nano_node connects (their preconfigured peers don't allow ports)!
        info!("Sending keepalives...");
        for i in 0..args.prs {
            for k in 0..args.prs {
                if k != i {
                    rpc_clients[i]
                        .keepalive("::1", peering_port(k))
                        .await
                        .unwrap();
                }
            }
        }
        // Give time to connect
        sleep(Duration::from_secs(5)).await;
    }
    children
}
