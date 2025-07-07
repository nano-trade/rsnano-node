# Tool: nanospam

## Overview
Nanospam creates random blocks (without PoW) and sends them with increasing BPS to a local test node.

This tool is still early work and rough to work with, but it can already be used for measuring node performance.

This is how it works:
- It connects to a local running nano node in the test network via the default port and the websocket port.
- This nano node has the genesis key in its wallet and PoW disabled
- It crates 500k random account keys
- It sends 100m nano from genesis to one of the test accounts
- Then it publishes random blocks (random sends and receives between the test accounts). A new block is only created if the previous got confirmed.
- It waits for all blocks to be confirmed (via websocket feedback)
- If a block isn't confirmed within 10s it gets republished
- It starts with 50 bps and continually increases with a rate of 1000/min


Because PoW is disabled, creating blocks is very fast. On my old and slow laptop it can consistently create 51k bps on a single core.

## Running it

### Single Node Spam Test
```
Usage: nanospam [OPTIONS]

Options:
      --attach     Attach to an already running node
      --prs <PRS>  Number of principal representatives [default: 1]
      --cpp        Use C++ nano_node implementation
  -h, --help       Print help
```

Running it with no arguments will start a single node that is a PR. The node data is in `~/NanoSpam/pr0/`. 
`nanospam` does all the configuration for you! It creates the node and rpc configuration files and it
inserts the genesis key into the wallet


### Running it with custom nodes
You can start the nodes yourself. This enables spam tests with docker images for example. You need to set the following environment variables:
```bash
export NANO_TEST_GENESIS_BLOCK='{
    "type": "open",
    "source": "B0311EA55708D6A53C75CDBF88300259C6D018522FE3D4D0A242E431F9E8B6D0",
    "representative": "xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo",
    "account": "xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo",
    "work": "7b42a00ee91d5810",
    "signature": "ECDA914373A2F0CA1296475BAEE40500A7F0A7AD72A5A80C81D7FAB7F6C802B2CC7DB50F5DD0FB25B2EF11761FA7344A158DD5A700B21BD47DE5BD0F63153A02"
}'

export NANO_TEST_GENESIS_PRV="34F0A37AAD20F4A260F0A5B3CB3D7FB50673212263E58A380BC10474BB039CE4"
export NANO_TEST_EPOCH_1="0"
export NANO_TEST_EPOCH_2="0"
export NANO_TEST_EPOCH_2_RECV="0"
```
Make sure that the ledger is empty!: `rm ~/NanoSpam/pr0/data.ldb`. _This has to be done before each spam run!_

## Misc
If you would like to know how fast your computer can generate blocks on a single core, then run:
```
cd tools/nanospam/
cargo test --release -- --nocapture --ignored benchmark
```
It will output something like this:
```running 1 test
Created 50000 blocks. 53958 bps
Created 50000 blocks. 53827 bps
Created 50000 blocks. 53793 bps
Created 50000 blocks. 53779 bps
Created 50000 blocks. 53717 bps
```
