use lmdb::{DatabaseFlags, WriteFlags};
use lmdb_sys::{MDB_FIRST, MDB_NEXT};
use num_traits::FromPrimitive;
use tracing::{error, info};

use rsnano_core::BlockType;

use crate::{
    LmdbEnv, LmdbVersionStore, Transaction, FIRST_INCOMPATIBLE_STORE_VERSION,
    STORE_VERSION_CURRENT, STORE_VERSION_MINIMUM,
};

pub(crate) fn do_upgrades(env: &LmdbEnv) -> anyhow::Result<()> {
    let version_store = LmdbVersionStore::new(env)?;

    let mut version = {
        let mut tx = env.tx_begin_write();
        match version_store.get(&tx) {
            Some(v) => v,
            None => {
                let new_version = STORE_VERSION_CURRENT;
                info!("Setting db version to {}", new_version);
                version_store.put(&mut tx, new_version);
                new_version
            }
        }
    };

    if version == STORE_VERSION_CURRENT {
        return Ok(());
    }

    if version < STORE_VERSION_MINIMUM {
        error!("The version of the ledger ({version}) is lower than the minimum ({STORE_VERSION_MINIMUM}) which is supported for upgrades. Either upgrade to an older version of RsNano first or delete the ledger.");
        bail!("version too low");
    }

    if version > STORE_VERSION_MINIMUM && version < FIRST_INCOMPATIBLE_STORE_VERSION {
        error!("The version of the ledger ({version}) is not supported for upgrades!");
        bail!("unsupported version");
    }

    if version > STORE_VERSION_CURRENT {
        error!("The version of the ledger ({version}) is too high for this node");
        bail!("version too high");
    }

    loop {
        if version == STORE_VERSION_CURRENT {
            break;
        }

        version = match version {
            24 => {
                info!("Filling new block successor table...");
                let block_db = env
                    .environment
                    .create_db(Some("blocks"), DatabaseFlags::empty())?;

                let successor_db = env
                    .environment
                    .create_db(Some("successors"), DatabaseFlags::empty())?;

                let tx_read = env.tx_begin_read();
                let mut tx_write = env.tx_begin_write();
                let mut processed = 0;

                {
                    let mut cursor = tx_read.open_ro_cursor(block_db)?;
                    for row in cursor.iter_start() {
                        let (k, v) = row?;
                        let block_type = BlockType::from_u8(v[0]).expect("invalid block type");
                        let old_sideband_len = match block_type {
                            BlockType::LegacySend => 80 + 32,
                            BlockType::LegacyReceive => 96 + 32,
                            BlockType::LegacyOpen => 56 + 32,
                            BlockType::LegacyChange => 96 + 32,
                            BlockType::State => 50 + 32,
                            blk_type => panic!("Got block type: {blk_type:?}"),
                        };

                        // the first value in the old sideband is the successor
                        let successor_start = v.len() - old_sideband_len;

                        let successor = &v[successor_start..successor_start + 32];
                        tx_write.put(successor_db, k, successor, WriteFlags::APPEND)?;
                        processed += 1;
                        if processed % 100_000 == 0 {
                            info!("Processed {processed} blocks");
                        }
                    }
                }
                drop(tx_read);

                // switch to RsNano store versions, which are above 10_000
                10_000
            }
            10_000 => {
                info!("Removing successor from sideband...");
                let block_db = env
                    .environment
                    .create_db(Some("blocks"), DatabaseFlags::empty())?;
                let mut processed = 0;
                let mut tx_write = env.tx_begin_write();
                let mut cursor = tx_write.open_rw_cursor(block_db)?;
                let mut op = MDB_FIRST;
                let mut hash_bytes = [0; 32];
                let mut new_data = Vec::new();
                loop {
                    match cursor.get(None, None, op) {
                        Ok((Some(k), v)) => {
                            let block_type = BlockType::from_u8(v[0]).expect("invalid block type");
                            let old_sideband_len = match block_type {
                                BlockType::LegacySend => 80 + 32,
                                BlockType::LegacyReceive => 96 + 32,
                                BlockType::LegacyOpen => 56 + 32,
                                BlockType::LegacyChange => 96 + 32,
                                BlockType::State => 50 + 32,
                                blk_type => panic!("Got block type: {blk_type:?}"),
                            };
                            let new_sideband_len = old_sideband_len - 32;
                            let new_sideband_start = v.len() - new_sideband_len;
                            let data_without_sideband = &v[..v.len() - old_sideband_len];
                            let new_sideband = &v[new_sideband_start..];

                            // build new data
                            new_data.clear();
                            new_data.extend_from_slice(data_without_sideband);
                            new_data.extend_from_slice(&new_sideband);

                            hash_bytes.copy_from_slice(k);

                            cursor.put(&hash_bytes, &new_data, WriteFlags::CURRENT)?;
                            processed += 1;
                            op = MDB_NEXT;
                            if processed % 100_000 == 0 {
                                info!("Processed {processed} blocks");
                            }
                        }
                        Ok((None, _)) => bail!("Block data without key found!"),
                        Err(lmdb::Error::NotFound) => break,
                        Err(e) => bail!("Could not iter blocks table: {e:?}"),
                    }
                }

                10_001
            }
            _ => unreachable!(),
        };

        let mut tx = env.tx_begin_write();
        version_store.put(&mut tx, version);
    }

    // most recent version
    Ok(())
}
