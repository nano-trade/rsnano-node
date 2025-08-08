use crate::{LmdbEnv, Transaction, WriteTransaction};
use lmdb::{DatabaseFlags, WriteFlags};
use rsnano_core::BlockHash;
use rsnano_nullable_lmdb::LmdbDatabase;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::sync::Arc;

/// Stores the hash of the successor block for a given block hash
pub struct LmdbSuccessorStore {
    database: LmdbDatabase,
    put_listener: OutputListenerMt<(BlockHash, BlockHash)>,
}

impl LmdbSuccessorStore {
    pub fn new(env: &LmdbEnv) -> anyhow::Result<Self> {
        let database = env.create_db(Some(TABLE_NAME), DatabaseFlags::empty())?;
        Ok(Self {
            database,
            put_listener: OutputListenerMt::new(),
        })
    }

    pub fn track_puts(&self) -> Arc<OutputTrackerMt<(BlockHash, BlockHash)>> {
        self.put_listener.track()
    }

    pub fn put(&self, tx: &mut WriteTransaction, block: &BlockHash, successor: &BlockHash) {
        if self.put_listener.is_tracked() {
            self.put_listener.emit((*block, *successor));
        }

        tx.put(
            self.database,
            block.as_bytes(),
            successor.as_bytes(),
            WriteFlags::empty(),
        )
        .unwrap();
    }

    pub fn del(&self, tx: &mut WriteTransaction, block: &BlockHash) {
        tx.delete(self.database, block.as_bytes(), None).unwrap();
    }

    pub fn get(&self, tx: &dyn Transaction, block: &BlockHash) -> Option<BlockHash> {
        match tx.get(self.database, block.as_bytes()) {
            Ok(bytes) => BlockHash::from_slice(bytes),
            Err(lmdb::Error::NotFound) => None,
            Err(e) => panic!("Could not load successor hash: {:?}", e),
        }
    }

    pub fn count(&self, tx: &dyn Transaction) -> u64 {
        tx.count(self.database)
    }
}

const TABLE_NAME: &str = "successors";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeleteEvent, LmdbEnv, PutEvent};
    use rsnano_nullable_lmdb::LmdbEnvironment;

    #[test]
    fn initialize() {
        let (store, _) = create_test_store(&[]);
        assert_eq!(store.database, TEST_DATABASE);
    }

    #[test]
    fn count() {
        let (store, env) = create_test_store(&[
            (1.into(), 2.into()),
            (3.into(), 4.into()),
            (5.into(), 6.into()),
        ]);
        let tx = env.begin_read();
        assert_eq!(store.count(&tx), 3);
    }

    #[test]
    fn put() {
        let (store, env) = create_test_store(&[]);
        let mut tx = env.begin_write();
        let put_tracker = tx.track_puts();
        let block = BlockHash::from(1);
        let successor = BlockHash::from(2);

        store.put(&mut tx, &block, &successor);

        assert_eq!(
            put_tracker.output(),
            vec![PutEvent {
                database: TEST_DATABASE,
                key: block.as_bytes().to_vec(),
                value: successor.as_bytes().to_vec(),
                flags: WriteFlags::empty()
            }]
        );
    }

    #[test]
    fn track_puts() {
        let (store, env) = create_test_store(&[]);
        let put_tracker = store.track_puts();
        let mut tx = env.begin_write();
        let block = BlockHash::from(1);
        let successor = BlockHash::from(2);

        store.put(&mut tx, &block, &successor);

        assert_eq!(put_tracker.output(), vec![(block, successor)]);
    }

    #[test]
    fn get() {
        let (store, env) = create_test_store(&[
            (1.into(), 2.into()),
            (3.into(), 4.into()),
            (5.into(), 6.into()),
        ]);

        let tx = env.begin_read();
        let successor = store.get(&tx, &3.into());
        assert_eq!(successor, Some(4.into()))
    }

    #[test]
    fn no_successor_found() {
        let (store, env) = create_test_store(&[]);

        let tx = env.begin_read();
        let successor = store.get(&tx, &3.into());
        assert_eq!(successor, None);
    }

    #[test]
    #[should_panic = "Could not load successor hash: PageNotFound"]
    fn get_unexpected_error() {
        let block_hash = BlockHash::from(1);
        let lmdb_env = LmdbEnvironment::null_builder()
            .database(TABLE_NAME, TEST_DATABASE)
            .error(block_hash.as_bytes(), lmdb::Error::PageNotFound)
            .finish()
            .finish();
        let env = LmdbEnv::new(lmdb_env, "/nulled-env");
        let store = LmdbSuccessorStore::new(&env).unwrap();
        let tx = env.begin_read();
        store.get(&tx, &block_hash);
    }

    #[test]
    fn delete() {
        let (store, env) = create_test_store(&[]);
        let mut tx = env.begin_write();
        let delete_tracker = tx.track_deletions();

        let block_hash = BlockHash::from(123);
        store.del(&mut tx, &block_hash);

        assert_eq!(
            delete_tracker.output(),
            vec![DeleteEvent {
                database: TEST_DATABASE,
                key: block_hash.as_bytes().to_vec()
            }]
        );
    }

    const TEST_DATABASE: LmdbDatabase = LmdbDatabase::new_null(42);

    fn create_test_store(entries: &[(BlockHash, BlockHash)]) -> (LmdbSuccessorStore, LmdbEnv) {
        let mut env_builder = LmdbEnvironment::null_builder().database(TABLE_NAME, TEST_DATABASE);

        for (block_hash, successor) in entries {
            env_builder = env_builder.entry(block_hash.as_bytes(), successor.as_bytes());
        }

        let lmdb_env = env_builder.finish().finish();
        let env = LmdbEnv::new(lmdb_env, "/nulled-env");
        let store = LmdbSuccessorStore::new(&env).unwrap();
        (store, env)
    }
}
