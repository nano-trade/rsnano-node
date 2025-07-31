use crate::{
    LmdbDatabase, LmdbEnv, LmdbIterator, LmdbRangeIterator, LmdbWriteTransaction, Transaction,
    BLOCK_DATA_DATABASE, BLOCK_INDEX_DATABASE,
};
use lmdb::{DatabaseFlags, WriteFlags};
use lmdb_sys::MDB_LAST;
use rsnano_core::{
    utils::{BufferReader, Deserialize},
    BlockHash, SavedBlock,
};
use rsnano_nullable_lmdb::ConfiguredDatabase;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{
    ops::RangeBounds,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub struct LmdbBlockStore {
    /// block hash => id
    index_db: LmdbDatabase,

    /// id => block data
    block_db: LmdbDatabase,

    put_listener: OutputListenerMt<SavedBlock>,
    next_id: AtomicU64,
}

pub struct ConfiguredBlockDatabaseBuilder {
    index_db: ConfiguredDatabase,
    block_db: ConfiguredDatabase,
    next_id: u64,
}

impl ConfiguredBlockDatabaseBuilder {
    pub fn new() -> Self {
        Self {
            index_db: ConfiguredDatabase::new(BLOCK_INDEX_DATABASE, BLOCK_INDEX_DB_NAME),
            block_db: ConfiguredDatabase::new(BLOCK_DATA_DATABASE, BLOCK_DATA_DB_NAME),
            next_id: 0,
        }
    }

    pub fn block(mut self, block: &SavedBlock) -> Self {
        let id = self.next_id;
        self.next_id += 1;

        self.index_db
            .insert(block.hash().as_bytes(), &id.to_be_bytes());
        self.block_db
            .insert(&id.to_be_bytes(), block.serialize_with_sideband());
        self
    }

    pub fn build(self) -> (ConfiguredDatabase, ConfiguredDatabase) {
        (self.index_db, self.block_db)
    }
}

impl LmdbBlockStore {
    pub fn configured_responses() -> ConfiguredBlockDatabaseBuilder {
        ConfiguredBlockDatabaseBuilder::new()
    }

    pub fn new(env: &LmdbEnv) -> anyhow::Result<Self> {
        let index_db = env
            .environment
            .create_db(Some(BLOCK_INDEX_DB_NAME), DatabaseFlags::empty())?;

        let block_db = env
            .environment
            .create_db(Some(BLOCK_DATA_DB_NAME), DatabaseFlags::empty())?;

        let next_id = find_next_free_id(env, index_db)?;

        Ok(Self {
            index_db,
            block_db,
            put_listener: OutputListenerMt::new(),
            next_id: AtomicU64::new(next_id),
        })
    }

    pub fn track_puts(&self) -> Arc<OutputTrackerMt<SavedBlock>> {
        self.put_listener.track()
    }

    pub fn put(&self, txn: &mut LmdbWriteTransaction, block: &SavedBlock) {
        if self.put_listener.is_tracked() {
            self.put_listener.emit(block.clone());
        }

        self.raw_put(txn, &block.serialize_with_sideband(), &block.hash());
    }

    pub fn exists(&self, transaction: &dyn Transaction, hash: &BlockHash) -> bool {
        transaction.exists(self.index_db, hash.as_bytes())
    }

    pub fn get(&self, txn: &dyn Transaction, hash: &BlockHash) -> Option<SavedBlock> {
        self.block_raw_get(txn, hash).map(|block_bytes| {
            let mut stream = BufferReader::new(block_bytes);
            SavedBlock::deserialize(&mut stream)
                .unwrap_or_else(|_| panic!("Could not deserialize block {}!", hash))
        })
    }

    pub fn del(&self, txn: &mut LmdbWriteTransaction, hash: &BlockHash) {
        let id = match txn.get(self.index_db, hash.as_bytes()) {
            Ok(id_bytes) => get_block_id(id_bytes),
            Err(lmdb::Error::NotFound) => return,
            Err(e) => panic!("Could not delete block: {e:?} (hash: {hash})"),
        };

        txn.delete(self.block_db, &id.to_be_bytes(), None)
            .expect("Could not delete block data (ID: {id})");
        txn.delete(self.index_db, hash.as_bytes(), None)
            .expect("Could not delete block index (hash: {hash})");
    }

    pub fn count(&self, txn: &dyn Transaction) -> u64 {
        txn.count(self.index_db)
    }

    pub fn iter<'tx>(
        &'tx self,
        tx: &'tx dyn Transaction,
    ) -> impl Iterator<Item = SavedBlock> + 'tx {
        let cursor = tx
            .open_ro_cursor(self.index_db)
            .expect("Could not open cursor for block index table");

        LmdbIterator::new(cursor, |_, v| (0, get_block_id(v))).map(move |(_, id)| {
            let data = tx
                .get(self.block_db, &id.to_be_bytes())
                .expect("Block data missing (id: {id})");

            let mut stream = BufferReader::new(data);
            SavedBlock::deserialize(&mut stream).expect("Invalid block data (id: {id})")
        })
    }

    pub fn iter_range<'txn, R>(
        &'txn self,
        tx: &'txn dyn Transaction,
        range: R,
    ) -> impl Iterator<Item = SavedBlock> + 'txn
    where
        R: RangeBounds<BlockHash> + 'static,
    {
        let cursor = tx
            .open_ro_cursor(self.index_db)
            .expect("Could not open cursor for block table");

        LmdbRangeIterator::<BlockHash, u64, R>::new(cursor, range).map(move |(_, id)| {
            let data = tx
                .get(self.block_db, &id.to_be_bytes())
                .expect("Block data missing (id: {id})");
            let mut stream = BufferReader::new(data);
            let block =
                SavedBlock::deserialize(&mut stream).expect("Invalid block data (id: {id})");
            block
        })
    }

    fn raw_put(&self, txn: &mut LmdbWriteTransaction, data: &[u8], hash: &BlockHash) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        txn.put(
            self.index_db,
            hash.as_bytes(),
            &id.to_be_bytes(),
            WriteFlags::NO_OVERWRITE,
        )
        .expect("Couldn't insert into block index table");

        txn.put(self.block_db, &id.to_be_bytes(), data, WriteFlags::APPEND)
            .expect("Couldn't insert into block data table'");
    }

    fn block_raw_get<'a>(&self, txn: &'a dyn Transaction, hash: &BlockHash) -> Option<&'a [u8]> {
        match txn.get(self.index_db, hash.as_bytes()) {
            Err(lmdb::Error::NotFound) => None,
            Ok(id_bytes) => Some(
                txn.get(self.block_db, id_bytes)
                    .expect("Block data missing"),
            ),
            Err(e) => panic!("Could not load block. {:?}", e),
        }
    }
}

fn get_block_id(id_bytes: &[u8]) -> u64 {
    u64::from_be_bytes(id_bytes.try_into().expect("Invalid block ID"))
}

fn find_next_free_id(env: &LmdbEnv, database: LmdbDatabase) -> Result<u64, anyhow::Error> {
    let tx = env.tx_begin_read();
    let cursor = tx.open_ro_cursor(database)?;
    match cursor.get(None, None, MDB_LAST) {
        Ok((_, data)) => Ok(get_block_id(data) + 1),
        Err(lmdb::Error::NotFound) => Ok(0),
        Err(e) => Err(anyhow!("Couldn't load highest block id: {e:?}")),
    }
}

const BLOCK_INDEX_DB_NAME: &str = "block_index";
const BLOCK_DATA_DB_NAME: &str = "block_data";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PutEvent;

    struct Fixture {
        env: Arc<LmdbEnv>,
        store: LmdbBlockStore,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_env(LmdbEnv::new_null())
        }

        fn with_env(env: LmdbEnv) -> Self {
            let env = Arc::new(env);
            Self {
                env: env.clone(),
                store: LmdbBlockStore::new(&env).unwrap(),
            }
        }
    }

    #[test]
    fn empty() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let txn = fixture.env.tx_begin_read();

        assert!(store.get(&txn, &BlockHash::from(1)).is_none());
        assert_eq!(store.exists(&txn, &BlockHash::from(1)), false);
        assert_eq!(store.count(&txn), 0);
    }

    #[test]
    fn load_block_by_hash() {
        let block = SavedBlock::new_test_instance();

        let env = LmdbEnv::new_null_with()
            .database(BLOCK_INDEX_DB_NAME, LmdbDatabase::new_null(99))
            .entry(block.hash().as_bytes(), &1u64.to_be_bytes())
            .build()
            .database(BLOCK_DATA_DB_NAME, LmdbDatabase::new_null(100))
            .entry(&1u64.to_be_bytes(), &block.serialize_with_sideband())
            .build()
            .build();

        let fixture = Fixture::with_env(env);
        let txn = fixture.env.tx_begin_read();

        let result = fixture.store.get(&txn, &block.hash());
        assert_eq!(result, Some(block));
    }

    #[test]
    fn add_block() {
        let fixture = Fixture::new();
        let mut txn = fixture.env.tx_begin_write();
        let put_tracker = txn.track_puts();
        let block = SavedBlock::new_test_open_block();

        fixture.store.put(&mut txn, &block);

        assert_eq!(
            put_tracker.output(),
            vec![
                PutEvent {
                    database: LmdbDatabase::new_null(42),
                    key: block.hash().as_bytes().to_vec(),
                    value: 0u64.to_be_bytes().to_vec(),
                    flags: lmdb::WriteFlags::empty(),
                },
                PutEvent {
                    database: LmdbDatabase::new_null(43),
                    key: 0u64.to_be_bytes().to_vec(),
                    value: block.serialize_with_sideband(),
                    flags: lmdb::WriteFlags::APPEND,
                }
            ]
        );
    }

    #[test]
    fn track_inserted_blocks() {
        let fixture = Fixture::new();
        let block = SavedBlock::new_test_open_block();
        let mut txn = fixture.env.tx_begin_write();
        let put_tracker = fixture.store.track_puts();

        fixture.store.put(&mut txn, &block);

        assert_eq!(put_tracker.output(), vec![block]);
    }

    #[test]
    fn can_be_nulled() {
        let block = SavedBlock::new_test_instance();
        let (block_index, block_data) =
            LmdbBlockStore::configured_responses().block(&block).build();

        let env = LmdbEnv::new_null_with()
            .configured_database(block_index)
            .configured_database(block_data)
            .build();
        let txn = env.tx_begin_read();
        let block_store = LmdbBlockStore::new(&env).unwrap();
        assert_eq!(block_store.get(&txn, &block.hash()), Some(block));
    }
}
