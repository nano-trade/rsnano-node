use std::ops::RangeBounds;

use rsnano_core::{
    utils::{BufferReader, Deserialize},
    Account, ConfirmationHeightInfo,
};
use rsnano_nullable_lmdb::{
    ConfiguredDatabase, DatabaseFlags, LmdbDatabase, LmdbEnv, Transaction, WriteFlags,
    WriteTransaction,
};

use crate::{
    parallel_traversal, LmdbIterator, LmdbRangeIterator, CONFIRMATION_HEIGHT_TEST_DATABASE,
};

pub struct LmdbConfirmationHeightStore {
    database: LmdbDatabase,
}

impl LmdbConfirmationHeightStore {
    pub fn new(env: &LmdbEnv) -> anyhow::Result<Self> {
        let database = env.create_db(Some("confirmation_height"), DatabaseFlags::empty())?;

        Ok(Self { database })
    }

    pub fn database(&self) -> LmdbDatabase {
        self.database
    }

    pub fn put(
        &self,
        txn: &mut WriteTransaction,
        account: &Account,
        info: &ConfirmationHeightInfo,
    ) {
        txn.put(
            self.database,
            account.as_bytes(),
            &info.to_bytes(),
            WriteFlags::empty(),
        )
        .unwrap();
    }

    pub fn get(&self, txn: &dyn Transaction, account: &Account) -> Option<ConfirmationHeightInfo> {
        match txn.get(self.database, account.as_bytes()) {
            Err(lmdb::Error::NotFound) => None,
            Ok(bytes) => {
                let mut stream = BufferReader::new(bytes);
                ConfirmationHeightInfo::deserialize(&mut stream).ok()
            }
            Err(e) => {
                panic!("Could not load confirmation height info: {:?}", e);
            }
        }
    }

    pub fn exists(&self, txn: &dyn Transaction, account: &Account) -> bool {
        txn.exists(self.database, account.as_bytes())
    }

    pub fn del(&self, txn: &mut WriteTransaction, account: &Account) {
        txn.delete(self.database, account.as_bytes(), None).unwrap();
    }

    pub fn count(&self, txn: &dyn Transaction) -> u64 {
        txn.count(self.database)
    }

    pub fn clear(&self, txn: &mut WriteTransaction) {
        txn.clear_db(self.database).unwrap()
    }

    pub fn iter<'tx>(
        &self,
        tx: &'tx dyn Transaction,
    ) -> impl Iterator<Item = (Account, ConfirmationHeightInfo)> + 'tx {
        let cursor = tx.open_ro_cursor(self.database).unwrap();

        LmdbIterator::new(cursor, |key, value| {
            let account = Account::from_bytes(key.try_into().unwrap());
            let mut stream = BufferReader::new(value);
            let info = ConfirmationHeightInfo::deserialize(&mut stream).unwrap();
            (account, info)
        })
    }

    pub fn iter_range<'txn>(
        &self,
        tx: &'txn dyn Transaction,
        range: impl RangeBounds<Account> + 'static,
    ) -> impl Iterator<Item = (Account, ConfirmationHeightInfo)> + 'txn {
        let cursor = tx.open_ro_cursor(self.database).unwrap();
        LmdbRangeIterator::new(cursor, range)
    }

    pub fn for_each_par(
        &self,
        env: &LmdbEnv,
        thread_count: usize,
        action: impl Fn(&mut dyn Iterator<Item = (Account, ConfirmationHeightInfo)>) + Send + Sync,
    ) {
        parallel_traversal(thread_count, &|start, end, is_last| {
            let tx = env.begin_read();
            let start_account = Account::from(start);
            let end_account = Account::from(end);
            if is_last {
                let mut iter = self.iter_range(&tx, start_account..);
                action(&mut iter);
            } else {
                let mut iter = self.iter_range(&tx, start_account..end_account);
                action(&mut iter);
            }
        })
    }
}

pub struct ConfiguredConfirmationHeightDatabaseBuilder {
    database: ConfiguredDatabase,
}

impl ConfiguredConfirmationHeightDatabaseBuilder {
    pub fn new() -> Self {
        Self {
            database: ConfiguredDatabase::new(
                CONFIRMATION_HEIGHT_TEST_DATABASE,
                "confirmation_height",
            ),
        }
    }

    pub fn height(mut self, account: &Account, info: &ConfirmationHeightInfo) -> Self {
        self.database.insert(account.as_bytes(), info.to_bytes());
        self
    }

    pub fn build(self) -> ConfiguredDatabase {
        self.database
    }

    pub fn create(hashes: Vec<(Account, ConfirmationHeightInfo)>) -> ConfiguredDatabase {
        let mut builder = Self::new();
        for (account, info) in hashes {
            builder = builder.height(&account, &info);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::BlockHash;
    use rsnano_nullable_lmdb::PutEvent;
    use std::sync::Arc;

    struct Fixture {
        env: Arc<LmdbEnv>,
        store: LmdbConfirmationHeightStore,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_env(LmdbEnv::new_null())
        }

        fn with_env(env: LmdbEnv) -> Self {
            let env = Arc::new(env);
            Self {
                store: LmdbConfirmationHeightStore::new(&env).unwrap(),
                env,
            }
        }
    }

    #[test]
    fn empty_store() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let tx = fixture.env.begin_read();
        assert!(store.get(&tx, &Account::from(0)).is_none());
        assert_eq!(store.exists(&tx, &Account::from(0)), false);
        assert!(store.iter(&tx).next().is_none());
        assert!(store.iter_range(&tx, Account::from(0)..).next().is_none());
    }

    #[test]
    fn add_account() {
        let fixture = Fixture::new();
        let mut txn = fixture.env.begin_write();
        let put_tracker = txn.track_puts();

        let account = Account::from(1);
        let info = ConfirmationHeightInfo::new(1, BlockHash::from(2));
        fixture.store.put(&mut txn, &account, &info);

        assert_eq!(
            put_tracker.output(),
            vec![PutEvent {
                database: LmdbDatabase::new_null(42),
                key: account.as_bytes().to_vec(),
                value: info.to_bytes().to_vec(),
                flags: WriteFlags::empty(),
            }]
        )
    }

    #[test]
    fn load() {
        let account = Account::from(1);
        let info = ConfirmationHeightInfo::new(1, BlockHash::from(2));

        let env = LmdbEnv::new_null_with()
            .database("confirmation_height", LmdbDatabase::new_null(100))
            .entry(account.as_bytes(), &info.to_bytes())
            .build()
            .build();

        let fixture = Fixture::with_env(env);
        let txn = fixture.env.begin_read();
        let result = fixture.store.get(&txn, &account);

        assert_eq!(result, Some(info))
    }

    #[test]
    fn iterate_one_account() -> anyhow::Result<()> {
        let account = Account::from(1);
        let info = ConfirmationHeightInfo::new(1, BlockHash::from(2));

        let env = LmdbEnv::new_null_with()
            .database("confirmation_height", LmdbDatabase::new_null(100))
            .entry(account.as_bytes(), &info.to_bytes())
            .build()
            .build();

        let fixture = Fixture::with_env(env);
        let tx = fixture.env.begin_read();
        let mut it = fixture.store.iter(&tx);
        assert_eq!(it.next(), Some((account, info)));
        assert!(it.next().is_none());
        Ok(())
    }

    #[test]
    fn clear() {
        let fixture = Fixture::new();
        let mut txn = fixture.env.begin_write();
        let clear_tracker = txn.track_clears();

        fixture.store.clear(&mut txn);

        assert_eq!(clear_tracker.output(), vec![LmdbDatabase::new_null(42)])
    }
}
