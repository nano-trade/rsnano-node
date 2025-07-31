pub struct RwCursor<'txn>(RwCursorStrategy<'txn>);

impl<'txn> RwCursor<'txn> {
    pub fn new(cursor: lmdb::RwCursor<'txn>) -> Self {
        Self(RwCursorStrategy::Real(cursor))
    }

    pub fn get(
        &self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        op: u32,
    ) -> lmdb::Result<(Option<&'txn [u8]>, &'txn [u8])> {
        match &self.0 {
            RwCursorStrategy::Real(s) => lmdb::Cursor::get(s, key, data, op),
        }
    }

    pub fn put<K, D>(&mut self, key: &K, data: &D, flags: lmdb::WriteFlags) -> lmdb::Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        match &mut self.0 {
            RwCursorStrategy::Real(cursor) => cursor.put(key, data, flags),
        }
    }
}

enum RwCursorStrategy<'txn> {
    // TODO don't use static lifetimes!
    Real(lmdb::RwCursor<'txn>),
    // TODO nullable implementation
}
