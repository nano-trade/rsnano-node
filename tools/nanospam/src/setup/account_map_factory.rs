use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

use tracing::info;

use rsnano_core::RawKey;

use crate::domain::AccountMap;

pub(crate) fn create_account_map(
    data_dir: &std::path::PathBuf,
    account_count: usize,
) -> AccountMap {
    let mut account_map = AccountMap::default();

    let mut account_keys_path = data_dir.clone();
    account_keys_path.push("account_keys.txt");

    let mut should_create_new_file = true;

    if account_keys_path.exists() {
        info!("Loading account keys from {account_keys_path:?}");
        let file = File::open(&account_keys_path).unwrap();
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let key = RawKey::decode_hex(line.unwrap()).unwrap();
            account_map.add_unopened(key.into());
        }
        should_create_new_file = account_map.len() != account_count;
    }

    if should_create_new_file {
        info!("Creating account keys file with {account_count} keys: {account_keys_path:?}");

        account_map = AccountMap::default();
        account_map.fill(account_count);
        let mut file = File::create(account_keys_path).unwrap();
        for key in account_map.private_keys() {
            writeln!(file, "{}", key.raw_key().encode_hex()).unwrap();
        }
    }
    account_map
}
