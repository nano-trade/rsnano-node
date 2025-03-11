use std::sync::LazyLock;

use rand::Rng;

use crate::{Account, PublicKey};

pub struct HardenedConstants {
    pub not_an_account: Account,
    pub not_an_account_key: PublicKey,
    pub random_128: u128,
}

impl HardenedConstants {
    pub fn get() -> &'static HardenedConstants {
        &INSTANCE
    }
}

static INSTANCE: LazyLock<HardenedConstants> = LazyLock::new(|| {
    let mut rng = rand::rng();
    let not_an_account = Account::from_bytes(rng.random::<[u8; 32]>());
    HardenedConstants {
        not_an_account_key: not_an_account.into(),
        not_an_account,
        random_128: u128::from_ne_bytes(rng.random::<[u8; 16]>()),
    }
});
