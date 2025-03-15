use std::sync::LazyLock;

use rand::Rng;

pub struct HardenedConstants {
    pub random_128: u128,
}

impl HardenedConstants {
    pub fn get() -> &'static HardenedConstants {
        &INSTANCE
    }
}

static INSTANCE: LazyLock<HardenedConstants> = LazyLock::new(|| {
    let mut rng = rand::rng();
    HardenedConstants {
        random_128: u128::from_ne_bytes(rng.random::<[u8; 16]>()),
    }
});
