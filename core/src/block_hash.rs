use super::Account;
use crate::serialize_32_byte_string;
use crate::u256_struct;
use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use rand::Rng;

u256_struct!(Blake2Hash);
serialize_32_byte_string!(Blake2Hash);

pub type BlockHash = Blake2Hash;

impl Blake2Hash {
    pub fn random() -> Self {
        BlockHash::from_bytes(rand::rng().random())
    }
}

impl From<&Account> for Blake2Hash {
    fn from(account: &Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

impl From<Account> for Blake2Hash {
    fn from(account: Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

pub struct Blake2HashBuilder {
    blake: Blake2bVar,
}

impl Default for Blake2HashBuilder {
    fn default() -> Self {
        Self {
            blake: Blake2bVar::new(32).unwrap(),
        }
    }
}

impl Blake2HashBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn update(mut self, data: impl AsRef<[u8]>) -> Self {
        self.blake.update(data.as_ref());
        self
    }

    pub fn build(self) -> Blake2Hash {
        let mut buffer = [0; 32];
        self.blake.finalize_variable(&mut buffer).unwrap();
        BlockHash::from_bytes(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_serialize() {
        let serialized = serde_json::to_string_pretty(&BlockHash::from(123)).unwrap();
        assert_eq!(
            serialized,
            "\"000000000000000000000000000000000000000000000000000000000000007B\""
        );
    }

    #[test]
    fn serde_deserialize() {
        let deserialized: BlockHash = serde_json::from_str(
            "\"000000000000000000000000000000000000000000000000000000000000007B\"",
        )
        .unwrap();
        assert_eq!(deserialized, BlockHash::from(123));
    }
}
