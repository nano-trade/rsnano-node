use super::Account;
use crate::serialize_32_byte_string;
use crate::u256_struct;
use blake2::{
    digest::{Update, VariableOutput},
    VarBlake2b,
};
use rand::Rng;

u256_struct!(BlockHash);
serialize_32_byte_string!(BlockHash);

impl BlockHash {
    pub fn random() -> Self {
        BlockHash::from_bytes(rand::rng().random())
    }
}

impl From<&Account> for BlockHash {
    fn from(account: &Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

impl From<Account> for BlockHash {
    fn from(account: Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

pub struct BlockHashBuilder {
    blake: VarBlake2b,
}

impl Default for BlockHashBuilder {
    fn default() -> Self {
        Self {
            blake: VarBlake2b::new(32).unwrap(),
        }
    }
}

impl BlockHashBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn update(mut self, data: impl AsRef<[u8]>) -> Self {
        self.blake.update(data.as_ref());
        self
    }

    pub fn build(self) -> BlockHash {
        let mut result = BlockHash::zero();
        self.blake
            .finalize_variable(|bytes| result = BlockHash::from_bytes(bytes.try_into().unwrap()));
        result
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
