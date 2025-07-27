use crate::RpcCommand;
use rsnano_core::JsonBlock;
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn block_hash(block: JsonBlock) -> Self {
        Self::BlockHash(BlockHashArgs::new(block))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockHashArgs {
    pub block: JsonBlock,
}

impl BlockHashArgs {
    pub fn new(block: JsonBlock) -> Self {
        Self { block }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::Block;

    #[test]
    fn serialize_block_hash_args() {
        let block = Block::new_test_instance();
        let block_hash_args = BlockHashArgs::new(block.json_representation());
        let serialized = serde_json::to_string_pretty(&block_hash_args).unwrap();

        assert_eq!(
            serialized,
            r#"{
  "block": {
    "type": "state",
    "account": "nano_39y535msmkzb31bx73tdnf8iken5ucw9jt98re7nriduus6cgs6uonjdm8r5",
    "previous": "9FC308E799CBE90813D2874BA34D093283DAB878E8E6C30B4C417BDE48A7649B",
    "representative": "nano_11111111111111111111111111111111111111111111111111ros3kc7wyy",
    "balance": "420",
    "link": "000000000000000000000000000000000000000000000000000000000000006F",
    "link_as_account": "nano_111111111111111111111111111111111111111111111111115hkrzwewgm",
    "signature": "93075D94F698BB37A8A6DE1146DC250D98AFAC6A3A742734AAFE3743B4CFC3BDD5B31C85026A1D6EF71554606B1F9912C51E7C5536697636BBE6173DE266490E",
    "work": "0000000000010F2C"
  }
}"#
        );
    }

    #[test]
    fn deserialize_block_hash_args() {
        let json = r#"{
  "block": {
    "type": "state",
    "account": "nano_39y535msmkzb31bx73tdnf8iken5ucw9jt98re7nriduus6cgs6uonjdm8r5",
    "previous": "9FC308E799CBE90813D2874BA34D093283DAB878E8E6C30B4C417BDE48A7649B",
    "representative": "nano_11111111111111111111111111111111111111111111111111ros3kc7wyy",
    "balance": "420",
    "link": "000000000000000000000000000000000000000000000000000000000000006F",
    "link_as_account": "nano_111111111111111111111111111111111111111111111111115hkrzwewgm",
    "signature": "93075D94F698BB37A8A6DE1146DC250D98AFAC6A3A742734AAFE3743B4CFC3BDD5B31C85026A1D6EF71554606B1F9912C51E7C5536697636BBE6173DE266490E",
    "work": "0000000000010F2C"
  }
}"#;

        let deserialized: BlockHashArgs = serde_json::from_str(json).unwrap();
        let block = Block::new_test_instance();

        assert_eq!(
            deserialized,
            BlockHashArgs::new(block.json_representation())
        );
    }

    #[test]
    fn serialize_block_hash_command() {
        let block = Block::new_test_instance();
        let block_hash_command = RpcCommand::block_hash(block.json_representation());
        let serialized = serde_json::to_string_pretty(&block_hash_command).unwrap();

        assert_eq!(
            serialized,
            r#"{
  "action": "block_hash",
  "block": {
    "type": "state",
    "account": "nano_39y535msmkzb31bx73tdnf8iken5ucw9jt98re7nriduus6cgs6uonjdm8r5",
    "previous": "9FC308E799CBE90813D2874BA34D093283DAB878E8E6C30B4C417BDE48A7649B",
    "representative": "nano_11111111111111111111111111111111111111111111111111ros3kc7wyy",
    "balance": "420",
    "link": "000000000000000000000000000000000000000000000000000000000000006F",
    "link_as_account": "nano_111111111111111111111111111111111111111111111111115hkrzwewgm",
    "signature": "93075D94F698BB37A8A6DE1146DC250D98AFAC6A3A742734AAFE3743B4CFC3BDD5B31C85026A1D6EF71554606B1F9912C51E7C5536697636BBE6173DE266490E",
    "work": "0000000000010F2C"
  }
}"#
        );
    }

    #[test]
    fn deserialize_block_hash_command() {
        let json = r#"{
  "action": "block_hash",
  "block": {
    "type": "state",
    "account": "nano_39y535msmkzb31bx73tdnf8iken5ucw9jt98re7nriduus6cgs6uonjdm8r5",
    "previous": "9FC308E799CBE90813D2874BA34D093283DAB878E8E6C30B4C417BDE48A7649B",
    "representative": "nano_11111111111111111111111111111111111111111111111111ros3kc7wyy",
    "balance": "420",
    "link": "000000000000000000000000000000000000000000000000000000000000006F",
    "link_as_account": "nano_111111111111111111111111111111111111111111111111115hkrzwewgm",
    "signature": "93075D94F698BB37A8A6DE1146DC250D98AFAC6A3A742734AAFE3743B4CFC3BDD5B31C85026A1D6EF71554606B1F9912C51E7C5536697636BBE6173DE266490E",
    "work": "0000000000010F2C"
  }
}"#;

        let deserialized: RpcCommand = serde_json::from_str(json).unwrap();

        let block = Block::new_test_instance();
        let block_hash_command = RpcCommand::block_hash(block.json_representation());

        assert_eq!(deserialized, block_hash_command);
    }
}
